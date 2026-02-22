impl PersistEntityRuntime {
    /// Appends a new record to the journal file.
    ///
    /// This handles serializing the record, writing to the file, and ensuring durability
    /// based on the configured policy (`Strict` vs `Eventual`).
    /// Also triggers replication of the journal line.
    async fn append_record(&mut self, op: RuntimeJournalOp) -> Result<()> {
        let seq = self.seq_next;
        self.seq_next = self.seq_next.saturating_add(1);

        let record = RuntimeJournalRecord {
            seq,
            ts_unix_ms: Utc::now().timestamp_millis(),
            op,
        };

        let line = serde_json::to_string(&record).map_err(|err| {
            DbError::ExecutionError(format!("serialize runtime journal: {}", err))
        })?;

        let journal_path = self.journal_path();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let mut journal_line = line.clone();
        journal_line.push('\n');
        file.write_all(journal_line.as_bytes())
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        file.flush()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let now_ms = Utc::now().timestamp_millis();
        match self.policy.durability {
            RuntimeDurabilityMode::Strict => {
                file.sync_data()
                    .await
                    .map_err(|err| DbError::IoError(err.to_string()))?;
                self.last_sync_unix_ms = now_ms;
            }
            RuntimeDurabilityMode::Eventual { sync_interval_ms } => {
                if now_ms - self.last_sync_unix_ms >= sync_interval_ms as i64 {
                    file.sync_data()
                        .await
                        .map_err(|err| DbError::IoError(err.to_string()))?;
                    self.last_sync_unix_ms = now_ms;
                }
            }
        }

        self.ship_journal_line_to_replicas(&line).await;

        self.ops_since_snapshot = self.ops_since_snapshot.saturating_add(1);
        Ok(())
    }

    /// Checks if a snapshot should be triggered based on `ops_since_snapshot` threshold.
    ///
    /// If a background worker is configured and running, this is a no-op (letting the worker handle it).
    async fn maybe_snapshot_and_compact(&mut self) -> Result<()> {
        if self.policy.snapshot.background_worker_interval_ms.is_some()
            && self.snapshot_worker_running
        {
            return Ok(());
        }

        let _ = self.run_snapshot_tick().await?;
        Ok(())
    }

    /// Performs a full snapshot and journal compaction cycle.
    ///
    /// 1. Prunes expired tombstones.
    /// 2. Exports current state to a snapshot object.
    /// 3. Writes the snapshot to disk atomically.
    /// 4. Compacts the journal (removing records subsumed by the snapshot).
    /// 5. Replicates the snapshot to targets.
    async fn write_snapshot_and_compact(&mut self) -> Result<()> {
        let pruned = self.prune_expired_tombstones_at(Utc::now().timestamp_millis());
        self.tombstones_pruned_total = self.tombstones_pruned_total.saturating_add(pruned as u64);
        let snapshot = self.export_snapshot();
        self.write_snapshot_file(&snapshot).await?;
        self.compact_journal_at(self.journal_path(), snapshot.last_seq)
            .await?;
        self.ship_snapshot_to_replicas(&snapshot).await;

        self.ops_since_snapshot = 0;
        Ok(())
    }

    /// Reads the snapshot file from disk, returning `None` if it doesn't exist.
    async fn read_snapshot_file(&self) -> Result<Option<RuntimeSnapshotFile>> {
        let path = self.snapshot_path();
        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(&path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let snapshot = serde_json::from_slice::<RuntimeSnapshotFile>(&bytes)
            .map_err(|err| DbError::ExecutionError(format!("parse runtime snapshot: {}", err)))?;

        Ok(Some(snapshot))
    }

    /// Writes the snapshot to a temporary file and atomically renames it to the target path.
    async fn write_snapshot_file(&self, snapshot: &RuntimeSnapshotFile) -> Result<()> {
        let path = self.snapshot_path();
        let tmp_path = path.with_extension("tmp");

        let json = serde_json::to_vec_pretty(snapshot).map_err(|err| {
            DbError::ExecutionError(format!("serialize runtime snapshot: {}", err))
        })?;

        fs::write(&tmp_path, json)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        fs::rename(&tmp_path, &path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        Ok(())
    }

    /// Triggers journal compaction for the main runtime journal.
    async fn compact_journal_at(&self, journal_path: PathBuf, keep_after_seq: u64) -> Result<()> {
        Self::compact_journal_path(journal_path, keep_after_seq).await
    }

    /// Compacts a specific journal file by removing records with sequence numbers <= `keep_after_seq`.
    ///
    /// This reads the journal line by line and writes valid records to a temporary file,
    /// then atomically renames it.
    async fn compact_journal_path(journal_path: PathBuf, keep_after_seq: u64) -> Result<()> {
        if !journal_path.exists() {
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&journal_path)
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
            return Ok(());
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&journal_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        let mut reader = BufReader::new(file).lines();
        let mut retained = Vec::new();

        while let Some(line) = reader
            .next_line()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?
        {
            if line.trim().is_empty() {
                continue;
            }

            let record = serde_json::from_str::<RuntimeJournalRecord>(&line).map_err(|err| {
                DbError::ExecutionError(format!("parse runtime journal record: {}", err))
            })?;

            if record.seq > keep_after_seq {
                let serialized = serde_json::to_string(&record).map_err(|err| {
                    DbError::ExecutionError(format!("serialize runtime journal record: {}", err))
                })?;
                retained.push(serialized);
            }
        }

        let tmp_path = journal_path.with_extension("tmp");
        let mut tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        for line in retained {
            tmp.write_all(line.as_bytes())
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
            tmp.write_all(b"\n")
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        tmp.flush()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        fs::rename(&tmp_path, &journal_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        Ok(())
    }
}
