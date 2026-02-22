impl PersistEntityRuntime {
    /// Ships a journal line to all configured replicas.
    ///
    /// - `Sync` mode: Writes to all replicas sequentially and waits for success. Stops on error? (Depends on implementation, here it logs error but continues to next replica).
    /// - `AsyncBestEffort`: Spawns logging tasks to write to replicas in background.
    async fn ship_journal_line_to_replicas(&self, line: &str) {
        if self.replica_targets.is_empty() {
            return;
        }

        match self.policy.replication.mode {
            RuntimeReplicationMode::Sync => {
                for replica in &self.replica_targets {
                    if let Err(err) = Self::append_line_to_journal_path(
                        replica.journal_file.clone(),
                        line.to_string(),
                        true,
                    )
                    .await
                    {
                        self.record_replication_error(
                            &format!("sync journal shipping to {}", replica.root_dir.display()),
                            &err,
                        );
                    }
                }
            }
            RuntimeReplicationMode::AsyncBestEffort => {
                let line = line.to_string();
                for replica in self.replica_targets.clone() {
                    let failures = self.replication_failures.clone();
                    let target_path = replica.journal_file.clone();
                    let target_root = replica.root_dir.clone();
                    let line_copy = line.clone();
                    tokio::spawn(async move {
                        if let Err(err) = PersistEntityRuntime::append_line_to_journal_path(
                            target_path,
                            line_copy,
                            false,
                        )
                        .await
                        {
                            failures.fetch_add(1, AtomicOrdering::Relaxed);
                            eprintln!(
                                "runtime replication (async journal) failed for {}: {}",
                                target_root.display(),
                                err
                            );
                        }
                    });
                }
            }
        }
    }

    /// Ships a snapshot file to all configured replicas and triggers journal compaction on them.
    async fn ship_snapshot_to_replicas(&self, snapshot: &RuntimeSnapshotFile) {
        if self.replica_targets.is_empty() {
            return;
        }

        let snapshot_bytes = match serde_json::to_vec_pretty(snapshot) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.record_replication_error(
                    "serialize snapshot for replication",
                    &DbError::ExecutionError(err.to_string()),
                );
                return;
            }
        };

        match self.policy.replication.mode {
            RuntimeReplicationMode::Sync => {
                for replica in &self.replica_targets {
                    let write_result = Self::write_snapshot_path(
                        replica.snapshot_file.clone(),
                        snapshot_bytes.clone(),
                    )
                    .await;
                    if let Err(err) = write_result {
                        self.record_replication_error(
                            &format!("sync snapshot shipping to {}", replica.root_dir.display()),
                            &err,
                        );
                        continue;
                    }

                    if let Err(err) =
                        Self::compact_journal_path(replica.journal_file.clone(), snapshot.last_seq)
                            .await
                    {
                        self.record_replication_error(
                            &format!(
                                "sync journal compaction on replica {}",
                                replica.root_dir.display()
                            ),
                            &err,
                        );
                    }
                }
            }
            RuntimeReplicationMode::AsyncBestEffort => {
                for replica in self.replica_targets.clone() {
                    let failures = self.replication_failures.clone();
                    let bytes = snapshot_bytes.clone();
                    let snapshot_path = replica.snapshot_file.clone();
                    let replica_root = replica.root_dir.clone();
                    tokio::spawn(async move {
                        if let Err(err) =
                            PersistEntityRuntime::write_snapshot_path(snapshot_path, bytes).await
                        {
                            failures.fetch_add(1, AtomicOrdering::Relaxed);
                            eprintln!(
                                "runtime replication (async snapshot) failed for {}: {}",
                                replica_root.display(),
                                err
                            );
                        }
                    });
                }
            }
        }
    }

    /// Helper to append a line to a journal file at a specific path.
    ///
    /// Accepts `force_sync` to control `fsync` behavior (used for Sync replication).
    async fn append_line_to_journal_path(
        journal_path: PathBuf,
        line: String,
        force_sync: bool,
    ) -> Result<()> {
        if let Some(parent) = journal_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let mut journal_line = line;
        journal_line.push('\n');
        file.write_all(journal_line.as_bytes())
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        file.flush()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        if force_sync {
            file.sync_data()
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        Ok(())
    }

    /// Helper to write a snapshot to a specific path (atomic write).
    async fn write_snapshot_path(snapshot_path: PathBuf, bytes: Vec<u8>) -> Result<()> {
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        let tmp_path = snapshot_path.with_extension("tmp");
        fs::write(&tmp_path, bytes)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        fs::rename(&tmp_path, &snapshot_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        Ok(())
    }

    /// Logs a replication error and increments the failure counter.
    fn record_replication_error(&self, context: &str, err: &DbError) {
        self.replication_failures
            .fetch_add(1, AtomicOrdering::Relaxed);
        eprintln!("runtime replication error ({context}): {err}");
    }

    /// Logs a snapshot worker error and increments the failure counter.
    pub(super) fn record_snapshot_worker_error(&self, err: &DbError) {
        self.snapshot_worker_errors
            .fetch_add(1, AtomicOrdering::Relaxed);
        eprintln!("runtime snapshot worker error: {err}");
    }
}
