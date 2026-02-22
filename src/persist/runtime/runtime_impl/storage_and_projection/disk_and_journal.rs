impl PersistEntityRuntime {
    /// Returns the path to the snapshot file in the runtime root directory.
    fn snapshot_path(&self) -> PathBuf {
        self.root_dir.join(RUNTIME_SNAPSHOT_FILE)
    }

    /// Returns the path to the journal file in the runtime root directory.
    fn journal_path(&self) -> PathBuf {
        self.root_dir.join(RUNTIME_JOURNAL_FILE)
    }

    /// Rebuilds the runtime in-memory state by loading the latest snapshot and replaying
    /// the journal.
    ///
    /// This process involves:
    /// 1. Reading the snapshot file (if it exists).
    /// 2. Validating the snapshot format version.
    /// 3. Populating `hot_entities`, `cold_entities`, `tombstones`, and `outbox_records`.
    /// 4. Replaying any journal records that occurred after the snapshot.
    /// 5. Pruning expired tombstones and cleaning up related state.
    async fn load_from_disk(&mut self) -> Result<()> {
        let mut last_seq = 0u64;

        if let Some(snapshot) = self.read_snapshot_file().await? {
            if snapshot.format_version != RUNTIME_FORMAT_VERSION {
                return Err(DbError::ExecutionError(format!(
                    "Unsupported runtime snapshot format version {}",
                    snapshot.format_version
                )));
            }

            for mut entity in snapshot.entities {
                if entity.state.metadata.schema_version == 0 {
                    entity.state.metadata.schema_version = 1;
                }
                let key = RuntimeEntityKey::from_state(&entity.state);
                if entity.resident {
                    self.hot_entities.insert(key, entity);
                } else {
                    self.cold_entities.insert(key, entity);
                }
            }
            let now_unix_ms = Utc::now().timestamp_millis();
            for tombstone in snapshot.tombstones {
                if tombstone.is_expired_at(now_unix_ms) {
                    continue;
                }
                self.tombstones.insert(tombstone.key.clone(), tombstone);
            }
            for record in snapshot.outbox {
                self.outbox_records.insert(record.outbox_id.clone(), record);
            }
            self.idempotency_index = snapshot.idempotency_index;
            last_seq = snapshot.last_seq;
        }

        let records = self.read_journal_records(last_seq).await?;
        let mut max_seq = last_seq;
        for record in records {
            max_seq = max_seq.max(record.seq);
            self.apply_journal_record_to_memory(record);
        }

        let pruned = self.prune_expired_tombstones_at(Utc::now().timestamp_millis());
        self.tombstones_pruned_total = self.tombstones_pruned_total.saturating_add(pruned as u64);
        let tombstoned_keys = self.tombstones.keys().cloned().collect::<Vec<_>>();
        for key in tombstoned_keys {
            self.hot_entities.remove(&key);
            self.cold_entities.remove(&key);
            self.mailbox_drop_entity(&key);
            let _ = self.apply_projection_delete(&key);
        }

        self.seq_next = max_seq.saturating_add(1).max(1);
        Ok(())
    }

    /// Applies a single journal record to the in-memory state.
    ///
    /// This handles:
    /// - `Upsert`: Updating entity state, residency (hot/cold), tombstones, and outbox/idempotency.
    /// - `Delete`: removing entity state, creating tombstones.
    /// - `OutboxUpsert`: Updating outbox status (e.g., dispatched).
    fn apply_journal_record_to_memory(&mut self, record: RuntimeJournalRecord) {
        let RuntimeJournalRecord { ts_unix_ms, op, .. } = record;
        match op {
            RuntimeJournalOp::Upsert {
                entity,
                envelope,
                outbox,
                idempotency_scope_key,
                ..
            } => {
                let key = RuntimeEntityKey::from_state(&entity.state);
                if entity.resident {
                    self.cold_entities.remove(&key);
                    self.hot_entities.insert(key.clone(), entity);
                } else {
                    self.hot_entities.remove(&key);
                    self.cold_entities.insert(key.clone(), entity);
                }
                self.tombstones.remove(&key);

                for record in &outbox {
                    self.outbox_records
                        .insert(record.outbox_id.clone(), record.clone());
                }

                if let (Some(scope_key), Some(envelope)) = (idempotency_scope_key, envelope) {
                    let state = self
                        .hot_entities
                        .get(&RuntimeEntityKey::new(
                            envelope.entity_type.clone(),
                            envelope.entity_id.clone(),
                        ))
                        .map(|stored| stored.state.clone())
                        .or_else(|| {
                            self.cold_entities
                                .get(&RuntimeEntityKey::new(
                                    envelope.entity_type.clone(),
                                    envelope.entity_id.clone(),
                                ))
                                .map(|stored| stored.state.clone())
                        });

                    if let Some(state) = state {
                        self.idempotency_index.insert(
                            scope_key,
                            RuntimeIdempotencyReceipt {
                                envelope_id: envelope.envelope_id,
                                entity_type: envelope.entity_type,
                                entity_id: envelope.entity_id,
                                command_name: envelope.command_name,
                                state,
                                outbox,
                            },
                        );
                    }
                }
            }
            RuntimeJournalOp::Delete {
                key,
                reason,
                expires_at_unix_ms,
            } => {
                self.hot_entities.remove(&key);
                self.cold_entities.remove(&key);
                self.mailbox_drop_entity(&key);
                let effective_expiry = expires_at_unix_ms
                    .or_else(|| self.tombstone_expiry_for_reason(reason.as_str(), ts_unix_ms));
                self.apply_tombstone_for_delete(key, reason, ts_unix_ms, effective_expiry);
            }
            RuntimeJournalOp::OutboxUpsert { record } => {
                self.outbox_records
                    .insert(record.outbox_id.clone(), record.clone());
                self.update_idempotency_outbox_status(&record);
            }
        }
    }
}
