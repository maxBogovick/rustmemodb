impl PersistEntityRuntime {
    /// Runs lifecycle management tasks: passivation (hot -> cold), GC (cold -> deleted),
    /// and tombstone pruning.
    ///
    /// Returns a report detailing how many entities were transitioned.
    pub async fn run_lifecycle_maintenance(&mut self) -> Result<RuntimeLifecycleReport> {
        let now = Utc::now();
        let now_unix_ms = now.timestamp_millis();
        let passivate_after = TokioDuration::from_millis(self.policy.lifecycle.passivate_after_ms);
        let gc_after = TokioDuration::from_millis(self.policy.lifecycle.gc_after_ms);

        let mut passivated = 0usize;
        let mut gc_deleted = 0usize;
        let tombstones_pruned = self.prune_expired_tombstones_at(now_unix_ms);
        let mut passivated_in_this_cycle = HashSet::new();

        let mut to_passivate = Vec::new();
        for (key, entity) in &self.hot_entities {
            if self.mailbox_is_busy(key) {
                continue;
            }
            let idle_ms = (now - entity.last_access_at).num_milliseconds();
            if idle_ms >= passivate_after.as_millis() as i64 {
                to_passivate.push(key.clone());
            }
        }

        if self.hot_entities.len() > self.policy.lifecycle.max_hot_objects {
            let mut candidates = self
                .hot_entities
                .iter()
                .map(|(k, v)| (k.clone(), v.last_access_at))
                .collect::<Vec<_>>();
            candidates.sort_by(|a, b| a.1.cmp(&b.1));

            let extra = self.hot_entities.len() - self.policy.lifecycle.max_hot_objects;
            for (candidate, _) in candidates.into_iter().take(extra) {
                if !to_passivate.contains(&candidate) {
                    to_passivate.push(candidate);
                }
            }
        }

        for key in to_passivate {
            if let Some(mut entity) = self.hot_entities.remove(&key) {
                entity.resident = false;
                self.cold_entities.insert(key.clone(), entity);
                passivated = passivated.saturating_add(1);
                passivated_in_this_cycle.insert(key);
            }
        }

        let mut to_gc = Vec::new();
        for (key, entity) in &self.cold_entities {
            if passivated_in_this_cycle.contains(key) {
                continue;
            }

            let idle_ms = (now - entity.last_access_at).num_milliseconds();
            let old_enough = idle_ms >= gc_after.as_millis() as i64;
            let eligible_by_touch = if self.policy.lifecycle.gc_only_if_never_touched {
                entity.state.metadata.touch_count == 0
            } else {
                true
            };

            if old_enough && eligible_by_touch && !self.mailbox_is_busy(key) {
                to_gc.push(key.clone());
            }
        }

        for key in to_gc {
            if let Some(removed) = self.cold_entities.remove(&key) {
                let projection_undo = self.apply_projection_delete(&key);
                let reason = "lifecycle_gc".to_string();
                let expires_at_unix_ms =
                    self.tombstone_expiry_for_reason(reason.as_str(), now_unix_ms);
                let append = self
                    .append_record(RuntimeJournalOp::Delete {
                        key: key.clone(),
                        reason: reason.clone(),
                        expires_at_unix_ms,
                    })
                    .await;
                if let Err(err) = append {
                    self.rollback_projection_undo(projection_undo);
                    self.cold_entities.insert(key, removed);
                    return Err(err);
                }
                gc_deleted = gc_deleted.saturating_add(1);
                self.mailbox_drop_entity(&key);
                self.apply_tombstone_for_delete(key, reason, now_unix_ms, expires_at_unix_ms);
            }
        }

        self.maybe_snapshot_and_compact().await?;
        self.lifecycle_passivated_total = self
            .lifecycle_passivated_total
            .saturating_add(passivated as u64);
        self.lifecycle_gc_deleted_total = self
            .lifecycle_gc_deleted_total
            .saturating_add(gc_deleted as u64);
        self.tombstones_pruned_total = self
            .tombstones_pruned_total
            .saturating_add(tombstones_pruned as u64);

        let resurrected = self.resurrected_since_last_report;
        self.resurrected_since_last_report = 0;

        Ok(RuntimeLifecycleReport {
            passivated,
            resurrected,
            gc_deleted,
            tombstones_pruned,
        })
    }

    /// Manually triggers a snapshot and compaction cycle locally.
    ///
    /// This will flatten the journal into a new snapshot file and clear the journal.
    pub async fn force_snapshot_and_compact(&mut self) -> Result<()> {
        self.write_snapshot_and_compact().await
    }

    /// Ticks the automatic snapshot logic.
    ///
    /// Returns true if a snapshot was performed.
    pub async fn run_snapshot_tick(&mut self) -> Result<bool> {
        let snapshot_due =
            self.ops_since_snapshot >= self.policy.snapshot.snapshot_every_ops.max(1);
        let journal_too_large = self
            .journal_size_bytes()
            .await?
            .cmp(&self.policy.snapshot.compact_if_journal_exceeds_bytes)
            == Ordering::Greater;

        if snapshot_due || journal_too_large {
            self.write_snapshot_and_compact().await?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Generates the full serializable snapshot struct for the requested time.
    pub fn export_snapshot(&self) -> RuntimeSnapshotFile {
        let mut entities = Vec::with_capacity(self.hot_entities.len() + self.cold_entities.len());
        entities.extend(self.hot_entities.values().cloned());
        entities.extend(self.cold_entities.values().cloned());
        let mut tombstones = self.tombstones.values().cloned().collect::<Vec<_>>();
        tombstones.sort_by(|a, b| {
            a.key
                .entity_type
                .cmp(&b.key.entity_type)
                .then(a.key.persist_id.cmp(&b.key.persist_id))
        });
        let mut outbox = self.outbox_records.values().cloned().collect::<Vec<_>>();
        outbox.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then(a.outbox_id.cmp(&b.outbox_id))
        });

        RuntimeSnapshotFile {
            format_version: RUNTIME_FORMAT_VERSION,
            created_at_unix_ms: Utc::now().timestamp_millis(),
            last_seq: self.seq_next.saturating_sub(1),
            entities,
            tombstones,
            outbox,
            idempotency_index: self.idempotency_index.clone(),
        }
    }
}
