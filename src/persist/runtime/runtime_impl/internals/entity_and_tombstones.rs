impl PersistEntityRuntime {
    /// Records metrics when a cold entity is brought back into memory (resurrected).
    fn record_resurrection(&mut self) {
        self.resurrected_since_last_report = self.resurrected_since_last_report.saturating_add(1);
        self.lifecycle_resurrected_total = self.lifecycle_resurrected_total.saturating_add(1);
    }

    /// Calculates the expiry time for a tombstone based on the deletion reason and policy.
    ///
    /// Returns `None` if the tombstone should not expire or if the policy disables it.
    fn tombstone_expiry_for_reason(&self, reason: &str, deleted_at_unix_ms: i64) -> Option<i64> {
        if reason == "lifecycle_gc" && !self.policy.tombstone.retain_for_lifecycle_gc {
            return None;
        }

        if self.policy.tombstone.ttl_ms == 0 {
            return None;
        }

        let ttl_ms = self.policy.tombstone.ttl_ms.min(i64::MAX as u64) as i64;
        Some(deleted_at_unix_ms.saturating_add(ttl_ms))
    }

    /// Creates and properly indexes a tombstone for a deleted entity.
    ///
    /// If `expires_at_unix_ms` is `None`, the tombstone is not created (fire and forget delete).
    fn apply_tombstone_for_delete(
        &mut self,
        key: RuntimeEntityKey,
        reason: String,
        deleted_at_unix_ms: i64,
        expires_at_unix_ms: Option<i64>,
    ) {
        if expires_at_unix_ms.is_none() {
            self.tombstones.remove(&key);
            return;
        }

        self.tombstones.insert(
            key.clone(),
            RuntimeEntityTombstone {
                key,
                reason,
                deleted_at_unix_ms,
                expires_at_unix_ms,
            },
        );
    }

    /// Removes all tombstones that have expired before the given timestamp.
    ///
    /// Returns the count of removed tombstones.
    fn prune_expired_tombstones_at(&mut self, now_unix_ms: i64) -> usize {
        let expired_keys = self
            .tombstones
            .iter()
            .filter_map(|(key, tombstone)| {
                if tombstone.is_expired_at(now_unix_ms) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for key in &expired_keys {
            self.tombstones.remove(key);
        }

        expired_keys.len()
    }

    /// Retrieves an entity from storage (hot or cold) for mutation.
    ///
    /// This removes the entity from the internal maps, giving the caller exclusive ownership.
    /// If the entity is in cold storage, it is marked as resident and resurrection metrics are updated.
    ///
    /// Returns an error if the entity is not found.
    fn take_entity_for_mutation(&mut self, key: &RuntimeEntityKey) -> Result<RuntimeStoredEntity> {
        if let Some(entity) = self.hot_entities.remove(key) {
            return Ok(entity);
        }

        if let Some(mut entity) = self.cold_entities.remove(key) {
            entity.resident = true;
            self.record_resurrection();
            return Ok(entity);
        }

        Err(DbError::ExecutionError(format!(
            "Entity not found: {}:{}",
            key.entity_type, key.persist_id
        )))
    }

    /// Persists an updated entity state to the journal and updates in-memory structures.
    ///
    /// This is the final step of a command execution or closure invocation.
    /// - Writes `Upsert` op to journal.
    /// - Updates projections.
    /// - Re-inserts entity into `hot_entities`.
    /// - Triggers snapshot/compaction if necessary.
    async fn apply_upsert(
        &mut self,
        mut managed: RuntimeStoredEntity,
        reason: impl Into<String>,
        command: Option<RuntimeCommandInvocation>,
    ) -> Result<()> {
        managed.state.metadata.persisted = true;
        managed.resident = true;

        let projection_undo = self.apply_projection_upsert(&managed.state)?;

        let append_result = self
            .append_record(RuntimeJournalOp::Upsert {
                entity: managed.clone(),
                reason: reason.into(),
                command,
                envelope: None,
                outbox: Vec::new(),
                idempotency_scope_key: None,
            })
            .await;
        if let Err(err) = append_result {
            self.rollback_projection_undo(projection_undo);
            return Err(err);
        }

        let key = RuntimeEntityKey::from_state(&managed.state);
        self.cold_entities.remove(&key);
        self.tombstones.remove(&key);
        self.hot_entities.insert(key, managed);

        self.maybe_snapshot_and_compact().await
    }
}
