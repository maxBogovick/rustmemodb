impl PersistEntityRuntime {
    /// Creates a new entity with the given initial state.
    pub async fn create_entity(
        &mut self,
        entity_type: impl Into<String>,
        table_name: impl Into<String>,
        fields: serde_json::Value,
        schema_version: u32,
    ) -> Result<String> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;

        let now = Utc::now();
        let persist_id = new_persist_id();
        let mut metadata = PersistMetadata::new(now);
        metadata.schema_version = schema_version.max(1);
        metadata.version = 1;
        metadata.touch_count = 1;
        metadata.persisted = true;

        let state = PersistState {
            persist_id: persist_id.clone(),
            type_name: entity_type.into(),
            table_name: table_name.into(),
            metadata,
            fields,
        };

        let managed = RuntimeStoredEntity::new(state, true);
        self.apply_upsert(managed, "create", None).await?;
        Ok(persist_id)
    }

    /// Updates or inserts an entity's state directly (bypassing command handlers).
    ///
    /// This is typically used for internal updates or recovery/migrations.
    pub async fn upsert_state(
        &mut self,
        state: PersistState,
        reason: impl Into<String>,
    ) -> Result<()> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;

        let mut managed = RuntimeStoredEntity::new(state, true);
        managed.state.metadata.persisted = true;
        self.apply_upsert(managed, reason, None).await
    }

    /// Deletes an entity by marking it with a tombstone.
    pub async fn delete_entity(
        &mut self,
        entity_type: &str,
        persist_id: &str,
        reason: impl Into<String>,
    ) -> Result<()> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;
        let delete_reason = reason.into();
        let deleted_at_unix_ms = Utc::now().timestamp_millis();
        let expires_at_unix_ms =
            self.tombstone_expiry_for_reason(delete_reason.as_str(), deleted_at_unix_ms);
        let span = info_span!(
            "runtime.entity.delete",
            entity_type = %entity_type,
            entity_id = %persist_id,
            reason = %delete_reason
        );
        let _enter = span.enter();

        let key = RuntimeEntityKey::new(entity_type, persist_id);
        let projection_undo = self.apply_projection_delete(&key);

        let append_result = self
            .append_record(RuntimeJournalOp::Delete {
                key: key.clone(),
                reason: delete_reason.clone(),
                expires_at_unix_ms,
            })
            .await;
        if let Err(err) = append_result {
            self.rollback_projection_undo(projection_undo);
            event!(Level::ERROR, error = %err, "runtime entity delete append failed");
            return Err(err);
        }

        self.hot_entities.remove(&key);
        self.cold_entities.remove(&key);
        self.mailbox_drop_entity(&key);
        self.apply_tombstone_for_delete(key, delete_reason, deleted_at_unix_ms, expires_at_unix_ms);

        self.maybe_snapshot_and_compact().await?;
        event!(Level::DEBUG, "runtime entity deleted");
        Ok(())
    }

    /// Retrieves an entity's state, resurrecting it from cold storage if needed.
    pub fn get_state(&mut self, entity_type: &str, persist_id: &str) -> Result<PersistState> {
        let key = RuntimeEntityKey::new(entity_type, persist_id);

        if let Some(hot) = self.hot_entities.get_mut(&key) {
            hot.touch();
            return Ok(hot.state.clone());
        }

        if let Some(mut cold) = self.cold_entities.remove(&key) {
            cold.resident = true;
            cold.touch();
            let state = cold.state.clone();
            self.hot_entities.insert(key, cold);
            self.record_resurrection();
            return Ok(state);
        }

        Err(DbError::ExecutionError(format!(
            "Entity not found: {}:{}",
            entity_type, persist_id
        )))
    }

    /// Lists all entities currently in memory (hot or cold).
    pub fn list_states(&self) -> Vec<PersistState> {
        let mut states = Vec::with_capacity(self.hot_entities.len() + self.cold_entities.len());
        states.extend(self.hot_entities.values().map(|m| m.state.clone()));
        states.extend(self.cold_entities.values().map(|m| m.state.clone()));
        states
    }

    /// Lists all active tombstones.
    pub fn list_tombstones(&self) -> Vec<RuntimeEntityTombstone> {
        let mut tombstones = self.tombstones.values().cloned().collect::<Vec<_>>();
        tombstones.sort_by(|a, b| {
            a.key
                .entity_type
                .cmp(&b.key.entity_type)
                .then(a.key.persist_id.cmp(&b.key.persist_id))
        });
        tombstones
    }

    /// Lists all records currently in the outbox.
    pub fn list_outbox_records(&self) -> Vec<RuntimeOutboxRecord> {
        let mut records = self.outbox_records.values().cloned().collect::<Vec<_>>();
        records.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then(a.outbox_id.cmp(&b.outbox_id))
        });
        records
    }

    /// Lists only pending outbox records.
    pub fn list_pending_outbox_records(&self) -> Vec<RuntimeOutboxRecord> {
        self.list_outbox_records()
            .into_iter()
            .filter(|record| record.status == RuntimeOutboxStatus::Pending)
            .collect()
    }

    /// Marks an outbox record as dispatched.
    pub async fn mark_outbox_dispatched(&mut self, outbox_id: &str) -> Result<()> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;
        let span = info_span!("runtime.outbox.dispatch", outbox_id = %outbox_id);
        let _enter = span.enter();

        let Some(current) = self.outbox_records.get(outbox_id).cloned() else {
            return Err(DbError::ExecutionError(format!(
                "Outbox record not found: {}",
                outbox_id
            )));
        };

        if current.status == RuntimeOutboxStatus::Dispatched {
            event!(Level::DEBUG, "outbox already dispatched");
            return Ok(());
        }

        let mut updated = current;
        updated.status = RuntimeOutboxStatus::Dispatched;
        let persisted = updated.clone();
        self.append_record(RuntimeJournalOp::OutboxUpsert { record: updated })
            .await?;
        self.outbox_records
            .insert(outbox_id.to_string(), persisted.clone());
        self.update_idempotency_outbox_status(&persisted);
        self.maybe_snapshot_and_compact().await?;
        event!(Level::INFO, envelope_id = %persisted.envelope_id, "outbox dispatched");
        Ok(())
    }
}
