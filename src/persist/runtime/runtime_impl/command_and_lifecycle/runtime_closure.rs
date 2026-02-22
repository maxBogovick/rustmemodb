impl PersistEntityRuntime {
    /// Invokes a runtime closure on an entity.
    ///
    /// Runtime closures are arbitrary async functions that can modify entity state.
    /// Unlike deterministic commands, they are NOT journaled as events and cannot be replayed.
    /// Only the SIDE EFFECTS (state changes) are persisted.
    ///
    /// # Warning
    /// Use this for transient logic or complex queries/updates that don't need event sourcing
    /// guarantees.
    pub async fn invoke_runtime_closure(
        &mut self,
        entity_type: &str,
        persist_id: &str,
        function: &str,
        args: Vec<Value>,
    ) -> Result<Value> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;
        let span = info_span!(
            "runtime.closure.invoke",
            entity_type = %entity_type,
            entity_id = %persist_id,
            function = %function
        );
        let _enter = span.enter();

        let runtime_handler = self
            .runtime_closure_registry
            .get(entity_type)
            .and_then(|commands| commands.get(function))
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Runtime closure '{}' is not registered for entity type '{}'",
                    function, entity_type
                ))
            })?;

        let key = RuntimeEntityKey::new(entity_type, persist_id);
        let mut entity = self.take_entity_for_mutation(&key)?;
        self.mailbox_start_command(&key);
        let base = entity.clone();
        let result = runtime_handler(&mut entity.state, args);
        let result = match result {
            Ok(value) => value,
            Err(err) => {
                self.hot_entities.insert(key.clone(), base);
                self.mailbox_complete_command(&key);
                event!(Level::ERROR, error = %err, "runtime closure handler failed");
                return Err(err);
            }
        };

        // Runtime closures are intentionally not deterministic/serializable.
        // We keep them available for local runtime behavior, and persist only
        // the final state snapshot as an upsert event.
        entity.touch();
        if let Err(err) = self.apply_upsert(entity, "runtime_closure", None).await {
            self.hot_entities.insert(key.clone(), base);
            self.mailbox_complete_command(&key);
            event!(Level::ERROR, error = %err, "runtime closure persist failed");
            return Err(err);
        }
        self.mailbox_complete_command(&key);
        event!(Level::DEBUG, "runtime closure applied");

        Ok(result)
    }
}
