impl HeteroPersistVec {
    /// Ensures that the backing tables for all items exist in the database.
    pub async fn ensure_all_tables(&mut self, session: &PersistSession) -> Result<()> {
        for item in &mut self.items {
            item.ensure_table(session).await?;
        }
        Ok(())
    }

    /// Saves all items in the collection to the database.
    pub async fn save_all(&mut self, session: &PersistSession) -> Result<()> {
        for item in &mut self.items {
            item.save(session).await?;
        }
        Ok(())
    }

    /// Invokes a named function on all items that support it.
    ///
    /// Returns a list of outcomes, including values from successful invocations and errors from failed ones.
    /// Items that do not support the function are skipped (with a status of `SkippedUnsupported`).
    pub async fn invoke_supported(
        &mut self,
        function: &str,
        args: Vec<Value>,
        session: &PersistSession,
    ) -> Result<Vec<InvokeOutcome>> {
        let mut outcomes = Vec::with_capacity(self.items.len());
        for item in &mut self.items {
            if !item.supports_function(function) {
                outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::SkippedUnsupported,
                    result: None,
                });
                continue;
            }

            match item.invoke(function, args.clone(), session).await {
                Ok(value) => outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::Invoked,
                    result: Some(value),
                }),
                Err(err) => outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::Failed(err.to_string()),
                    result: None,
                }),
            }
        }

        Ok(outcomes)
    }

    /// Removes items that have not been touched for longer than `max_age`.
    ///
    /// Returns the number of items removed.
    pub async fn prune_stale(
        &mut self,
        max_age: Duration,
        session: &PersistSession,
    ) -> Result<usize> {
        let now = Utc::now();
        let mut kept = Vec::with_capacity(self.items.len());
        let mut removed = 0usize;

        for mut item in self.items.drain(..) {
            let metadata = item.metadata().clone();
            let is_stale = metadata.touch_count == 0 && (now - metadata.created_at) > max_age;
            if is_stale {
                let _ = item.delete(session).await;
                removed += 1;
            } else {
                kept.push(item);
            }
        }

        self.items = kept;
        Ok(removed)
    }
}
