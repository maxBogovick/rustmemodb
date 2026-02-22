impl<V> PersistAutonomousAggregate<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel + Clone,
    <V::Item as PersistCommandModel>::Command: PersistCommandName,
{
    /// Executes a workflow command that affects another domain store.
    ///
    /// This allows atomic cross-aggregate operations where:
    /// - The primary aggregate (`self`) is modified by the workflow command.
    /// - A secondary aggregate in `other` is created or updated as a side effect.
    pub async fn workflow_if_match_with_create<U, C>(
        &mut self,
        other: &mut PersistDomainStore<U>,
        persist_id: &str,
        expected_version: i64,
        workflow_command: C,
    ) -> Result<Option<V::Item>>
    where
        U: PersistIndexedCollection,
        V::Item: PersistWorkflowCommandModel<C, U::Item>,
        C: Send + 'static,
    {
        self.aggregate
            .execute_workflow_if_match_with_create(
                &mut other.aggregate,
                persist_id,
                expected_version,
                workflow_command,
            )
            .await
    }

    /// High-level API for executing a cross-aggregate workflow with automatic retry.
    ///
    /// Similar to `intent`, but supports side-effects on another store.
    pub async fn workflow_with_create<U, C>(
        &mut self,
        other: &mut PersistDomainStore<U>,
        persist_id: &str,
        workflow_command: C,
    ) -> Result<Option<V::Item>>
    where
        U: PersistIndexedCollection,
        V::Item: PersistWorkflowCommandModel<C, U::Item>,
        C: Clone + Send + 'static,
    {
        let persist_id = persist_id.to_string();
        let mut attempt = 1usize;
        loop {
            let Some(expected_version) = self
                .get(&persist_id)
                .map(|current| current.metadata().version)
            else {
                return Ok(None);
            };

            match self
                .workflow_if_match_with_create(
                    other,
                    &persist_id,
                    expected_version,
                    workflow_command.clone(),
                )
                .await
            {
                Ok(updated) => return Ok(updated),
                Err(err) => {
                    if !self.should_retry_convenience_conflict(attempt, &err) {
                        return Err(err);
                    }

                    let backoff_ms = self.convenience_retry_backoff_ms(attempt);
                    warn!(
                        "PersistAutonomousAggregate.workflow_with_create retry on conflict (attempt {} of {}): {} (backoff={}ms)",
                        attempt,
                        self.conflict_retry.max_attempts.max(1),
                        err,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Helper for testing robust failure handling.
    ///
    /// Simulates a failure during the command application phase.
    pub async fn apply_injected_failure<C>(
        &mut self,
        persist_id: &str,
        expected_version: i64,
        command: C,
        failure_message: impl Into<String>,
    ) -> Result<Option<V::Item>>
    where
        C: PersistAutonomousCommand<V::Item>,
    {
        self.aggregate
            .execute_command_if_match_with_audit_injected_failure(
                &mut self.audits,
                persist_id,
                expected_version,
                command.to_persist_command(),
                failure_message,
            )
            .await
    }

    /// Compatibility bridge for existing callers migrating to `patch_if_match`.
    ///
    /// Deprecated: prefer `patch_if_match`.
    pub async fn execute_patch_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> Result<Option<V::Item>> {
        self.patch_if_match(persist_id, expected_version, patch)
            .await
    }

    /// Compatibility bridge for existing callers migrating to `delete_if_match`.
    ///
    /// Deprecated: prefer `delete_if_match`.
    pub async fn execute_delete_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
    ) -> Result<bool> {
        self.delete_if_match(persist_id, expected_version).await
    }
}
