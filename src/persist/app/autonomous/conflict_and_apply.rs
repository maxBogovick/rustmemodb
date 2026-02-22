impl<V> PersistAutonomousAggregate<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel + Clone,
    <V::Item as PersistCommandModel>::Command: PersistCommandName,
{
    fn should_retry_convenience_conflict(&self, attempt: usize, err: &DbError) -> bool {
        let retry = &self.conflict_retry;
        if attempt >= retry.max_attempts.max(1) {
            return false;
        }

        let Some(kind) = classify_managed_conflict(err) else {
            return false;
        };

        matches!(kind, ManagedConflictKind::OptimisticLock)
            || (matches!(kind, ManagedConflictKind::WriteWrite) && retry.retry_write_write)
    }

    fn convenience_retry_backoff_ms(&self, attempt: usize) -> u64 {
        let retry = &self.conflict_retry;
        let base = retry.base_backoff_ms.max(1);
        let cap = retry.max_backoff_ms.max(base);

        let mut backoff = base;
        for _ in 1..attempt {
            backoff = backoff.saturating_mul(2).min(cap);
        }
        backoff
    }

    /// Applies a patch if the version matches.
    ///
    /// This is a low-level optimistic locking primitive.
    /// Returns `None` if the patch was applied successfully but the item was not returned,
    /// or `Some(item)` if the underlying store returns it (implementation dependent).
    pub async fn patch_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> Result<Option<V::Item>> {
        self.aggregate
            .execute_patch_if_match(persist_id, expected_version, patch)
            .await
    }

    /// Deletes an item if the version matches.
    ///
    /// Returns `true` if deleted, `false` upon version mismatch or if not found.
    pub async fn delete_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
    ) -> Result<bool> {
        self.aggregate
            .execute_delete_if_match(persist_id, expected_version)
            .await
    }

    /// Applies a domain command to an aggregate, with automatic audit logging.
    ///
    /// This method:
    /// 1. Converts the autonomous command to a persist command.
    /// 2. Derives audit metadata.
    /// 3. Executes the command transactionally.
    pub async fn apply<C>(
        &mut self,
        persist_id: &str,
        expected_version: i64,
        command: C,
    ) -> Result<Option<V::Item>>
    where
        C: PersistAutonomousCommand<V::Item>,
    {
        let persist_command = command.clone().to_persist_command();
        let event_type = command.audit_event_type(&persist_command);
        let event_message = command.audit_message(&persist_command);
        self.aggregate
            .execute_command_if_match_with_audit(
                &mut self.audits,
                persist_id,
                expected_version,
                persist_command,
                event_type,
                event_message,
            )
            .await
    }

    /// Applies a domain command to multiple aggregates, with automatic audit logging.
    ///
    /// Returns the number of successfully modified aggregates.
    pub async fn apply_many<C>(&mut self, persist_ids: &[String], command: C) -> Result<u64>
    where
        C: PersistAutonomousCommand<V::Item>,
    {
        let persist_command = command.clone().to_persist_command();
        let bulk_event_type = command.bulk_audit_event_type(&persist_command);
        let bulk_event_message = command.bulk_audit_message(&persist_command);
        let command_for_factory = command.clone();
        self.aggregate
            .execute_command_for_many_with_audit(
                &mut self.audits,
                persist_ids,
                move || command_for_factory.clone().to_persist_command(),
                bulk_event_type,
                bulk_event_message,
            )
            .await
    }
}
