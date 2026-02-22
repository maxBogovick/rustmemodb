impl<V> PersistAggregateStore<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel + Clone,
{
    /// Executes an intent on a single aggregate, automatically generating an audit record.
    ///
    /// This method:
    /// 1. Converts the intent to a command.
    /// 2. Derives audit event type and message from the intent.
    /// 3. Executes the command with optimistic locking.
    /// 4. Creates an audit record in the provided `audits` store if successful.
    pub async fn execute_intent_if_match_auto_audit<
        I,
        ToCommand,
        EventType,
        EventMessage,
        EventTypeValue,
        EventMessageValue,
    >(
        &mut self,
        audits: &mut PersistAggregateStore<PersistAuditRecordVec>,
        persist_id: &str,
        expected_version: i64,
        intent: I,
        to_command: ToCommand,
        event_type: EventType,
        event_message: EventMessage,
    ) -> Result<Option<V::Item>>
    where
        I: Clone,
        ToCommand: Fn(I) -> <V::Item as PersistCommandModel>::Command,
        EventType: Fn(I) -> EventTypeValue,
        EventMessage: Fn(I) -> EventMessageValue,
        EventTypeValue: Into<String>,
        EventMessageValue: Into<String>,
    {
        let command = to_command(intent.clone());
        let event_type = event_type(intent.clone()).into();
        let message = event_message(intent).into();
        self.execute_command_if_match_with_audit(
            audits,
            persist_id,
            expected_version,
            command,
            event_type,
            message,
        )
        .await
    }

    /// Executes an intent on multiple aggregates, automatically generating audit records.
    ///
    /// The same intent is applied to all specified `persist_ids`.
    /// Audit records are created for each successfully modified aggregate.
    pub async fn execute_intent_for_many_auto_audit<
        I,
        ToCommand,
        EventType,
        EventMessage,
        EventTypeValue,
        EventMessageValue,
    >(
        &mut self,
        audits: &mut PersistAggregateStore<PersistAuditRecordVec>,
        persist_ids: &[String],
        intent: I,
        to_command: ToCommand,
        bulk_event_type: EventType,
        bulk_event_message: EventMessage,
    ) -> Result<u64>
    where
        I: Clone + Send + Sync + 'static,
        ToCommand: Fn(I) -> <V::Item as PersistCommandModel>::Command + Send + Sync + 'static,
        EventType: Fn(I) -> EventTypeValue,
        EventMessage: Fn(I) -> EventMessageValue,
        EventTypeValue: Into<String>,
        EventMessageValue: Into<String>,
    {
        let event_type = bulk_event_type(intent.clone()).into();
        let message = bulk_event_message(intent.clone()).into();

        self.execute_command_for_many_with_audit(
            audits,
            persist_ids,
            move || to_command(intent.clone()),
            event_type,
            message,
        )
        .await
    }

    /// Executes a command on an aggregate and creates an audit record.
    ///
    /// This is the core transactional method for audited operations.
    ///
    /// # Arguments
    /// * `audits` - The store where audit records will be saved.
    /// * `persist_id` - The ID of the aggregate target.
    /// * `expected_version` - Optimistic locking version.
    /// * `command` - The command to apply.
    /// * `audit_event_type` - Type string for the audit record.
    /// * `audit_message` - Human-readable message for the audit record.
    pub async fn execute_command_if_match_with_audit(
        &mut self,
        audits: &mut PersistAggregateStore<PersistAuditRecordVec>,
        persist_id: &str,
        expected_version: i64,
        command: <V::Item as PersistCommandModel>::Command,
        audit_event_type: impl Into<String>,
        audit_message: impl Into<String>,
    ) -> Result<Option<V::Item>> {
        let audit_event_type = audit_event_type.into();
        let audit_message = audit_message.into();

        self.managed
            .execute_command_if_match_with_create(
                &mut audits.managed,
                persist_id,
                expected_version,
                command,
                move |updated| {
                    Ok(PersistAuditRecord::new(
                        updated.persist_id().to_string(),
                        audit_event_type.clone(),
                        audit_message.clone(),
                        updated.metadata().version,
                    ))
                },
            )
            .await
    }

    /// Executes a command on multiple aggregates transactionally, with audit records.
    ///
    /// This method ensures that all modifications and audit records are committed
    /// in a single transaction.
    ///
    /// Returns the number of aggregates that were successfully updated.
    pub async fn execute_command_for_many_with_audit<F>(
        &mut self,
        audits: &mut PersistAggregateStore<PersistAuditRecordVec>,
        persist_ids: &[String],
        command_factory: F,
        audit_event_type: impl Into<String>,
        audit_message: impl Into<String>,
    ) -> Result<u64>
    where
        F: Fn() -> <V::Item as PersistCommandModel>::Command + Send + Sync + 'static,
    {
        if persist_ids.is_empty() {
            return Ok(0);
        }

        let mut deduped_ids = persist_ids.to_vec();
        deduped_ids.sort();
        deduped_ids.dedup();

        let audit_event_type = audit_event_type.into();
        let audit_message = audit_message.into();

        self.managed
            .atomic_with(&mut audits.managed, move |tx, users, events| {
                Box::pin(async move {
                    let mut updated = Vec::new();

                    for persist_id in &deduped_ids {
                        let found = users
                            .apply_command_with_tx(&tx, persist_id, command_factory())
                            .await?;
                        if !found {
                            continue;
                        }

                        let item = users.get(persist_id).cloned().ok_or_else(|| {
                            DbError::ExecutionError(format!(
                                "command applied but entity '{}' is missing in '{}'",
                                persist_id,
                                users.name()
                            ))
                        })?;
                        updated.push(item);
                    }

                    if updated.is_empty() {
                        return Ok(0u64);
                    }

                    let audit_records = updated
                        .iter()
                        .map(|item| {
                            PersistAuditRecord::new(
                                item.persist_id().to_string(),
                                audit_event_type.clone(),
                                audit_message.clone(),
                                item.metadata().version,
                            )
                        })
                        .collect::<Vec<_>>();

                    events.create_many_with_tx(&tx, audit_records).await?;
                    Ok(u64::try_from(updated.len()).unwrap_or(u64::MAX))
                })
            })
            .await
    }

    /// Helper for testing failure scenarios.
    ///
    /// Simulates a failure during the command execution phase after the command has matched.
    pub async fn execute_command_if_match_with_audit_injected_failure(
        &mut self,
        audits: &mut PersistAggregateStore<PersistAuditRecordVec>,
        persist_id: &str,
        expected_version: i64,
        command: <V::Item as PersistCommandModel>::Command,
        failure_message: impl Into<String>,
    ) -> Result<Option<V::Item>> {
        let failure_message = failure_message.into();
        self.managed
            .execute_command_if_match_with_create(
                &mut audits.managed,
                persist_id,
                expected_version,
                command,
                move |_updated| Err(DbError::ExecutionError(failure_message.clone())),
            )
            .await
    }
}
