impl<V> PersistAggregateStore<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel + Clone,
{
    /// Executes a multi-aggregate workflow transactionally.
    ///
    /// This method allows executing a command on the primary aggregate (`self`)
    /// while simultaneously creating/updating another aggregate in a secondary store (`other`).
    ///
    /// Useful for operations that have side-effects on other aggregates.
    pub async fn execute_workflow_if_match_with_create<U, C>(
        &mut self,
        other: &mut PersistAggregateStore<U>,
        persist_id: &str,
        expected_version: i64,
        workflow_command: C,
    ) -> Result<Option<V::Item>>
    where
        U: PersistIndexedCollection,
        V::Item: PersistWorkflowCommandModel<C, U::Item>,
        C: Send + 'static,
    {
        self.managed
            .execute_workflow_if_match_with_create(
                &mut other.managed,
                persist_id,
                expected_version,
                workflow_command,
            )
            .await
    }

    /// Executes a bulk workflow transaction where multiple aggregates are updated.
    ///
    /// Applies the same workflow command to multiple items in `persist_ids`.
    pub async fn execute_workflow_for_many_with_create_many<U, C>(
        &mut self,
        other: &mut PersistAggregateStore<U>,
        persist_ids: &[String],
        workflow_command: C,
    ) -> Result<u64>
    where
        U: PersistIndexedCollection,
        V::Item: PersistWorkflowCommandModel<C, U::Item>,
        C: Send + Sync + 'static,
    {
        self.managed
            .execute_workflow_for_many_with_create_many(
                &mut other.managed,
                persist_ids,
                workflow_command,
            )
            .await
    }
}
