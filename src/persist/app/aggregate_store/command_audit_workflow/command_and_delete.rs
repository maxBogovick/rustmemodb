impl<V> PersistAggregateStore<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel + Clone,
{
    /// Executes a partial update (patch) with optimistic locking.
    ///
    /// Only fields present in the patch are updated.
    pub async fn execute_patch_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> Result<Option<V::Item>> {
        self.managed
            .execute_patch_if_match(persist_id, expected_version, patch)
            .await
    }

    /// Deletes an aggregate with optimistic locking.
    ///
    /// Returns `true` if the item was deleted, `false` if it didn't exist or version mismatch.
    pub async fn execute_delete_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
    ) -> Result<bool> {
        self.managed
            .execute_delete_if_match(persist_id, expected_version)
            .await
    }
}
