impl<V> ManagedPersistVec<V>
where
    V: PersistIndexedCollection,
{
    /// Deletes a single item by its persistent ID.
    ///
    /// Manages internal transaction.
    /// Validates the item exists before deleting.
    pub async fn delete(&mut self, persist_id: &str) -> Result<bool>
    where
        V::Item: PersistEntityFactory,
    {
        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;
        self.mark_persisted_index_dirty();
        let maybe_index = self
            .ensure_item_loaded_by_id_with_session(&tx_session, &persist_id)
            .await?;

        let operation_result = match maybe_index.and_then(|index| self.collection.remove_by_index(index)) {
            Some(mut item) => item.delete(&tx_session).await.map(|_| true),
            None => Ok(false),
        };

        let deleted = self
            .finalize_atomic_scope(
                "delete",
                rollback_snapshot,
                transaction_id,
                operation_result,
            )
            .await?;

        if deleted {
            self.on_mutation_committed().await?;
        }
        Ok(deleted)
    }

    /// Deletes an item if its version matches the expected version.
    ///
    /// Optimistic concurrency control for deletions.
    pub async fn execute_delete_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
    ) -> Result<bool>
    where
        V::Item: PersistEntityFactory,
    {
        let persist_id = persist_id.to_string();
        let Some(actual_version) = self.get_version_db(&persist_id).await? else {
            return Ok(false);
        };

        if actual_version != expected_version {
            return Err(map_managed_conflict_error(
                "execute_delete_if_match",
                DbError::ExecutionError(format!(
                    "Optimistic lock conflict for '{}:{}': expected version {}, actual {}",
                    self.name, persist_id, expected_version, actual_version
                )),
            ));
        }

        self.delete(&persist_id).await
    }

    /// Deletes multiple items by ID in a single atomic batch.
    ///
    /// Ignores items that are not found.
    /// Returns the count of actually deleted items.
    pub async fn delete_many(&mut self, persist_ids: &[String]) -> Result<usize>
    where
        V::Item: PersistEntityFactory,
    {
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;
        let mut indexes = Vec::with_capacity(persist_ids.len());
        for persist_id in persist_ids {
            if let Some(index) = self
                .ensure_item_loaded_by_id_with_session(&tx_session, persist_id)
                .await?
            {
                indexes.push(index);
            }
        }
        indexes.sort_unstable();
        indexes.dedup();
        indexes.reverse();

        self.mark_persisted_index_dirty();

        let mut removed = 0usize;
        let mut operation_result = Ok(());
        for index in indexes {
            let mut item = match self.collection.remove_by_index(index) {
                Some(item) => item,
                None => continue,
            };

            if let Err(err) = item.delete(&tx_session).await {
                operation_result = Err(err);
                break;
            }
            removed += 1;
        }

        let removed = self
            .finalize_atomic_scope(
                "delete_many",
                rollback_snapshot,
                transaction_id,
                operation_result.map(|_| removed),
            )
            .await?;

        if removed > 0 {
            self.on_mutation_committed().await?;
        }
        Ok(removed)
    }
}
