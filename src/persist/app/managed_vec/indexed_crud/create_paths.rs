impl<V> ManagedPersistVec<V>
where
    V: PersistIndexedCollection,
{
    /// Adds a single item to the collection.
    ///
    /// Manages an internal transaction to ensure atomicity and constraint validation.
    /// Triggers `on_mutation_committed` if successful.
    pub async fn create(&mut self, item: V::Item) -> Result<()> {
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;
        self.collection.add_one(item);
        let operation_result = self.save_all_checked(&tx_session).await;
        self.finalize_atomic_scope(
            "create",
            rollback_snapshot,
            transaction_id,
            operation_result,
        )
        .await?;
        self.on_mutation_committed().await
    }

    /// Adds a single item using an existing session.
    ///
    /// The caller is responsible for committing the session.
    pub async fn create_with_session(
        &mut self,
        session: &PersistSession,
        item: V::Item,
    ) -> Result<()> {
        self.collection.add_one(item);
        self.save_all_checked(session).await
    }

    /// Adds a single item using an explicit transaction.
    ///
    /// The caller is responsible for committing the transaction.
    pub async fn create_with_tx(&mut self, tx: &PersistTx, item: V::Item) -> Result<()> {
        let session = tx.session();
        self.create_with_session(&session, item).await
    }

    /// Adds multiple items to the collection in a single atomic batch.
    ///
    /// Efficiently saves all items together.
    pub async fn create_many(&mut self, items: Vec<V::Item>) -> Result<usize> {
        let count = items.len();
        if count == 0 {
            return Ok(0);
        }

        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;
        self.collection.add_many(items);
        let operation_result = self.save_all_checked(&tx_session).await;
        self.finalize_atomic_scope(
            "create_many",
            rollback_snapshot,
            transaction_id,
            operation_result,
        )
        .await?;
        self.on_mutation_committed().await?;
        Ok(count)
    }

    /// Adds multiple items using an existing session.
    pub async fn create_many_with_session(
        &mut self,
        session: &PersistSession,
        items: Vec<V::Item>,
    ) -> Result<usize> {
        let count = items.len();
        if count == 0 {
            return Ok(0);
        }
        self.collection.add_many(items);
        self.save_all_checked(session).await?;
        Ok(count)
    }

    /// Adds multiple items using an explicit transaction.
    pub async fn create_many_with_tx(
        &mut self,
        tx: &PersistTx,
        items: Vec<V::Item>,
    ) -> Result<usize> {
        let session = tx.session();
        self.create_many_with_session(&session, items).await
    }
}
