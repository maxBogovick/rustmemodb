impl<V> ManagedPersistVec<V>
where
    V: PersistIndexedCollection,
{
    /// Updates a single item identified by `persist_id`.
    ///
    /// The `mutator` closure receives a mutable reference to the item.
    /// If the mutations result in valid state (checked by `save_all_checked`), the changes are committed.
    /// Returns `true` if the item was found and updated.
    pub async fn update<F>(&mut self, persist_id: &str, mutator: F) -> Result<bool>
    where
        F: FnOnce(&mut V::Item) -> Result<()>,
    {
        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let operation_result = match self
            .collection
            .items()
            .iter()
            .position(|item| item.persist_id() == persist_id && item.metadata().persisted)
        {
            Some(index) => {
                let mutator_result = {
                    let item = &mut self.collection.items_mut()[index];
                    mutator(item)
                };
                match mutator_result {
                    Ok(()) => self.save_all_checked(&tx_session).await.map(|_| true),
                    Err(err) => Err(err),
                }
            }
            None => Ok(false),
        };

        let updated = self
            .finalize_atomic_scope(
                "update",
                rollback_snapshot,
                transaction_id,
                operation_result,
            )
            .await?;

        if updated {
            self.on_mutation_committed().await?;
        }
        Ok(updated)
    }

    /// Updates one item with a user-defined error type.
    ///
    /// Unlike `update`, this API preserves business-validation errors from the mutator
    /// without converting them into `DbError` strings.
    pub async fn update_with<F, E>(
        &mut self,
        persist_id: &str,
        mutator: F,
    ) -> Result<std::result::Result<bool, E>>
    where
        F: FnOnce(&mut V::Item) -> std::result::Result<(), E>,
    {
        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let Some(index) = self
            .collection
            .items()
            .iter()
            .position(|item| item.persist_id() == persist_id && item.metadata().persisted)
        else {
            let updated = self
                .finalize_atomic_scope(
                    "update_with",
                    rollback_snapshot,
                    transaction_id,
                    Ok(false),
                )
                .await?;
            return Ok(Ok(updated));
        };

        let mutator_result = {
            let item = &mut self.collection.items_mut()[index];
            mutator(item)
        };

        if let Err(user_error) = mutator_result {
            return self
                .abort_atomic_scope_with_user_error(
                    "update_with",
                    rollback_snapshot,
                    transaction_id,
                    user_error,
                )
                .await;
        }

        let save_result = self.save_all_checked(&tx_session).await.map(|_| true);
        let updated = self
            .finalize_atomic_scope("update_with", rollback_snapshot, transaction_id, save_result)
            .await?;

        if updated {
            self.on_mutation_committed().await?;
        }
        Ok(Ok(updated))
    }

    /// Updates one item inside an externally managed transaction/session and returns mutator output.
    ///
    /// This method is intended for framework-level orchestration (for example,
    /// auto-idempotent REST command execution) where multiple managed collections
    /// must be updated atomically in one transaction.
    ///
    /// Returns:
    /// - `Ok(Ok(Some(output)))` when entity exists and mutator succeeds;
    /// - `Ok(Ok(None))` when entity is absent;
    /// - `Ok(Err(user_error))` when user mutator rejects;
    /// - `Err(DbError)` for persistence/runtime failures.
    pub async fn update_with_result_with_session<F, E, R>(
        &mut self,
        session: &PersistSession,
        persist_id: &str,
        mutator: F,
    ) -> Result<std::result::Result<Option<R>, E>>
    where
        F: FnOnce(&mut V::Item) -> std::result::Result<R, E>,
    {
        let persist_id = persist_id.to_string();
        let Some(index) = self
            .collection
            .items()
            .iter()
            .position(|item| item.persist_id() == persist_id && item.metadata().persisted)
        else {
            return Ok(Ok(None));
        };

        let output = {
            let item = &mut self.collection.items_mut()[index];
            match mutator(item) {
                Ok(output) => output,
                Err(user_error) => return Ok(Err(user_error)),
            }
        };

        self.save_all_checked(session).await?;
        Ok(Ok(Some(output)))
    }

    /// Transaction wrapper for `update_with_result_with_session`.
    pub async fn update_with_result_with_tx<F, E, R>(
        &mut self,
        tx: &PersistTx,
        persist_id: &str,
        mutator: F,
    ) -> Result<std::result::Result<Option<R>, E>>
    where
        F: FnOnce(&mut V::Item) -> std::result::Result<R, E>,
    {
        let session = tx.session();
        self.update_with_result_with_session(&session, persist_id, mutator)
            .await
    }

    /// Applies a mutator to multiple items based on their IDs.
    ///
    /// - Atomically updates all found items.
    /// - Skips items not found.
    /// - Aborts and rolls back if the mutator fails for any item.
    pub async fn apply_many<F>(&mut self, persist_ids: &[String], mutator: F) -> Result<usize>
    where
        F: Fn(&mut V::Item) -> Result<()>,
    {
        let persist_ids = persist_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let mut updated = 0usize;
        let mut operation_result = Ok(());

        for item in self.collection.items_mut().iter_mut() {
            if !item.metadata().persisted {
                continue;
            }
            if !persist_ids.contains(item.persist_id()) {
                continue;
            }

            if let Err(err) = mutator(item) {
                operation_result = Err(err);
                break;
            }
            updated += 1;
        }

        if operation_result.is_ok() && updated > 0 {
            operation_result = self.save_all_checked(&tx_session).await;
        }

        let updated = self
            .finalize_atomic_scope(
                "apply_many",
                rollback_snapshot,
                transaction_id,
                operation_result.map(|_| updated),
            )
            .await?;

        if updated > 0 {
            self.on_mutation_committed().await?;
        }
        Ok(updated)
    }

    /// Applies a mutator to many items and preserves user-defined mutator errors.
    pub async fn apply_many_with<F, E>(
        &mut self,
        persist_ids: &[String],
        mutator: F,
    ) -> Result<std::result::Result<usize, E>>
    where
        F: Fn(&mut V::Item) -> std::result::Result<(), E>,
    {
        let persist_ids = persist_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let mut updated = 0usize;
        for item in self.collection.items_mut().iter_mut() {
            if !item.metadata().persisted {
                continue;
            }
            if !persist_ids.contains(item.persist_id()) {
                continue;
            }

            if let Err(user_error) = mutator(item) {
                return self
                    .abort_atomic_scope_with_user_error(
                        "apply_many_with",
                        rollback_snapshot,
                        transaction_id,
                        user_error,
                    )
                    .await;
            }
            updated += 1;
        }

        let save_result = if updated > 0 {
            self.save_all_checked(&tx_session).await.map(|_| updated)
        } else {
            Ok(updated)
        };
        let updated = self
            .finalize_atomic_scope(
                "apply_many_with",
                rollback_snapshot,
                transaction_id,
                save_result,
            )
            .await?;

        if updated > 0 {
            self.on_mutation_committed().await?;
        }
        Ok(Ok(updated))
    }

    /// Applies a mutator to multiple items using an existing session.
    pub async fn apply_many_with_session<F>(
        &mut self,
        session: &PersistSession,
        persist_ids: &[String],
        mutator: F,
    ) -> Result<usize>
    where
        F: Fn(&mut V::Item) -> Result<()>,
    {
        let persist_ids = persist_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        let mut updated = 0usize;
        for item in self.collection.items_mut().iter_mut() {
            if !item.metadata().persisted {
                continue;
            }
            if !persist_ids.contains(item.persist_id()) {
                continue;
            }
            mutator(item)?;
            updated += 1;
        }

        if updated > 0 {
            self.save_all_checked(session).await?;
        }

        Ok(updated)
    }

    /// Applies a mutator to multiple items using an explicit transaction.
    pub async fn apply_many_with_tx<F>(
        &mut self,
        tx: &PersistTx,
        persist_ids: &[String],
        mutator: F,
    ) -> Result<usize>
    where
        F: Fn(&mut V::Item) -> Result<()>,
    {
        let session = tx.session();
        self.apply_many_with_session(&session, persist_ids, mutator)
            .await
    }
}
