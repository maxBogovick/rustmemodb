impl<V> ManagedPersistVec<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel,
{
    /// Returns the patch validation contract for this collection's items.
    ///
    /// Describes allowed patch operations and field constraints.
    pub fn patch_contract(&self) -> Vec<PersistPatchContract> {
        <V::Item as PersistCommandModel>::patch_contract()
    }

    /// Returns the command validation contract for this collection's items.
    ///
    /// Describes available domain commands and their schemas.
    pub fn command_contract(&self) -> Vec<PersistCommandContract> {
        <V::Item as PersistCommandModel>::command_contract()
    }

    /// Creates a new item from a draft payload.
    ///
    /// Validates the draft against the schema before converting it to an item and persisting it.
    pub async fn create_from_draft(
        &mut self,
        draft: <V::Item as PersistCommandModel>::Draft,
    ) -> Result<String> {
        <V::Item as PersistCommandModel>::validate_draft_payload(&draft)?;
        let item = <V::Item as PersistCommandModel>::try_from_draft(draft)?;
        let persist_id = item.persist_id().to_string();
        self.create(item).await?;
        Ok(persist_id)
    }

    /// Applies a patch to an existing item by ID.
    ///
    /// Returns `true` if the item was found and `false` otherwise.
    /// Note: Returns `Ok(true)` even if the patch resulted in no changes (idempotent),
    /// as long as the item exists.
    pub async fn patch(
        &mut self,
        persist_id: &str,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> Result<bool> {
        <V::Item as PersistCommandModel>::validate_patch_payload(&patch)?;

        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let operation_result = match self
            .collection
            .items()
            .iter()
            .position(|item| item.persist_id() == persist_id && item.metadata().persisted)
        {
            Some(index) => {
                let changed = {
                    let item = &mut self.collection.items_mut()[index];
                    <V::Item as PersistCommandModel>::apply_patch_model(item, patch)?
                };
                if changed {
                    self.save_all_checked(&tx_session)
                        .await
                        .map(|_| (true, true))
                } else {
                    Ok((true, false))
                }
            }
            None => Ok((false, false)),
        };

        let (found, changed) = self
            .finalize_atomic_scope("patch", rollback_snapshot, transaction_id, operation_result)
            .await?;

        if changed {
            self.on_mutation_committed().await?;
        }

        Ok(found)
    }

    /// Applies a domain command to an existing item by ID.
    ///
    /// Returns `true` if the item was found.
    /// Uses internal transaction management for safety.
    pub async fn apply_command(
        &mut self,
        persist_id: &str,
        command: <V::Item as PersistCommandModel>::Command,
    ) -> Result<bool> {
        <V::Item as PersistCommandModel>::validate_command_payload(&command)?;

        let persist_id = persist_id.to_string();
        let (rollback_snapshot, transaction_id, tx_session) = self.begin_atomic_scope().await?;

        let operation_result = match self
            .collection
            .items()
            .iter()
            .position(|item| item.persist_id() == persist_id && item.metadata().persisted)
        {
            Some(index) => {
                let changed = {
                    let item = &mut self.collection.items_mut()[index];
                    <V::Item as PersistCommandModel>::apply_command_model(item, command)?
                };
                if changed {
                    self.save_all_checked(&tx_session)
                        .await
                        .map(|_| (true, true))
                } else {
                    Ok((true, false))
                }
            }
            None => Ok((false, false)),
        };

        let (found, changed) = self
            .finalize_atomic_scope(
                "apply_command",
                rollback_snapshot,
                transaction_id,
                operation_result,
            )
            .await?;

        if changed {
            self.on_mutation_committed().await?;
        }

        Ok(found)
    }

    /// Applies a domain command using an existing session/transaction context.
    ///
    /// WARNING: This does NOT commit the transaction. The caller is responsible for committing.
    /// Does not trigger `on_mutation_committed` automatically.
    pub async fn apply_command_with_session(
        &mut self,
        session: &PersistSession,
        persist_id: &str,
        command: <V::Item as PersistCommandModel>::Command,
    ) -> Result<bool> {
        <V::Item as PersistCommandModel>::validate_command_payload(&command)?;

        let persist_id = persist_id.to_string();
        let Some(index) = self
            .collection
            .items()
            .iter()
            .position(|item| item.persist_id() == persist_id && item.metadata().persisted)
        else {
            return Ok(false);
        };

        let changed = {
            let item = &mut self.collection.items_mut()[index];
            <V::Item as PersistCommandModel>::apply_command_model(item, command)?
        };

        if changed {
            self.save_all_checked(session).await?;
        }

        Ok(true)
    }

    /// Applies a domain command using an explicit transaction handle.
    ///
    /// Wrapper around `apply_command_with_session` using the transaction's session.
    pub async fn apply_command_with_tx(
        &mut self,
        tx: &PersistTx,
        persist_id: &str,
        command: <V::Item as PersistCommandModel>::Command,
    ) -> Result<bool> {
        let session = tx.session();
        self.apply_command_with_session(&session, persist_id, command)
            .await
    }
}
