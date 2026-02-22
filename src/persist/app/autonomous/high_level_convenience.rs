impl<V> PersistAutonomousAggregate<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel + Clone,
    <V::Item as PersistCommandModel>::Command: PersistCommandName,
{
    /// High-level API for executing a command with automatic retry on conflict.
    ///
    /// This method simplifies the read-modify-write loop:
    /// 1. Reads the current version.
    /// 2. Attempts to apply the command.
    /// 3. If an optimistic lock conflict occurs, it retries according to the configured policy.
    pub async fn intent<C>(&mut self, persist_id: &str, command: C) -> Result<Option<V::Item>>
    where
        C: PersistAutonomousCommand<V::Item>,
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
                .apply(&persist_id, expected_version, command.clone())
                .await
            {
                Ok(updated) => return Ok(updated),
                Err(err) => {
                    if !self.should_retry_convenience_conflict(attempt, &err) {
                        return Err(err);
                    }

                    let backoff_ms = self.convenience_retry_backoff_ms(attempt);
                    warn!(
                        "PersistAutonomousAggregate.intent retry on conflict (attempt {} of {}): {} (backoff={}ms)",
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

    /// Executes a domain intent and returns a concrete domain outcome.
    ///
    /// This is the preferred app-facing variant when services should not branch
    /// on `Option` or inspect low-level `DbError`.
    pub async fn intent_one<C>(
        &mut self,
        persist_id: &str,
        command: C,
    ) -> std::result::Result<V::Item, PersistDomainError>
    where
        C: PersistAutonomousCommand<V::Item>,
    {
        self.intent(persist_id, command)
            .await
            .map_err(PersistDomainError::from)?
            .ok_or(PersistDomainError::NotFound)
    }

    /// Applies a command to multiple aggregates.
    ///
    /// Note: Currently `intent_many` does not support automatic retry because
    /// multi-aggregate transactions (atomic commits) are harder to retry granularly.
    /// It delegates directly to `apply_many`.
    pub async fn intent_many<C>(&mut self, persist_ids: &[String], command: C) -> Result<u64>
    where
        C: PersistAutonomousCommand<V::Item>,
    {
        self.apply_many(persist_ids, command).await
    }

    /// High-level API for applying a patch with automatic retry.
    ///
    /// Fetches the current version and applies the patch. If a conflict occurs,
    /// it fetches the new version and retries the patch (which might still be valid).
    pub async fn patch(
        &mut self,
        persist_id: &str,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> Result<Option<V::Item>> {
        let Some(expected_version) = self
            .get(persist_id)
            .map(|current| current.metadata().version)
        else {
            return Ok(None);
        };

        self.patch_if_match(persist_id, expected_version, patch)
            .await
    }

    /// Applies a patch and returns a concrete domain outcome.
    pub async fn patch_one(
        &mut self,
        persist_id: &str,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> std::result::Result<V::Item, PersistDomainError> {
        self.patch(persist_id, patch)
            .await
            .map_err(PersistDomainError::from)?
            .ok_or(PersistDomainError::NotFound)
    }

    /// High-level API for deleting an aggregate with automatic retry.
    pub async fn remove(&mut self, persist_id: &str) -> Result<bool> {
        let persist_id = persist_id.to_string();
        let mut attempt = 1usize;
        loop {
            let Some(expected_version) = self
                .get(&persist_id)
                .map(|current| current.metadata().version)
            else {
                return Ok(false);
            };

            match self.delete_if_match(&persist_id, expected_version).await {
                Ok(deleted) => return Ok(deleted),
                Err(err) => {
                    if !self.should_retry_convenience_conflict(attempt, &err) {
                        return Err(err);
                    }

                    let backoff_ms = self.convenience_retry_backoff_ms(attempt);
                    warn!(
                        "PersistAutonomousAggregate.remove retry on conflict (attempt {} of {}): {} (backoff={}ms)",
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

    /// Removes an aggregate and returns a concrete domain outcome.
    pub async fn remove_one(
        &mut self,
        persist_id: &str,
    ) -> std::result::Result<(), PersistDomainError> {
        if self
            .remove(persist_id)
            .await
            .map_err(PersistDomainError::from)?
        {
            Ok(())
        } else {
            Err(PersistDomainError::NotFound)
        }
    }
}

impl<V> PersistAutonomousAggregate<V>
where
    V: PersistIndexedCollection,
{
    /// Applies an in-place closure mutation to one aggregate and returns updated state.
    ///
    /// This variant preserves mutator-level business errors (`E`) as-is and separates
    /// them from persistence/runtime failures.
    pub async fn mutate_one_with<F, E>(
        &mut self,
        persist_id: &str,
        mutator: F,
    ) -> std::result::Result<V::Item, PersistDomainMutationError<E>>
    where
        V::Item: Clone,
        F: FnOnce(&mut V::Item) -> std::result::Result<(), E>,
    {
        let updated = self
            .aggregate
            .update_with(persist_id, mutator)
            .await
            .map_err(PersistDomainError::from)
            .map_err(PersistDomainMutationError::Domain)?;

        let updated = match updated {
            Ok(updated) => updated,
            Err(user_error) => return Err(PersistDomainMutationError::User(user_error)),
        };

        if !updated {
            return Err(PersistDomainMutationError::Domain(PersistDomainError::NotFound));
        }

        self.get(persist_id).cloned().ok_or_else(|| {
            PersistDomainMutationError::Domain(PersistDomainError::Internal(format!(
                "entity '{}' missing after successful mutate in '{}'",
                persist_id,
                self.name()
            )))
        })
    }

    /// Applies an in-place closure mutation to multiple aggregates.
    ///
    /// This variant preserves mutator-level business errors (`E`) as-is and separates
    /// them from persistence/runtime failures.
    pub async fn mutate_many_with<F, E>(
        &mut self,
        persist_ids: &[String],
        mutator: F,
    ) -> std::result::Result<u64, PersistDomainMutationError<E>>
    where
        F: Fn(&mut V::Item) -> std::result::Result<(), E>,
    {
        let updated = self
            .aggregate
            .apply_many_with(persist_ids, mutator)
            .await
            .map_err(PersistDomainError::from)
            .map_err(PersistDomainMutationError::Domain)?;

        let updated = match updated {
            Ok(updated) => updated,
            Err(user_error) => return Err(PersistDomainMutationError::User(user_error)),
        };

        Ok(u64::try_from(updated).unwrap_or(u64::MAX))
    }

    /// Backward-compatible mutation API where mutator errors are `DbError`.
    ///
    /// For app/business code prefer `mutate_one_with` to avoid coupling mutator logic
    /// to low-level database error types.
    pub async fn mutate_one<F>(
        &mut self,
        persist_id: &str,
        mutator: F,
    ) -> std::result::Result<V::Item, PersistDomainError>
    where
        V::Item: Clone,
        F: FnOnce(&mut V::Item) -> Result<()>,
    {
        self.mutate_one_with(persist_id, mutator)
            .await
            .map_err(|error| match error {
                PersistDomainMutationError::Domain(domain) => domain,
                PersistDomainMutationError::User(db_error) => PersistDomainError::from(db_error),
            })
    }

    /// Backward-compatible bulk mutation API where mutator errors are `DbError`.
    ///
    /// For app/business code prefer `mutate_many_with` to avoid coupling mutator logic
    /// to low-level database error types.
    pub async fn mutate_many<F>(
        &mut self,
        persist_ids: &[String],
        mutator: F,
    ) -> std::result::Result<u64, PersistDomainError>
    where
        F: Fn(&mut V::Item) -> Result<()>,
    {
        self.mutate_many_with(persist_ids, mutator)
            .await
            .map_err(|error| match error {
                PersistDomainMutationError::Domain(domain) => domain,
                PersistDomainMutationError::User(db_error) => PersistDomainError::from(db_error),
            })
    }
}
