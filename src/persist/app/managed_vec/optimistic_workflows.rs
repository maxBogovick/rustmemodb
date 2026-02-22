impl<V> ManagedPersistVec<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel + Clone,
{
    /// Applies a patch if the item's current version matches the expected version.
    ///
    /// This enables optimistic concurrency control.
    /// Returns `Ok(None)` if the item is not found or version mismatch (via error mapping usually, but here signature suggests None).
    /// Wait, looking at implementation:
    /// - Returns `Ok(None)` if item not found.
    /// - Returns `Err` if version mismatch.
    /// - Returns `Ok(Some(item))` if successful.
    pub async fn execute_patch_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> Result<Option<V::Item>> {
        let persist_id = persist_id.to_string();
        let Some(existing) = self.get(&persist_id).cloned() else {
            return Ok(None);
        };

        let actual_version = existing.metadata().version;
        if actual_version != expected_version {
            return Err(map_managed_conflict_error(
                "execute_patch_if_match",
                DbError::ExecutionError(format!(
                    "Optimistic lock conflict for '{}:{}': expected version {}, actual {}",
                    self.name, persist_id, expected_version, actual_version
                )),
            ));
        }

        let found = self.patch(&persist_id, patch).await?;
        if !found {
            return Ok(None);
        }

        Ok(self.get(&persist_id).cloned())
    }

    /// Executes a workflow command that updates one item and optionally creates another in a different collection.
    ///
    /// Atomically updates `self` (if version matches) and creates a related item in `other`.
    pub async fn execute_workflow_if_match_with_create<U, C>(
        &mut self,
        other: &mut ManagedPersistVec<U>,
        persist_id: &str,
        expected_version: i64,
        workflow_command: C,
    ) -> Result<Option<V::Item>>
    where
        U: PersistIndexedCollection,
        V::Item: PersistWorkflowCommandModel<C, U::Item>,
        C: Send + 'static,
    {
        let command = <V::Item as PersistWorkflowCommandModel<C, U::Item>>::to_persist_command(
            &workflow_command,
        );

        self.execute_command_if_match_with_create(
            other,
            persist_id,
            expected_version,
            command,
            move |updated| {
                <V::Item as PersistWorkflowCommandModel<C, U::Item>>::to_related_record(
                    &workflow_command,
                    updated,
                )
            },
        )
        .await
    }

    /// Executes a workflow command across multiple items, creating related records for each.
    ///
    /// - Duplicates in `persist_ids` are ignored.
    /// - Items not found are skipped (not an error).
    /// - This is NOT optimistic; it does not check versions. It blindly applies the command to whatever version exists.
    /// - Atomically updates all found items and creates all related records.
    pub async fn execute_workflow_for_many_with_create_many<U, C>(
        &mut self,
        other: &mut ManagedPersistVec<U>,
        persist_ids: &[String],
        workflow_command: C,
    ) -> Result<u64>
    where
        U: PersistIndexedCollection,
        V::Item: PersistWorkflowCommandModel<C, U::Item>,
        C: Send + Sync + 'static,
    {
        if persist_ids.is_empty() {
            return Ok(0);
        }

        let mut deduped_ids = persist_ids.to_vec();
        deduped_ids.sort();
        deduped_ids.dedup();

        self.atomic_with(other, move |tx, left, right| {
            Box::pin(async move {
                let mut updated_items = Vec::new();

                for persist_id in &deduped_ids {
                    let command =
                        <V::Item as PersistWorkflowCommandModel<C, U::Item>>::to_persist_command(
                            &workflow_command,
                        );
                    let found = left.apply_command_with_tx(&tx, persist_id, command).await?;
                    if !found {
                        continue;
                    }

                    let updated = left.get(persist_id).cloned().ok_or_else(|| {
                        DbError::ExecutionError(format!(
                            "command applied but entity '{}' is missing in '{}'",
                            persist_id, left.name
                        ))
                    })?;
                    updated_items.push(updated);
                }

                if updated_items.is_empty() {
                    return Ok(0u64);
                }

                let mut related_items = Vec::with_capacity(updated_items.len());
                for updated in &updated_items {
                    related_items.push(
                        <V::Item as PersistWorkflowCommandModel<C, U::Item>>::to_related_record(
                            &workflow_command,
                            updated,
                        )?,
                    );
                }

                right.create_many_with_tx(&tx, related_items).await?;
                Ok(u64::try_from(updated_items.len()).unwrap_or(u64::MAX))
            })
        })
        .await
    }

    /// Applies a command if the version matches.
    ///
    /// Optimistic concurrency control for domain commands.
    pub async fn execute_command_if_match(
        &mut self,
        persist_id: &str,
        expected_version: i64,
        command: <V::Item as PersistCommandModel>::Command,
    ) -> Result<Option<V::Item>> {
        let persist_id = persist_id.to_string();
        let Some(existing) = self.get(&persist_id).cloned() else {
            return Ok(None);
        };

        let actual_version = existing.metadata().version;
        if actual_version != expected_version {
            return Err(map_managed_conflict_error(
                "execute_command_if_match",
                DbError::ExecutionError(format!(
                    "Optimistic lock conflict for '{}:{}': expected version {}, actual {}",
                    self.name, persist_id, expected_version, actual_version
                )),
            ));
        }

        let found = self.apply_command(&persist_id, command).await?;
        if !found {
            return Ok(None);
        }

        Ok(self.get(&persist_id).cloned())
    }

    /// Applies a command if match, and atomically creates a related item in another collection.
    ///
    /// Fundamental primitive for complex business transactions (e.g. Audit Logs).
    pub async fn execute_command_if_match_with_create<U, F>(
        &mut self,
        other: &mut ManagedPersistVec<U>,
        persist_id: &str,
        expected_version: i64,
        command: <V::Item as PersistCommandModel>::Command,
        build_related_item: F,
    ) -> Result<Option<V::Item>>
    where
        U: PersistIndexedCollection,
        F: FnOnce(&V::Item) -> Result<U::Item> + Send + 'static,
    {
        let persist_id = persist_id.to_string();
        let Some(existing) = self.get(&persist_id).cloned() else {
            return Ok(None);
        };

        let actual_version = existing.metadata().version;
        if actual_version != expected_version {
            return Err(map_managed_conflict_error(
                "execute_command_if_match_with_create",
                DbError::ExecutionError(format!(
                    "Optimistic lock conflict for '{}:{}': expected version {}, actual {}",
                    self.name, persist_id, expected_version, actual_version
                )),
            ));
        }

        self.atomic_with(other, move |tx, left, right| {
            Box::pin(async move {
                let found = left
                    .apply_command_with_tx(&tx, &persist_id, command)
                    .await?;
                if !found {
                    return Ok(None);
                }

                let updated = left.get(&persist_id).cloned().ok_or_else(|| {
                    DbError::ExecutionError(format!(
                        "command applied but entity '{}' is missing in '{}'",
                        persist_id, left.name
                    ))
                })?;

                let related = build_related_item(&updated)?;
                right.create_with_tx(&tx, related).await?;
                Ok(Some(updated))
            })
        })
        .await
    }
}
