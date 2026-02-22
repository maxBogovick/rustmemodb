impl<T: PersistEntityFactory> PersistVec<T> {
    /// Creates a snapshot of the collection.
    ///
    /// The snapshot includes metadata for the contained type and, if `mode` is `WithData`,
    /// the serialized state of all items.
    pub fn snapshot(&self, mode: SnapshotMode) -> PersistVecSnapshot {
        let object_type = self
            .items
            .first()
            .map(|item| item.type_name().to_string())
            .unwrap_or_else(|| T::entity_type_name().to_string());

        let table_name = self
            .items
            .first()
            .map(|item| item.table_name().to_string())
            .unwrap_or_else(T::default_table_name);

        PersistVecSnapshot {
            format_version: 1,
            created_at_unix_ms: Utc::now().timestamp_millis(),
            mode: mode.clone(),
            vec_name: self.name.clone(),
            object_type,
            table_name,
            schema_version: self
                .items
                .first()
                .map(|item| item.metadata().schema_version)
                .unwrap_or_else(T::schema_version),
            states: if mode == SnapshotMode::WithData {
                self.states()
            } else {
                Vec::new()
            },
        }
    }

    /// Restores the collection from a snapshot using the default `FailFast` conflict policy.
    pub async fn restore(
        &mut self,
        snapshot: PersistVecSnapshot,
        session: &PersistSession,
    ) -> Result<()> {
        self.restore_with_policy(snapshot, session, RestoreConflictPolicy::FailFast)
            .await
    }

    /// Restores the collection from a snapshot with a specific conflict policy, using the default migration plan.
    pub async fn restore_with_policy(
        &mut self,
        snapshot: PersistVecSnapshot,
        session: &PersistSession,
        conflict_policy: RestoreConflictPolicy,
    ) -> Result<()> {
        self.restore_with_custom_migration_plan(snapshot, session, conflict_policy, T::migration_plan())
            .await
    }

    /// Restores the collection with a specific conflict policy and migration plan.
    ///
    /// This process:
    /// 1. Validates the migration plan.
    /// 2. Creates the table if it doesn't exist and ensures schema compatibility.
    /// 3. Clears existing items in memory.
    /// 4. If the snapshot contains data, reconstructs entities from state, handles conflicts, and saves them.
    pub async fn restore_with_custom_migration_plan(
        &mut self,
        snapshot: PersistVecSnapshot,
        session: &PersistSession,
        conflict_policy: RestoreConflictPolicy,
        migration_plan: PersistMigrationPlan,
    ) -> Result<()> {
        migration_plan.validate()?;
        let create_sql = T::create_table_sql(&snapshot.table_name);
        session.execute(&create_sql).await?;
        migration_plan
            .ensure_table_schema_version(session, &snapshot.table_name)
            .await?;

        self.name = snapshot.vec_name;
        self.items.clear();

        if snapshot.mode == SnapshotMode::WithData {
            for mut state in snapshot.states {
                if state.metadata.schema_version == 0 {
                    state.metadata.schema_version = snapshot.schema_version.max(default_schema_version());
                }
                migration_plan.migrate_state_to_current(&mut state)?;

                let exists = session
                    .persist_row_exists(&state.table_name, &state.persist_id)
                    .await?;

                if exists {
                    match conflict_policy {
                        RestoreConflictPolicy::FailFast => {
                            return Err(DbError::ExecutionError(format!(
                                "Restore conflict: row {} already exists in table {}",
                                state.persist_id, state.table_name
                            )));
                        }
                        RestoreConflictPolicy::SkipExisting => {
                            continue;
                        }
                        RestoreConflictPolicy::OverwriteExisting => {
                            session
                                .delete_persist_row(&state.table_name, &state.persist_id)
                                .await?;
                        }
                    }
                }

                let mut entity = T::from_state(&state)?;
                entity.restore_into_db(session).await?;
                self.items.push(entity);
            }
        }

        Ok(())
    }
}
