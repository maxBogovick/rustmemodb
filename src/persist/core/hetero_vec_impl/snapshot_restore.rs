impl HeteroPersistVec {
    /// Creates a snapshot of the collection.
    ///
    /// The snapshot includes metadata for all registered types and, if `mode` is `WithData`,
    /// the serialized state of all items.
    pub fn snapshot(&self, mode: SnapshotMode) -> HeteroPersistVecSnapshot {
        let mut table_and_version_by_type = HashMap::<String, (String, u32)>::new();
        for item in &self.items {
            table_and_version_by_type
                .entry(item.type_name().to_string())
                .or_insert_with(|| {
                    (
                        item.table_name().to_string(),
                        item.metadata().schema_version.max(default_schema_version()),
                    )
                });
        }

        for (type_name, registration) in &self.registrations {
            table_and_version_by_type
                .entry(type_name.clone())
                .or_insert_with(|| {
                    (
                        (registration.default_table_name)(),
                        (registration.schema_version)(),
                    )
                });
        }

        let mut types = table_and_version_by_type
            .into_iter()
            .map(
                |(type_name, (table_name, schema_version))| HeteroTypeSnapshot {
                    type_name,
                    table_name,
                    schema_version,
                },
            )
            .collect::<Vec<_>>();
        types.sort_by(|a, b| a.type_name.cmp(&b.type_name));

        HeteroPersistVecSnapshot {
            format_version: 1,
            created_at_unix_ms: Utc::now().timestamp_millis(),
            mode: mode.clone(),
            vec_name: self.name.clone(),
            types,
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
        snapshot: HeteroPersistVecSnapshot,
        session: &PersistSession,
    ) -> Result<()> {
        self.restore_with_policy(snapshot, session, RestoreConflictPolicy::FailFast)
            .await
    }

    /// Restores the collection from a snapshot with a specific conflict policy.
    ///
    /// This process:
    /// 1. Clears any existing items in memory.
    /// 2. Ensures tables exist and are migrated to the current schema for all types in the snapshot.
    /// 3. If the snapshot contains data, reconstructs entities from state, handles conflicts, and saves them.
    pub async fn restore_with_policy(
        &mut self,
        snapshot: HeteroPersistVecSnapshot,
        session: &PersistSession,
        conflict_policy: RestoreConflictPolicy,
    ) -> Result<()> {
        self.name = snapshot.vec_name.clone();
        self.items.clear();

        let mut created_pairs = HashSet::<(String, String)>::new();
        for t in &snapshot.types {
            created_pairs.insert((t.type_name.clone(), t.table_name.clone()));
        }
        if snapshot.mode == SnapshotMode::WithData {
            for state in &snapshot.states {
                created_pairs.insert((state.type_name.clone(), state.table_name.clone()));
            }
        }

        for (type_name, table_name) in &created_pairs {
            let registration = self.registrations.get(type_name).ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Type '{}' is not registered for hetero restore",
                    type_name
                ))
            })?;

            let ddl = (registration.create_table_sql)(table_name);
            session.execute(&ddl).await?;
            let migration_plan = (registration.migration_plan)();
            migration_plan
                .ensure_table_schema_version(session, table_name)
                .await?;
        }

        if snapshot.mode == SnapshotMode::WithData {
            let type_version_hints = snapshot
                .types
                .iter()
                .map(|item| (item.type_name.clone(), item.schema_version))
                .collect::<HashMap<_, _>>();

            for mut state in snapshot.states {
                let registration = self.registrations.get(&state.type_name).ok_or_else(|| {
                    DbError::ExecutionError(format!(
                        "Type '{}' is not registered for hetero restore",
                        state.type_name
                    ))
                })?;

                if state.metadata.schema_version == 0 {
                    state.metadata.schema_version = type_version_hints
                        .get(&state.type_name)
                        .copied()
                        .unwrap_or(default_schema_version());
                }

                let migration_plan = (registration.migration_plan)();
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

                let mut entity = (registration.from_state)(&state)?;
                entity.save(session).await?;
                self.items.push(entity);
            }
        }

        Ok(())
    }
}
