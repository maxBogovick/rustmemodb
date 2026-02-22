impl PersistEntityRuntime {
    /// Rebuilds the projection table for a specific entity type from scratch.
    ///
    /// This iterates over all live entities of the given type (loaded in memory) and
    /// re-upserts them into the projection table.
    fn rebuild_projection_for_entity_type(&mut self, entity_type: &str) -> Result<()> {
        let contract = self
            .projection_registry
            .get(entity_type)
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Projection contract is not registered for entity type '{}'",
                    entity_type
                ))
            })?;

        let states = self
            .hot_entities
            .values()
            .chain(self.cold_entities.values())
            .filter(|entity| entity.state.type_name == entity_type)
            .map(|entity| entity.state.clone())
            .collect::<Vec<_>>();

        self.projection_tables.insert(
            entity_type.to_string(),
            RuntimeProjectionTable::new(contract),
        );
        let table = self.projection_tables.get_mut(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection table is not initialized for entity type '{}'",
                entity_type
            ))
        })?;

        for state in states {
            table.upsert_state(&state)?;
        }
        Ok(())
    }

    /// Updates the projection table for an entity that has been upserted.
    ///
    /// Returns a `RuntimeProjectionUndo` object that can be used to rollback
    /// this change if the journaling fails.
    fn apply_projection_upsert(
        &mut self,
        state: &PersistState,
    ) -> Result<Option<RuntimeProjectionUndo>> {
        let Some(table) = self.projection_tables.get_mut(state.type_name.as_str()) else {
            return Ok(None);
        };

        let previous_row = table.upsert_state(state)?;
        Ok(Some(RuntimeProjectionUndo {
            entity_type: state.type_name.clone(),
            entity_id: state.persist_id.clone(),
            previous_row,
        }))
    }

    /// Removes an entity from the projection table.
    ///
    /// Returns a `RuntimeProjectionUndo` object for rollback.
    fn apply_projection_delete(&mut self, key: &RuntimeEntityKey) -> Option<RuntimeProjectionUndo> {
        let table = self.projection_tables.get_mut(key.entity_type.as_str())?;
        let previous_row = table.remove_entity(key.persist_id.as_str());
        Some(RuntimeProjectionUndo {
            entity_type: key.entity_type.clone(),
            entity_id: key.persist_id.clone(),
            previous_row,
        })
    }

    /// Rolls back a projection change using the undo object.
    ///
    /// This is used during transaction failure recovery to keep in-memory projections
    /// consistent with the journal.
    fn rollback_projection_undo(&mut self, undo: Option<RuntimeProjectionUndo>) {
        let Some(undo) = undo else {
            return;
        };
        if let Some(table) = self.projection_tables.get_mut(undo.entity_type.as_str()) {
            table.restore_entity(undo.entity_id.as_str(), undo.previous_row);
        }
    }

    /// Calculates the number of entities that are out of sync with their projection tables.
    ///
    /// This performs a full scan of memory vs projection tables and counts discrepancies.
    /// Used for SLO metrics.
    fn projection_lag_entities_count(&self) -> usize {
        let mut lag = 0usize;

        for (entity_type, table) in &self.projection_tables {
            let states = self
                .hot_entities
                .values()
                .chain(self.cold_entities.values())
                .filter(|entity| &entity.state.type_name == entity_type)
                .map(|entity| &entity.state)
                .collect::<Vec<_>>();

            let mut expected_ids = HashSet::<&str>::new();
            for state in states {
                expected_ids.insert(state.persist_id.as_str());
                let Some(row) = table.rows.get(state.persist_id.as_str()) else {
                    lag = lag.saturating_add(1);
                    continue;
                };

                match build_projection_row(&table.contract, state) {
                    Ok(expected_row) => {
                        if row.values != expected_row.values
                            || row.updated_at != expected_row.updated_at
                        {
                            lag = lag.saturating_add(1);
                        }
                    }
                    Err(_) => {
                        lag = lag.saturating_add(1);
                    }
                }
            }

            for entity_id in table.rows.keys() {
                if !expected_ids.contains(entity_id.as_str()) {
                    lag = lag.saturating_add(1);
                }
            }
        }

        lag
    }
}
