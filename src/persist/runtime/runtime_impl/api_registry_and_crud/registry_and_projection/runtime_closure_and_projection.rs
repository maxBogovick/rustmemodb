impl PersistEntityRuntime {
    /// Registers a runtime closure handler for an entity type.
    pub fn register_runtime_closure(
        &mut self,
        entity_type: impl Into<String>,
        function: impl Into<String>,
        handler: RuntimeClosureHandler,
    ) {
        let entry = self
            .runtime_closure_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(function.into(), handler);
    }

    /// Registers a projection contract, enabling automated projection view maintenance.
    pub fn register_projection_contract(
        &mut self,
        contract: RuntimeProjectionContract,
    ) -> Result<()> {
        contract.validate()?;
        let entity_type = contract.entity_type.clone();
        self.projection_registry
            .insert(entity_type.clone(), contract.clone());
        self.projection_tables
            .insert(entity_type.clone(), RuntimeProjectionTable::new(contract));
        self.rebuild_projection_for_entity_type(&entity_type)
    }

    /// Returns the registered projection contract for an entity type, if any.
    pub fn projection_contract(&self, entity_type: &str) -> Option<&RuntimeProjectionContract> {
        self.projection_registry.get(entity_type)
    }

    /// Lists all rows currently in the projection table for an entity type.
    pub fn list_projection_rows(&self, entity_type: &str) -> Result<Vec<RuntimeProjectionRow>> {
        let table = self.projection_tables.get(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection contract is not registered for entity type '{}'",
                entity_type
            ))
        })?;
        Ok(table.rows_sorted())
    }

    /// Finds entity IDs in a projection table by a specific index value.
    pub fn find_projection_entity_ids_by_index(
        &self,
        entity_type: &str,
        column: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<String>> {
        let contract = self.projection_registry.get(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection contract is not registered for entity type '{}'",
                entity_type
            ))
        })?;

        let indexed = contract
            .fields
            .iter()
            .any(|field| field.column_name == column && field.indexed);
        if !indexed {
            return Err(DbError::ExecutionError(format!(
                "Projection column '{}.{}' is not indexed",
                entity_type, column
            )));
        }

        let table = self.projection_tables.get(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection table is not initialized for entity type '{}'",
                entity_type
            ))
        })?;

        Ok(table.find_entity_ids_by_index(column, value))
    }

    /// Finds full projection rows by a specific index value.
    pub fn find_projection_rows_by_index(
        &self,
        entity_type: &str,
        column: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<RuntimeProjectionRow>> {
        let table = self.projection_tables.get(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection table is not initialized for entity type '{}'",
                entity_type
            ))
        })?;

        let ids = self.find_projection_entity_ids_by_index(entity_type, column, value)?;
        let mut rows = ids
            .into_iter()
            .filter_map(|entity_id| table.rows.get(&entity_id).cloned())
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));
        Ok(rows)
    }

    /// Rebuilds all registered projections from the current entity states.
    pub fn rebuild_registered_projections(&mut self) -> Result<()> {
        let entity_types = self.projection_registry.keys().cloned().collect::<Vec<_>>();
        for entity_type in entity_types {
            self.rebuild_projection_for_entity_type(&entity_type)?;
        }
        Ok(())
    }
}
