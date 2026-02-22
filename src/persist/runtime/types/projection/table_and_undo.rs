/// Internal storage for a projection table.
///
/// Maintains a collection of rows and secondary indexes for fast lookup.
struct RuntimeProjectionTable {
    contract: RuntimeProjectionContract,
    rows: HashMap<String, RuntimeProjectionRow>,
    indexes: HashMap<String, HashMap<String, HashSet<String>>>,
}

impl RuntimeProjectionTable {
    fn new(contract: RuntimeProjectionContract) -> Self {
        let mut indexes = HashMap::new();
        for field in &contract.fields {
            if field.indexed {
                indexes.insert(field.column_name.clone(), HashMap::new());
            }
        }

        Self {
            contract,
            rows: HashMap::new(),
            indexes,
        }
    }

    /// Updates or inserts a row based on the entity state.
    ///
    /// Returns the previous row if one existed, useful for undo/rollback.
    fn upsert_state(&mut self, state: &PersistState) -> Result<Option<RuntimeProjectionRow>> {
        let row = build_projection_row(&self.contract, state)?;
        let entity_id = state.persist_id.clone();
        let previous = self.rows.insert(entity_id.clone(), row.clone());
        if let Some(prev) = &previous {
            self.remove_from_indexes(prev);
        }
        self.add_to_indexes(&row);
        Ok(previous)
    }

    /// Removes a row from the table and its indexes.
    ///
    /// Returns the removed row if it existed.
    fn remove_entity(&mut self, entity_id: &str) -> Option<RuntimeProjectionRow> {
        let previous = self.rows.remove(entity_id);
        if let Some(prev) = &previous {
            self.remove_from_indexes(prev);
        }
        previous
    }

    /// Restores a previously removed or updated row (rollback).
    fn restore_entity(&mut self, entity_id: &str, previous: Option<RuntimeProjectionRow>) {
        self.remove_entity(entity_id);
        if let Some(previous) = previous {
            self.add_to_indexes(&previous);
            self.rows.insert(entity_id.to_string(), previous);
        }
    }

    /// Returns all rows sorted by entity ID.
    fn rows_sorted(&self) -> Vec<RuntimeProjectionRow> {
        let mut rows = self.rows.values().cloned().collect::<Vec<_>>();
        rows.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));
        rows
    }

    /// Finds entity IDs matching a specific value in an indexed column.
    fn find_entity_ids_by_index(&self, column: &str, value: &serde_json::Value) -> Vec<String> {
        let key = projection_index_key(value);
        let mut ids = self
            .indexes
            .get(column)
            .and_then(|entries| entries.get(&key))
            .map(|set| set.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        ids.sort();
        ids
    }

    fn add_to_indexes(&mut self, row: &RuntimeProjectionRow) {
        for field in &self.contract.fields {
            if !field.indexed {
                continue;
            }
            let Some(value) = row.values.get(field.column_name.as_str()) else {
                continue;
            };
            let key = projection_index_key(value);
            let bucket = self
                .indexes
                .entry(field.column_name.clone())
                .or_default()
                .entry(key)
                .or_default();
            bucket.insert(row.entity_id.clone());
        }
    }

    fn remove_from_indexes(&mut self, row: &RuntimeProjectionRow) {
        for field in &self.contract.fields {
            if !field.indexed {
                continue;
            }
            let Some(value) = row.values.get(field.column_name.as_str()) else {
                continue;
            };
            let key = projection_index_key(value);
            if let Some(entries) = self.indexes.get_mut(field.column_name.as_str()) {
                if let Some(bucket) = entries.get_mut(&key) {
                    bucket.remove(row.entity_id.as_str());
                    if bucket.is_empty() {
                        entries.remove(&key);
                    }
                }
            }
        }
    }
}

/// Helper struct for projection undo operations (transaction rollback support).
pub(crate) struct RuntimeProjectionUndo {
    pub(crate) entity_type: String,
    pub(crate) entity_id: String,
    pub(crate) previous_row: Option<RuntimeProjectionRow>,
}
