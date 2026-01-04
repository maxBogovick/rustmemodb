use crate::core::{Column, DbError, Result, Row, Schema, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    schema: TableSchema,
    rows: BTreeMap<usize, Row>,
    next_row_id: usize,
    indexes: HashMap<String, BTreeMap<Value, Vec<usize>>>,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            rows: BTreeMap::new(),
            next_row_id: 0,
            indexes: HashMap::new(),
        }
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn rows_iter(&self) -> impl Iterator<Item = (&usize, &Row)> {
        self.rows.iter()
    }

    pub fn get_row(&self, id: usize) -> Option<&Row> {
        self.rows.get(&id)
    }

    pub fn insert(&mut self, row: Row) -> Result<usize> {
        self.validate_row(&row)?;
        let id = self.next_row_id;
        self.next_row_id += 1;

        self.rows.insert(id, row.clone());
        self.update_indexes(id, &row);
        
        Ok(id)
    }

    /// Insert a row with a specific ID (used for Rollback or restore)
    pub fn insert_at_id(&mut self, id: usize, row: Row) -> Result<()> {
        self.validate_row(&row)?;
        
        if id >= self.next_row_id {
            self.next_row_id = id + 1;
        }

        self.rows.insert(id, row.clone());
        self.update_indexes(id, &row);
        Ok(())
    }

    /// Delete rows by IDs
    pub fn delete_rows(&mut self, ids: Vec<usize>) -> Result<usize> {
        let mut count = 0;
        for id in ids {
            if let Some(row) = self.rows.remove(&id) {
                self.remove_from_indexes(id, &row);
                count += 1;
            }
        }
        Ok(count)
    }

    /// Update a specific row by ID
    pub fn update_row(&mut self, id: usize, new_row: Row) -> Result<()> {
        if let Some(old_row) = self.rows.get(&id) {
            self.validate_row(&new_row)?;
            
            // Remove old index entries
            // We need to clone old_row because remove_from_indexes borrows indexes
            let old_row_clone = old_row.clone();
            self.remove_from_indexes(id, &old_row_clone);

            // Update row
            self.rows.insert(id, new_row.clone());

            // Add new index entries
            self.update_indexes(id, &new_row);
            
            Ok(())
        } else {
             Err(DbError::ExecutionError(format!(
                "Row id {} not found",
                id
            )))
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn validate_row(&self, row: &Row) -> Result<()> {
        let columns = self.schema.schema().columns();

        if row.len() != columns.len() {
            return Err(DbError::ExecutionError(format!(
                "Expected {} columns, got {}",
                columns.len(),
                row.len()
            )));
        }

        for (column, value) in columns.iter().zip(row.iter()) {
            column.validate(value)?;
        }

        Ok(())
    }

    // ========================================================================
    // Index Management
    // ========================================================================

    pub fn create_index(&mut self, column_name: &str) -> Result<()> {
        if self.indexes.contains_key(column_name) {
            return Ok(()); // Index already exists
        }

        let col_idx = self.schema.schema().find_column_index(column_name)
            .ok_or_else(|| DbError::ColumnNotFound(column_name.to_string(), self.schema.name.clone()))?;

        let mut index = BTreeMap::new();

        // Build index from existing rows
        for (id, row) in &self.rows {
            let value = row[col_idx].clone();
            index.entry(value).or_insert_with(Vec::new).push(*id);
        }

        self.indexes.insert(column_name.to_string(), index);
        self.schema.indexes.push(column_name.to_string());
        Ok(())
    }

    pub fn get_index(&self, column_name: &str) -> Option<&BTreeMap<Value, Vec<usize>>> {
        self.indexes.get(column_name)
    }

    fn update_indexes(&mut self, id: usize, row: &Row) {
        for (col_name, index) in &mut self.indexes {
            if let Some(col_idx) = self.schema.schema().find_column_index(col_name) {
                let value = row[col_idx].clone();
                index.entry(value).or_insert_with(Vec::new).push(id);
            }
        }
    }

    fn remove_from_indexes(&mut self, id: usize, row: &Row) {
        for (col_name, index) in &mut self.indexes {
            if let Some(col_idx) = self.schema.schema().find_column_index(col_name) {
                let value = &row[col_idx];
                if let Some(ids) = index.get_mut(value) {
                    ids.retain(|&x| x != id);
                    if ids.is_empty() {
                         // Optional: clean up empty keys
                         // But we can't remove while iterating mutable
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    name: String,
    schema: Schema,
    pub indexes: Vec<String>,
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            schema: Schema::new(columns),
            indexes: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    
    pub fn is_indexed(&self, column: &str) -> bool {
        self.indexes.iter().any(|idx| idx == column)
    }
}
