use crate::core::{Column, DbError, Result, Row, Schema, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvccRow {
    pub row: Row,
    pub xmin: u64,       // Transaction ID that created this row
    pub xmax: Option<u64>, // Transaction ID that deleted/updated this row
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    schema: TableSchema,
    rows: BTreeMap<usize, Vec<MvccRow>>,
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

    pub fn insert(&mut self, row: Row, snapshot: &Snapshot) -> Result<usize> {
        self.validate_row(&row)?;
        self.check_uniqueness(&row, None, snapshot)?;

        let id = self.next_row_id;
        self.next_row_id += 1;

        let mvcc_row = MvccRow {
            row: row.clone(),
            xmin: snapshot.tx_id,
            xmax: None,
        };

        self.rows.insert(id, vec![mvcc_row]);
        self.update_indexes(id, &row);
        
        Ok(id)
    }

    pub fn delete(&mut self, id: usize, tx_id: u64) -> Result<bool> {
        if let Some(versions) = self.rows.get_mut(&id)
            && let Some(latest) = versions.last_mut() {
                if latest.xmax.is_some() {
                    return Ok(false);
                }
                latest.xmax = Some(tx_id);
                return Ok(true);
            }
        Ok(false)
    }

    pub fn update(&mut self, id: usize, new_row: Row, snapshot: &Snapshot) -> Result<bool> {
        self.validate_row(&new_row)?;
        self.check_uniqueness(&new_row, Some(id), snapshot)?;
        
        let row_to_index_delete;
        let row_to_index_add;

        if let Some(versions) = self.rows.get_mut(&id) {
            if let Some(latest) = versions.last_mut() {
                if latest.xmax.is_some() {
                    return Ok(false);
                }
                latest.xmax = Some(snapshot.tx_id);
                row_to_index_delete = Some(latest.row.clone());
            } else {
                return Ok(false);
            }
            
            let new_version = MvccRow {
                row: new_row.clone(),
                xmin: snapshot.tx_id,
                xmax: None,
            };
            versions.push(new_version);
            row_to_index_add = Some(new_row);
        } else {
            return Ok(false);
        }

        if let Some(old) = row_to_index_delete {
            self.remove_from_indexes(id, &old);
        }
        if let Some(new) = row_to_index_add {
            self.update_indexes(id, &new);
        }

        Ok(true)
    }

    pub fn scan(&self, snapshot: &Snapshot) -> Vec<Row> {
        let mut results = Vec::new();
        for versions in self.rows.values() {
            for version in versions.iter().rev() {
                if self.is_visible(version, snapshot) {
                    results.push(version.row.clone());
                    break;
                }
            }
        }
        results
    }

    pub fn scan_with_ids(&self, snapshot: &Snapshot) -> Vec<(usize, Row)> {
        let mut results = Vec::new();
        for (id, versions) in &self.rows {
            for version in versions.iter().rev() {
                if self.is_visible(version, snapshot) {
                    results.push((*id, version.row.clone()));
                    break;
                }
            }
        }
        results
    }

    pub fn get_visible_row(&self, id: usize, snapshot: &Snapshot) -> Option<Row> {
        if let Some(versions) = self.rows.get(&id) {
            for version in versions.iter().rev() {
                if self.is_visible(version, snapshot) {
                    return Some(version.row.clone());
                }
            }
        }
        None
    }

    fn check_uniqueness(&self, row: &Row, ignore_id: Option<usize>, snapshot: &Snapshot) -> Result<()> {
        for (col_idx, column) in self.schema.schema().columns().iter().enumerate() {
            if column.primary_key || column.unique {
                let value = &row[col_idx];
                if matches!(value, Value::Null) {
                    continue; 
                }

                // Check using index or scan
                // We need to check if ANY row (ignoring visibility to current tx, but respecting aborts/commits)
                // has this value and is "live".
                
                // Logic: A row is "live" if xmin is NOT aborted, and xmax is either None or NOT committed (or active).
                // Wait, if xmax is active (uncommitted delete), it's still live for uniqueness purposes (pessimistic).
                // So conflict if: !is_aborted(xmin) && (xmax.is_none() || !is_committed(xmax))
                // Note: is_committed checks if tx is committed.
                
                if let Some(index) = self.indexes.get(&column.name) {
                    if let Some(ids) = index.get(value) {
                        for id in ids {
                            if let Some(ign) = ignore_id
                                && *id == ign { continue; }
                            
                            // Check version chain for this ID
                            if let Some(versions) = self.rows.get(id) {
                                // Iterate newest to oldest
                                for version in versions.iter().rev() {
                                    if self.is_version_live(version, snapshot) {
                                        return Err(DbError::ConstraintViolation(format!(
                                            "Unique constraint violation: Column '{}' already contains value {}",
                                            column.name, value
                                        )));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Full Scan
                    for (id, versions) in &self.rows {
                        if let Some(ign) = ignore_id
                            && *id == ign { continue; }
                        
                        // Check if any version matches value AND is live
                        for version in versions.iter().rev() {
                            if &version.row[col_idx] == value && self.is_version_live(version, snapshot) {
                                return Err(DbError::ConstraintViolation(format!(
                                    "Unique constraint violation: Column '{}' already contains value {}",
                                    column.name, value
                                )));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // Check if version is "live" (valid candidate for conflict)
    fn is_version_live(&self, row: &MvccRow, snapshot: &Snapshot) -> bool {
        // xmin must not be aborted
        if snapshot.aborted.contains(&row.xmin) {
            return false;
        }
        
        // If xmax is set
        if let Some(xmax) = row.xmax {
            // If xmax is aborted, then row is still live (delete failed)
            if snapshot.aborted.contains(&xmax) {
                return true;
            }
            // If xmax is active or committed, row is "being deleted" or "deleted".
            // If committed, row is gone -> not live.
            // If active, row is locked/being deleted -> treat as live (conflict) to prevent concurrent insert.
            // Wait, is_committed returns true for committed.
            if self.is_committed(xmax, snapshot) {
                return false; // Deleted permanently
            }
            // Active deletion -> conflict
            return true;
        }

        true // No xmax -> live
    }

    fn is_visible(&self, row: &MvccRow, snapshot: &Snapshot) -> bool {
        if row.xmin == snapshot.tx_id {
            if let Some(xmax) = row.xmax
                && xmax == snapshot.tx_id {
                    return false;
                }
            return true;
        }

        if !self.is_committed(row.xmin, snapshot) {
            return false;
        }

        if let Some(xmax) = row.xmax {
            if xmax == snapshot.tx_id {
                return false;
            }
            if self.is_committed(xmax, snapshot) {
                return false;
            }
        }

        true
    }

    fn is_committed(&self, tx_id: u64, snapshot: &Snapshot) -> bool {
        if tx_id >= snapshot.max_tx_id {
            return false;
        }
        if snapshot.active.contains(&tx_id) {
            return false;
        }
        if snapshot.aborted.contains(&tx_id) {
            return false;
        }
        true
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

    pub fn create_index(&mut self, column_name: &str) -> Result<()> {
        if self.indexes.contains_key(column_name) {
            return Ok(());
        }
        let col_idx = self.schema.schema().find_column_index(column_name)
            .ok_or_else(|| DbError::ColumnNotFound(column_name.to_string(), self.schema.name.clone()))?;
        let mut index = BTreeMap::new();
        for (id, versions) in &self.rows {
            for version in versions {
                let value = version.row[col_idx].clone();
                index.entry(value).or_insert_with(Vec::new).push(*id);
            }
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
    pub fn name(&self) -> &str { &self.name }
    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn is_indexed(&self, column: &str) -> bool {
        self.indexes.iter().any(|idx| idx == column)
    }
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub tx_id: u64,
    pub active: HashSet<u64>,
    pub aborted: HashSet<u64>,
    pub max_tx_id: u64,
}
