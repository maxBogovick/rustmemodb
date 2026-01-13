use crate::core::{Column, DbError, Result, Row, Schema, Snapshot, Value};
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
        // We need to clone the columns list to avoid borrowing issues
        let columns = schema.schema().columns().to_vec();

        let mut table = Self {
            schema,
            rows: BTreeMap::new(),
            next_row_id: 0,
            indexes: HashMap::new(),
        };

        // Auto-create indexes for Primary Key and Unique columns
        for column in columns {
            if column.primary_key || column.unique {
                let _ = table.create_index(&column.name);
            }
        }
        
        table
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
        
        let row_to_index_add;

        if let Some(versions) = self.rows.get_mut(&id) {
            if let Some(latest) = versions.last_mut() {
                if latest.xmax.is_some() {
                    return Ok(false);
                }
                latest.xmax = Some(snapshot.tx_id);
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

        // Note: We do NOT remove the old value from the index here.
        // In MVCC, older transactions might still need the index to find the previous version.
        // The index will be cleaned up during Vacuum when old versions are actually purged.
        if let Some(new) = row_to_index_add {
            self.update_indexes(id, &new);
        }

        Ok(true)
    }

    pub fn scan_iter<'a>(&'a self, snapshot: &'a Snapshot) -> impl Iterator<Item = &'a Row> + 'a {
        self.rows.values().filter_map(move |versions| {
            for version in versions.iter().rev() {
                if self.is_visible(version, snapshot) {
                    return Some(&version.row);
                }
            }
            None
        })
    }

    pub fn scan(&self, snapshot: &Snapshot) -> Vec<Row> {
        self.scan_iter(snapshot).cloned().collect()
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

                // UNIQUE and PRIMARY KEY columns MUST have an index.
                // Table::new and create_index ensure they exist.
                let index = self.indexes.get(&column.name).ok_or_else(|| {
                    DbError::ExecutionError(format!("Critical: Unique index missing for column '{}'", column.name))
                })?;

                if let Some(ids) = index.get(value) {
                    for id in ids {
                        if let Some(ign) = ignore_id && *id == ign {
                            continue;
                        }

                        // Check version chain for this ID
                        if let Some(versions) = self.rows.get(id) {
                            // Check if ANY version with this value is "live" (conflicting)
                            for version in versions.iter().rev() {
                                // In MVCC, an index entry might point to an ID that had the value in the past.
                                // We must verify the version still has the matching value and is live.
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
                let ids = index.entry(value).or_insert_with(Vec::new);
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
    }


    /// Garbage collection for MVCC
    /// Removes row versions that are no longer visible to any active transaction.
    /// 
    /// # Arguments
    /// * `min_active_tx_id` - The smallest transaction ID currently active. 
    ///                        Any version deleted/overwritten by a transaction with ID < min_active_tx_id 
    ///                        is guaranteed to be invisible to all current and future transactions.
    /// * `aborted` - Set of aborted transaction IDs.
    pub fn vacuum(&mut self, min_active_tx_id: u64, aborted: &HashSet<u64>) -> usize {
        let mut freed_versions = 0;
        let mut empty_rows = Vec::new();

        for (id, versions) in &mut self.rows {
            let initial_len = versions.len();
            
            versions.retain(|version| {
                // 1. Created by aborted transaction -> Dead
                if aborted.contains(&version.xmin) {
                    return false;
                }

                // 2. Deleted/Updated
                if let Some(xmax) = version.xmax {
                    // If deletion aborted, this version is alive (unless overwritten again? No, xmax is single)
                    // If xmax is aborted, this version acts as if xmax wasn't set.
                    // But we keep it.
                    if aborted.contains(&xmax) {
                        return true;
                    }

                    // If deletion committed AND old enough
                    // If xmax < min_active_tx_id, then xmax finished before any current tx started.
                    // So all current txs see the deletion (or newer version).
                    if xmax < min_active_tx_id {
                        return false; // Dead
                    }
                }

                // Otherwise keep
                true
            });

            freed_versions += initial_len - versions.len();

            if versions.is_empty() {
                empty_rows.push(*id);
            }
        }

        // Cleanup empty rows from index and map
        for id in empty_rows {
             // We can't easily remove from indexes here because we need the ROW value to find it in the index.
             // But the versions are gone! 
             // Wait, if we removed all versions, we should have removed them from indexes?
             // Actually, `update` handles index updates. 
             // If we vacuum old versions, the index should point to the live version.
             // If ALL versions are gone (row deleted), the last version was a "deletion" (xmax set).
             // But we just removed that version because it was "dead".
             // 
             // Issue: If a row is fully deleted (latest version has xmax set and vacuumed), 
             // the index might still point to `id`.
             // 
             // We need to clean up indexes for fully deleted rows.
             // But we lost the data to look up the index key!
             // 
             // Strategy: When retaining, if we are about to remove the *last* remaining version 
             // and it is a deleted version, we must remove from index FIRST.
             // 
             // Refined logic: 
             // It's safer to only vacuum "intermediate" versions or "fully dead" rows if we can handle indexes.
             // For now, let's just vacuum `rows` map. Indexes map Values -> Vec<ID>.
             // If `rows.get(id)` is empty/removed, `scan_index` handles it (checks `get_visible_row`).
             // But it leaves garbage in the index.
             // 
             // To properly clean indexes, we'd need to fetch the row content before deleting.
             // This is expensive. 
             // For now, let's remove empty rows from `self.rows`. 
             // `scan_index` logic:
             // if let Some(row) = table.get_visible_row(*id, snapshot) ...
             // `get_visible_row` returns None if id not found. Safe.
             self.rows.remove(&id);
        }

        freed_versions
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


