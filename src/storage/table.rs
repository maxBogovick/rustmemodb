use crate::core::{Column, DbError, Result, Row, Schema, Snapshot, Value};
use crate::planner::logical_plan::IndexOp;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet};
use im::{OrdMap, HashMap};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexEntry {
    pub row_id: usize,
    pub version_idx: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MvccRow {
    pub row: Row,
    pub xmin: u64,       // Transaction ID that created this row
    pub xmax: Option<u64>, // Transaction ID that deleted/updated this row
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    schema: TableSchema,
    // Using immutable data structures for O(1) cloning/forking
    rows: OrdMap<usize, Vec<MvccRow>>,
    next_row_id: usize,
    indexes: HashMap<String, OrdMap<Value, Vec<IndexEntry>>>,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        // We need to clone the columns list to avoid borrowing issues
        let columns = schema.schema().columns().to_vec();

        let mut table = Self {
            schema,
            rows: OrdMap::new(),
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

    pub fn set_unique(&mut self, column_name: &str) -> Result<()> {
        let col_idx = self.schema.schema.find_column_index(column_name)
            .ok_or_else(|| DbError::ColumnNotFound(column_name.to_string(), self.schema.name.clone()))?;
        self.schema.schema.columns[col_idx].unique = true;
        Ok(())
    }

    pub fn add_column(&mut self, column: Column) -> Result<()> {
        if self.schema.schema.find_column_index(&column.name).is_some() {
            return Err(DbError::ExecutionError(format!("Column '{}' already exists", column.name)));
        }

        // Add column to schema
        self.schema.schema.columns.push(column.clone());

        // Update all existing rows with default value (or NULL)
        // Since we use persistent data structures, we need to be careful about performance.
        // Updating all rows is O(N).
        // For MVP, we iterate and update.

        let default_value = if let Some(ref def) = column.default {
            def.clone()
        } else {
            Value::Null
        };

        // We need to update every row version in every row entry
        // This is heavy.
        let mut updates = Vec::new();
        for (id, versions) in &self.rows {
            let mut new_versions = Vec::with_capacity(versions.len());
            for version in versions {
                let mut new_row = version.row.clone();
                new_row.push(default_value.clone());
                new_versions.push(MvccRow {
                    row: new_row,
                    xmin: version.xmin,
                    xmax: version.xmax,
                });
            }
            updates.push((*id, new_versions));
        }

        for (id, new_versions) in updates {
            self.rows.insert(id, new_versions);
        }

        Ok(())
    }

    pub fn drop_column(&mut self, column_name: &str) -> Result<()> {
        let col_idx = self.schema.schema.find_column_index(column_name)
            .ok_or_else(|| DbError::ColumnNotFound(column_name.to_string(), self.schema.name.clone()))?;

        // Remove from schema
        self.schema.schema.columns.remove(col_idx);

        // Remove index if exists
        if self.indexes.contains_key(column_name) {
            self.indexes.remove(column_name);
            if let Some(pos) = self.schema.indexes.iter().position(|x| x == column_name) {
                self.schema.indexes.remove(pos);
            }
        }

        // Update all rows
        let mut updates = Vec::new();
        for (id, versions) in &self.rows {
            let mut new_versions = Vec::with_capacity(versions.len());
            for version in versions {
                let mut new_row = version.row.clone();
                new_row.remove(col_idx);
                new_versions.push(MvccRow {
                    row: new_row,
                    xmin: version.xmin,
                    xmax: version.xmax,
                });
            }
            updates.push((*id, new_versions));
        }

        for (id, new_versions) in updates {
            self.rows.insert(id, new_versions);
        }

        Ok(())
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let col_idx = self.schema.schema.find_column_index(old_name)
            .ok_or_else(|| DbError::ColumnNotFound(old_name.to_string(), self.schema.name.clone()))?;

        if self.schema.schema.find_column_index(new_name).is_some() {
            return Err(DbError::ExecutionError(format!("Column '{}' already exists", new_name)));
        }

        self.schema.schema.columns[col_idx].name = new_name.to_string();

        // Rename index if exists
        if let Some(index) = self.indexes.remove(old_name) {
            self.indexes.insert(new_name.to_string(), index);
            if let Some(pos) = self.schema.indexes.iter().position(|x| x == old_name) {
                self.schema.indexes[pos] = new_name.to_string();
            }
        }

        Ok(())
    }

    pub fn set_name(&mut self, name: String) {
        self.schema.name = name;
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

        // im::OrdMap::update returns the new map
        self.rows.insert(id, vec![mvcc_row]);
        self.add_index_entry(id, 0, &row);

        Ok(id)
    }

    pub fn delete(&mut self, id: usize, tx_id: u64) -> Result<bool> {
        // Since we need to mutate the vector inside the map, we need to handle this carefully with immutable maps.
        // im::OrdMap::get returns reference.
        // To modify, we effectively need to clone the vector (CoW), modify it, and re-insert.

        if let Some(versions) = self.rows.get(&id) {
             let mut new_versions = versions.clone();
             if let Some(latest) = new_versions.last_mut() {
                if latest.xmax.is_some() {
                    return Ok(false);
                }
                latest.xmax = Some(tx_id);

                // Update the map with the modified versions
                self.rows.insert(id, new_versions);
                return Ok(true);
             }
        }
        Ok(false)
    }

    pub fn update(&mut self, id: usize, new_row: Row, snapshot: &Snapshot) -> Result<bool> {
        self.validate_row(&new_row)?;
        self.check_uniqueness(&new_row, Some(id), snapshot)?;

        let (row_to_index_add, new_version_idx) = if let Some(versions) = self.rows.get(&id) {
            let mut new_versions = versions.clone();

            if let Some(latest) = new_versions.last_mut() {
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
            new_versions.push(new_version);
            let version_idx = new_versions.len() - 1;

            // Update map
            self.rows.insert(id, new_versions);
            (Some(new_row), Some(version_idx))
        } else {
            return Ok(false);
        };

        if let (Some(new), Some(version_idx)) = (row_to_index_add, new_version_idx) {
            self.add_index_entry(id, version_idx, &new);
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

                let index = self.indexes.get(&column.name).ok_or_else(|| {
                    DbError::ExecutionError(format!("Critical: Unique index missing for column '{}'", column.name))
                })?;

                if let Some(entries) = index.get(value) {
                    for entry in entries {
                        if let Some(ign) = ignore_id && entry.row_id == ign {
                            continue;
                        }

                        if let Some(version) = self.get_version(*entry) {
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

    fn is_version_live(&self, row: &MvccRow, snapshot: &Snapshot) -> bool {
        if snapshot.aborted.contains(&row.xmin) {
            return false;
        }

        if let Some(xmax) = row.xmax {
            if snapshot.aborted.contains(&xmax) {
                return true;
            }
            if self.is_committed(xmax, snapshot) {
                return false;
            }
            return true;
        }

        true
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
        let mut index = OrdMap::new();
        for (id, versions) in &self.rows {
            for (version_idx, version) in versions.iter().enumerate() {
                let value = version.row[col_idx].clone();
                // im::OrdMap::entry is not available in the same way, need manual update
                // Or use `update` with a closure?
                // im::OrdMap update: map.update(k, v)
                // For multimap behavior (Vec<usize>), we need to get, clone, modify, insert.
                let mut entries: Vec<IndexEntry> = index.get(&value).cloned().unwrap_or_default();
                entries.push(IndexEntry { row_id: *id, version_idx });
                index.insert(value, entries);
            }
        }
        self.indexes.insert(column_name.to_string(), index);
        if !self.schema.indexes.iter().any(|idx| idx == column_name) {
            self.schema.indexes.push(column_name.to_string());
        }
        Ok(())
    }

    pub fn scan_index_op(
        &self,
        column_name: &str,
        value: &Value,
        end_value: &Option<Value>,
        op: &IndexOp,
        snapshot: &Snapshot
    ) -> Option<Vec<Row>> {
        let index = self.indexes.get(column_name)?;
        
        let entries: Vec<IndexEntry> = match op {
            IndexOp::Eq => index.get(value).cloned().unwrap_or_default(),
            IndexOp::Gt => {
                use std::ops::Bound;
                index.range((Bound::Excluded(value), Bound::Unbounded))
                    .flat_map(|(_, v)| v)
                    .cloned()
                    .collect()
            },
            IndexOp::GtEq => {
                use std::ops::Bound;
                index.range((Bound::Included(value), Bound::Unbounded))
                    .flat_map(|(_, v)| v)
                    .cloned()
                    .collect()
            },
            IndexOp::Lt => {
                 use std::ops::Bound;
                 index.range((Bound::Unbounded, Bound::Excluded(value)))
                    .flat_map(|(_, v)| v)
                    .cloned()
                    .collect()
            },
            IndexOp::LtEq => {
                 use std::ops::Bound;
                 index.range((Bound::Unbounded, Bound::Included(value)))
                    .flat_map(|(_, v)| v)
                    .cloned()
                    .collect()
            },
            IndexOp::Between => {
                if let Some(end) = end_value {
                    use std::ops::Bound;
                    index.range((Bound::Included(value), Bound::Included(end)))
                        .flat_map(|(_, v)| v)
                        .cloned()
                        .collect()
                } else {
                    return None;
                }
            }
        };

        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(version) = self.get_version(entry) {
                if self.is_visible(version, snapshot) {
                    rows.push(version.row.clone());
                }
            }
        }
        Some(rows)
    }

    pub fn get_index(&self, column_name: &str) -> Option<&OrdMap<Value, Vec<IndexEntry>>> {
        self.indexes.get(column_name)
    }

    fn add_index_entry(&mut self, id: usize, version_idx: usize, row: &Row) {
        // We need to iterate over indexes and update them.
        // im::HashMap iteration
        let mut updates = Vec::new();

        for (col_name, index) in &self.indexes {
             if let Some(col_idx) = self.schema.schema().find_column_index(col_name) {
                let value = row[col_idx].clone();
                // Prepare update
                let mut entries: Vec<IndexEntry> = index.get(&value).cloned().unwrap_or_default();
                let entry = IndexEntry { row_id: id, version_idx };
                if !entries.contains(&entry) {
                    entries.push(entry);
                    updates.push((col_name.clone(), value, entries));
                }
             }
        }

        // Apply updates
        for (col_name, value, entries) in updates {
            if let Some(index) = self.indexes.get_mut(&col_name) {
                index.insert(value, entries);
            }
        }
    }

    fn get_version(&self, entry: IndexEntry) -> Option<&MvccRow> {
        self.rows
            .get(&entry.row_id)
            .and_then(|versions| versions.get(entry.version_idx))
    }

    fn rebuild_indexes(&mut self) {
        let index_columns = self.schema.indexes.clone();
        self.indexes.clear();
        for column in index_columns {
            let _ = self.create_index(&column);
        }
    }

    pub fn vacuum(&mut self, min_active_tx_id: u64, aborted: &HashSet<u64>) -> usize {
        let mut freed_versions = 0;
        let _empty_rows: Vec<usize> = Vec::new();

        // With im::OrdMap, we can't mutate in place during iteration easily if we want to be efficient.
        // We can collect updates.
        let mut updates = Vec::new();

        for (id, versions) in &self.rows {
            let initial_len = versions.len();

            // Filter versions
            let new_versions: Vec<MvccRow> = versions.iter().filter(|version| {
                if aborted.contains(&version.xmin) {
                    return false;
                }
                if let Some(xmax) = version.xmax {
                    if aborted.contains(&xmax) {
                        return true;
                    }
                    if xmax < min_active_tx_id {
                        return false;
                    }
                }
                true
            }).cloned().collect();

            if new_versions.len() != initial_len {
                freed_versions += initial_len - new_versions.len();
                updates.push((*id, new_versions));
            }
        }

        // Apply updates
        for (id, new_versions) in updates {
            if new_versions.is_empty() {
                self.rows.remove(&id);
                // Also need to clean indexes... (skipped for now as per previous logic)
            } else {
                self.rows.insert(id, new_versions);
            }
        }

        if freed_versions > 0 {
            self.rebuild_indexes();
        }

        freed_versions
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub schema: Schema,
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
