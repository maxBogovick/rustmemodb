use crate::core::{Column, DbError, Result, Row, Schema};

pub struct Table {
    schema: TableSchema,
    rows: Vec<Row>,
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            rows: Vec::new(),
        }
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn insert(&mut self, row: Row) -> Result<()> {
        self.validate_row(&row)?;
        self.rows.push(row);
        Ok(())
    }

    /// Delete rows by indices (in reverse order to maintain validity)
    pub fn delete_rows(&mut self, mut indices: Vec<usize>) -> Result<usize> {
        // Sort in reverse order to maintain index validity during deletion
        indices.sort_by(|a, b| b.cmp(a));
        indices.dedup();

        let count = indices.len();
        for idx in indices {
            if idx >= self.rows.len() {
                return Err(DbError::ExecutionError(format!(
                    "Row index {} out of bounds",
                    idx
                )));
            }
            self.rows.remove(idx);
        }

        Ok(count)
    }

    /// Update a specific row
    pub fn update_row(&mut self, index: usize, row: Row) -> Result<()> {
        if index >= self.rows.len() {
            return Err(DbError::ExecutionError(format!(
                "Row index {} out of bounds",
                index
            )));
        }

        self.validate_row(&row)?;
        self.rows[index] = row;
        Ok(())
    }

    /// Insert a row at a specific index (for transaction rollback)
    pub fn insert_row_at_index(&mut self, index: usize, row: Row) -> Result<()> {
        if index > self.rows.len() {
            return Err(DbError::ExecutionError(format!(
                "Cannot insert at index {} (length is {})",
                index, self.rows.len()
            )));
        }

        self.validate_row(&row)?;
        self.rows.insert(index, row);
        Ok(())
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
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    name: String,
    schema: Schema,
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            schema: Schema::new(columns),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}
