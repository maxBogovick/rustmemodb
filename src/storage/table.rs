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

#[derive(Clone)]
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
