use super::table::TableSchema;
use crate::core::{Result, Row};

/// Storage engine trait - allows pluggable storage backends
pub trait StorageEngine: Send + Sync {
    /// Create a new table with the given schema
    fn create_table(&mut self, schema: TableSchema) -> Result<()>;

    /// Insert a row into a table
    fn insert_row(&mut self, table: &str, row: Row) -> Result<()>;

    /// Scan all rows in a table
    fn scan_table(&self, table: &str) -> Result<Vec<Row>>;

    /// Get the schema for a table
    fn get_schema(&self, table: &str) -> Result<TableSchema>;

    /// Check if a table exists
    fn table_exists(&self, name: &str) -> bool;

    /// List all table names
    fn list_tables(&self) -> Vec<String>;

    /// Get table row count
    fn row_count(&self, table: &str) -> Result<usize>;
}
