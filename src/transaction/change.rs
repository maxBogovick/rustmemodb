// ============================================================================
// Transaction Change Tracking
// ============================================================================
//
// Implements the Command Pattern for reversible database operations.
// Each Change represents an operation that can be applied (commit) or
// reversed (rollback).
//
// ============================================================================

use crate::core::{Row, Schema};
use crate::storage::TableSchema;

/// Represents a single reversible change in a transaction
///
/// This implements the Command Pattern, allowing operations to be:
/// - Recorded during transaction execution
/// - Applied during COMMIT
/// - Discarded during ROLLBACK
#[derive(Debug, Clone)]
pub enum Change {
    /// Insert a new row into a table
    InsertRow { table: String, row: Row },

    /// Update an existing row
    UpdateRow {
        table: String,
        row_index: usize,
        old_row: Row,
        new_row: Row,
    },

    /// Delete an existing row
    DeleteRow {
        table: String,
        row_index: usize,
        old_row: Row,
    },

    /// Create a new table
    CreateTable { table_schema: TableSchema },

    /// Drop an existing table
    DropTable {
        name: String,
        schema: Schema,
        rows: Vec<Row>,
    },
}

impl Change {
    /// Get the table name affected by this change
    pub fn table_name(&self) -> &str {
        match self {
            Change::InsertRow { table, .. } => table,
            Change::UpdateRow { table, .. } => table,
            Change::DeleteRow { table, .. } => table,
            Change::CreateTable { table_schema } => table_schema.name(),
            Change::DropTable { name, .. } => name,
        }
    }

    /// Check if this is a DDL (Data Definition Language) change
    pub fn is_ddl(&self) -> bool {
        matches!(self, Change::CreateTable { .. } | Change::DropTable { .. })
    }

    /// Check if this is a DML (Data Manipulation Language) change
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            Change::InsertRow { .. } | Change::UpdateRow { .. } | Change::DeleteRow { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Value;

    #[test]
    fn test_change_table_name() {
        let change = Change::InsertRow {
            table: "users".to_string(),
            row: vec![Value::Integer(1)],
        };
        assert_eq!(change.table_name(), "users");
    }

    #[test]
    fn test_change_classification() {
        let insert = Change::InsertRow {
            table: "users".to_string(),
            row: vec![],
        };
        assert!(insert.is_dml());
        assert!(!insert.is_ddl());

        use crate::storage::TableSchema;
        let create = Change::CreateTable {
            table_schema: TableSchema::new("users", vec![]),
        };
        assert!(create.is_ddl());
        assert!(!create.is_dml());
    }
}
