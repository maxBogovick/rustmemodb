use super::{Executor, ExecutionContext};
use crate::core::{Result, Value};
use crate::parser::ast::{DeleteStmt, Statement};
use crate::result::QueryResult;
use crate::evaluator::{EvaluationContext, EvaluatorRegistry};
use crate::storage::WalEntry;
use crate::transaction::Change;

pub struct DeleteExecutor {
    evaluator_registry: EvaluatorRegistry,
}

impl DeleteExecutor {
    pub fn new() -> Self {
        Self {
            evaluator_registry: EvaluatorRegistry::with_default_evaluators(),
        }
    }
}

impl Executor for DeleteExecutor {
    fn name(&self) -> &'static str {
        "DELETE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Delete(_))
    }

    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        let Statement::Delete(delete) = stmt else {
            unreachable!();
        };

        self.execute_delete(delete, ctx)
    }
}

impl DeleteExecutor {
    fn execute_delete(&self, delete: &DeleteStmt, ctx: &ExecutionContext) -> Result<QueryResult> {
        let table_handle = ctx.storage.get_table(&delete.table_name)?;
        let schema = ctx.storage.get_schema(&delete.table_name)?;

        // Get all rows
        let rows: Vec<(usize, crate::core::Row)> = {
            let table = table_handle
                .read()
                .map_err(|_| crate::core::DbError::ExecutionError("Table lock poisoned".into()))?;
            table.rows_iter().map(|(id, row)| (*id, row.clone())).collect()
        };

        // Create evaluation context
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // Find rows to delete (id and old row data)
        let mut rows_to_delete = Vec::new();
        for (id, row) in rows {
            let should_delete = if let Some(ref condition) = delete.selection {
                match eval_ctx.evaluate(condition, &row, schema.schema()) {
                    Ok(Value::Boolean(b)) => b,
                    Ok(Value::Null) => false,
                    Ok(_) => false,
                    Err(_) => false,
                }
            } else {
                true // Delete all rows if no condition
            };

            if should_delete {
                rows_to_delete.push((id, row));
            }
        }

        let deleted_count = rows_to_delete.len();

        // Delete rows from storage (both in transaction and auto-commit mode)
        {
            let indices_to_delete: Vec<usize> = rows_to_delete.iter().map(|(idx, _)| *idx).collect();
            let deleted_rows: Vec<_> = rows_to_delete.iter().map(|(_, row)| row.clone()).collect();

            let mut table = table_handle
                .write()
                .map_err(|_| crate::core::DbError::ExecutionError("Table lock poisoned".into()))?;
            table.delete_rows(indices_to_delete.clone())?;

            // Log to WAL if persistence is enabled
            if let Some(ref persistence) = ctx.persistence {
                let mut persistence_guard = persistence.lock()
                    .map_err(|e| crate::core::DbError::ExecutionError(format!("Persistence lock poisoned: {}", e)))?;
                persistence_guard.log(&WalEntry::Delete {
                    table: delete.table_name.clone(),
                    row_indices: indices_to_delete,
                    deleted_rows,
                })?;
            }

            // If in transaction, record changes for potential rollback
            if let Some(txn_id) = ctx.transaction_id {
                for (row_index, old_row) in rows_to_delete {
                    let change = Change::DeleteRow {
                        table: delete.table_name.clone(),
                        row_index,
                        old_row,
                    };
                    ctx.transaction_manager.record_change(txn_id, change)?;
                }
            }
        }

        Ok(QueryResult::deleted(deleted_count))
    }
}
