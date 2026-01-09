use super::{Executor, ExecutionContext};
use crate::core::{Result, Value};
use crate::parser::ast::{DeleteStmt, Statement};
use crate::result::QueryResult;
use crate::evaluator::{EvaluationContext, EvaluatorRegistry};
use crate::storage::WalEntry;

use async_trait::async_trait;

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

#[async_trait]
impl Executor for DeleteExecutor {
    fn name(&self) -> &'static str {
        "DELETE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Delete(_))
    }

    async fn execute(&self, stmt: &Statement, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let Statement::Delete(delete) = stmt else {
            unreachable!();
        };

        self.execute_delete(delete, ctx).await
    }
}

impl DeleteExecutor {
    async fn execute_delete(&self, delete: &DeleteStmt, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let schema = ctx.storage.get_schema(&delete.table_name).await?;

        // Get visible rows with IDs (MVCC scan)
        let rows = ctx.storage.scan_table_with_ids(&delete.table_name, &ctx.snapshot).await?;

        // Create evaluation context
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // Find rows to delete
        let mut rows_to_delete = Vec::new();
        for (id, row) in rows {
            let should_delete = if let Some(ref condition) = delete.selection {
                match eval_ctx.evaluate(condition, &row, schema.schema()).await {
                    Ok(Value::Boolean(b)) => b,
                    Ok(Value::Null) => false,
                    Ok(_) => false,
                    Err(_) => false,
                }
            } else {
                true 
            };

            if should_delete {
                rows_to_delete.push((id, row));
            }
        }

        let deleted_count = rows_to_delete.len();

        // Delete rows from storage (MVCC delete)
        let mut deleted_indices = Vec::new();
        let mut deleted_rows_data = Vec::new();

        for (idx, row) in &rows_to_delete {
            let success = ctx.storage.delete_row(&delete.table_name, *idx, ctx.snapshot.tx_id).await?;
            if success {
                deleted_indices.push(*idx);
                deleted_rows_data.push(row.clone());
            }
        }

        // Log to WAL if persistence is enabled
        if !deleted_indices.is_empty()
            && let Some(persistence) = ctx.persistence {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Delete {
                    table: delete.table_name.clone(),
                    row_indices: deleted_indices,
                    deleted_rows: deleted_rows_data,
                })?;
            }

        Ok(QueryResult::deleted(deleted_count))
    }
}