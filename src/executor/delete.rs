use super::{Executor, ExecutionContext};
use crate::core::{Result, Value};
use crate::parser::ast::{DeleteStmt, Statement};
use crate::result::QueryResult;
use crate::evaluator::{EvaluationContext, EvaluatorRegistry};

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
        let rows = {
            let table = table_handle
                .read()
                .map_err(|_| crate::core::DbError::ExecutionError("Table lock poisoned".into()))?;
            table.rows().to_vec()
        };

        // Create evaluation context
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // Find indices of rows to delete
        let mut indices_to_delete = Vec::new();
        for (idx, row) in rows.iter().enumerate() {
            let should_delete = if let Some(ref condition) = delete.selection {
                match eval_ctx.evaluate(condition, row, schema.schema()) {
                    Ok(Value::Boolean(b)) => b,
                    Ok(Value::Null) => false,
                    Ok(_) => false,
                    Err(_) => false,
                }
            } else {
                true // Delete all rows if no condition
            };

            if should_delete {
                indices_to_delete.push(idx);
            }
        }

        // Delete rows
        let deleted_count = {
            let mut table = table_handle
                .write()
                .map_err(|_| crate::core::DbError::ExecutionError("Table lock poisoned".into()))?;
            table.delete_rows(indices_to_delete)?
        };

        Ok(QueryResult::deleted(deleted_count))
    }
}
