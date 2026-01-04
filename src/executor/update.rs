use super::{Executor, ExecutionContext};
use crate::core::{Result, Value};
use crate::parser::ast::{UpdateStmt, Statement};
use crate::result::QueryResult;
use crate::evaluator::{EvaluationContext, EvaluatorRegistry};
use crate::storage::WalEntry;
use crate::transaction::Change;

pub struct UpdateExecutor {
    evaluator_registry: EvaluatorRegistry,
}

impl UpdateExecutor {
    pub fn new() -> Self {
        Self {
            evaluator_registry: EvaluatorRegistry::with_default_evaluators(),
        }
    }
}

impl Executor for UpdateExecutor {
    fn name(&self) -> &'static str {
        "UPDATE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Update(_))
    }

    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        let Statement::Update(update) = stmt else {
            unreachable!();
        };

        self.execute_update(update, ctx)
    }
}

impl UpdateExecutor {
    fn execute_update(&self, update: &UpdateStmt, ctx: &ExecutionContext) -> Result<QueryResult> {
        let table_handle = ctx.storage.get_table(&update.table_name)?;
        let schema = ctx.storage.get_schema(&update.table_name)?;

        // Get all rows
        let rows: Vec<(usize, crate::core::Row)> = {
            let table = table_handle
                .read()
                .map_err(|_| crate::core::DbError::ExecutionError("Table lock poisoned".into()))?;
            table.rows_iter().map(|(id, row)| (*id, row.clone())).collect()
        };

        // Create evaluation context
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // Find rows to update and compute new values
        let mut updates = Vec::new();
        for (id, row) in rows {
            let should_update = if let Some(ref condition) = update.selection {
                match eval_ctx.evaluate(condition, &row, schema.schema()) {
                    Ok(Value::Boolean(b)) => b,
                    Ok(Value::Null) => false,
                    Ok(_) => false,
                    Err(_) => false,
                }
            } else {
                true // Update all rows if no condition
            };

            if should_update {
                // Create updated row
                let mut new_row = row.clone();

                for assignment in &update.assignments {
                    // Find column index
                    let col_idx = schema.schema()
                        .columns()
                        .iter()
                        .position(|c| c.name == assignment.column)
                        .ok_or_else(|| crate::core::DbError::ExecutionError(
                            format!("Column '{}' not found", assignment.column)
                        ))?;

                    // Evaluate new value
                    let new_value = eval_ctx.evaluate(&assignment.value, &row, schema.schema())?;
                    new_row[col_idx] = new_value;
                }

                updates.push((id, row, new_row));
            }
        }

        let updated_count = updates.len();

        // Apply updates to storage (both in transaction and auto-commit mode)
        {
            let mut table = table_handle
                .write()
                .map_err(|_| crate::core::DbError::ExecutionError("Table lock poisoned".into()))?;

            for (idx, old_row, new_row) in &updates {
                table.update_row(*idx, new_row.clone())?;

                // Log to WAL if persistence is enabled
                if let Some(ref persistence) = ctx.persistence {
                    let mut persistence_guard = persistence.lock()
                        .map_err(|e| crate::core::DbError::ExecutionError(format!("Persistence lock poisoned: {}", e)))?;
                    persistence_guard.log(&WalEntry::Update {
                        table: update.table_name.clone(),
                        row_index: *idx,
                        old_row: old_row.clone(),
                        new_row: new_row.clone(),
                    })?;
                }

                // If in transaction, record change for potential rollback
                if let Some(txn_id) = ctx.transaction_id {
                    let change = Change::UpdateRow {
                        table: update.table_name.clone(),
                        row_index: *idx,
                        old_row: old_row.clone(),
                        new_row: new_row.clone(),
                    };
                    ctx.transaction_manager.record_change(txn_id, change)?;
                }
            }
        }

        Ok(QueryResult::updated(updated_count))
    }
}
