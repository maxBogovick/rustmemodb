use super::{Executor, ExecutionContext};
use crate::core::{Result, Value};
use crate::parser::ast::{UpdateStmt, Statement};
use crate::result::QueryResult;
use crate::evaluator::{EvaluationContext, EvaluatorRegistry};
use crate::storage::WalEntry;

use async_trait::async_trait;

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

#[async_trait]
impl Executor for UpdateExecutor {
    fn name(&self) -> &'static str {
        "UPDATE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Update(_))
    }

    async fn execute(&self, stmt: &Statement, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let Statement::Update(update) = stmt else {
            unreachable!();
        };

        self.execute_update(update, ctx).await
    }
}

impl UpdateExecutor {
    async fn execute_update(&self, update: &UpdateStmt, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let schema = ctx.storage.get_schema(&update.table_name).await?;

        // Get visible rows with IDs (MVCC scan)
        let rows = ctx.storage.scan_table_with_ids(&update.table_name, &ctx.snapshot).await?;

        // Create evaluation context
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry, None);

        // Find rows to update and compute new values
        let mut updates = Vec::new();
        for (id, row) in rows {
            let should_update = if let Some(ref condition) = update.selection {
                match eval_ctx.evaluate(condition, &row, schema.schema()).await {
                    Ok(Value::Boolean(b)) => b,
                    Ok(Value::Null) => false,
                    Ok(_) => false,
                    Err(_) => false,
                }
            } else {
                true 
            };

            if should_update {
                let mut new_row = row.clone();

                for assignment in &update.assignments {
                    let col_idx = schema.schema()
                        .columns()
                        .iter()
                        .position(|c| c.name == assignment.column)
                        .ok_or_else(|| crate::core::DbError::ExecutionError(
                            format!("Column '{}' not found", assignment.column)
                        ))?;

                    let new_value = eval_ctx.evaluate(&assignment.value, &row, schema.schema()).await?;
                    new_row[col_idx] = new_value;
                }

                updates.push((id, row, new_row));
            }
        }

        let updated_count = updates.len();

        // Apply updates to storage (MVCC write)
        for (idx, old_row, new_row) in &updates {
            // Check concurrency? If row was updated by another tx since scan?
            // update_row will return false if it can't find visible version.
            // But we scanned visible version. 
            // If another tx updated it and committed, we might overwrite?
            // In Read Committed, we should re-check visibility or lock?
            // Our Table::update checks if latest version is deleted.
            // But it doesn't check if latest version is same as what we read.
            // This is a "Lost Update" anomaly risk in Read Committed if not careful.
            // But for now, we follow standard MVCC: create new version.
            // Table::update handles setting xmax on latest version.
            
            let success = ctx.storage.update_row(&update.table_name, *idx, new_row.clone(), &ctx.snapshot).await?;
            
            if success {
                // Log to WAL
                if let Some(persistence) = ctx.persistence {
                    let mut persistence_guard = persistence.lock().await;
                    persistence_guard.log(&WalEntry::Update {
                        table: update.table_name.clone(),
                        row_index: *idx,
                        old_row: old_row.clone(),
                        new_row: new_row.clone(),
                    })?;
                }
            }
        }

        Ok(QueryResult::updated(updated_count))
    }
}