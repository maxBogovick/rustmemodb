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
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, None, &ctx.params);

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
                    let target_type = &schema.schema().columns()[col_idx].data_type;
                    new_row[col_idx] = target_type.cast_value(&new_value)?;
                }

                // Enforce FK constraints on updated row
                for (i, column) in schema.schema().columns().iter().enumerate() {
                    if let Some(ref fk) = column.references {
                        let val = &new_row[i];
                        if val.is_null() {
                            continue;
                        }
                        if !ctx.storage.table_exists(&fk.table) {
                            return Err(crate::core::DbError::TableNotFound(fk.table.clone()));
                        }
                        let exists = if let Some(rows) = ctx.storage.scan_index(&fk.table, &fk.column, val, &None, &crate::planner::logical_plan::IndexOp::Eq, &ctx.snapshot).await? {
                            !rows.is_empty()
                        } else {
                            let all_rows = ctx.storage.scan_table(&fk.table, &ctx.snapshot).await?;
                            let ref_schema = ctx.storage.get_schema(&fk.table).await?;
                            let col_idx = ref_schema.schema().find_column_index(&fk.column)
                                .ok_or_else(|| crate::core::DbError::ColumnNotFound(fk.column.clone(), fk.table.clone()))?;
                            all_rows.iter().any(|r| &r[col_idx] == val)
                        };
                        if !exists {
                            return Err(crate::core::DbError::ConstraintViolation(format!(
                                "Foreign key violation: Value {} in '{}.{}' references non-existent key in '{}.{}'",
                                val, update.table_name, column.name, fk.table, fk.column
                            )));
                        }
                    }
                }

                // Enforce CHECK constraints (NULL = pass)
                for check in schema.checks() {
                    let value = eval_ctx.evaluate(check, &new_row, schema.schema()).await?;
                    if matches!(value, Value::Boolean(false)) {
                        return Err(crate::core::DbError::ConstraintViolation(format!(
                            "CHECK constraint violation: {}",
                            check
                        )));
                    }
                }

                updates.push((id, row, new_row));
            }
        }

        let updated_count = updates.len();

        // Apply updates to storage (MVCC write)
        let autocommit = ctx.transaction_id.is_none();
        let tx_id = ctx.snapshot.tx_id;
        let mut logged_begin = false;
        let mut logged_any = false;

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
            
            if let Some(persistence) = ctx.persistence {
                if autocommit && !logged_begin {
                    let mut persistence_guard = persistence.lock().await;
                    persistence_guard.log(&WalEntry::BeginTransaction(tx_id))?;
                    logged_begin = true;
                }
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Update {
                    tx_id,
                    table: update.table_name.clone(),
                    row_index: *idx,
                    old_row: old_row.clone(),
                    new_row: new_row.clone(),
                })?;
                logged_any = true;
            }

            let success = ctx.storage.update_row(&update.table_name, *idx, new_row.clone(), &ctx.snapshot).await?;
            if !success {
                if let Some(tx_id) = ctx.transaction_id {
                    ctx.transaction_manager.mark_conflict(tx_id).await;
                } else {
                    return Err(crate::core::DbError::ExecutionError("Write-write conflict detected".into()));
                }
            }
        }

        if autocommit && logged_any {
            if let Some(persistence) = ctx.persistence {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Commit(tx_id))?;
            }
        }

        Ok(QueryResult::updated(updated_count))
    }
}
