use super::{Executor, ExecutionContext};
use crate::core::{Result, Value, DbError};
use crate::parser::ast::{DeleteStmt, Statement};
use crate::planner::logical_plan::IndexOp;
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
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, None, &ctx.params);

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

        // Validate Foreign Keys (RESTRICT)
        // Ensure no other table references the rows we are about to delete
        let all_tables = ctx.storage.list_tables();
        
        for (_del_idx, del_row) in &rows_to_delete {
             for table_name in &all_tables {
                 // Skip self-reference check if we are deleting from the same table (unless we support self-ref constraints, which we should)
                 // But strictly speaking, if a row references itself, deleting it deletes the reference too, so it's fine?
                 // SQL standard says check constraints.
                 
                 let table_schema = ctx.storage.get_schema(table_name).await?;
                 for (col_idx, column) in table_schema.schema().columns().iter().enumerate() {
                     if let Some(ref fk) = column.references {
                         if fk.table == delete.table_name {
                             // This column references the table we are deleting from.
                             // We assume FK references match by name or PK.
                             // Currently, our FK struct maps `fk.column` (in child) -> `parent_table`.
                             // Wait, FK definition in Column is: `references: Option<ForeignKey>`.
                             // `ForeignKey { table: "parent", column: "parent_col" }`.
                             // So `column` (in child) references `fk.table` (parent) column `fk.column` (parent_col).
                             
                             let parent_col_idx = schema.schema().find_column_index(&fk.column)
                                 .ok_or_else(|| DbError::ColumnNotFound(fk.column.clone(), delete.table_name.clone()))?;
                             
                             let parent_val = &del_row[parent_col_idx];
                             
                             // If parent value is NULL, it can't be referenced? PKs are not NULL.
                             if parent_val.is_null() {
                                 continue;
                             }

                             // Check if child table has this value in `column.name`
                             // Use index if available
                             let exists = if let Some(rows) = ctx.storage.scan_index(table_name, &column.name, parent_val, &None, &IndexOp::Eq, &ctx.snapshot).await? {
                                 // Check if we found rows that are NOT the ones being deleted (if self-referencing)
                                 // If self-referencing, we need to check if the referencing row is also in `rows_to_delete`.
                                 // For now, let's implement strict RESTRICT which fails if ANY reference exists.
                                 // Refinement: If table_name == delete.table_name, we must check if the found row is `del_idx`.
                                 // But `scan_index` returns values, not IDs. `scan_index` implementation returns `Vec<Row>`.
                                 
                                 !rows.is_empty()
                             } else {
                                 // Fallback scan
                                 let child_rows = ctx.storage.scan_table(table_name, &ctx.snapshot).await?;
                                 child_rows.iter().any(|r| &r[col_idx] == parent_val)
                             };

                             if exists {
                                 // If self-referencing, we might be deleting the child row too.
                                 // Doing a strict check for now: if ANY row exists in child table with this value, block delete.
                                 // This is slightly incorrect for cascading deletes or self-ref batch deletes, but safe for RESTRICT.
                                 
                                 return Err(DbError::ConstraintViolation(format!(
                                     "Delete on table '{}' violates foreign key constraint on table '{}' (column '{}')",
                                     delete.table_name, table_name, column.name
                                 )));
                             }
                         }
                     }
                 }
             }
        }

        let deleted_count = rows_to_delete.len();

        let mut deleted_indices = Vec::new();
        let mut deleted_rows_data = Vec::new();
        for (idx, row) in &rows_to_delete {
            deleted_indices.push(*idx);
            deleted_rows_data.push(row.clone());
        }

        let autocommit = ctx.transaction_id.is_none();
        let tx_id = ctx.snapshot.tx_id;

        if !deleted_indices.is_empty()
            && let Some(persistence) = ctx.persistence {
                if autocommit {
                    let mut persistence_guard = persistence.lock().await;
                    persistence_guard.log(&WalEntry::BeginTransaction(tx_id))?;
                }
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Delete {
                    tx_id,
                    table: delete.table_name.clone(),
                    row_indices: deleted_indices.clone(),
                    deleted_rows: deleted_rows_data.clone(),
                })?;
            }

        // Delete rows from storage (MVCC delete)
        for idx in &deleted_indices {
            let _success = ctx.storage.delete_row(&delete.table_name, *idx, ctx.snapshot.tx_id).await?;
        }

        if autocommit
            && !deleted_indices.is_empty()
            && let Some(persistence) = ctx.persistence {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Commit(tx_id))?;
            }

        Ok(QueryResult::deleted(deleted_count))
    }
}
