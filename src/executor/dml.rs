use super::{ExecutionContext, Executor};
use crate::core::{Column, DbError, Result, Row, Value};
use crate::parser::ast::{Expr, InsertStmt, Statement};
use crate::result::QueryResult;
use crate::storage::WalEntry;

use async_trait::async_trait;

pub struct InsertExecutor;

#[async_trait]
impl Executor for InsertExecutor {
    fn name(&self) -> &'static str {
        "INSERT"
    }
    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Insert(_))
    }

    async fn execute(&self, stmt: &Statement, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let Statement::Insert(insert) = stmt else {
            unreachable!();
        };

        self.execute_insert(insert, ctx).await
    }
}

impl InsertExecutor {
    async fn execute_insert(&self, insert: &InsertStmt, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        // Получаем схему (read lock на одну таблицу)
        let schema = ctx.storage.get_schema(&insert.table_name).await?;

        // Вычисляем строки
        let rows: Vec<Row> = insert
            .values
            .iter()
            .map(|row_exprs| self.evaluate_row(row_exprs, schema.schema().columns()))
            .collect::<Result<Vec<_>>>()?;

        // Validate Foreign Keys
        for row in &rows {
            for (i, column) in schema.schema().columns().iter().enumerate() {
                if let Some(ref fk) = column.references {
                    let val = &row[i];
                    if val.is_null() {
                        continue; // NULLs usually skip FK check unless Match Full
                    }

                    // Check if referenced table exists
                    if !ctx.storage.table_exists(&fk.table) {
                        return Err(DbError::TableNotFound(fk.table.clone()));
                    }

                    // Check if value exists in referenced table
                    // Use index if available (highly recommended for FK targets)
                    let exists = if let Some(rows) = ctx.storage.scan_index(&fk.table, &fk.column, val, &ctx.snapshot).await? {
                        !rows.is_empty()
                    } else {
                        // Fallback to full scan (slow!)
                        let all_rows = ctx.storage.scan_table(&fk.table, &ctx.snapshot).await?;
                        let ref_schema = ctx.storage.get_schema(&fk.table).await?;
                        let col_idx = ref_schema.schema().find_column_index(&fk.column)
                            .ok_or_else(|| DbError::ColumnNotFound(fk.column.clone(), fk.table.clone()))?;
                        
                        all_rows.iter().any(|r| &r[col_idx] == val)
                    };

                    if !exists {
                        return Err(DbError::ConstraintViolation(format!(
                            "Foreign key violation: Value {} in '{}.{}' references non-existent key in '{}.{}'",
                            val, insert.table_name, column.name, fk.table, fk.column
                        )));
                    }
                }
            }
        }

        // Insert rows into storage (MVCC write)
        for row in rows {
            ctx.storage.insert_row(&insert.table_name, row.clone(), &ctx.snapshot).await?;

            // Log to WAL if persistence is enabled
            if let Some(persistence) = ctx.persistence {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Insert {
                    table: insert.table_name.clone(),
                    row: row.clone(),
                })?;
            }
        }

        Ok(QueryResult::empty())
    }

    fn evaluate_row(&self, exprs: &[Expr], columns: &[Column]) -> Result<Row> {
        if exprs.len() != columns.len() {
            return Err(DbError::ExecutionError(format!(
                "Expected {} values, got {}",
                columns.len(),
                exprs.len()
            )));
        }

        exprs
            .iter()
            .enumerate()
            .map(|(i, expr)| self.evaluate_literal(expr, &columns[i].data_type))
            .collect()
    }

    fn evaluate_literal(
        &self,
        expr: &Expr,
        expected_type: &crate::core::DataType,
    ) -> Result<Value> {
        match expr {
            Expr::Literal(val) => {
                if matches!(val, Value::Null) {
                    return Ok(Value::Null);
                }

                // If types match directly, return value
                if expected_type.is_compatible(val) && val.type_name() == expected_type.to_string() {
                     return Ok(val.clone());
                }

                // Attempt type coercion for Strings -> Complex Types
                if let Value::Text(s) = val {
                    match expected_type {
                        crate::core::DataType::Timestamp => {
                            // Try parsing ISO8601
                            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                                return Ok(Value::Timestamp(dt.with_timezone(&chrono::Utc)));
                            }
                            // Allow flexible parsing? For now strict ISO8601
                        },
                        crate::core::DataType::Date => {
                            if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                return Ok(Value::Date(d));
                            }
                        },
                        crate::core::DataType::Uuid => {
                            if let Ok(u) = uuid::Uuid::parse_str(s) {
                                return Ok(Value::Uuid(u));
                            }
                        },
                        _ => {}
                    }
                }

                if !expected_type.is_compatible(val) {
                    return Err(DbError::TypeMismatch(format!(
                        "Expected {}, got {}",
                        expected_type,
                        val.type_name()
                    )));
                }
                Ok(val.clone())
            }
            _ => Err(DbError::UnsupportedOperation(
                "Only literal values supported in INSERT".into(),
            )),
        }
    }
}