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
                if !expected_type.is_compatible(val) && !matches!(val, Value::Null) {
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