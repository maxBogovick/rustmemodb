use super::{ExecutionContext, Executor};
use crate::core::{Column, DbError, Result, Row, Value};
use crate::parser::ast::{Expr, InsertStmt, Statement, InsertSource, Statement as AstStatement};
use crate::planner::logical_plan::IndexOp;
use crate::result::QueryResult;
use crate::storage::{WalEntry, Catalog};
use crate::executor::query::QueryExecutor;

use async_trait::async_trait;

pub struct InsertExecutor {
    catalog: Catalog,
}

impl InsertExecutor {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}

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
        let schema_columns = schema.schema().columns();
        let insert_col_indices = self.resolve_insert_columns(insert.columns.as_deref(), schema_columns, &insert.table_name)?;

        // Вычисляем строки
        let rows: Vec<Row> = match &insert.source {
            InsertSource::Values(values) => {
                values.iter()
                    .map(|row_exprs| self.build_row_from_exprs(row_exprs, schema_columns, insert_col_indices.as_deref(), ctx))
                    .collect::<Result<Vec<_>>>()?
            }
            InsertSource::Select(query) => {
                let executor = QueryExecutor::new(self.catalog.clone());
                let result = executor.execute(&AstStatement::Query(*query.clone()), ctx).await?;
                // TODO: Validate column types against schema?
                // QueryExecutor returns generic rows.
                // Insert expects specific types? evaluate_row does casting.
                // Here we might need casting too if types strictly don't match.
                // For MVP, assume types match or rely on implicit compat.
                // But strict types: Integer vs Text.
                // I should iterate rows and cast?
                
                let mut cast_rows = Vec::new();
                for row in result.rows() {
                    cast_rows.push(self.build_row_from_values(row, schema_columns, insert_col_indices.as_deref())?);
                }
                cast_rows
            }
        };

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
                    let exists = if let Some(rows) = ctx.storage.scan_index(&fk.table, &fk.column, val, &None, &IndexOp::Eq, &ctx.snapshot).await? {
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
        let autocommit = ctx.transaction_id.is_none();
        let tx_id = ctx.snapshot.tx_id;
        let mut logged_begin = false;
        let mut logged_any = false;

        for row in rows {
            // Log to WAL before applying
            if let Some(persistence) = ctx.persistence {
                if autocommit && !logged_begin {
                    let mut persistence_guard = persistence.lock().await;
                    persistence_guard.log(&WalEntry::BeginTransaction(tx_id))?;
                    logged_begin = true;
                }
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Insert {
                    tx_id,
                    table: insert.table_name.clone(),
                    row: row.clone(),
                })?;
                logged_any = true;
            }

            ctx.storage.insert_row(&insert.table_name, row.clone(), &ctx.snapshot).await?;
        }

        if autocommit && logged_any {
            if let Some(persistence) = ctx.persistence {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Commit(tx_id))?;
            }
        }

        Ok(QueryResult::empty())
    }

    fn build_row_from_exprs(
        &self,
        exprs: &[Expr],
        columns: &[Column],
        insert_col_indices: Option<&[usize]>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Row> {
        if let Some(indices) = insert_col_indices {
            if exprs.len() != indices.len() {
                return Err(DbError::ExecutionError(format!(
                    "Expected {} values, got {}",
                    indices.len(),
                    exprs.len()
                )));
            }

            let mut row: Row = columns
                .iter()
                .map(|col| col.default.clone().unwrap_or(Value::Null))
                .collect();

            for (expr, col_idx) in exprs.iter().zip(indices.iter()) {
                let value = self.evaluate_literal(expr, &columns[*col_idx].data_type, ctx)?;
                row[*col_idx] = value;
            }

            Ok(row)
        } else {
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
                .map(|(i, expr)| self.evaluate_literal(expr, &columns[i].data_type, ctx))
                .collect()
        }
    }

    fn build_row_from_values(
        &self,
        values: &[Value],
        columns: &[Column],
        insert_col_indices: Option<&[usize]>,
    ) -> Result<Row> {
        if let Some(indices) = insert_col_indices {
            if values.len() != indices.len() {
                return Err(DbError::ExecutionError(format!(
                    "Expected {} values, got {}",
                    indices.len(),
                    values.len()
                )));
            }

            let mut row: Row = columns
                .iter()
                .map(|col| col.default.clone().unwrap_or(Value::Null))
                .collect();

            for (val, col_idx) in values.iter().zip(indices.iter()) {
                let cast = columns[*col_idx].data_type.cast_value(val)?;
                row[*col_idx] = cast;
            }

            Ok(row)
        } else {
            if values.len() != columns.len() {
                return Err(DbError::ExecutionError(format!(
                    "Expected {} values, got {}",
                    columns.len(),
                    values.len()
                )));
            }

            values
                .iter()
                .enumerate()
                .map(|(i, val)| columns[i].data_type.cast_value(val))
                .collect()
        }
    }

    fn resolve_insert_columns(
        &self,
        insert_columns: Option<&[String]>,
        columns: &[Column],
        table_name: &str,
    ) -> Result<Option<Vec<usize>>> {
        let Some(col_names) = insert_columns else {
            return Ok(None);
        };

        let mut seen = std::collections::HashSet::new();
        let mut indices = Vec::with_capacity(col_names.len());

        for name in col_names {
            if !seen.insert(name) {
                return Err(DbError::ExecutionError(format!(
                    "Duplicate column '{}' in INSERT",
                    name
                )));
            }
            let idx = columns
                .iter()
                .position(|c| c.name == *name)
                .ok_or_else(|| DbError::ColumnNotFound(name.clone(), table_name.to_string()))?;
            indices.push(idx);
        }

        Ok(Some(indices))
    }

    fn evaluate_literal(
        &self,
        expr: &Expr,
        expected_type: &crate::core::DataType,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Value> {
        match expr {
            Expr::Literal(val) => {
                expected_type.cast_value(val)
            }
            Expr::Parameter(idx) => {
                if *idx == 0 || *idx > ctx.params.len() {
                    return Err(DbError::ExecutionError(format!("Parameter index out of range: ${}", idx)));
                }
                let val = &ctx.params[*idx - 1];
                expected_type.cast_value(val)
            }
            _ => Err(DbError::UnsupportedOperation(
                "Only literal values or parameters supported in INSERT".into(),
            )),
        }
    }
}
