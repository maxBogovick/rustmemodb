use super::{ExecutionContext, Executor};
use crate::core::Result;
use crate::parser::ast::{AlterTableOperation, AlterTableStmt, CreateTableStmt, DropTableStmt, Statement};
use crate::result::QueryResult;

use async_trait::async_trait;

pub struct CreateTableExecutor;

#[async_trait]
impl Executor for CreateTableExecutor {
    fn name(&self) -> &'static str {
        "CREATE_TABLE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::CreateTable(_))
    }

    async fn execute(&self, stmt: &Statement, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let Statement::CreateTable(create) = stmt else {
            unreachable!();
        };

        self.execute_create_table(create, ctx).await
    }
}

impl CreateTableExecutor {
    async fn execute_create_table(
        &self,
        _create: &CreateTableStmt,
        _ctx: &ExecutionContext<'_>,
    ) -> Result<QueryResult> {
        // NOTE: Таблица создается через facade, который владеет storage
        // Executor только валидирует, но не создает таблицу напрямую
        Ok(QueryResult::empty())
    }
}

pub struct DropTableExecutor;

#[async_trait]
impl Executor for DropTableExecutor {
    fn name(&self) -> &'static str {
        "DROP_TABLE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::DropTable(_))
    }

    async fn execute(&self, stmt: &Statement, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let Statement::DropTable(drop) = stmt else {
            unreachable!();
        };

        self.execute_drop_table(drop, ctx).await
    }
}

impl DropTableExecutor {
    async fn execute_drop_table(
        &self,
        _drop: &DropTableStmt,
        _ctx: &ExecutionContext<'_>,
    ) -> Result<QueryResult> {
        // NOTE: Таблица удаляется через facade, который владеет storage
        // Executor только валидирует, но не удаляет таблицу напрямую
        Ok(QueryResult::empty())
    }
}

pub struct AlterTableExecutor;

#[async_trait]
impl Executor for AlterTableExecutor {
    fn name(&self) -> &'static str {
        "ALTER_TABLE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::AlterTable(_))
    }

    async fn execute(&self, stmt: &Statement, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let Statement::AlterTable(alter) = stmt else {
            unreachable!();
        };

        self.execute_alter_table(alter, ctx).await
    }
}

impl AlterTableExecutor {
    async fn execute_alter_table(
        &self,
        alter: &AlterTableStmt,
        ctx: &ExecutionContext<'_>,
    ) -> Result<QueryResult> {
        match &alter.operation {
            AlterTableOperation::AddColumn(col_def) => {
                let column = crate::core::Column::new(col_def.name.clone(), col_def.data_type.clone());
                // TODO: Handle constraints like NOT NULL, DEFAULT, etc.
                // For now, basic column addition.
                ctx.storage.add_column(&alter.table_name, column).await?;
            }
            AlterTableOperation::DropColumn(col_name) => {
                ctx.storage.drop_column(&alter.table_name, col_name).await?;
            }
            AlterTableOperation::RenameColumn { old_name, new_name } => {
                ctx.storage.rename_column(&alter.table_name, old_name, new_name).await?;
            }
            AlterTableOperation::RenameTable(_) => {
                return Err(crate::core::DbError::UnsupportedOperation("RENAME TABLE not implemented yet".into()));
            }
        }
        Ok(QueryResult::empty())
    }
}
