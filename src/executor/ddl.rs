use super::{ExecutionContext, Executor};
use crate::core::Result;
use crate::parser::ast::{CreateTableStmt, DropTableStmt, Statement};
use crate::result::QueryResult;

pub struct CreateTableExecutor;

impl Executor for CreateTableExecutor {
    fn name(&self) -> &'static str {
        "CREATE_TABLE"
    }
    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::CreateTable(_))
    }

    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        let Statement::CreateTable(create) = stmt else {
            unreachable!();
        };

        self.execute_create_table(create, ctx)
    }
}

impl CreateTableExecutor {
    fn execute_create_table(
        &self,
        create: &CreateTableStmt,
        _ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        // NOTE: Таблица создается через facade, который владеет storage
        // Executor только валидирует, но не создает таблицу напрямую
        Ok(QueryResult::empty())
    }
}

pub struct DropTableExecutor;

impl Executor for DropTableExecutor {
    fn name(&self) -> &'static str {
        "DROP_TABLE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::DropTable(_))
    }

    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        let Statement::DropTable(drop) = stmt else {
            unreachable!();
        };

        self.execute_drop_table(drop, ctx)
    }
}

impl DropTableExecutor {
    fn execute_drop_table(
        &self,
        drop: &DropTableStmt,
        _ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        // NOTE: Таблица удаляется через facade, который владеет storage
        // Executor только валидирует, но не удаляет таблицу напрямую
        Ok(QueryResult::empty())
    }
}