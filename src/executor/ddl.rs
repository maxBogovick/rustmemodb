use super::{ExecutionContext, Executor};
use crate::core::Result;
use crate::parser::ast::{CreateTableStmt, Statement};
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