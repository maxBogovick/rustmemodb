use super::{Executor, ExecutionContext};
use crate::parser::ast::Statement;
use crate::result::QueryResult;
use crate::core::Result;

pub struct DeleteExecutor;

impl Executor for DeleteExecutor {
    fn name(&self) -> &'static str {
        "DELETE"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Delete(_))
    }

    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        // Реализация DELETE
        // ...
        Ok(QueryResult::empty())
    }
}