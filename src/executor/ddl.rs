use super::{ExecutionContext, Executor};
use crate::core::Result;
use crate::parser::ast::{CreateTableStmt, DropTableStmt, Statement};
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
