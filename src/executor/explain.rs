use crate::executor::Executor;
use crate::executor::context::ExecutionContext;
use crate::parser::ast::Statement;
use crate::planner::QueryPlanner;
use crate::result::QueryResult;
use crate::core::{Result, Column, DataType, Value, DbError};
use crate::storage::Catalog;
use async_trait::async_trait;

pub struct ExplainExecutor {
    catalog: Catalog,
}

impl ExplainExecutor {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl Executor for ExplainExecutor {
    fn name(&self) -> &'static str {
        "EXPLAIN"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Explain(_))
    }

    async fn execute(&self, stmt: &Statement, _ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let Statement::Explain(explain) = stmt else {
            unreachable!();
        };

        match *explain.statement {
            Statement::Query(ref query) => {
                let planner = QueryPlanner::new();
                let plan = planner.plan(&Statement::Query(query.clone()), &self.catalog)?;
                
                let plan_str = format!("{:#?}", plan);
                let lines: Vec<&str> = plan_str.lines().collect();
                
                let rows: Vec<Vec<Value>> = lines.into_iter()
                    .map(|line| vec![Value::Text(line.to_string())])
                    .collect();

                Ok(QueryResult::new(
                    vec![Column::new("QUERY PLAN", DataType::Text)],
                    rows
                ))
            }
            _ => Err(DbError::UnsupportedOperation("EXPLAIN only supports SELECT for now".into())),
        }
    }
}
