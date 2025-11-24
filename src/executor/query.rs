// ============================================================================
// src/executor/query.rs - Полная рефакторированная версия с plugin system
// ============================================================================

use crate::parser::ast::{Statement, Expr};
use crate::planner::{LogicalPlan, QueryPlanner};
use crate::storage::Catalog;
use crate::core::{Result, DbError, Row, Value};
use crate::evaluator::{EvaluationContext, EvaluatorRegistry};
use crate::result::QueryResult;
use super::{Executor, ExecutionContext};

// ============================================================================
// QUERY EXECUTOR - С evaluator registry
// ============================================================================

pub struct QueryExecutor {
    planner: QueryPlanner,
    catalog: Catalog,
    evaluator_registry: EvaluatorRegistry,
}

impl QueryExecutor {
    /// Создать executor с дефолтными evaluators
    pub fn new(catalog: Catalog) -> Self {
        Self {
            planner: QueryPlanner::new(),
            catalog,
            evaluator_registry: EvaluatorRegistry::with_default_evaluators(),
        }
    }

    /// Создать executor с кастомными evaluators
    pub fn with_evaluators(catalog: Catalog, evaluator_registry: EvaluatorRegistry) -> Self {
        Self {
            planner: QueryPlanner::new(),
            catalog,
            evaluator_registry,
        }
    }

    /// Обновить catalog (вызывается при DDL операциях)
    pub fn update_catalog(&mut self, new_catalog: Catalog) {
        self.catalog = new_catalog;
    }

    /// Выполнить логический план
    fn execute_plan(&self, plan: &LogicalPlan, ctx: &ExecutionContext) -> Result<Vec<Row>> {
        match plan {
            LogicalPlan::TableScan(scan) => self.execute_scan(scan, ctx),
            LogicalPlan::Filter(filter) => self.execute_filter(filter, ctx),
            LogicalPlan::Projection(proj) => self.execute_projection(proj, ctx),
            LogicalPlan::Sort(sort) => self.execute_sort(sort, ctx),
            LogicalPlan::Limit(limit) => self.execute_limit(limit, ctx),
        }
    }

    /// Выполнить table scan
    fn execute_scan(
        &self,
        scan: &crate::planner::logical_plan::TableScanNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        // Read lock только на одну таблицу
        ctx.storage.scan_table(&scan.table_name)
    }

    /// Выполнить filter
    fn execute_filter(
        &self,
        filter: &crate::planner::logical_plan::FilterNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        // Получаем строки из input плана
        let input_rows = self.execute_plan(&filter.input, ctx)?;

        // Получаем имя таблицы для схемы
        let table_name = self.get_table_name(&filter.input)?;
        let schema = ctx.storage.get_schema(table_name.as_str())?;

        // Создаем evaluation context
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // Фильтруем строки
        Ok(input_rows
            .into_iter()
            .filter(|row| {
                eval_ctx
                    .evaluate(&filter.predicate, row, schema.schema())
                    .map(|v| v.as_bool())
                    .unwrap_or(false)
            })
            .collect())
    }

    /// Выполнить projection
    fn execute_projection(
        &self,
        proj: &crate::planner::logical_plan::ProjectionNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        // Получаем строки из input плана
        let input_rows = self.execute_plan(&proj.input, ctx)?;

        // Получаем схему
        let table_name = self.get_table_name(&proj.input)?;
        let schema = ctx.storage.get_schema(table_name.as_str())?;

        // Создаем evaluation context
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // Проецируем каждую строку
        input_rows
            .into_iter()
            .map(|row| self.project_row(&proj.expressions, &row, schema.schema(), &eval_ctx))
            .collect()
    }

    /// Выполнить sort
    fn execute_sort(
        &self,
        _sort: &crate::planner::logical_plan::SortNode,
        _ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        // TODO: Implement ORDER BY
        Err(DbError::UnsupportedOperation("ORDER BY not yet implemented".into()))
    }

    /// Выполнить limit
    fn execute_limit(
        &self,
        limit: &crate::planner::logical_plan::LimitNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        let mut input_rows = self.execute_plan(&limit.input, ctx)?;
        input_rows.truncate(limit.limit);
        Ok(input_rows)
    }

    /// Получить имя таблицы из плана (рекурсивно)
    fn get_table_name(&self, plan: &LogicalPlan) -> Result<String> {
        match plan {
            LogicalPlan::TableScan(scan) => Ok(scan.table_name.clone()),
            LogicalPlan::Filter(filter) => self.get_table_name(&filter.input),
            LogicalPlan::Projection(proj) => self.get_table_name(&proj.input),
            LogicalPlan::Sort(sort) => self.get_table_name(&sort.input),
            LogicalPlan::Limit(limit) => self.get_table_name(&limit.input),
        }
    }

    /// Проецировать строку согласно expressions
    fn project_row(
        &self,
        expressions: &[Expr],
        row: &Row,
        schema: &crate::core::Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Row> {
        expressions
            .iter()
            .map(|expr| eval_ctx.evaluate(expr, row, schema))
            .collect()
    }

    /// Получить имена выходных колонок из плана
    fn get_output_columns(&self, plan: &LogicalPlan, ctx: &ExecutionContext) -> Result<Vec<String>> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let schema = ctx.storage.get_schema(&scan.table_name)?;

                // Если указаны конкретные колонки, используем их
                if let Some(ref cols) = scan.projected_columns {
                    Ok(cols.clone())
                } else {
                    // Иначе все колонки
                    Ok(schema
                        .schema()
                        .columns()
                        .iter()
                        .map(|col| col.name.clone())
                        .collect())
                }
            }

            LogicalPlan::Projection(proj) => {
                // Для projection используем имена из expressions
                Ok(proj
                    .expressions
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| match expr {
                        Expr::Column(name) => name.clone(),
                        _ => format!("col_{}", i),
                    })
                    .collect())
            }

            // Для других узлов - рекурсивно получаем из input
            LogicalPlan::Filter(filter) => self.get_output_columns(&filter.input, ctx),
            LogicalPlan::Sort(sort) => self.get_output_columns(&sort.input, ctx),
            LogicalPlan::Limit(limit) => self.get_output_columns(&limit.input, ctx),
        }
    }
}

// ============================================================================
// EXECUTOR TRAIT IMPLEMENTATION
// ============================================================================

impl Executor for QueryExecutor {
    fn name(&self) -> &'static str {
        "SELECT"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Query(_))
    }

    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        let Statement::Query(query) = stmt else {
            return Err(DbError::ExecutionError(
                "QueryExecutor called with non-query statement".into()
            ));
        };

        // 1. Планирование (БЕЗ блокировок - catalog immutable!)
        let plan = self.planner.plan(&Statement::Query(query.clone()), &self.catalog)?;

        // 2. Выполнение плана
        let rows = self.execute_plan(&plan, ctx)?;

        // 3. Получение имен колонок
        let columns = self.get_output_columns(&plan, ctx)?;

        // 4. Возврат результата
        Ok(QueryResult::new(columns, rows))
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{InMemoryStorage, Catalog, TableSchema};
    use crate::core::{Column, DataType, Value};

    fn setup_test_storage() -> (InMemoryStorage, Catalog) {
        let mut storage = InMemoryStorage::new();
        let mut catalog = Catalog::new();

        // Create users table
        let columns = vec![
            Column::new("id", DataType::Integer).not_null(),
            Column::new("name", DataType::Text),
            Column::new("age", DataType::Integer),
        ];
        let schema = TableSchema::new("users", columns);

        storage.create_table(schema.clone()).unwrap();
        catalog = catalog.with_table(schema).unwrap();

        // Insert test data
        storage.insert_row("users", vec![
            Value::Integer(1),
            Value::Text("Alice".into()),
            Value::Integer(30),
        ]).unwrap();

        storage.insert_row("users", vec![
            Value::Integer(2),
            Value::Text("Bob".into()),
            Value::Integer(25),
        ]).unwrap();

        storage.insert_row("users", vec![
            Value::Integer(3),
            Value::Text("Charlie".into()),
            Value::Integer(35),
        ]).unwrap();

        (storage, catalog)
    }

    #[test]
    fn test_simple_scan() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode};
        let plan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let rows = executor.execute_plan(&plan, &ctx).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_filter_execution() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, FilterNode};
        use crate::parser::ast::{Expr, BinaryOp};

        // SELECT * FROM users WHERE age > 26
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Value::Integer(26))),
            },
        });

        let rows = executor.execute_plan(&filter, &ctx).unwrap();
        assert_eq!(rows.len(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_projection_execution() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, ProjectionNode};
        use crate::parser::ast::Expr;

        // SELECT name FROM users
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let projection = LogicalPlan::Projection(ProjectionNode {
            input: Box::new(scan),
            expressions: vec![Expr::Column("name".to_string())],
        });

        let rows = executor.execute_plan(&projection, &ctx).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].len(), 1); // Only one column
    }

    #[test]
    fn test_limit_execution() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, LimitNode};

        // SELECT * FROM users LIMIT 2
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let limit = LogicalPlan::Limit(LimitNode {
            input: Box::new(scan),
            limit: 2,
        });

        let rows = executor.execute_plan(&limit, &ctx).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_complex_query() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, FilterNode, ProjectionNode, LimitNode};
        use crate::parser::ast::{Expr, BinaryOp};

        // SELECT name FROM users WHERE age > 26 LIMIT 1
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Value::Integer(26))),
            },
        });

        let projection = LogicalPlan::Projection(ProjectionNode {
            input: Box::new(filter),
            expressions: vec![Expr::Column("name".to_string())],
        });

        let limit = LogicalPlan::Limit(LimitNode {
            input: Box::new(projection),
            limit: 1,
        });

        let rows = executor.execute_plan(&limit, &ctx).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
    }

    #[test]
    fn test_like_evaluation() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, FilterNode};
        use crate::parser::ast::Expr;

        // SELECT * FROM users WHERE name LIKE 'A%'
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::Like {
                expr: Box::new(Expr::Column("name".to_string())),
                pattern: Box::new(Expr::Literal(Value::Text("A%".to_string()))),
                negated: false,
                case_insensitive: false,
            },
        });

        let rows = executor.execute_plan(&filter, &ctx).unwrap();
        assert_eq!(rows.len(), 1); // Only Alice
    }

    #[test]
    fn test_between_evaluation() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, FilterNode};
        use crate::parser::ast::Expr;

        // SELECT * FROM users WHERE age BETWEEN 25 AND 30
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::Between {
                expr: Box::new(Expr::Column("age".to_string())),
                low: Box::new(Expr::Literal(Value::Integer(25))),
                high: Box::new(Expr::Literal(Value::Integer(30))),
                negated: false,
            },
        });

        let rows = executor.execute_plan(&filter, &ctx).unwrap();
        assert_eq!(rows.len(), 2); // Alice (30) and Bob (25)
    }

    #[test]
    fn test_is_null_evaluation() {
        let mut storage = InMemoryStorage::new();
        let mut catalog = Catalog::new();

        // Create table with nullable column
        let columns = vec![
            Column::new("id", DataType::Integer).not_null(),
            Column::new("name", DataType::Text), // nullable
        ];
        let schema = TableSchema::new("test", columns);

        storage.create_table(schema.clone()).unwrap();
        catalog = catalog.with_table(schema).unwrap();

        // Insert data with NULL
        storage.insert_row("test", vec![
            Value::Integer(1),
            Value::Text("Alice".into()),
        ]).unwrap();

        storage.insert_row("test", vec![
            Value::Integer(2),
            Value::Null,
        ]).unwrap();

        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, FilterNode};
        use crate::parser::ast::Expr;

        // SELECT * FROM test WHERE name IS NULL
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "test".to_string(),
            projected_columns: None,
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::IsNull {
                expr: Box::new(Expr::Column("name".to_string())),
                negated: false,
            },
        });

        let rows = executor.execute_plan(&filter, &ctx).unwrap();
        assert_eq!(rows.len(), 1); // Only row with NULL
    }

    #[test]
    fn test_logical_and() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, FilterNode};
        use crate::parser::ast::{Expr, BinaryOp};

        // SELECT * FROM users WHERE age > 26 AND age < 32
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOp::Gt,
                    right: Box::new(Expr::Literal(Value::Integer(26))),
                }),
                op: BinaryOp::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOp::Lt,
                    right: Box::new(Expr::Literal(Value::Integer(32))),
                }),
            },
        });

        let rows = executor.execute_plan(&filter, &ctx).unwrap();
        assert_eq!(rows.len(), 1); // Only Alice (30)
    }

    #[test]
    fn test_get_output_columns() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode};

        // Test wildcard
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let columns = executor.get_output_columns(&scan, &ctx).unwrap();
        assert_eq!(columns, vec!["id", "name", "age"]);
    }
}

// ============================================================================
// USAGE EXAMPLE
// ============================================================================

/*
use crate::executor::query::QueryExecutor;
use crate::storage::Catalog;

// Создание executor'а
let catalog = Catalog::new();
let executor = QueryExecutor::new(catalog);

// Executor автоматически использует все зарегистрированные evaluators:
// - ComparisonEvaluator (=, !=, <, <=, >, >=)
// - ArithmeticEvaluator (+, -, *, /, %)
// - LogicalEvaluator (AND, OR)
// - LikeEvaluator (LIKE, NOT LIKE)
// - BetweenEvaluator (BETWEEN, NOT BETWEEN)
// - IsNullEvaluator (IS NULL, IS NOT NULL)

// Выполнение запроса
let result = executor.execute(&query_stmt, &ctx)?;

// Добавление кастомного evaluator'а
let mut registry = EvaluatorRegistry::new();
registry.register(Box::new(MyCustomEvaluator));
let executor = QueryExecutor::with_evaluators(catalog, registry);
*/