// ============================================================================
// src/executor/query.rs - Refactored QueryExecutor with improved architecture
// ============================================================================

use crate::parser::ast::{Statement, Expr, OrderByExpr};
use crate::planner::{LogicalPlan, QueryPlanner};
use crate::planner::logical_plan::SortNode;
use crate::storage::Catalog;
use crate::core::{Result, DbError, Row, Value, Schema};
use crate::evaluator::{EvaluationContext, EvaluatorRegistry};
use crate::result::QueryResult;
use super::{Executor, ExecutionContext};
use std::cmp::Ordering;

// ============================================================================
// QUERY EXECUTOR - Main structure
// ============================================================================

pub struct QueryExecutor {
    planner: QueryPlanner,
    catalog: Catalog,
    evaluator_registry: EvaluatorRegistry,
}

impl QueryExecutor {
    /// Create executor with default evaluators
    pub fn new(catalog: Catalog) -> Self {
        Self {
            planner: QueryPlanner::new(),
            catalog,
            evaluator_registry: EvaluatorRegistry::with_default_evaluators(),
        }
    }

    /// Create executor with custom evaluators
    pub fn with_evaluators(catalog: Catalog, evaluator_registry: EvaluatorRegistry) -> Self {
        Self {
            planner: QueryPlanner::new(),
            catalog,
            evaluator_registry,
        }
    }

    /// Update catalog (called during DDL operations)
    pub fn update_catalog(&mut self, new_catalog: Catalog) {
        self.catalog = new_catalog;
    }

    /// Execute logical plan - main entry point
    pub fn execute_plan(&self, plan: &LogicalPlan, ctx: &ExecutionContext) -> Result<Vec<Row>> {
        match plan {
            LogicalPlan::TableScan(scan) => self.execute_scan(scan, ctx),
            LogicalPlan::Filter(filter) => self.execute_filter(filter, ctx),
            LogicalPlan::Projection(proj) => self.execute_projection(proj, ctx),
            LogicalPlan::Sort(sort) => self.execute_sort(sort, ctx),
            LogicalPlan::Limit(limit) => self.execute_limit(limit, ctx),
        }
    }

    /// Get table name from plan recursively
    fn get_table_name(&self, plan: &LogicalPlan) -> Result<String> {
        match plan {
            LogicalPlan::TableScan(scan) => Ok(scan.table_name.clone()),
            LogicalPlan::Filter(filter) => self.get_table_name(&filter.input),
            LogicalPlan::Projection(proj) => self.get_table_name(&proj.input),
            LogicalPlan::Sort(sort) => self.get_table_name(&sort.input),
            LogicalPlan::Limit(limit) => self.get_table_name(&limit.input),
        }
    }

    /// Get output column names from plan
    fn get_output_columns(&self, plan: &LogicalPlan, ctx: &ExecutionContext) -> Result<Vec<String>> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let schema = ctx.storage.get_schema(&scan.table_name)?;

                if let Some(ref cols) = scan.projected_columns {
                    Ok(cols.clone())
                } else {
                    Ok(schema.schema().columns().iter().map(|col| col.name.clone()).collect())
                }
            }

            LogicalPlan::Projection(proj) => {
                Ok(proj.expressions.iter().enumerate().map(|(i, expr)| {
                    match expr {
                        Expr::Column(name) => name.clone(),
                        _ => format!("col_{}", i),
                    }
                }).collect())
            }

            // Recurse for other nodes
            LogicalPlan::Filter(filter) => self.get_output_columns(&filter.input, ctx),
            LogicalPlan::Sort(sort) => self.get_output_columns(&sort.input, ctx),
            LogicalPlan::Limit(limit) => self.get_output_columns(&limit.input, ctx),
        }
    }
}

// ============================================================================
// PLAN EXECUTION - Individual operators
// ============================================================================

impl QueryExecutor {
    /// Execute table scan
    fn execute_scan(
        &self,
        scan: &crate::planner::logical_plan::TableScanNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        ctx.storage.scan_table(&scan.table_name)
    }

    /// Execute filter operation
    fn execute_filter(
        &self,
        filter: &crate::planner::logical_plan::FilterNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute_plan(&filter.input, ctx)?;
        let table_name = self.get_table_name(&filter.input)?;
        let schema = ctx.storage.get_schema(&table_name)?;

        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        Ok(input_rows
            .into_iter()
            .filter(|row| self.evaluate_predicate(&eval_ctx, &filter.predicate, row, schema.schema()))
            .collect())
    }

    /// Execute projection operation
    fn execute_projection(
        &self,
        proj: &crate::planner::logical_plan::ProjectionNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute_plan(&proj.input, ctx)?;
        let table_name = self.get_table_name(&proj.input)?;
        let schema = ctx.storage.get_schema(&table_name)?;

        // Check for aggregate functions
        if self.has_aggregate_functions(&proj.expressions) {
            return self.execute_aggregation(&proj.expressions, &input_rows, schema.schema());
        }

        // Regular projection
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);
        input_rows
            .into_iter()
            .map(|row| self.project_row(&proj.expressions, &row, schema.schema(), &eval_ctx))
            .collect()
    }

    /// Execute sort operation
    fn execute_sort(
        &self,
        sort: &SortNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        let mut rows = self.execute_plan(&sort.input, ctx)?;

        if sort.order_by.is_empty() {
            return Ok(rows);
        }

        let table_name = self.get_table_name(&sort.input)?;
        let schema = ctx.storage.get_schema(&table_name)?;
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // Sort with error handling
        let sort_result = self.sort_rows(&mut rows, &sort.order_by, schema.schema(), &eval_ctx);
        sort_result.map(|_| rows)
    }

    /// Execute limit operation
    fn execute_limit(
        &self,
        limit: &crate::planner::logical_plan::LimitNode,
        ctx: &ExecutionContext,
    ) -> Result<Vec<Row>> {
        let mut rows = self.execute_plan(&limit.input, ctx)?;
        rows.truncate(limit.limit);
        Ok(rows)
    }
}

// ============================================================================
// HELPER METHODS - Expression evaluation
// ============================================================================

impl QueryExecutor {
    /// Evaluate predicate for filtering
    fn evaluate_predicate(
        &self,
        eval_ctx: &EvaluationContext,
        predicate: &Expr,
        row: &Row,
        schema: &Schema,
    ) -> bool {
        eval_ctx
            .evaluate(predicate, row, schema)
            .map(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Project a single row
    fn project_row(
        &self,
        expressions: &[Expr],
        row: &Row,
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Row> {
        expressions
            .iter()
            .map(|expr| eval_ctx.evaluate(expr, row, schema))
            .collect()
    }

    /// Check if expressions contain aggregate functions
    fn has_aggregate_functions(&self, expressions: &[Expr]) -> bool {
        expressions.iter().any(|expr| self.is_aggregate_function(expr))
    }

    /// Check if expression is an aggregate function
    fn is_aggregate_function(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                )
            }
            _ => false,
        }
    }
}

// ============================================================================
// AGGREGATION LOGIC
// ============================================================================

impl QueryExecutor {
    /// Execute aggregation functions
    fn execute_aggregation(
        &self,
        expressions: &[Expr],
        rows: &[Row],
        schema: &Schema,
    ) -> Result<Vec<Row>> {
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);
        let mut result_row = Vec::new();

        for expr in expressions {
            let value = match expr {
                Expr::Function { name, args } => {
                    self.evaluate_aggregate(name, args, rows, schema, &eval_ctx)?
                }
                _ => {
                    // Non-aggregate expressions evaluated on first row
                    if !rows.is_empty() {
                        eval_ctx.evaluate(expr, &rows[0], schema)?
                    } else {
                        Value::Null
                    }
                }
            };
            result_row.push(value);
        }

        Ok(vec![result_row])
    }

    /// Evaluate aggregate function
    fn evaluate_aggregate(
        &self,
        name: &str,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Value> {
        match name.to_uppercase().as_str() {
            "COUNT" => self.aggregate_count(args, rows, schema, eval_ctx),
            "SUM" => self.aggregate_sum(args, rows, schema, eval_ctx),
            "AVG" => self.aggregate_avg(args, rows, schema, eval_ctx),
            "MIN" => self.aggregate_min(args, rows, schema, eval_ctx),
            "MAX" => self.aggregate_max(args, rows, schema, eval_ctx),
            _ => Err(DbError::UnsupportedOperation(format!(
                "Unknown aggregate function: {}",
                name
            ))),
        }
    }

    fn aggregate_count(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Value> {
        if args.is_empty() || matches!(args[0], Expr::Literal(Value::Text(ref s)) if s == "*") {
            return Ok(Value::Integer(rows.len() as i64));
        }

        let count = rows.iter().fold(0i64, |acc, row| {
            let val = eval_ctx.evaluate(&args[0], row, schema).unwrap();
            if matches!(val, Value::Null) { acc } else { acc + 1 }
        });

        Ok(Value::Integer(count))
    }

    fn aggregate_sum(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("SUM requires an argument".into()));
        }

        let mut int_sum: i64 = 0;
        let mut float_sum: f64 = 0.0;
        let mut is_integer = true;

        for row in rows {
            let val = eval_ctx.evaluate(&args[0], row, schema)?;
            match val {
                Value::Integer(i) => {
                    if is_integer {
                        int_sum += i;
                    } else {
                        float_sum += i as f64;
                    }
                }
                Value::Float(f) => {
                    if is_integer {
                        float_sum = int_sum as f64 + f;
                        is_integer = false;
                    } else {
                        float_sum += f;
                    }
                }
                Value::Null => {}
                _ => return Err(DbError::TypeMismatch("SUM requires numeric values".into())),
            }
        }

        Ok(if is_integer {
            Value::Integer(int_sum)
        } else {
            Value::Float(float_sum)
        })
    }

    fn aggregate_avg(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("AVG requires an argument".into()));
        }

        let (sum, count) = rows.iter().try_fold((0.0f64, 0usize), |(sum, count), row| {
            let val = eval_ctx.evaluate(&args[0], row, schema)?;
            match val {
                Value::Integer(i) => Ok((sum + i as f64, count + 1)),
                Value::Float(f) => Ok((sum + f, count + 1)),
                Value::Null => Ok((sum, count)),
                _ => Err(DbError::TypeMismatch("AVG requires numeric values".into())),
            }
        })?;

        Ok(if count == 0 {
            Value::Null
        } else {
            Value::Float(sum / count as f64)
        })
    }

    fn aggregate_min(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("MIN requires an argument".into()));
        }

        rows.iter()
            .try_fold(None, |min_val: Option<Value>, row| {
                let val = eval_ctx.evaluate(&args[0], row, schema)?;
                if matches!(val, Value::Null) {
                    return Ok(min_val);
                }

                Ok(Some(match min_val {
                    None => val,
                    Some(current) => {
                        if val.compare(&current)? == Ordering::Less {
                            val
                        } else {
                            current
                        }
                    }
                }))
            })
            .map(|opt| opt.unwrap_or(Value::Null))
    }

    fn aggregate_max(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("MAX requires an argument".into()));
        }

        rows.iter()
            .try_fold(None, |max_val: Option<Value>, row| {
                let val = eval_ctx.evaluate(&args[0], row, schema)?;
                if matches!(val, Value::Null) {
                    return Ok(max_val);
                }

                Ok(Some(match max_val {
                    None => val,
                    Some(current) => {
                        if val.compare(&current)? == Ordering::Greater {
                            val
                        } else {
                            current
                        }
                    }
                }))
            })
            .map(|opt| opt.unwrap_or(Value::Null))
    }
}

// ============================================================================
// SORTING LOGIC
// ============================================================================

impl QueryExecutor {
    /// Sort rows with proper error handling
    fn sort_rows(
        &self,
        rows: &mut [Row],
        order_by: &[OrderByExpr],
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<()> {
        let mut sort_error: Option<DbError> = None;

        rows.sort_by(|row_a, row_b| {
            if sort_error.is_some() {
                return Ordering::Equal;
            }

            for order_expr in order_by {
                let cmp = self.compare_rows_by_expr(
                    row_a,
                    row_b,
                    &order_expr.expr,
                    order_expr.descending,
                    schema,
                    eval_ctx,
                );

                match cmp {
                    Ok(Ordering::Equal) => continue,
                    Ok(ordering) => return ordering,
                    Err(e) => {
                        sort_error = Some(e);
                        return Ordering::Equal;
                    }
                }
            }

            Ordering::Equal
        });

        if let Some(err) = sort_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Compare two rows by expression
    fn compare_rows_by_expr(
        &self,
        row_a: &Row,
        row_b: &Row,
        expr: &Expr,
        descending: bool,
        schema: &Schema,
        eval_ctx: &EvaluationContext,
    ) -> Result<Ordering> {
        let val_a = eval_ctx.evaluate(expr, row_a, schema)?;
        let val_b = eval_ctx.evaluate(expr, row_b, schema)?;

        let mut cmp = val_a.compare(&val_b)?;

        if descending {
            cmp = cmp.reverse();
        }

        Ok(cmp)
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

        // Planning (no locks - catalog is immutable)
        let plan = self.planner.plan(&Statement::Query(query.clone()), &self.catalog)?;

        // Execute plan
        let rows = self.execute_plan(&plan, ctx)?;

        // Get column names
        let columns = self.get_output_columns(&plan, ctx)?;

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
    use crate::planner::TableScanNode;

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

        storage.insert_row("users", vec![
            Value::Integer(4),
            Value::Text("Diana".into()),
            Value::Integer(25),
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
        assert_eq!(rows.len(), 4);
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
        assert_eq!(rows.len(), 4);
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

    // ========================================================================
    // ✅ ORDER BY TESTS
    // ========================================================================

    #[test]
    fn test_order_by_asc() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, SortNode};

        // SELECT * FROM users ORDER BY age ASC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: false,
            }],
        });

        let rows = executor.execute_plan(&sort, &ctx).unwrap();

        // Expected order by age ASC: 25, 25, 30, 35
        assert_eq!(rows[0][2], Value::Integer(25));
        assert_eq!(rows[1][2], Value::Integer(25));
        assert_eq!(rows[2][2], Value::Integer(30));
        assert_eq!(rows[3][2], Value::Integer(35));
    }

    #[test]
    fn test_order_by_desc() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, SortNode};

        // SELECT * FROM users ORDER BY age DESC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: true,
            }],
        });

        let rows = executor.execute_plan(&sort, &ctx).unwrap();

        // Expected order by age DESC: 35, 30, 25, 25
        assert_eq!(rows[0][2], Value::Integer(35));
        assert_eq!(rows[1][2], Value::Integer(30));
        assert_eq!(rows[2][2], Value::Integer(25));
        assert_eq!(rows[3][2], Value::Integer(25));
    }

    #[test]
    fn test_order_by_multiple_columns() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, SortNode};

        // SELECT * FROM users ORDER BY age ASC, name ASC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![
                OrderByExpr {
                    expr: Expr::Column("age".to_string()),
                    descending: false,
                },
                OrderByExpr {
                    expr: Expr::Column("name".to_string()),
                    descending: false,
                },
            ],
        });

        let rows = executor.execute_plan(&sort, &ctx).unwrap();

        // Expected: age 25 (Bob, Diana alphabetically), age 30 (Alice), age 35 (Charlie)
        assert_eq!(rows[0][1], Value::Text("Bob".into()));    // age 25, name Bob
        assert_eq!(rows[1][1], Value::Text("Diana".into()));  // age 25, name Diana
        assert_eq!(rows[2][1], Value::Text("Alice".into()));  // age 30
        assert_eq!(rows[3][1], Value::Text("Charlie".into())); // age 35
    }

    #[test]
    fn test_order_by_with_filter() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, FilterNode, SortNode};
        use crate::parser::ast::BinaryOp;

        // SELECT * FROM users WHERE age > 24 ORDER BY name DESC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Value::Integer(24))),
            },
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(filter),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("name".to_string()),
                descending: true,
            }],
        });

        let rows = executor.execute_plan(&sort, &ctx).unwrap();

        // All 4 users have age > 24, sorted by name DESC: Diana, Charlie, Bob, Alice
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][1], Value::Text("Diana".into()));
        assert_eq!(rows[1][1], Value::Text("Charlie".into()));
        assert_eq!(rows[2][1], Value::Text("Bob".into()));
        assert_eq!(rows[3][1], Value::Text("Alice".into()));
    }

    #[test]
    fn test_order_by_with_limit() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, SortNode, LimitNode};

        // SELECT * FROM users ORDER BY age DESC LIMIT 2
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: true,
            }],
        });

        let limit = LogicalPlan::Limit(LimitNode {
            input: Box::new(sort),
            limit: 2,
        });

        let rows = executor.execute_plan(&limit, &ctx).unwrap();

        // Top 2 oldest: Charlie (35), Alice (30)
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][2], Value::Integer(35)); // Charlie
        assert_eq!(rows[1][2], Value::Integer(30)); // Alice
    }

    #[test]
    fn test_order_by_with_nulls() {
        let mut storage = InMemoryStorage::new();
        let mut catalog = Catalog::new();

        // Create table with nullable column
        let columns = vec![
            Column::new("id", DataType::Integer).not_null(),
            Column::new("value", DataType::Integer), // nullable
        ];
        let schema = TableSchema::new("test", columns);

        storage.create_table(schema.clone()).unwrap();
        catalog = catalog.with_table(schema).unwrap();

        // Insert data with NULLs
        storage.insert_row("test", vec![Value::Integer(1), Value::Integer(10)]).unwrap();
        storage.insert_row("test", vec![Value::Integer(2), Value::Null]).unwrap();
        storage.insert_row("test", vec![Value::Integer(3), Value::Integer(5)]).unwrap();
        storage.insert_row("test", vec![Value::Integer(4), Value::Null]).unwrap();

        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, SortNode};

        // SELECT * FROM test ORDER BY value ASC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "test".to_string(),
            projected_columns: None,
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("value".to_string()),
                descending: false,
            }],
        });

        let rows = executor.execute_plan(&sort, &ctx).unwrap();

        // NULL LAST by default: 5, 10, NULL, NULL
        assert_eq!(rows[0][1], Value::Integer(5));
        assert_eq!(rows[1][1], Value::Integer(10));
        assert_eq!(rows[2][1], Value::Null);
        assert_eq!(rows[3][1], Value::Null);
    }

    #[test]
    fn test_order_by_expression() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, SortNode};
        use crate::parser::ast::BinaryOp;

        // SELECT * FROM users ORDER BY age * -1 ASC (equivalent to ORDER BY age DESC)
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column("age".to_string())),
                    op: BinaryOp::Multiply,
                    right: Box::new(Expr::Literal(Value::Integer(-1))),
                },
                descending: false,
            }],
        });

        let rows = executor.execute_plan(&sort, &ctx).unwrap();

        // ORDER BY age * -1 ASC = ORDER BY age DESC
        // -35 < -30 < -25 < -25
        assert_eq!(rows[0][2], Value::Integer(35));
        assert_eq!(rows[1][2], Value::Integer(30));
        // age 25 rows at the end
    }

    #[test]
    fn test_complex_query() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        use crate::planner::logical_plan::{LogicalPlan, TableScanNode, FilterNode, ProjectionNode, LimitNode};
        use crate::parser::ast::BinaryOp;

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
        assert_eq!(rows.len(), 3); // Alice (30), Bob (25), Diana (25)
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
        use crate::parser::ast::BinaryOp;

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
    #[test]
    fn test_order_by_with_value_compare() {
        let (storage, catalog) = setup_test_storage();
        let executor = QueryExecutor::new(catalog);
        let ctx = ExecutionContext::new(&storage);

        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: false,
            }],
        });

        let rows = executor.execute_plan(&sort, &ctx).unwrap();

        // Should be sorted by age ASC
        assert_eq!(rows[0][2], Value::Integer(25)); // Bob
        assert_eq!(rows[2][2], Value::Integer(30)); // Alice
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

// Выполнение запроса с ORDER BY
let result = executor.execute(&query_stmt, &ctx)?;

// Добавление кастомного evaluator'а
let mut registry = EvaluatorRegistry::new();
registry.register(Box::new(MyCustomEvaluator));
let executor = QueryExecutor::with_evaluators(catalog, registry);
*/