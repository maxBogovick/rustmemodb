// ============================================================================
// src/executor/query.rs - Refactored QueryExecutor with improved architecture
// ============================================================================

use crate::parser::ast::{Statement, Expr, QueryStmt, OrderByExpr};
use crate::planner::{LogicalPlan, QueryPlanner};
use crate::planner::logical_plan::{SortNode};
use crate::storage::Catalog;
use crate::core::{Result, DbError, Row, Value, Schema};
use crate::evaluator::{EvaluationContext, EvaluatorRegistry, SubqueryHandler};
use crate::result::QueryResult;
use super::{Executor, ExecutionContext};
use std::cmp::Ordering;

use async_trait::async_trait;
use async_recursion::async_recursion;

// ============================================================================
// SUBQUERY HANDLER
// ============================================================================

struct ExecutorSubqueryHandler<'a> {
    executor: &'a QueryExecutor,
    ctx: &'a ExecutionContext<'a>,
}

#[async_trait]
impl<'a> SubqueryHandler for ExecutorSubqueryHandler<'a> {
    async fn execute(&self, query: &QueryStmt) -> Result<Vec<Row>> {
        let plan = self.executor.planner.plan(&Statement::Query(query.clone()), &self.executor.catalog)?;
        self.executor.execute_plan(&plan, self.ctx).await
    }
}

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
    #[allow(dead_code)]
    pub fn with_evaluators(catalog: Catalog, evaluator_registry: EvaluatorRegistry) -> Self {
        Self {
            planner: QueryPlanner::new(),
            catalog,
            evaluator_registry,
        }
    }

    /// Update catalog (called during DDL operations)
    #[allow(dead_code)]
    pub fn update_catalog(&mut self, new_catalog: Catalog) {
        self.catalog = new_catalog;
    }

    /// Execute logical plan - main entry point
    #[async_recursion]
    pub async fn execute_plan(&self, plan: &LogicalPlan, ctx: &ExecutionContext<'_>) -> Result<Vec<Row>> {
        match plan {
            LogicalPlan::TableScan(scan) => self.execute_scan(scan, ctx).await,
            LogicalPlan::Filter(filter) => self.execute_filter(filter, ctx).await,
            LogicalPlan::Projection(proj) => self.execute_projection(proj, ctx).await,
            LogicalPlan::Sort(sort) => self.execute_sort(sort, ctx).await,
            LogicalPlan::Limit(limit) => self.execute_limit(limit, ctx).await,
            LogicalPlan::Join(join) => self.execute_join(join, ctx).await,
            LogicalPlan::Aggregate(aggr) => self.execute_aggregate(aggr, ctx).await,
        }
    }

    /// Get output column names from plan
    pub fn get_output_columns(&self, plan: &LogicalPlan, _ctx: &ExecutionContext<'_>) -> Result<Vec<crate::core::Column>> {
        let schema = plan.schema();
        Ok(schema.columns().to_vec())
    }
}

// ============================================================================
// PLAN EXECUTION - Individual operators
// ============================================================================

impl QueryExecutor {
    /// Execute aggregation (GROUP BY)
    async fn execute_aggregate(
        &self,
        aggr: &crate::planner::logical_plan::AggregateNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute_plan(&aggr.input, ctx).await?;
        let input_schema = aggr.input.schema();
        
        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);

        // 1. Group rows
        let mut groups: std::collections::HashMap<Vec<Value>, Vec<Row>> = std::collections::HashMap::new();

        if aggr.group_exprs.is_empty() {
            // Implicit global group
            groups.insert(vec![], input_rows);
        } else {
            for row in input_rows {
                let mut key = Vec::new();
                for expr in &aggr.group_exprs {
                    key.push(eval_ctx.evaluate(expr, &row, input_schema).await?);
                }
                groups.entry(key).or_default().push(row);
            }
        }

        // 2. Compute aggregates for each group
        let mut result_rows = Vec::new();

        for (group_key, group_rows) in groups {
            let mut row = Vec::new();
            
            // Append grouping values first
            row.extend(group_key);
            
            // Append aggregate results
            for expr in &aggr.aggr_exprs {
                if let Expr::Function { name, args } = expr {
                    let val = self.evaluate_aggregate(name, args, &group_rows, input_schema, &eval_ctx).await?;
                    row.push(val);
                } else {
                    return Err(DbError::ExecutionError("Non-aggregate expression in aggregate list".into()));
                }
            }
            
            result_rows.push(row);
        }

        Ok(result_rows)
    }

    /// Execute join operation
    async fn execute_join(
        &self,
        join: &crate::planner::logical_plan::JoinNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        use crate::planner::logical_plan::JoinType;

        // Try Hash Join for Equi-Joins (Inner and Left only for now)
        if matches!(join.join_type, JoinType::Inner | JoinType::Left) {
            if let Some((left_key_expr, right_key_expr)) = self.extract_join_keys(&join.on, &join.left.schema(), &join.right.schema()) {
                return self.execute_hash_join(join, left_key_expr, right_key_expr, ctx).await;
            }
        }

        // Fallback to Nested Loop Join
        let left_rows = self.execute_plan(&join.left, ctx).await?;
        let right_rows = self.execute_plan(&join.right, ctx).await?;
        let schema = &join.schema;

        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);
        
        let mut result = Vec::new();

        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        let mut combined_row = left_row.clone();
                        combined_row.extend(right_row.clone());

                        if join.join_type == JoinType::Cross || self.evaluate_predicate(&eval_ctx, &join.on, &combined_row, schema).await {
                            result.push(combined_row);
                        }
                    }
                }
            }
            JoinType::Left => {
                let right_width = join.right.schema().column_count();

                for left_row in &left_rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        let mut combined_row = left_row.clone();
                        combined_row.extend(right_row.clone());

                        if self.evaluate_predicate(&eval_ctx, &join.on, &combined_row, schema).await {
                            result.push(combined_row);
                            matched = true;
                        }
                    }

                    if !matched {
                        let mut combined_row = left_row.clone();
                        combined_row.extend(vec![Value::Null; right_width]);
                        result.push(combined_row);
                    }
                }
            }
            JoinType::Right => {
                let left_width = join.left.schema().column_count();
                
                for right_row in &right_rows {
                    let mut matched = false;
                    for left_row in &left_rows {
                        let mut combined_row = left_row.clone();
                        combined_row.extend(right_row.clone());

                        if self.evaluate_predicate(&eval_ctx, &join.on, &combined_row, schema).await {
                            result.push(combined_row);
                            matched = true;
                        }
                    }

                    if !matched {
                        let mut combined_row = vec![Value::Null; left_width];
                        combined_row.extend(right_row.clone());
                        result.push(combined_row);
                    }
                }
            }
            JoinType::Full => {
                return Err(DbError::UnsupportedOperation("Full Outer Join not yet implemented".into()));
            }
        }

        Ok(result)
    }

    /// Execute Hash Join (O(N+M))
    async fn execute_hash_join(
        &self,
        join: &crate::planner::logical_plan::JoinNode,
        left_key_expr: Expr,
        right_key_expr: Expr,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        use crate::planner::logical_plan::JoinType;

        let left_rows = self.execute_plan(&join.left, ctx).await?;
        let right_rows = self.execute_plan(&join.right, ctx).await?;
        
        let mut build_map = std::collections::HashMap::new();
        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);
        
        for row in right_rows {
            let key_val = eval_ctx.evaluate(&right_key_expr, &row, join.right.schema()).await?;
            if key_val.is_null() {
                continue;
            }
            let key = JoinKey(key_val);
            build_map.entry(key).or_insert_with(Vec::new).push(row);
        }

        let mut result = Vec::new();
        let right_width = join.right.schema().column_count();

        for left_row in left_rows {
            let key_val = eval_ctx.evaluate(&left_key_expr, &left_row, join.left.schema()).await?;
            let key = JoinKey(key_val);

            if let Some(matches) = build_map.get(&key) {
                for right_row in matches {
                    let mut combined_row = left_row.clone();
                    combined_row.extend(right_row.clone());
                    result.push(combined_row);
                }
            } else if join.join_type == JoinType::Left {
                let mut combined_row = left_row.clone();
                combined_row.extend(vec![Value::Null; right_width]);
                result.push(combined_row);
            }
        }

        Ok(result)
    }

    /// Extract join keys from ON clause if it's a simple equality
    fn extract_join_keys(&self, on: &Expr, left_schema: &Schema, right_schema: &Schema) -> Option<(Expr, Expr)> {
        use crate::parser::ast::BinaryOp;
        
        if let Expr::BinaryOp { left, op: BinaryOp::Eq, right } = on {
            if let (Some(l), Some(r)) = (
                self.resolve_expr_to_schema(left, left_schema),
                self.resolve_expr_to_schema(right, right_schema)
            ) {
                return Some((l, r));
            }
            if let (Some(l), Some(r)) = (
                self.resolve_expr_to_schema(right, left_schema),
                self.resolve_expr_to_schema(left, right_schema)
            ) {
                return Some((l, r));
            }
        }
        None
    }

    fn resolve_expr_to_schema(&self, expr: &Expr, schema: &Schema) -> Option<Expr> {
        match expr {
            Expr::Column(name) => {
                if schema.find_column_index(name).is_some() {
                    Some(expr.clone())
                } else {
                    None
                }
            },
            Expr::CompoundIdentifier(parts) => {
                let name = parts.join(".");
                if schema.find_column_index(&name).is_some() {
                    return Some(expr.clone());
                }
                if let Some(col_name) = parts.last() {
                    if schema.find_column_index(col_name).is_some() {
                        return Some(Expr::Column(col_name.clone()));
                    }
                }
                None
            },
            Expr::Literal(_) => Some(expr.clone()),
            _ => None,
        }
    }

    /// Execute table scan
    async fn execute_scan(
        &self,
        scan: &crate::planner::logical_plan::TableScanNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        if let Some(ref idx) = scan.index_scan {
            if let Some(rows) = ctx.storage.scan_index(&scan.table_name, &idx.column, &idx.value, &ctx.snapshot).await? {
                return Ok(rows);
            }
        }
        ctx.storage.scan_table(&scan.table_name, &ctx.snapshot).await
    }

    /// Execute filter operation
    async fn execute_filter(
        &self,
        filter: &crate::planner::logical_plan::FilterNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute_plan(&filter.input, ctx).await?;
        let schema = &filter.schema;

        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);

        let mut filtered_rows = Vec::new();
        for row in input_rows {
            if self.evaluate_predicate(&eval_ctx, &filter.predicate, &row, schema).await {
                filtered_rows.push(row);
            }
        }
        Ok(filtered_rows)
    }

    /// Execute projection operation
    async fn execute_projection(
        &self,
        proj: &crate::planner::logical_plan::ProjectionNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute_plan(&proj.input, ctx).await?;
        let input_schema = proj.input.schema();

        if self.has_aggregate_functions(&proj.expressions) {
            return self.execute_aggregation(&proj.expressions, &input_rows, input_schema, ctx).await;
        }

        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);
        
        let mut projected_rows = Vec::new();
        for row in input_rows {
            projected_rows.push(self.project_row(&proj.expressions, &row, input_schema, &eval_ctx).await?);
        }
        Ok(projected_rows)
    }

    /// Execute sort operation
    async fn execute_sort(
        &self,
        sort: &SortNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let mut rows = self.execute_plan(&sort.input, ctx).await?;

        if sort.order_by.is_empty() {
            return Ok(rows);
        }

        let schema = &sort.schema;
        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);

        self.sort_rows(&mut rows, &sort.order_by, schema, &eval_ctx).await?;
        Ok(rows)
    }

    /// Execute limit operation
    async fn execute_limit(
        &self,
        limit: &crate::planner::logical_plan::LimitNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let mut rows = self.execute_plan(&limit.input, ctx).await?;
        rows.truncate(limit.limit);
        Ok(rows)
    }
}

// ============================================================================
// HELPER METHODS - Expression evaluation
// ============================================================================

impl QueryExecutor {
    /// Evaluate predicate for filtering
    async fn evaluate_predicate(
        &self,
        eval_ctx: &EvaluationContext<'_>,
        predicate: &Expr,
        row: &Row,
        schema: &Schema,
    ) -> bool {
        eval_ctx
            .evaluate(predicate, row, schema)
            .await
            .map(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Project a single row
    async fn project_row(
        &self,
        expressions: &[Expr],
        row: &Row,
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Row> {
        let mut result = Vec::with_capacity(expressions.len());
        for expr in expressions {
            result.push(eval_ctx.evaluate(expr, row, schema).await?);
        }
        Ok(result)
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
// AGGREGATION LOGIC (Non-Group By Aggregation)
// ============================================================================

impl QueryExecutor {
    /// Execute aggregation functions (called from Projection if no Group By)
    async fn execute_aggregation(
        &self,
        expressions: &[Expr],
        rows: &[Row],
        schema: &Schema,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);
        
        let mut result_row = Vec::new();

        for expr in expressions {
            let value = match expr {
                Expr::Function { name, args } => {
                    self.evaluate_aggregate(name, args, rows, schema, &eval_ctx).await?
                }
                _ => {
                    // Non-aggregate expressions evaluated on first row
                    if !rows.is_empty() {
                        eval_ctx.evaluate(expr, &rows[0], schema).await?
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
    async fn evaluate_aggregate(
        &self,
        name: &str,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        match name.to_uppercase().as_str() {
            "COUNT" => self.aggregate_count(args, rows, schema, eval_ctx).await,
            "SUM" => self.aggregate_sum(args, rows, schema, eval_ctx).await,
            "AVG" => self.aggregate_avg(args, rows, schema, eval_ctx).await,
            "MIN" => self.aggregate_min(args, rows, schema, eval_ctx).await,
            "MAX" => self.aggregate_max(args, rows, schema, eval_ctx).await,
            _ => Err(DbError::UnsupportedOperation(format!(
                "Unknown aggregate function: {}",
                name
            ))),
        }
    }

    async fn aggregate_count(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if args.is_empty() || matches!(args[0], Expr::Literal(Value::Text(ref s)) if s == "*") {
            return Ok(Value::Integer(rows.len() as i64));
        }

        let mut count = 0i64;
        for row in rows {
            let val = eval_ctx.evaluate(&args[0], row, schema).await?;
            if !matches!(val, Value::Null) {
                count += 1;
            }
        }

        Ok(Value::Integer(count))
    }

    async fn aggregate_sum(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("SUM requires an argument".into()));
        }

        let mut int_sum: i64 = 0;
        let mut float_sum: f64 = 0.0;
        let mut is_integer = true;

        for row in rows {
            let val = eval_ctx.evaluate(&args[0], row, schema).await?;
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

    async fn aggregate_avg(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("AVG requires an argument".into()));
        }

        let mut sum = 0.0f64;
        let mut count = 0usize;

        for row in rows {
            let val = eval_ctx.evaluate(&args[0], row, schema).await?;
            match val {
                Value::Integer(i) => {
                    sum += i as f64;
                    count += 1;
                }
                Value::Float(f) => {
                    sum += f;
                    count += 1;
                }
                Value::Null => {}
                _ => return Err(DbError::TypeMismatch("AVG requires numeric values".into())),
            }
        }

        Ok(if count == 0 {
            Value::Null
        } else {
            Value::Float(sum / count as f64)
        })
    }

    async fn aggregate_min(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("MIN requires an argument".into()));
        }

        let mut min_val: Option<Value> = None;
        for row in rows {
            let val = eval_ctx.evaluate(&args[0], row, schema).await?;
            if matches!(val, Value::Null) {
                continue;
            }

            min_val = Some(match min_val {
                None => val,
                Some(current) => {
                    if val.compare(&current)? == Ordering::Less {
                        val
                    } else {
                        current
                    }
                }
            });
        }

        Ok(min_val.unwrap_or(Value::Null))
    }

    async fn aggregate_max(
        &self,
        args: &[Expr],
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("MAX requires an argument".into()));
        }

        let mut max_val: Option<Value> = None;
        for row in rows {
            let val = eval_ctx.evaluate(&args[0], row, schema).await?;
            if matches!(val, Value::Null) {
                continue;
            }

            max_val = Some(match max_val {
                None => val,
                Some(current) => {
                    if val.compare(&current)? == Ordering::Greater {
                        val
                    } else {
                        current
                    }
                }
            });
        }

        Ok(max_val.unwrap_or(Value::Null))
    }
}

// ============================================================================
// SORTING LOGIC
// ============================================================================

impl QueryExecutor {
    /// Sort rows with proper error handling
    async fn sort_rows(
        &self,
        rows: &mut Vec<Row>,
        order_by: &[OrderByExpr],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<()> {
        if rows.is_empty() || order_by.is_empty() {
            return Ok(());
        }

        // Pre-evaluate sorting keys to avoid async in sort_by
        let mut rows_with_keys = Vec::with_capacity(rows.len());
        let original_rows = std::mem::take(rows);
        for row in original_rows {
            let mut keys = Vec::with_capacity(order_by.len());
            for order_expr in order_by {
                keys.push(eval_ctx.evaluate(&order_expr.expr, &row, schema).await?);
            }
            rows_with_keys.push((row, keys));
        }

        let mut sort_error: Option<DbError> = None;
        rows_with_keys.sort_by(|(_, keys_a), (_, keys_b)| {
            if sort_error.is_some() {
                return Ordering::Equal;
            }

            for (i, order_expr) in order_by.iter().enumerate() {
                let val_a: &Value = &keys_a[i];
                let val_b: &Value = &keys_b[i];

                let mut cmp = match val_a.compare(val_b) {
                    Ok(c) => c,
                    Err(e) => {
                        sort_error = Some(e);
                        return Ordering::Equal;
                    }
                };

                if order_expr.descending {
                    cmp = cmp.reverse();
                }

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }

            Ordering::Equal
        });

        if let Some(err) = sort_error {
            return Err(err);
        }

        for (row, _) in rows_with_keys {
            rows.push(row);
        }

        Ok(())
    }
}

// ============================================================================
// EXECUTOR TRAIT IMPLEMENTATION
// ============================================================================

#[async_trait]
impl Executor for QueryExecutor {
    fn name(&self) -> &'static str {
        "SELECT"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Query(_))
    }

    async fn execute(&self, stmt: &Statement, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        let Statement::Query(query) = stmt else {
            return Err(DbError::ExecutionError(
                "QueryExecutor called with non-query statement".into()
            ));
        };

        // Planning
        let plan = self.planner.plan(&Statement::Query(query.clone()), &self.catalog)?;

        // Execute plan
        let rows = self.execute_plan(&plan, ctx).await?;

        // Get column names
        let columns = self.get_output_columns(&plan, ctx)?;

        Ok(QueryResult::new(columns, rows))
    }
}

/// Wrapper for Value to implement strict Hash/Eq for joins
#[derive(Debug, Clone)]
struct JoinKey(Value);

impl std::hash::Hash for JoinKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl PartialEq for JoinKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            // Strict type equality required for Hash Map lookups
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for JoinKey {}
