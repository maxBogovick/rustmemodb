// ============================================================================
// src/executor/query.rs - Refactored QueryExecutor with improved architecture
// ============================================================================

use crate::parser::ast::{Statement, Expr, OrderByExpr};
use crate::planner::{LogicalPlan, QueryPlanner};
use crate::planner::logical_plan::{SortNode};
use crate::storage::Catalog;
use crate::core::{Result, DbError, Row, Value, Schema};
use crate::evaluator::{EvaluationContext, EvaluatorRegistry};
use crate::result::QueryResult;
use super::{Executor, ExecutionContext};
use std::cmp::Ordering;

use async_trait::async_trait;
use async_recursion::async_recursion;

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
    fn get_output_columns(&self, plan: &LogicalPlan, _ctx: &ExecutionContext<'_>) -> Result<Vec<String>> {
        let schema = plan.schema();
        Ok(schema.columns().iter().map(|c| c.name.clone()).collect())
    }
}

// ============================================================================
// PLAN EXECUTION - Individual operators
// ============================================================================

impl QueryExecutor {
    /// Execute aggregation
    async fn execute_aggregate(
        &self,
        aggr: &crate::planner::logical_plan::AggregateNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute_plan(&aggr.input, ctx).await?;
        let input_schema = aggr.input.schema();
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // 1. Group rows
        // Key: Grouping values, Value: List of rows in this group
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
        let schema = &join.schema; // Schema of the join result

        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);
        let mut result = Vec::new();

        match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        // Combine rows temporarily to evaluate ON condition
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
        
        // Build Phase (Build hash map from RIGHT table)
        // Map: JoinKey -> Vec<Row>
        let mut build_map = std::collections::HashMap::new();
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);
        
        for row in right_rows {
            let key_val = eval_ctx.evaluate(&right_key_expr, &row, join.right.schema()).await?;
            if key_val.is_null() {
                continue; // Nulls never match in SQL joins
            }
            let key = JoinKey(key_val);
            build_map.entry(key).or_insert_with(Vec::new).push(row);
        }

        // Probe Phase (Scan LEFT table)
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
                // No match for Left Join -> emit row with NULLs
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
            // Try (Left=Left, Right=Right)
            if let (Some(l), Some(r)) = (
                self.resolve_expr_to_schema(left, left_schema),
                self.resolve_expr_to_schema(right, right_schema)
            ) {
                return Some((l, r));
            }
            
            // Try Swap (Left=Right, Right=Left)
            if let (Some(l), Some(r)) = (
                self.resolve_expr_to_schema(right, left_schema),
                self.resolve_expr_to_schema(left, right_schema)
            ) {
                return Some((l, r));
            }
        }
        None
    }

    /// Check if expr belongs to schema and return a normalized expr (e.g. stripping qualifiers if needed)
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
                // Try treating the last part as the column name if exact match failed
                // This handles "t1.id" matching "id" in t1's schema
                if let Some(col_name) = parts.last() {
                    if schema.find_column_index(col_name).is_some() {
                        return Some(Expr::Column(col_name.clone()));
                    }
                }
                None
            },
            Expr::Literal(_) => Some(expr.clone()),
            _ => None, // Only support simple columns/literals as keys for now
        }
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

// End of QueryExecutor extension
impl QueryExecutor {
    // ... rest of the impl block ... (placeholder to attach to)
    // Actually we need to replace the existing execute_join
    // So this block is just a container for the replacement logic.


    /// Execute table scan
    async fn execute_scan(
        &self,
        scan: &crate::planner::logical_plan::TableScanNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        if let Some(ref idx) = scan.index_scan {
            // Try to use index
            if let Some(rows) = ctx.storage.scan_index(&scan.table_name, &idx.column, &idx.value, &ctx.snapshot).await? {
                return Ok(rows);
            }
            // If index scan returned None (e.g. index dropped concurrently?), fallback to full scan
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

        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

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
        let input_schema = proj.input.schema(); // Projection needs input schema to evaluate expressions

        // Check for aggregate functions
        if self.has_aggregate_functions(&proj.expressions) {
            return self.execute_aggregation(&proj.expressions, &input_rows, input_schema).await;
        }

        // Regular projection
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);
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
        let eval_ctx = EvaluationContext::new(&self.evaluator_registry);

        // Sort with error handling
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
// AGGREGATION LOGIC
// ============================================================================

impl QueryExecutor {
    /// Execute aggregation functions
    async fn execute_aggregation(
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
            let val = eval_ctx.evaluate(&args[0], row, schema).await.unwrap();
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
                let val_a = &keys_a[i];
                let val_b = &keys_b[i];

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

        // Planning (no locks - catalog is immutable)
        let plan = self.planner.plan(&Statement::Query(query.clone()), &self.catalog)?;

        // Execute plan
        let rows = self.execute_plan(&plan, ctx).await?;

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
    use crate::parser::ast::BinaryOp;
    use crate::planner::logical_plan::{FilterNode, TableScanNode, ProjectionNode, LimitNode, SortNode};

    async fn setup_test_storage() -> (InMemoryStorage, Catalog) {
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let mut storage = InMemoryStorage::new();
        let mut catalog = Catalog::new();

        // Create users table
        let columns = vec![
            Column::new("id", DataType::Integer).not_null(),
            Column::new("name", DataType::Text),
            Column::new("age", DataType::Integer),
        ];
        let schema = TableSchema::new("users", columns);

        storage.create_table(schema.clone()).await.unwrap();
        catalog = catalog.with_table(schema).unwrap();

        // Insert test data
        storage.insert_row("users", vec![
            Value::Integer(1),
            Value::Text("Alice".into()),
            Value::Integer(30),
        ], &snapshot).await.unwrap();

        storage.insert_row("users", vec![
            Value::Integer(2),
            Value::Text("Bob".into()),
            Value::Integer(25),
        ], &snapshot).await.unwrap();

        storage.insert_row("users", vec![
            Value::Integer(3),
            Value::Text("Charlie".into()),
            Value::Integer(35),
        ], &snapshot).await.unwrap();

        storage.insert_row("users", vec![
            Value::Integer(4),
            Value::Text("Diana".into()),
            Value::Integer(25),
        ], &snapshot).await.unwrap();

        (storage, catalog)
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Integer).not_null(),
            Column::new("name", DataType::Text),
            Column::new("age", DataType::Integer),
        ])
    }

    #[tokio::test]
    async fn test_simple_scan() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        let plan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&plan, &ctx).await.unwrap();
        assert_eq!(rows.len(), 4);
    }

    #[tokio::test]
    async fn test_filter_execution() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Value::Integer(26))),
            },
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&filter, &ctx).await.unwrap();
        assert_eq!(rows.len(), 2); // Alice (30) and Charlie (35)
    }

    #[tokio::test]
    async fn test_projection_execution() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let projection = LogicalPlan::Projection(ProjectionNode {
            input: Box::new(scan),
            expressions: vec![Expr::Column("name".to_string())],
            schema: Schema::new(vec![Column::new("name", DataType::Text)]),
        });

        let rows = executor.execute_plan(&projection, &ctx).await.unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].len(), 1); // Only one column
    }

    #[tokio::test]
    async fn test_limit_execution() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let limit = LogicalPlan::Limit(LimitNode {
            input: Box::new(scan),
            limit: 2,
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&limit, &ctx).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ========================================================================
    // ✅ ORDER BY TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_order_by_asc() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users ORDER BY age ASC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: false,
            }],
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&sort, &ctx).await.unwrap();

        // Expected order by age ASC: 25, 25, 30, 35
        assert_eq!(rows[0][2], Value::Integer(25));
        assert_eq!(rows[1][2], Value::Integer(25));
        assert_eq!(rows[2][2], Value::Integer(30));
        assert_eq!(rows[3][2], Value::Integer(35));
    }

    #[tokio::test]
    async fn test_order_by_desc() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users ORDER BY age DESC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: true,
            }],
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&sort, &ctx).await.unwrap();

        // Expected order by age DESC: 35, 30, 25, 25
        assert_eq!(rows[0][2], Value::Integer(35));
        assert_eq!(rows[1][2], Value::Integer(30));
        assert_eq!(rows[2][2], Value::Integer(25));
        assert_eq!(rows[3][2], Value::Integer(25));
    }

    #[tokio::test]
    async fn test_order_by_multiple_columns() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users ORDER BY age ASC, name ASC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
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
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&sort, &ctx).await.unwrap();

        // Expected: age 25 (Bob, Diana alphabetically), age 30 (Alice), age 35 (Charlie)
        assert_eq!(rows[0][1], Value::Text("Bob".into()));    // age 25, name Bob
        assert_eq!(rows[1][1], Value::Text("Diana".into()));  // age 25, name Diana
        assert_eq!(rows[2][1], Value::Text("Alice".into()));  // age 30
        assert_eq!(rows[3][1], Value::Text("Charlie".into())); // age 35
    }

    #[tokio::test]
    async fn test_order_by_with_filter() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users WHERE age > 24 ORDER BY name DESC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Value::Integer(24))),
            },
            schema: create_test_schema(),
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(filter),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("name".to_string()),
                descending: true,
            }],
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&sort, &ctx).await.unwrap();

        // All 4 users have age > 24, sorted by name DESC: Diana, Charlie, Bob, Alice
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][1], Value::Text("Diana".into()));
        assert_eq!(rows[1][1], Value::Text("Charlie".into()));
        assert_eq!(rows[2][1], Value::Text("Bob".into()));
        assert_eq!(rows[3][1], Value::Text("Alice".into()));
    }

    #[tokio::test]
    async fn test_order_by_with_limit() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users ORDER BY age DESC LIMIT 2
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: true,
            }],
            schema: create_test_schema(),
        });

        let limit = LogicalPlan::Limit(LimitNode {
            input: Box::new(sort),
            limit: 2,
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&limit, &ctx).await.unwrap();

        // Top 2 oldest: Charlie (35), Alice (30)
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][2], Value::Integer(35)); // Charlie
        assert_eq!(rows[1][2], Value::Integer(30)); // Alice
    }

    #[tokio::test]
    async fn test_order_by_with_nulls() {
        let mut storage = InMemoryStorage::new();
        let mut catalog = Catalog::new();

        // Create table with nullable column
        let columns = vec![
            Column::new("id", DataType::Integer).not_null(),
            Column::new("value", DataType::Integer), // nullable
        ];
        let schema = TableSchema::new("test", columns.clone());
        let test_schema = Schema::new(columns);

        storage.create_table(schema.clone()).await.unwrap();
        catalog = catalog.with_table(schema).unwrap();
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        // Insert data with NULLs
        storage.insert_row("test", vec![Value::Integer(1), Value::Integer(10)], &snapshot).await.unwrap();
        storage.insert_row("test", vec![Value::Integer(2), Value::Null], &snapshot).await.unwrap();
        storage.insert_row("test", vec![Value::Integer(3), Value::Integer(5)], &snapshot).await.unwrap();
        storage.insert_row("test", vec![Value::Integer(4), Value::Null], &snapshot).await.unwrap();

        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM test ORDER BY value ASC
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "test".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: test_schema.clone(),
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("value".to_string()),
                descending: false,
            }],
            schema: test_schema,
        });

        let rows = executor.execute_plan(&sort, &ctx).await.unwrap();

        // NULL LAST by default: 5, 10, NULL, NULL
        assert_eq!(rows[0][1], Value::Integer(5));
        assert_eq!(rows[1][1], Value::Integer(10));
        assert_eq!(rows[2][1], Value::Null);
        assert_eq!(rows[3][1], Value::Null);
    }

    #[tokio::test]
    async fn test_order_by_expression() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users ORDER BY age * -1 ASC (equivalent to ORDER BY age DESC)
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
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
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&sort, &ctx).await.unwrap();

        // ORDER BY age * -1 ASC = ORDER BY age DESC
        // -35 < -30 < -25 < -25
        assert_eq!(rows[0][2], Value::Integer(35));
        assert_eq!(rows[1][2], Value::Integer(30));
        // age 25 rows at the end
    }

    #[tokio::test]
    async fn test_complex_query() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT name FROM users WHERE age > 26 LIMIT 1
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(Value::Integer(26))),
            },
            schema: create_test_schema(),
        });

        let projection = LogicalPlan::Projection(ProjectionNode {
            input: Box::new(filter),
            expressions: vec![Expr::Column("name".to_string())],
            schema: Schema::new(vec![Column::new("name", DataType::Text)]),
        });

        let limit = LogicalPlan::Limit(LimitNode {
            input: Box::new(projection),
            limit: 1,
            schema: Schema::new(vec![Column::new("name", DataType::Text)]),
        });

        let rows = executor.execute_plan(&limit, &ctx).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
    }

    #[tokio::test]
    async fn test_like_evaluation() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users WHERE name LIKE 'A%'
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::Like {
                expr: Box::new(Expr::Column("name".to_string())),
                pattern: Box::new(Expr::Literal(Value::Text("A%".to_string()))),
                negated: false,
                case_insensitive: false,
            },
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&filter, &ctx).await.unwrap();
        assert_eq!(rows.len(), 1); // Only Alice
    }

    #[tokio::test]
    async fn test_between_evaluation() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users WHERE age BETWEEN 25 AND 30
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::Between {
                expr: Box::new(Expr::Column("age".to_string())),
                low: Box::new(Expr::Literal(Value::Integer(25))),
                high: Box::new(Expr::Literal(Value::Integer(30))),
                negated: false,
            },
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&filter, &ctx).await.unwrap();
        assert_eq!(rows.len(), 3); // Alice (30), Bob (25), Diana (25)
    }

    #[tokio::test]
    async fn test_is_null_evaluation() {
        let mut storage = InMemoryStorage::new();
        let mut catalog = Catalog::new();
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();

        // Create table with nullable column
        let columns = vec![
            Column::new("id", DataType::Integer).not_null(),
            Column::new("name", DataType::Text), // nullable
        ];
        let schema = TableSchema::new("test", columns.clone());
        let test_schema = Schema::new(columns);

        storage.create_table(schema.clone()).await.unwrap();
        catalog = catalog.with_table(schema).unwrap();

        // Insert data with NULL
        storage.insert_row("test", vec![
            Value::Integer(1),
            Value::Text("Alice".into()),
        ],  &snapshot).await.unwrap();

        storage.insert_row("test", vec![
            Value::Integer(2),
            Value::Null,
        ],  &snapshot).await.unwrap();

        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM test WHERE name IS NULL
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "test".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: test_schema.clone(),
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: Expr::IsNull {
                expr: Box::new(Expr::Column("name".to_string())),
                negated: false,
            },
            schema: test_schema,
        });

        let rows = executor.execute_plan(&filter, &ctx).await.unwrap();
        assert_eq!(rows.len(), 1); // Only row with NULL
    }

    #[tokio::test]
    async fn test_logical_and() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // SELECT * FROM users WHERE age > 26 AND age < 32
        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
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
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&filter, &ctx).await.unwrap();
        assert_eq!(rows.len(), 1); // Only Alice (30)
    }

    #[tokio::test]
    async fn test_get_output_columns() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        // Test wildcard
        let scan = TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        };

        let columns = executor.get_output_columns(&LogicalPlan::TableScan(scan), &ctx).unwrap();
        assert_eq!(columns, vec!["id", "name", "age"]);
    }

    #[tokio::test]
    async fn test_order_by_with_value_compare() {
        let (storage, catalog) = setup_test_storage().await;
        let executor = QueryExecutor::new(catalog);
        let txn_mgr = std::sync::Arc::new(crate::transaction::TransactionManager::new());
        let snapshot = txn_mgr.get_auto_commit_snapshot().await.unwrap();
        let ctx = ExecutionContext::new(&storage, &txn_mgr, None, snapshot);

        let scan = LogicalPlan::TableScan(TableScanNode {
            table_name: "users".to_string(),
            projected_columns: None,
            index_scan: None,
            schema: create_test_schema(),
        });

        let sort = LogicalPlan::Sort(SortNode {
            input: Box::new(scan),
            order_by: vec![OrderByExpr {
                expr: Expr::Column("age".to_string()),
                descending: false,
            }],
            schema: create_test_schema(),
        });

        let rows = executor.execute_plan(&sort, &ctx).await.unwrap();

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