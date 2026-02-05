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
            LogicalPlan::Distinct(distinct) => self.execute_distinct(distinct, ctx).await,
            LogicalPlan::Window(window) => self.execute_window(window, ctx).await,
            LogicalPlan::Values(values) => self.execute_values(values, ctx).await,
            LogicalPlan::RecursiveQuery(rec) => self.execute_recursive_query(rec, ctx).await,
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
                let mut key = Vec::with_capacity(aggr.group_exprs.len());
                for expr in &aggr.group_exprs {
                    key.push(eval_ctx.evaluate(expr, &row, input_schema).await?);
                }
                groups.entry(key).or_default().push(row);
            }
        }

        // 2. Compute aggregates for each group
        let mut result_rows = Vec::with_capacity(groups.len());

        for (group_key, group_rows) in groups {
            let mut row = group_key;
            row.reserve(aggr.aggr_exprs.len());
            
            // Append aggregate results
            for expr in &aggr.aggr_exprs {
                if let Expr::Function { name, args, distinct, over: _ } = expr {
                    let val = self.evaluate_aggregate(name, args, *distinct, &group_rows, input_schema, &eval_ctx).await?;
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
                        let combined_row = self.combine_rows(left_row, right_row);

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
                        let combined_row = self.combine_rows(left_row, right_row);

                        if self.evaluate_predicate(&eval_ctx, &join.on, &combined_row, schema).await {
                            result.push(combined_row);
                            matched = true;
                        }
                    }

                    if !matched {
                        let mut combined_row = left_row.clone();
                        self.extend_nulls(&mut combined_row, right_width);
                        result.push(combined_row);
                    }
                }
            }
            JoinType::Right => {
                let left_width = join.left.schema().column_count();
                
                for right_row in &right_rows {
                    let mut matched = false;
                    for left_row in &left_rows {
                        let combined_row = self.combine_rows(left_row, right_row);

                        if self.evaluate_predicate(&eval_ctx, &join.on, &combined_row, schema).await {
                            result.push(combined_row);
                            matched = true;
                        }
                    }

                    if !matched {
                        let mut combined_row = Row::with_capacity(left_width + right_row.len());
                        self.extend_nulls(&mut combined_row, left_width);
                        combined_row.extend_from_slice(right_row);
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
        
        let mut build_map = std::collections::HashMap::with_capacity(right_rows.len());
        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);
        
        for (idx, row) in right_rows.iter().enumerate() {
            let key_val = eval_ctx.evaluate(&right_key_expr, row, join.right.schema()).await?;
            if key_val.is_null() {
                continue;
            }
            let key = JoinKey(key_val);
            build_map.entry(key).or_insert_with(Vec::new).push(idx);
        }

        let mut result = Vec::new();
        let right_width = join.right.schema().column_count();

        for left_row in left_rows {
            let key_val = eval_ctx.evaluate(&left_key_expr, &left_row, join.left.schema()).await?;
            let key = JoinKey(key_val);

            if let Some(matches) = build_map.get(&key) {
                for &right_idx in matches {
                    let right_row = &right_rows[right_idx];
                    let combined_row = self.combine_rows(&left_row, right_row);
                    result.push(combined_row);
                }
            } else if join.join_type == JoinType::Left {
                let mut combined_row = left_row.clone();
                self.extend_nulls(&mut combined_row, right_width);
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
            if let Some(rows) = ctx.storage.scan_index(
                &scan.table_name,
                &idx.column,
                &idx.value,
                &idx.end_value,
                &idx.op,
                &ctx.snapshot
            ).await? {
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

        let mut filtered_rows = Vec::with_capacity(input_rows.len());
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
        
        let mut projected_rows = Vec::with_capacity(input_rows.len());
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
        let rows = self.execute_plan(&limit.input, ctx).await?;
        let iter = rows.into_iter().skip(limit.offset);
        let rows = if let Some(max) = limit.limit {
            iter.take(max).collect()
        } else {
            iter.collect()
        };
        Ok(rows)
    }

    /// Execute distinct operation
    async fn execute_distinct(
        &self,
        distinct: &crate::planner::logical_plan::DistinctNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let input_rows = self.execute_plan(&distinct.input, ctx).await?;
        
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::with_capacity(input_rows.len());
        
        for row in input_rows {
            // Wrap in JoinKey for hashing
            let key: Vec<JoinKey> = row.iter().map(|v| JoinKey(v.clone())).collect();
            
            if seen.insert(key) {
                result.push(row);
            }
        }
        
        Ok(result)
    }

    /// Execute window functions
    async fn execute_window(
        &self,
        window: &crate::planner::logical_plan::WindowNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let mut rows = self.execute_plan(&window.input, ctx).await?;
        let input_schema = window.input.schema();
        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);

        for expr in &window.window_exprs {
            if let Expr::Function { name, over: Some(spec), .. } = expr {
                let func = name.to_uppercase();
                // 1. Sort by Partition Keys + Order Keys
                let mut sort_keys = Vec::with_capacity(spec.partition_by.len() + spec.order_by.len());
                for expr in &spec.partition_by {
                    sort_keys.push(OrderByExpr { expr: expr.clone(), descending: false });
                }
                sort_keys.extend(spec.order_by.clone());

                // Pre-evaluate keys
                let mut row_keys = Vec::with_capacity(rows.len());
                for row in &rows {
                    let mut keys = Vec::with_capacity(sort_keys.len());
                    for k in &sort_keys {
                        keys.push(eval_ctx.evaluate(&k.expr, row, input_schema).await?);
                    }
                    row_keys.push(keys);
                }
                
                let mut indices: Vec<usize> = (0..rows.len()).collect();
                indices.sort_by(|&i, &j| {
                    for (k, order_expr) in sort_keys.iter().enumerate() {
                        let val_a = &row_keys[i][k];
                        let val_b = &row_keys[j][k];
                        let cmp = val_a.compare(val_b).unwrap_or(Ordering::Equal);
                        if cmp != Ordering::Equal {
                            return if order_expr.descending { cmp.reverse() } else { cmp };
                        }
                    }
                    Ordering::Equal
                });

                // 3. Compute Window Function
                let mut results: Vec<(usize, Value)> = Vec::with_capacity(rows.len());
                
                let mut current_partition: Option<Vec<Value>> = None;
                let mut row_number = 0;
                let mut rank = 0;
                let mut last_order_values: Option<Vec<Value>> = None;

                for &idx in &indices {
                    let keys = &row_keys[idx];
                    let partition_keys = &keys[0..spec.partition_by.len()];
                    let order_keys = &keys[spec.partition_by.len()..];

                    let partition_changed = match &current_partition {
                        Some(p) => p.as_slice() != partition_keys,
                        None => true,
                    };

                    if partition_changed {
                        current_partition = Some(partition_keys.to_vec());
                        row_number = 0;
                        rank = 0;
                        last_order_values = None;
                    }

                    row_number += 1;

                    let order_changed = match &last_order_values {
                        Some(o) => o.as_slice() != order_keys,
                        None => true,
                    };
                    
                    if order_changed {
                        rank = row_number;
                        last_order_values = Some(order_keys.to_vec());
                    }

                    let val = match func.as_str() {
                        "ROW_NUMBER" => Value::Integer(row_number),
                        "RANK" => Value::Integer(rank),
                        _ => Value::Null, // TODO: Implement Aggregates over Window
                    };
                    results.push((idx, val));
                }

                // 3. Update rows
                for (idx, val) in results {
                    rows[idx].push(val);
                }
            }
        }

        Ok(rows)
    }

    /// Execute VALUES clause (constant rows)
    async fn execute_values(
        &self,
        values: &crate::planner::logical_plan::ValuesNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        let subquery_handler = ExecutorSubqueryHandler { executor: self, ctx };
        let eval_ctx = EvaluationContext::with_params(&self.evaluator_registry, Some(&subquery_handler), &ctx.params);
        let schema = &values.schema;

        let mut result = Vec::with_capacity(values.rows.len());
        let empty_row: Row = Vec::new();
        for row_exprs in &values.rows {
            let mut row = Vec::with_capacity(row_exprs.len());
            for expr in row_exprs {
                // Evaluate expression (e.g. literals, parameters)
                // Use empty row context as VALUES usually don't reference tables
                row.push(eval_ctx.evaluate(expr, &empty_row, schema).await?);
            }
            result.push(row);
        }
        Ok(result)
    }

    /// Execute recursive CTE query
    async fn execute_recursive_query(
        &self,
        node: &crate::planner::logical_plan::RecursiveQueryNode,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Vec<Row>> {
        // 1. Execute Anchor
        let anchor_rows = self.execute_plan(&node.anchor_plan, ctx).await?;
        
        // 2. Setup Working Table (Iterative)
        let mut working_table_rows = anchor_rows.clone();
        let mut total_rows = anchor_rows;
        
        let table_schema = crate::storage::TableSchema::new(node.cte_name.clone(), node.schema.columns().to_vec());
        
        // Iteration limit to prevent infinite loops (safety)
        let max_iterations = 100; 
        
        for _ in 0..max_iterations {
            if working_table_rows.is_empty() {
                break;
            }

            // Create a forked storage for this iteration
            let mut iter_storage = ctx.storage.fork().await?;
            iter_storage.create_table(table_schema.clone()).await?;
            
            let snapshot = ctx.snapshot.clone();
            
            for row in &working_table_rows {
                iter_storage.insert_row(&node.cte_name, row.clone(), &snapshot).await?;
            }
            
            let iter_ctx = ExecutionContext::new(
                &iter_storage,
                ctx.transaction_manager,
                ctx.persistence,
                snapshot.clone()
            ).with_params(ctx.params.clone());
            
            // Execute Recursive Term
            let new_rows = self.execute_plan(&node.recursive_plan, &iter_ctx).await?;
            
            if new_rows.is_empty() {
                break;
            }
            
            total_rows.extend(new_rows.clone());
            working_table_rows = new_rows;
        }
        
        // 3. Execute Final Query with Full CTE Table
        let mut final_storage = ctx.storage.fork().await?;
        final_storage.create_table(table_schema).await?;
        
        let snapshot = ctx.snapshot.clone();
        for row in &total_rows {
            final_storage.insert_row(&node.cte_name, row.clone(), &snapshot).await?;
        }
        
        let final_ctx = ExecutionContext::new(
            &final_storage,
            ctx.transaction_manager,
            ctx.persistence,
            snapshot
        ).with_params(ctx.params.clone());
        
        self.execute_plan(&node.final_plan, &final_ctx).await
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

    fn combine_rows(&self, left: &Row, right: &Row) -> Row {
        let mut combined = Row::with_capacity(left.len() + right.len());
        combined.extend_from_slice(left);
        combined.extend_from_slice(right);
        combined
    }

    fn extend_nulls(&self, row: &mut Row, count: usize) {
        row.reserve(count);
        for _ in 0..count {
            row.push(Value::Null);
        }
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

    fn is_constant_expression(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) | Expr::Parameter(_) => true,
            Expr::UnaryOp { expr, .. } => self.is_constant_expression(expr),
            Expr::BinaryOp { left, right, .. } => {
                self.is_constant_expression(left) && self.is_constant_expression(right)
            }
            Expr::Like { expr, pattern, .. } => {
                self.is_constant_expression(expr) && self.is_constant_expression(pattern)
            }
            Expr::Between { expr, low, high, .. } => {
                self.is_constant_expression(expr)
                    && self.is_constant_expression(low)
                    && self.is_constant_expression(high)
            }
            Expr::In { expr, list, .. } => {
                self.is_constant_expression(expr)
                    && list.iter().all(|item| self.is_constant_expression(item))
            }
            Expr::IsNull { expr, .. } => self.is_constant_expression(expr),
            Expr::Not { expr } => self.is_constant_expression(expr),
            Expr::Cast { expr, .. } => self.is_constant_expression(expr),
            Expr::Array(items) => items.iter().all(|item| self.is_constant_expression(item)),
            Expr::ArrayIndex { obj, index } => {
                self.is_constant_expression(obj) && self.is_constant_expression(index)
            }
            Expr::Function { .. }
            | Expr::Column(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Subquery(_)
            | Expr::InSubquery { .. }
            | Expr::Exists { .. } => false,
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

        for expr in expressions {
            if !self.is_aggregate_function(expr) && !self.is_constant_expression(expr) {
                return Err(DbError::ExecutionError(
                    "Non-aggregate expression used without GROUP BY".into(),
                ));
            }
        }

        let empty_row = Vec::new();
        let row_for_eval = rows.get(0).unwrap_or(&empty_row);
        
        let mut result_row = Vec::with_capacity(expressions.len());

        for expr in expressions {
            let value = match expr {
                Expr::Function { name, args, distinct, over: _ } => {
                    self.evaluate_aggregate(name, args, *distinct, rows, schema, &eval_ctx).await?
                }
                _ => {
                    // Constant expressions evaluated once
                    eval_ctx.evaluate(expr, row_for_eval, schema).await?
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
        distinct: bool,
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        match name.to_uppercase().as_str() {
            "COUNT" => self.aggregate_count(args, distinct, rows, schema, eval_ctx).await,
            "SUM" => self.aggregate_sum(args, distinct, rows, schema, eval_ctx).await,
            "AVG" => self.aggregate_avg(args, distinct, rows, schema, eval_ctx).await,
            "MIN" => self.aggregate_min(args, distinct, rows, schema, eval_ctx).await,
            "MAX" => self.aggregate_max(args, distinct, rows, schema, eval_ctx).await,
            _ => Err(DbError::UnsupportedOperation(format!(
                "Unknown aggregate function: {}",
                name
            ))),
        }
    }

    async fn aggregate_count(
        &self,
        args: &[Expr],
        distinct: bool,
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if args.is_empty() || matches!(args[0], Expr::Literal(Value::Text(ref s)) if s == "*") {
            if distinct {
                return Err(DbError::ExecutionError("COUNT(DISTINCT *) is not supported".into()));
            }
            return Ok(Value::Integer(rows.len() as i64));
        }

        let mut count = 0i64;
        
        if distinct {
            let mut seen = std::collections::HashSet::new();
            for row in rows {
                let val = eval_ctx.evaluate(&args[0], row, schema).await?;
                if !matches!(val, Value::Null) {
                    if seen.insert(JoinKey(val)) {
                        count += 1;
                    }
                }
            }
        } else {
            for row in rows {
                let val = eval_ctx.evaluate(&args[0], row, schema).await?;
                if !matches!(val, Value::Null) {
                    count += 1;
                }
            }
        }

        Ok(Value::Integer(count))
    }

    async fn aggregate_sum(
        &self,
        args: &[Expr],
        distinct: bool,
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
        let mut seen = if distinct { Some(std::collections::HashSet::new()) } else { None };

        for row in rows {
            let val = eval_ctx.evaluate(&args[0], row, schema).await?;
            
            if let Some(ref mut set) = seen {
                if !matches!(val, Value::Null) && !set.insert(JoinKey(val.clone())) {
                    continue;
                }
            }

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
        distinct: bool,
        rows: &[Row],
        schema: &Schema,
        eval_ctx: &EvaluationContext<'_>,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(DbError::ExecutionError("AVG requires an argument".into()));
        }

        let mut sum = 0.0f64;
        let mut count = 0usize;
        let mut seen = if distinct { Some(std::collections::HashSet::new()) } else { None };

        for row in rows {
            let val = eval_ctx.evaluate(&args[0], row, schema).await?;
            
            if let Some(ref mut set) = seen {
                if !matches!(val, Value::Null) && !set.insert(JoinKey(val.clone())) {
                    continue;
                }
            }

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
        _distinct: bool,
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
        _distinct: bool,
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
