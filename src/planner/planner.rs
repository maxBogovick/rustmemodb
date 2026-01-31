use crate::parser::ast::{Statement, QueryStmt, SelectItem, OrderByExpr, Expr, BinaryOp, TableWithJoins, TableFactor, JoinOperator, JoinConstraint};
use crate::storage::Catalog;
use crate::core::{DbError, Result, Value, Schema, Column};
use super::logical_plan::{LogicalPlan, TableScanNode, IndexScanInfo, IndexOp, SortNode, LimitNode, JoinNode, JoinType, ProjectionNode, FilterNode, AggregateNode};

/// Query planner - converts AST to LogicalPlan
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan(&self, stmt: &Statement, catalog: &Catalog) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => self.plan_query(query, catalog),
            _ => Err(DbError::UnsupportedOperation(
                "Only SELECT queries can be planned".into()
            )),
        }
    }

    fn plan_query(&self, query: &QueryStmt, catalog: &Catalog) -> Result<LogicalPlan> {
        // 1. Start with table scan / joins
        let mut plan = self.plan_from(&query.from, catalog)?;

        // 2. Apply WHERE clause
        if let Some(ref selection) = query.selection {
            plan = self.try_optimize_filter(plan, selection, catalog)?;
        }

        // 3. Apply GROUP BY / Aggregation
        let (has_aggr, aggr_exprs) = self.extract_aggregates(&query.projection, &query.having, &query.order_by);
        
        if !query.group_by.is_empty() || has_aggr {
            let schema = self.build_aggregate_schema(plan.schema(), &query.group_by, &aggr_exprs);
            
            plan = LogicalPlan::Aggregate(AggregateNode {
                input: Box::new(plan),
                group_exprs: query.group_by.clone(),
                aggr_exprs: aggr_exprs.clone(),
                schema,
            });
        }

        // 4. Apply HAVING clause
        if let Some(ref having) = query.having {
            let rewritten_having = self.rewrite_expression(having, &aggr_exprs);
            let schema = plan.schema().clone();
            plan = LogicalPlan::Filter(FilterNode {
                input: Box::new(plan),
                predicate: rewritten_having,
                schema,
            });
        }

        // 5. Apply ORDER BY
        if !query.order_by.is_empty() {
            let rewritten_order_by = query.order_by.iter().map(|ob| OrderByExpr {
                expr: self.rewrite_expression(&ob.expr, &aggr_exprs),
                descending: ob.descending,
            }).collect();
            
            let schema = plan.schema().clone();
            plan = LogicalPlan::Sort(SortNode {
                input: Box::new(plan),
                order_by: rewritten_order_by,
                schema,
            });
        }

        // 6. Apply Projection
        // Rewrite projection expressions to point to aggregate columns
        let mut rewritten_projection = query.projection.clone();
        for item in &mut rewritten_projection {
            if let SelectItem::Expr { expr, .. } = item {
                *expr = self.rewrite_expression(expr, &aggr_exprs);
            }
        }
        plan = self.plan_projection(plan, &rewritten_projection)?;

        // 7. Apply LIMIT
        if let Some(limit) = query.limit {
            let schema = plan.schema().clone();
            plan = LogicalPlan::Limit(LimitNode {
                input: Box::new(plan),
                limit,
                schema,
            });
        }

        Ok(plan)
    }

    fn rewrite_expression(&self, expr: &Expr, aggrs: &[Expr]) -> Expr {
        // If this expression matches one of the computed aggregates, replace with Column
        if let Some(pos) = aggrs.iter().position(|a| a == expr) {
            let col_name = self.format_aggregate(&aggrs[pos]);
            return Expr::Column(col_name);
        }

        // Otherwise recurse
        match expr {
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.rewrite_expression(left, aggrs)),
                op: *op,
                right: Box::new(self.rewrite_expression(right, aggrs)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.rewrite_expression(expr, aggrs)),
            },
            Expr::Not { expr } => Expr::Not {
                expr: Box::new(self.rewrite_expression(expr, aggrs)),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args.iter().map(|a| self.rewrite_expression(a, aggrs)).collect(),
            },
            // Other types (Literal, Column) don't need rewriting or are leaves
            _ => expr.clone(),
        }
    }

    fn format_aggregate(&self, expr: &Expr) -> String {
        match expr {
            Expr::Function { name, args } => {
                let arg_str = if args.is_empty() { "*".to_string() } else { "expr".to_string() };
                format!("{}({})", name, arg_str)
            }
            _ => "aggr".to_string(),
        }
    }

    fn extract_aggregates(&self, projection: &[SelectItem], having: &Option<Expr>, order_by: &[OrderByExpr]) -> (bool, Vec<Expr>) {
        let mut aggrs = Vec::new();
        let mut has_aggr = false;

        // Recursive extraction function
        fn collect_recursive(expr: &Expr, aggrs: &mut Vec<Expr>, has_aggr: &mut bool) {
            match expr {
                Expr::Function { name, args } => {
                    let name_upper = name.to_uppercase();
                    if matches!(name_upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                        *has_aggr = true;
                        if !aggrs.contains(expr) {
                            aggrs.push(expr.clone());
                        }
                        // Don't recurse into aggregate arguments (we don't support nested aggregates)
                        return; 
                    }
                    // Recurse into function args (e.g. ROUND(SUM(x))) - Wait, standard SQL allows this?
                    // Actually, usually you aggregate first then apply function. 
                    // But if it's FUNC(arg), we check arg.
                    for arg in args {
                        collect_recursive(arg, aggrs, has_aggr);
                    }
                }
                Expr::BinaryOp { left, right, .. } => {
                    collect_recursive(left, aggrs, has_aggr);
                    collect_recursive(right, aggrs, has_aggr);
                }
                Expr::UnaryOp { expr, .. } => collect_recursive(expr, aggrs, has_aggr),
                Expr::Not { expr } => collect_recursive(expr, aggrs, has_aggr),
                Expr::Like { expr: e, pattern, .. } => {
                    collect_recursive(e, aggrs, has_aggr);
                    collect_recursive(pattern, aggrs, has_aggr);
                }
                _ => {}
            }
        }

        for item in projection {
            if let SelectItem::Expr { expr, .. } = item {
                collect_recursive(expr, &mut aggrs, &mut has_aggr);
            }
        }
        
        if let Some(expr) = having {
            collect_recursive(expr, &mut aggrs, &mut has_aggr);
        }
        
        for item in order_by {
            collect_recursive(&item.expr, &mut aggrs, &mut has_aggr);
        }

        (has_aggr, aggrs)
    }

    fn build_aggregate_schema(&self, _input_schema: &Schema, group_by: &[Expr], aggrs: &[Expr]) -> Schema {
        let mut columns = Vec::new();
        
        // Group columns
        for expr in group_by {
            // In a real DB, we'd infer type. For MVP, Text.
            let name = format!("{}", expr); // Need Expr::Display or manual format
            columns.push(Column::new(name, crate::core::DataType::Text));
        }
        
        // Aggregate columns
        for expr in aggrs {
            // Aggregate result type depends on function. 
            // COUNT -> Integer, others -> depends on input.
            // For MVP, simplistic inference.
            let name = self.format_aggregate(expr);
            
            let data_type = if name.to_uppercase().starts_with("COUNT") {
                crate::core::DataType::Integer
            } else {
                crate::core::DataType::Float // Sum/Avg usually Float
            };
            
            columns.push(Column::new(name, data_type));
        }
        
        Schema::new(columns)
    }

    fn plan_from(&self, from: &[TableWithJoins], catalog: &Catalog) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Err(DbError::ParseError("No table in FROM clause".into()));
        }

        let mut plan = self.plan_table_with_joins(&from[0], catalog)?;

        for table in from.iter().skip(1) {
            let right = self.plan_table_with_joins(table, catalog)?;
            let schema = Schema::merge(plan.schema(), right.schema());
            
            plan = LogicalPlan::Join(JoinNode {
                left: Box::new(plan),
                right: Box::new(right),
                on: Expr::Literal(Value::Boolean(true)),
                join_type: JoinType::Cross,
                schema,
            });
        }

        Ok(plan)
    }

    fn plan_table_with_joins(&self, table: &TableWithJoins, catalog: &Catalog) -> Result<LogicalPlan> {
        let mut plan = self.plan_table_factor(&table.relation, catalog)?;

        for join in &table.joins {
            let right = self.plan_table_factor(&join.relation, catalog)?;
            let (join_type, on) = self.convert_join_operator(&join.join_operator)?;
            let schema = Schema::merge(plan.schema(), right.schema());

            plan = LogicalPlan::Join(JoinNode {
                left: Box::new(plan),
                right: Box::new(right),
                on,
                join_type,
                schema,
            });
        }

        Ok(plan)
    }

    fn plan_table_factor(&self, factor: &TableFactor, catalog: &Catalog) -> Result<LogicalPlan> {
        match factor {
            TableFactor::Table { name, alias } => {
                // Verify table exists
                let schema = catalog.get_table(name)?.schema().clone();
                let table_name = alias.as_ref().unwrap_or(name);
                
                // Qualify columns with table name/alias
                let qualified_schema = schema.qualify_columns(table_name);

                Ok(LogicalPlan::TableScan(TableScanNode {
                    table_name: name.clone(),
                    projected_columns: None,
                    index_scan: None,
                    schema: qualified_schema,
                }))
            }
            TableFactor::Derived { subquery, alias } => {
                let plan = self.plan_query(subquery, catalog)?;
                
                if let Some(alias_name) = alias {
                    let input_schema = plan.schema().clone();
                    let new_schema = input_schema.qualify_columns(alias_name);
                    
                    // Create identity projection with new schema names
                    let expressions: Vec<Expr> = input_schema.columns()
                        .iter()
                        .map(|c| Expr::Column(c.name.clone()))
                        .collect();
                        
                    Ok(LogicalPlan::Projection(ProjectionNode {
                        input: Box::new(plan),
                        expressions,
                        schema: new_schema,
                    }))
                } else {
                    Ok(plan)
                }
            }
        }
    }

    fn convert_join_operator(&self, op: &JoinOperator) -> Result<(JoinType, Expr)> {
        match op {
            JoinOperator::Inner(constraint) => Ok((JoinType::Inner, self.convert_join_constraint(constraint)?)),
            JoinOperator::LeftOuter(constraint) => Ok((JoinType::Left, self.convert_join_constraint(constraint)?)),
            JoinOperator::RightOuter(constraint) => Ok((JoinType::Right, self.convert_join_constraint(constraint)?)),
            JoinOperator::FullOuter(constraint) => Ok((JoinType::Full, self.convert_join_constraint(constraint)?)),
            JoinOperator::CrossJoin => Ok((JoinType::Cross, Expr::Literal(Value::Boolean(true)))),
        }
    }

    fn convert_join_constraint(&self, constraint: &JoinConstraint) -> Result<Expr> {
        match constraint {
            JoinConstraint::On(expr) => Ok(expr.clone()),
            JoinConstraint::None => Ok(Expr::Literal(Value::Boolean(true))),
        }
    }

    fn plan_projection(&self, input: LogicalPlan, projection: &[SelectItem]) -> Result<LogicalPlan> {
        // Check if it's SELECT *
        if projection.len() == 1 && matches!(projection[0], SelectItem::Wildcard) {
            return Ok(input); // No projection needed
        }

        // Extract expressions and build output schema
        let mut expressions = Vec::new();
        let mut columns = Vec::new();
        let input_schema = input.schema();

        for item in projection {
            match item {
                SelectItem::Wildcard => {
                    return Err(DbError::ParseError(
                        "Wildcard cannot be mixed with other columns".into()
                    ));
                }
                SelectItem::Expr { expr, alias } => {
                    expressions.push(expr.clone());
                    
                    // Infer column name and type
                    let name = alias.clone().unwrap_or_else(|| {
                        match expr {
                            Expr::Column(name) => name.clone(),
                            _ => format!("col_{}", columns.len()),
                        }
                    });
                    
                    // Simple type inference (fallback to Text if unknown)
                    // In a real DB, we would evaluate expression type
                    let data_type = if let Expr::Column(col_name) = expr {
                        input_schema.get_column(col_name)
                            .map(|c| c.data_type.clone())
                            .unwrap_or(crate::core::DataType::Text)
                    } else {
                        crate::core::DataType::Text
                    };

                    columns.push(Column::new(name, data_type));
                }
            }
        }

        Ok(LogicalPlan::Projection(ProjectionNode {
            input: Box::new(input),
            expressions,
            schema: Schema::new(columns),
        }))
    }

    /// Try to use an index for the WHERE clause
    fn try_optimize_filter(&self, input: LogicalPlan, predicate: &Expr, catalog: &Catalog) -> Result<LogicalPlan> {
        // Only optimize if input is a simple TableScan
        if let LogicalPlan::TableScan(ref scan) = input
            && let Expr::BinaryOp { left, op, right } = predicate {
                // Check for col = val
                if let (Expr::Column(col_name), BinaryOp::Eq, Expr::Literal(val)) = (&**left, op, &**right) {
                    let schema = catalog.get_table(&scan.table_name)?;
                    if schema.is_indexed(col_name) {
                        let mut new_scan = scan.clone();
                        new_scan.index_scan = Some(IndexScanInfo {
                            column: col_name.clone(),
                            value: val.clone(),
                            op: IndexOp::Eq,
                        });
                        return Ok(LogicalPlan::TableScan(new_scan));
                    }
                }
                
                // Check for val = col
                if let (Expr::Literal(val), BinaryOp::Eq, Expr::Column(col_name)) = (&**left, op, &**right) {
                    let schema = catalog.get_table(&scan.table_name)?;
                    if schema.is_indexed(col_name) {
                        let mut new_scan = scan.clone();
                        new_scan.index_scan = Some(IndexScanInfo {
                            column: col_name.clone(),
                            value: val.clone(),
                            op: IndexOp::Eq,
                        });
                        return Ok(LogicalPlan::TableScan(new_scan));
                    }
                }
            }

        // Fallback: Add Filter node
        let schema = input.schema().clone();
        Ok(LogicalPlan::Filter(FilterNode {
            input: Box::new(input),
            predicate: predicate.clone(),
            schema,
        }))
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}