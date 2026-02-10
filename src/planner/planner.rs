use super::logical_plan::{
    AggregateNode, DistinctNode, FilterNode, IndexOp, IndexScanInfo, JoinNode, JoinType, LimitNode,
    LogicalPlan, ProjectionNode, RecursiveQueryNode, SortNode, TableScanNode, ValuesNode,
    WindowNode,
};
use crate::core::{Column, DbError, Result, Schema, Value};
use crate::parser::ast::{
    BinaryOp, Expr, JoinConstraint, JoinOperator, OrderByExpr, QueryStmt, SelectItem, SetOperator,
    Statement, TableFactor, TableWithJoins,
};
use crate::storage::Catalog;

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
                "Only SELECT queries can be planned".into(),
            )),
        }
    }

    fn plan_query(&self, query: &QueryStmt, catalog: &Catalog) -> Result<LogicalPlan> {
        let mut local_catalog = catalog.clone();

        if let Some(ref with) = query.with {
            if with.recursive {
                // Recursive CTE handling (Single Recursive CTE support for MVP)
                for cte in &with.cte_tables {
                    if let Some(ref set_op) = cte.query.set_op {
                        if set_op.op == SetOperator::Union {
                            // Anchor term: Left side of UNION
                            let mut anchor_stmt = *cte.query.clone();
                            anchor_stmt.set_op = None; // Strip recursive part
                            anchor_stmt.with = None; // Strip WITH to avoid loops

                            let anchor_plan = self.plan_query(&anchor_stmt, catalog)?;

                            // Register CTE as Table in local catalog for Recursive term and Final Query
                            let cte_schema = anchor_plan.schema().clone();
                            let mut columns = cte_schema.columns().to_vec();
                            if !cte.columns.is_empty() {
                                if cte.columns.len() != columns.len() {
                                    return Err(DbError::ExecutionError(format!(
                                        "CTE '{}' column count mismatch: expected {}, got {}",
                                        cte.alias,
                                        cte.columns.len(),
                                        columns.len()
                                    )));
                                }
                                for (i, name) in cte.columns.iter().enumerate() {
                                    columns[i].name = name.clone();
                                }
                            }
                            let table_schema =
                                crate::storage::TableSchema::new(cte.alias.clone(), columns);

                            // Shadow existing table if any
                            if local_catalog.table_exists(&cte.alias) {
                                local_catalog = local_catalog.without_table(&cte.alias)?;
                            }
                            local_catalog = local_catalog.with_table(table_schema)?;

                            // Recursive term: Right side of UNION
                            let recursive_stmt = *set_op.right.clone();
                            // Recursive term uses local_catalog (which has CTE as table)
                            let recursive_plan =
                                self.plan_query(&recursive_stmt, &local_catalog)?;

                            // Final Query
                            let mut final_stmt = query.clone();
                            final_stmt.with = None; // Clear WITH

                            let final_plan = self.plan_query(&final_stmt, &local_catalog)?;
                            let schema = final_plan.schema().clone();

                            return Ok(LogicalPlan::RecursiveQuery(RecursiveQueryNode {
                                cte_name: cte.alias.clone(),
                                anchor_plan: Box::new(anchor_plan),
                                recursive_plan: Box::new(recursive_plan),
                                final_plan: Box::new(final_plan),
                                schema,
                            }));
                        }
                    }
                }
            } else {
                for cte in &with.cte_tables {
                    local_catalog = local_catalog.with_view(
                        cte.alias.clone(),
                        *cte.query.clone(),
                        cte.columns.clone(),
                    )?;
                }
            }
        }

        let catalog = &local_catalog;

        // 1. Start with table scan / joins
        let mut plan = if query.from.is_empty() {
            // Dummy relation with one empty row
            LogicalPlan::Values(ValuesNode {
                rows: vec![vec![]], // One row, no columns
                schema: Schema::new(vec![]),
            })
        } else {
            self.plan_from(&query.from, catalog)?
        };

        // 2. Apply WHERE clause
        if let Some(ref selection) = query.selection {
            plan = self.try_optimize_filter(plan, selection, catalog)?;
        }

        // 3. Apply GROUP BY / Aggregation
        let (has_aggr, aggr_exprs) =
            self.extract_aggregates(&query.projection, &query.having, &query.order_by);

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

        // 5. Apply Window Functions
        let (has_window, window_exprs) =
            self.extract_window_functions(&query.projection, &query.order_by);

        if has_window {
            let mut columns = plan.schema().columns().to_vec();
            for expr in &window_exprs {
                let name = format!("{}", expr);
                let data_type = if let Expr::Function { name, .. } = expr {
                    if matches!(
                        name.to_uppercase().as_str(),
                        "ROW_NUMBER" | "RANK" | "COUNT"
                    ) {
                        crate::core::DataType::Integer
                    } else {
                        crate::core::DataType::Float
                    }
                } else {
                    crate::core::DataType::Text
                };
                columns.push(Column::new(name, data_type));
            }
            let schema = Schema::new(columns);

            plan = LogicalPlan::Window(WindowNode {
                input: Box::new(plan),
                window_exprs: window_exprs.clone(),
                schema,
            });
        }

        // 6. Apply ORDER BY
        if !query.order_by.is_empty() {
            let rewritten_order_by = query
                .order_by
                .iter()
                .map(|ob| OrderByExpr {
                    expr: self.rewrite_window_expression(
                        &self.rewrite_expression(&ob.expr, &aggr_exprs),
                        &window_exprs,
                    ),
                    descending: ob.descending,
                })
                .collect();

            let schema = plan.schema().clone();
            plan = LogicalPlan::Sort(SortNode {
                input: Box::new(plan),
                order_by: rewritten_order_by,
                schema,
            });
        }

        // 7. Apply Projection
        // Rewrite projection expressions to point to aggregate/window columns
        let mut rewritten_projection = query.projection.clone();
        for item in &mut rewritten_projection {
            if let SelectItem::Expr { expr, .. } = item {
                let tmp = self.rewrite_expression(expr, &aggr_exprs);
                *expr = self.rewrite_window_expression(&tmp, &window_exprs);
            }
        }
        plan = self.plan_projection(plan, &rewritten_projection)?;

        // 8. Apply DISTINCT
        if query.distinct {
            let schema = plan.schema().clone();
            plan = LogicalPlan::Distinct(DistinctNode {
                input: Box::new(plan),
                schema,
            });
        }

        // 9. Apply LIMIT/OFFSET
        if query.limit.is_some() || query.offset.is_some() {
            let schema = plan.schema().clone();
            plan = LogicalPlan::Limit(LimitNode {
                input: Box::new(plan),
                limit: query.limit,
                offset: query.offset.unwrap_or(0),
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
            Expr::Function {
                name,
                args,
                distinct,
                over,
            } => Expr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| self.rewrite_expression(a, aggrs))
                    .collect(),
                distinct: *distinct,
                over: over.clone(),
            },
            // Other types (Literal, Column) don't need rewriting or are leaves
            _ => expr.clone(),
        }
    }

    fn format_aggregate(&self, expr: &Expr) -> String {
        match expr {
            Expr::Function {
                name,
                args,
                distinct,
                over,
            } => {
                let distinct_str = if *distinct { "DISTINCT " } else { "" };
                let arg_str = if args.is_empty() {
                    "*".to_string()
                } else {
                    args.iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let over_str = if over.is_some() { " OVER (...)" } else { "" };
                format!("{}({}{}){}", name, distinct_str, arg_str, over_str)
            }
            _ => "aggr".to_string(),
        }
    }

    fn extract_aggregates(
        &self,
        projection: &[SelectItem],
        having: &Option<Expr>,
        order_by: &[OrderByExpr],
    ) -> (bool, Vec<Expr>) {
        let mut aggrs = Vec::new();
        let mut has_aggr = false;

        // Recursive extraction function
        fn collect_recursive(expr: &Expr, aggrs: &mut Vec<Expr>, has_aggr: &mut bool) {
            match expr {
                Expr::Function { name, args, .. } => {
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
                Expr::Like {
                    expr: e, pattern, ..
                } => {
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

    fn build_aggregate_schema(
        &self,
        _input_schema: &Schema,
        group_by: &[Expr],
        aggrs: &[Expr],
    ) -> Schema {
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

    fn plan_table_with_joins(
        &self,
        table: &TableWithJoins,
        catalog: &Catalog,
    ) -> Result<LogicalPlan> {
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
                // Check if it's a view
                if let Some((view_query, view_columns)) = catalog.get_view(name) {
                    let mut plan = self.plan_query(view_query, catalog)?;

                    // Apply view column aliases if present
                    if !view_columns.is_empty() {
                        let input_schema = plan.schema().clone();
                        if input_schema.column_count() != view_columns.len() {
                            return Err(DbError::ExecutionError(format!(
                                "View '{}' column count mismatch: expected {}, got {}",
                                name,
                                view_columns.len(),
                                input_schema.column_count()
                            )));
                        }

                        let expressions: Vec<Expr> = input_schema
                            .columns()
                            .iter()
                            .map(|c| Expr::Column(c.name.clone()))
                            .collect();

                        let new_cols = view_columns
                            .iter()
                            .zip(input_schema.columns())
                            .map(|(alias, col)| Column::new(alias.clone(), col.data_type.clone()))
                            .collect();

                        plan = LogicalPlan::Projection(ProjectionNode {
                            input: Box::new(plan),
                            expressions,
                            schema: Schema::new(new_cols),
                        });
                    }

                    if let Some(alias_name) = alias {
                        let input_schema = plan.schema().clone();
                        let new_schema = input_schema.qualify_columns(alias_name);

                        // Create identity projection with new schema names
                        let expressions: Vec<Expr> = input_schema
                            .columns()
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
                } else {
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
            }
            TableFactor::Derived { subquery, alias } => {
                let plan = self.plan_query(subquery, catalog)?;

                if let Some(alias_name) = alias {
                    let input_schema = plan.schema().clone();
                    let new_schema = input_schema.qualify_columns(alias_name);

                    // Create identity projection with new schema names
                    let expressions: Vec<Expr> = input_schema
                        .columns()
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
            JoinOperator::Inner(constraint) => {
                Ok((JoinType::Inner, self.convert_join_constraint(constraint)?))
            }
            JoinOperator::LeftOuter(constraint) => {
                Ok((JoinType::Left, self.convert_join_constraint(constraint)?))
            }
            JoinOperator::RightOuter(constraint) => {
                Ok((JoinType::Right, self.convert_join_constraint(constraint)?))
            }
            JoinOperator::FullOuter(constraint) => {
                Ok((JoinType::Full, self.convert_join_constraint(constraint)?))
            }
            JoinOperator::CrossJoin => Ok((JoinType::Cross, Expr::Literal(Value::Boolean(true)))),
        }
    }

    fn convert_join_constraint(&self, constraint: &JoinConstraint) -> Result<Expr> {
        match constraint {
            JoinConstraint::On(expr) => Ok(expr.clone()),
            JoinConstraint::None => Ok(Expr::Literal(Value::Boolean(true))),
        }
    }

    fn plan_projection(
        &self,
        input: LogicalPlan,
        projection: &[SelectItem],
    ) -> Result<LogicalPlan> {
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
                        "Wildcard cannot be mixed with other columns".into(),
                    ));
                }
                SelectItem::Expr { expr, alias } => {
                    expressions.push(expr.clone());

                    // Infer column name and type
                    let name = alias.clone().unwrap_or_else(|| match expr {
                        Expr::Column(name) => name.clone(),
                        _ => format!("col_{}", columns.len()),
                    });

                    let data_type = self.infer_expr_type(expr, input_schema);

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
    fn try_optimize_filter(
        &self,
        input: LogicalPlan,
        predicate: &Expr,
        catalog: &Catalog,
    ) -> Result<LogicalPlan> {
        // Only optimize if input is a simple TableScan
        if let LogicalPlan::TableScan(ref scan) = input {
            let schema = catalog.get_table(&scan.table_name)?;

            match predicate {
                Expr::BinaryOp { left, op, right } => {
                    // Check for col op val/param
                    if let (Expr::Column(col_name), Expr::Literal(_) | Expr::Parameter(_)) =
                        (&**left, &**right)
                    {
                        if schema.is_indexed(col_name) {
                            let index_op = match op {
                                BinaryOp::Eq => Some(IndexOp::Eq),
                                BinaryOp::Gt => Some(IndexOp::Gt),
                                BinaryOp::GtEq => Some(IndexOp::GtEq),
                                BinaryOp::Lt => Some(IndexOp::Lt),
                                BinaryOp::LtEq => Some(IndexOp::LtEq),
                                _ => None,
                            };

                            if let Some(op) = index_op {
                                let mut new_scan = scan.clone();
                                new_scan.index_scan = Some(IndexScanInfo {
                                    column: col_name.clone(),
                                    value_expr: (**right).clone(),
                                    end_value_expr: None,
                                    op,
                                });
                                return Ok(LogicalPlan::TableScan(new_scan));
                            }
                        }
                    }

                    // Check for val op col
                    if let (Expr::Literal(_) | Expr::Parameter(_), Expr::Column(col_name)) =
                        (&**left, &**right)
                    {
                        if schema.is_indexed(col_name) {
                            let index_op = match op {
                                BinaryOp::Eq => Some(IndexOp::Eq),
                                BinaryOp::Gt => Some(IndexOp::Lt), // val > col <=> col < val
                                BinaryOp::GtEq => Some(IndexOp::LtEq),
                                BinaryOp::Lt => Some(IndexOp::Gt),
                                BinaryOp::LtEq => Some(IndexOp::GtEq),
                                _ => None,
                            };

                            if let Some(op) = index_op {
                                let mut new_scan = scan.clone();
                                new_scan.index_scan = Some(IndexScanInfo {
                                    column: col_name.clone(),
                                    value_expr: (**left).clone(),
                                    end_value_expr: None,
                                    op,
                                });
                                return Ok(LogicalPlan::TableScan(new_scan));
                            }
                        }
                    }
                }
                Expr::Between {
                    expr,
                    low,
                    high,
                    negated,
                } => {
                    if !*negated {
                        if let (
                            Expr::Column(col_name),
                            Expr::Literal(_) | Expr::Parameter(_),
                            Expr::Literal(_) | Expr::Parameter(_),
                        ) = (&**expr, &**low, &**high)
                        {
                            if schema.is_indexed(col_name) {
                                let mut new_scan = scan.clone();
                                new_scan.index_scan = Some(IndexScanInfo {
                                    column: col_name.clone(),
                                    value_expr: (**low).clone(),
                                    end_value_expr: Some((**high).clone()),
                                    op: IndexOp::Between,
                                });
                                return Ok(LogicalPlan::TableScan(new_scan));
                            }
                        }
                    }
                }
                _ => {}
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

    fn extract_window_functions(
        &self,
        projection: &[SelectItem],
        order_by: &[OrderByExpr],
    ) -> (bool, Vec<Expr>) {
        let mut wins = Vec::new();
        let mut has_win = false;

        fn collect(expr: &Expr, wins: &mut Vec<Expr>, has_win: &mut bool) {
            match expr {
                Expr::Function { over: Some(_), .. } => {
                    *has_win = true;
                    if !wins.contains(expr) {
                        wins.push(expr.clone());
                    }
                }
                Expr::BinaryOp { left, right, .. } => {
                    collect(left, wins, has_win);
                    collect(right, wins, has_win);
                }
                Expr::UnaryOp { expr, .. } => collect(expr, wins, has_win),
                Expr::Not { expr } => collect(expr, wins, has_win),
                _ => {}
            }
        }

        for item in projection {
            if let SelectItem::Expr { expr, .. } = item {
                collect(expr, &mut wins, &mut has_win);
            }
        }
        for item in order_by {
            collect(&item.expr, &mut wins, &mut has_win);
        }

        (has_win, wins)
    }

    fn rewrite_window_expression(&self, expr: &Expr, wins: &[Expr]) -> Expr {
        if let Some(pos) = wins.iter().position(|w| w == expr) {
            let col_name = format!("{}", wins[pos]);
            return Expr::Column(col_name);
        }

        match expr {
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.rewrite_window_expression(left, wins)),
                op: *op,
                right: Box::new(self.rewrite_window_expression(right, wins)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.rewrite_window_expression(expr, wins)),
            },
            Expr::Not { expr } => Expr::Not {
                expr: Box::new(self.rewrite_window_expression(expr, wins)),
            },
            Expr::Function {
                name,
                args,
                distinct,
                over,
            } => Expr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| self.rewrite_window_expression(a, wins))
                    .collect(),
                distinct: *distinct,
                over: over.clone(),
            },
            _ => expr.clone(),
        }
    }

    fn infer_expr_type(&self, expr: &Expr, schema: &Schema) -> crate::core::DataType {
        use crate::core::DataType;
        match expr {
            Expr::Column(name) => schema
                .get_column(name)
                .map(|c| c.data_type.clone())
                .unwrap_or(DataType::Text),
            Expr::Literal(val) => match val {
                Value::Integer(_) => DataType::Integer,
                Value::Float(_) => DataType::Float,
                Value::Boolean(_) => DataType::Boolean,
                Value::Timestamp(_) => DataType::Timestamp,
                Value::Date(_) => DataType::Date,
                Value::Uuid(_) => DataType::Uuid,
                Value::Json(_) => DataType::Json,
                _ => DataType::Text,
            },
            Expr::BinaryOp { left, .. } => self.infer_expr_type(left, schema),
            Expr::UnaryOp { expr, .. } => self.infer_expr_type(expr, schema),
            Expr::Function { name, .. } => match name.to_uppercase().as_str() {
                "COUNT" | "ROW_NUMBER" | "RANK" | "LENGTH" => DataType::Integer,
                "SUM" | "AVG" => DataType::Float,
                "NOW" => DataType::Timestamp,
                _ => DataType::Text,
            },
            _ => DataType::Text,
        }
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}
