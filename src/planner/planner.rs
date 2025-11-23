use crate::parser::ast::{Statement, QueryStmt, SelectItem};
use crate::storage::Catalog;
use crate::core::{DbError, Result};
use super::logical_plan::*;

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
        // Start with table scan
        let mut plan = self.plan_from(&query.from, catalog)?;

        // Apply WHERE clause
        if let Some(ref selection) = query.selection {
            plan = LogicalPlan::Filter(FilterNode {
                input: Box::new(plan),
                predicate: selection.clone(),
            });
        }

        // Apply projection
        plan = self.plan_projection(plan, &query.projection)?;

        // Apply ORDER BY
        if !query.order_by.is_empty() {
            let sort_keys = query
                .order_by
                .iter()
                .map(|order| (order.expr.clone(), order.descending))
                .collect();

            plan = LogicalPlan::Sort(SortNode {
                input: Box::new(plan),
                sort_keys,
            });
        }

        // Apply LIMIT
        if let Some(limit) = query.limit {
            plan = LogicalPlan::Limit(LimitNode {
                input: Box::new(plan),
                limit,
            });
        }

        Ok(plan)
    }

    fn plan_from(&self, from: &[crate::parser::ast::TableRef], catalog: &Catalog) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Err(DbError::ParseError("No table in FROM clause".into()));
        }

        if from.len() > 1 {
            return Err(DbError::UnsupportedOperation(
                "Multi-table queries not yet supported".into()
            ));
        }

        let table_ref = &from[0];

        // Verify table exists
        if !catalog.table_exists(&table_ref.name) {
            return Err(DbError::TableNotFound(table_ref.name.clone()));
        }

        Ok(LogicalPlan::TableScan(TableScanNode {
            table_name: table_ref.name.clone(),
            projected_columns: None,
        }))
    }

    fn plan_projection(&self, input: LogicalPlan, projection: &[SelectItem]) -> Result<LogicalPlan> {
        // Check if it's SELECT *
        if projection.len() == 1 && matches!(projection[0], SelectItem::Wildcard) {
            return Ok(input); // No projection needed
        }

        // Extract expressions
        let mut expressions = Vec::new();
        for item in projection {
            match item {
                SelectItem::Wildcard => {
                    return Err(DbError::ParseError(
                        "Wildcard cannot be mixed with other columns".into()
                    ));
                }
                SelectItem::Expr { expr, .. } => {
                    expressions.push(expr.clone());
                }
            }
        }

        Ok(LogicalPlan::Projection(ProjectionNode {
            input: Box::new(input),
            expressions,
        }))
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}