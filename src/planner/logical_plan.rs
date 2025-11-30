use crate::parser::ast::{Expr, OrderByExpr};
use crate::core::Schema;

/// Logical plan nodes - high-level operations
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan a table
    TableScan(TableScanNode),

    /// Filter rows
    Filter(FilterNode),

    /// Project columns
    Projection(ProjectionNode),

    /// Sort rows
    Sort(SortNode),

    /// Limit rows
    Limit(LimitNode),
}

#[derive(Debug, Clone)]
pub struct TableScanNode {
    pub table_name: String,
    pub projected_columns: Option<Vec<String>>, // None = all columns
}

#[derive(Debug, Clone)]
pub struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicate: Expr,
}

#[derive(Debug, Clone)]
pub struct ProjectionNode {
    pub input: Box<LogicalPlan>,
    pub expressions: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct SortNode {
    pub input: Box<LogicalPlan>,
    /// ORDER BY expressions - использует OrderByExpr из AST для консистентности
    pub order_by: Vec<OrderByExpr>,
}

impl SortNode {
    /// Создать из tuple формата (для обратной совместимости)
    pub fn from_tuples(input: Box<LogicalPlan>, sort_keys: Vec<(Expr, bool)>) -> Self {
        Self {
            input,
            order_by: sort_keys
                .into_iter()
                .map(|(expr, descending)| OrderByExpr { expr, descending })
                .collect(),
        }
    }

    /// Получить как tuples (для обратной совместимости)
    pub fn as_tuples(&self) -> Vec<(&Expr, bool)> {
        self.order_by
            .iter()
            .map(|o| (&o.expr, o.descending))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct LimitNode {
    pub input: Box<LogicalPlan>,
    pub limit: usize,
}

impl LogicalPlan {
    /// Get the output schema of this plan
    pub fn schema(&self) -> Option<&Schema> {
        // TODO: Implement schema calculation
        None
    }

    /// Get child plans
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::TableScan(_) => vec![],
            LogicalPlan::Filter(node) => vec![&*node.input],
            LogicalPlan::Projection(node) => vec![&*node.input],
            LogicalPlan::Sort(node) => vec![&*node.input],
            LogicalPlan::Limit(node) => vec![&*node.input],
        }
    }
}