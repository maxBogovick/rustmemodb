use crate::core::Schema;
use crate::parser::ast::{Expr, OrderByExpr};

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

    /// Join tables
    Join(JoinNode),

    /// Aggregate rows (GROUP BY)
    Aggregate(AggregateNode),

    /// Distinct rows
    Distinct(DistinctNode),

    /// Window functions
    Window(WindowNode),

    /// Constant values (e.g. SELECT 1)
    Values(ValuesNode),

    /// Recursive Query (WITH RECURSIVE)
    RecursiveQuery(RecursiveQueryNode),
}

#[derive(Debug, Clone)]
pub struct RecursiveQueryNode {
    pub cte_name: String,
    pub anchor_plan: Box<LogicalPlan>,
    pub recursive_plan: Box<LogicalPlan>,
    pub final_plan: Box<LogicalPlan>, // The main query using the CTE
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct TableScanNode {
    pub table_name: String,
    #[allow(dead_code)]
    pub projected_columns: Option<Vec<String>>, // None = all columns
    pub index_scan: Option<IndexScanInfo>,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct ValuesNode {
    pub rows: Vec<Vec<crate::parser::ast::Expr>>, // Expressions to evaluate to produce rows
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct IndexScanInfo {
    pub column: String,
    pub value_expr: Expr,
    pub end_value_expr: Option<Expr>,
    #[allow(dead_code)]
    pub op: IndexOp,
}

#[derive(Debug, Clone)]
pub enum IndexOp {
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    Between,
}

#[derive(Debug, Clone)]
pub struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicate: Expr,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct ProjectionNode {
    pub input: Box<LogicalPlan>,
    pub expressions: Vec<Expr>,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct SortNode {
    pub input: Box<LogicalPlan>,
    /// ORDER BY expressions - использует OrderByExpr из AST для консистентности
    pub order_by: Vec<OrderByExpr>,
    pub schema: Schema,
}

impl SortNode {
    /// Создать из tuple формата (для обратной совместимости)
    #[allow(dead_code)]
    pub fn from_tuples(input: Box<LogicalPlan>, sort_keys: Vec<(Expr, bool)>) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            order_by: sort_keys
                .into_iter()
                .map(|(expr, descending)| OrderByExpr { expr, descending })
                .collect(),
            schema,
        }
    }

    /// Получить как tuples (для обратной совместимости)
    #[allow(dead_code)]
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
    pub limit: Option<usize>,
    pub offset: usize,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_exprs: Vec<Expr>,
    pub aggr_exprs: Vec<Expr>,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub on: Expr,
    pub join_type: JoinType,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct DistinctNode {
    pub input: Box<LogicalPlan>,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
    pub input: Box<LogicalPlan>,
    pub window_exprs: Vec<Expr>,
    pub schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl LogicalPlan {
    /// Get the output schema of this plan
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::TableScan(node) => &node.schema,
            LogicalPlan::Filter(node) => &node.schema,
            LogicalPlan::Projection(node) => &node.schema,
            LogicalPlan::Sort(node) => &node.schema,
            LogicalPlan::Limit(node) => &node.schema,
            LogicalPlan::Join(node) => &node.schema,
            LogicalPlan::Aggregate(node) => &node.schema,
            LogicalPlan::Distinct(node) => &node.schema,
            LogicalPlan::Window(node) => &node.schema,
            LogicalPlan::Values(node) => &node.schema,
            LogicalPlan::RecursiveQuery(node) => &node.schema,
        }
    }

    /// Get child plans
    #[allow(dead_code)]
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::TableScan(_) => vec![],
            LogicalPlan::Filter(node) => vec![&*node.input],
            LogicalPlan::Projection(node) => vec![&*node.input],
            LogicalPlan::Sort(node) => vec![&*node.input],
            LogicalPlan::Limit(node) => vec![&*node.input],
            LogicalPlan::Join(node) => vec![&*node.left, &*node.right],
            LogicalPlan::Aggregate(node) => vec![&*node.input],
            LogicalPlan::Distinct(node) => vec![&*node.input],
            LogicalPlan::Window(node) => vec![&*node.input],
            LogicalPlan::Values(_) => vec![],
            LogicalPlan::RecursiveQuery(node) => {
                vec![&*node.anchor_plan, &*node.recursive_plan, &*node.final_plan]
            }
        }
    }
}
