use crate::core::{Value, DataType, ForeignKey};

/// Root statement type
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Insert(InsertStmt),
    Query(QueryStmt),
    Delete(DeleteStmt),
    Update(UpdateStmt),
    CreateIndex(CreateIndexStmt),
    AlterTable(AlterTableStmt),
    CreateView(CreateViewStmt),
    DropView(DropViewStmt),
    Explain(ExplainStmt),
    Begin,
    Commit,
    Rollback,
}

/// CREATE VIEW statement
#[derive(Debug, Clone)]
pub struct CreateViewStmt {
    pub name: String,
    pub query: Box<QueryStmt>,
    pub or_replace: bool,
}

/// DROP VIEW statement
#[derive(Debug, Clone)]
pub struct DropViewStmt {
    pub name: String,
    pub if_exists: bool,
}

/// EXPLAIN statement
#[derive(Debug, Clone)]
pub struct ExplainStmt {
    pub statement: Box<Statement>,
    pub analyze: bool, // Future: run and time it?
}

/// CREATE TABLE statement
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
}

/// CREATE INDEX statement
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    pub column: String, // Currently only single column index supported
    pub if_not_exists: bool,
    pub unique: bool,
}

/// ALTER TABLE statement
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AlterTableStmt {
    pub table_name: String,
    pub operation: AlterTableOperation,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum AlterTableOperation {
    AddColumn(ColumnDef),
    DropColumn(String),
    RenameColumn { old_name: String, new_name: String },
    RenameTable(String),
}

/// DROP TABLE statement
#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub table_name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
    pub unique: bool,
    pub references: Option<ForeignKey>,
}

/// INSERT statement
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct InsertStmt {
    pub table_name: String,
    pub columns: Option<Vec<String>>, // None = all columns
    pub values: Vec<Vec<Expr>>,
}

/// SELECT query statement
#[derive(Debug, Clone, PartialEq)]
pub struct QueryStmt {
    pub with: Option<With>,
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: Vec<TableWithJoins>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct With {
    pub recursive: bool,
    pub cte_tables: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub alias: String,
    pub query: Box<QueryStmt>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableFactor {
    Table { name: String, alias: Option<String> },
    Derived { subquery: Box<QueryStmt>, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

/// DELETE statement
#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table_name: String,
    pub selection: Option<Expr>,
}

/// UPDATE statement
#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

/// Expression types
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum Expr {
    /// Column reference
    Column(String),

    /// Compound identifier (e.g. table.column)
    CompoundIdentifier(Vec<String>),

    /// Literal value
    Literal(Value),

    /// Parameter placeholder ($1, $2, etc.)
    Parameter(usize),

    /// Binary operation (a + b, a = b, etc.)
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    /// Unary operation (NOT x, -x)
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    /// LIKE pattern matching
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
    },

    /// BETWEEN range check
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    /// IN list check
    In {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    /// IN subquery check
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<QueryStmt>,
        negated: bool,
    },

    /// Scalar subquery
    Subquery(Box<QueryStmt>),

    /// EXISTS check
    Exists {
        subquery: Box<QueryStmt>,
        negated: bool,
    },

    /// IS NULL check
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    
    Not {
        expr: Box<Expr>,
    },

    /// Function call (future: COUNT, SUM, etc.)
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
        over: Option<WindowSpec>,
    },
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,

    // Logical
    And,
    Or,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum UnaryOp {
    Not,
    Minus,
    Plus,
}

use std::fmt;



impl fmt::Display for Expr {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {

        match self {

            Expr::Column(name) => write!(f, "{}", name),

            Expr::CompoundIdentifier(parts) => write!(f, "{}", parts.join(".")),

            Expr::Literal(val) => write!(f, "{}", val),

            Expr::Parameter(idx) => write!(f, "${}", idx),

            Expr::BinaryOp { left, op, right } => write!(f, "({} {} {})", left, op, right),

            Expr::UnaryOp { op, expr } => write!(f, "{}{}", op, expr),

            Expr::Like { expr, pattern, negated, .. } => {

                write!(f, "{} {}LIKE {}", expr, if *negated { "NOT " } else { "" }, pattern)

            }

            Expr::Between { expr, low, high, negated } => {

                write!(f, "{} {}BETWEEN {} AND {}", expr, if *negated { "NOT " } else { "" }, low, high)

            }

            Expr::In { expr, list, negated } => {

                let list_str: Vec<String> = list.iter().map(|e| format!("{}", e)).collect();

                write!(f, "{} {}IN ({})", expr, if *negated { "NOT " } else { "" }, list_str.join(", "))

            }

            Expr::InSubquery { expr, subquery: _, negated } => {
                write!(f, "{} {}IN (SUBQUERY)", expr, if *negated { "NOT " } else { "" })
            }

            Expr::Subquery(_) => write!(f, "(SUBQUERY)"),

            Expr::Exists { subquery: _, negated } => write!(f, "{}EXISTS (SUBQUERY)", if *negated { "NOT " } else { "" }),

            Expr::IsNull { expr, negated } => {

                write!(f, "{} IS {}NULL", expr, if *negated { "NOT " } else { "" })

            }

            Expr::Not { expr } => write!(f, "NOT {}", expr),

            Expr::Function { name, args, distinct, over } => {

                let args_str: Vec<String> = args.iter().map(|e| format!("{}", e)).collect();
                let over_str = if over.is_some() { " OVER (...)" } else { "" };

                write!(f, "{}({}{}){}", name, if *distinct { "DISTINCT " } else { "" }, args_str.join(", "), over_str)

            }

        }

    }

}



impl fmt::Display for BinaryOp {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {

        match self {

            BinaryOp::Add => write!(f, "+"),

            BinaryOp::Subtract => write!(f, "-"),

            BinaryOp::Multiply => write!(f, "*"),

            BinaryOp::Divide => write!(f, "/"),

            BinaryOp::Modulo => write!(f, "%"),

            BinaryOp::Eq => write!(f, "="),

            BinaryOp::NotEq => write!(f, "!="),

            BinaryOp::Lt => write!(f, "<"),

            BinaryOp::LtEq => write!(f, "<="),

            BinaryOp::Gt => write!(f, ">"),

            BinaryOp::GtEq => write!(f, ">="),

            BinaryOp::And => write!(f, "AND"),

            BinaryOp::Or => write!(f, "OR"),

        }

    }

}



impl fmt::Display for UnaryOp {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {

        match self {

            UnaryOp::Not => write!(f, "NOT"),

            UnaryOp::Minus => write!(f, "-"),

            UnaryOp::Plus => write!(f, "+"),

        }

    }

}
