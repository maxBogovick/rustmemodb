use crate::core::{Value, DataType};

/// Root statement type
#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Insert(InsertStmt),
    Query(QueryStmt),
    Delete(DeleteStmt),
    Update(UpdateStmt),
    CreateIndex(CreateIndexStmt),
    AlterTable(AlterTableStmt),
    Begin,
    Commit,
    Rollback,
}

/// CREATE TABLE statement
#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
}

/// CREATE INDEX statement
#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    pub column: String, // Currently only single column index supported
    pub if_not_exists: bool,
    pub unique: bool,
}

/// ALTER TABLE statement
#[derive(Debug, Clone)]
pub struct AlterTableStmt {
    pub table_name: String,
    pub operation: AlterTableOperation,
}

#[derive(Debug, Clone)]
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
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
    pub unique: bool,
}

/// INSERT statement
#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table_name: String,
    pub columns: Option<Vec<String>>, // None = all columns
    pub values: Vec<Vec<Expr>>,
}

/// SELECT query statement
#[derive(Debug, Clone)]
pub struct QueryStmt {
    pub projection: Vec<SelectItem>,
    pub from: Vec<TableWithJoins>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone)]
pub enum TableFactor {
    Table { name: String, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

#[derive(Debug, Clone)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
}

#[derive(Debug, Clone)]
pub enum JoinConstraint {
    On(Expr),
    None,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard,
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub descending: bool,
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
pub enum Expr {
    /// Column reference
    Column(String),

    /// Compound identifier (e.g. table.column)
    CompoundIdentifier(Vec<String>),

    /// Literal value
    Literal(Value),

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

            Expr::IsNull { expr, negated } => {

                write!(f, "{} IS {}NULL", expr, if *negated { "NOT " } else { "" })

            }

            Expr::Not { expr } => write!(f, "NOT {}", expr),

            Expr::Function { name, args } => {

                let args_str: Vec<String> = args.iter().map(|e| format!("{}", e)).collect();

                write!(f, "{}({})", name, args_str.join(", "))

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
