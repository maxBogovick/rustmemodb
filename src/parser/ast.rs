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
}

/// CREATE TABLE statement
#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
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
    pub from: Vec<TableRef>,
    pub selection: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard,
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
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
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column reference
    Column(String),

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