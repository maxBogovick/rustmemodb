// Cargo.toml dependencies:
// sqlparser = "0.59.0"
// thiserror = "1.0"
// regex = "1.10"

pub mod core;
pub mod storage;
pub mod result;
pub mod facade;

use sqlparser::ast::{
    BinaryOperator, ColumnDef, DataType as SqlDataType, Expr, Ident, ObjectName, Select,
    SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;

// ============================================================================
// ERROR HANDLING - Comprehensive error types with context
// ============================================================================

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Table '{0}' already exists")]
    TableExists(String),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
}

pub type Result<T> = std::result::Result<T, DbError>;

// ============================================================================
// VALUE SYSTEM - Enhanced with arithmetic operations
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Integer(_) => "integer",
            Value::Float(_) => "float",
            Value::Text(_) => "text",
            Value::Boolean(_) => "boolean",
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Boolean(b) => *b,
            Value::Integer(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::Text(s) => !s.is_empty(),
        }
    }

    /// Coerce numeric types for operations
    fn coerce_numeric(self, other: Value) -> Result<(NumericValue, NumericValue)> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => {
                Ok((NumericValue::Integer(a), NumericValue::Integer(b)))
            }
            (Value::Float(a), Value::Float(b)) => {
                Ok((NumericValue::Float(a), NumericValue::Float(b)))
            }
            (Value::Integer(a), Value::Float(b)) => {
                Ok((NumericValue::Float(a as f64), NumericValue::Float(b)))
            }
            (Value::Float(a), Value::Integer(b)) => {
                Ok((NumericValue::Float(a), NumericValue::Float(b as f64)))
            }
            (a, b) => Err(DbError::TypeMismatch(format!(
                "Cannot coerce {} and {} to numeric types",
                a.type_name(), b.type_name()
            ))),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Text(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
        }
    }
}

/// Helper enum for numeric operations
#[derive(Debug, Clone, Copy)]
enum NumericValue {
    Integer(i64),
    Float(f64),
}

impl NumericValue {
    fn into_value(self) -> Value {
        match self {
            NumericValue::Integer(i) => Value::Integer(i),
            NumericValue::Float(f) => Value::Float(f),
        }
    }
}

// ============================================================================
// COMPARISON - Clean comparison interface
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl ComparisonOp {
    pub fn from_binary_op(op: &BinaryOperator) -> Result<Self> {
        match op {
            BinaryOperator::Eq => Ok(ComparisonOp::Eq),
            BinaryOperator::NotEq => Ok(ComparisonOp::Ne),
            BinaryOperator::Lt => Ok(ComparisonOp::Lt),
            BinaryOperator::LtEq => Ok(ComparisonOp::Le),
            BinaryOperator::Gt => Ok(ComparisonOp::Gt),
            BinaryOperator::GtEq => Ok(ComparisonOp::Ge),
            _ => Err(DbError::UnsupportedOperation(format!("Operator: {:?}", op))),
        }
    }

    pub fn apply(&self, left: &Value, right: &Value) -> Result<bool> {
        match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Ok(false),

            // Integer comparisons
            (Value::Integer(a), Value::Integer(b)) => Ok(self.compare_ord(a, b)),

            // Float comparisons
            (Value::Float(a), Value::Float(b)) => Ok(self.compare_float(*a, *b)),

            // Mixed numeric - coerce to float
            (Value::Integer(a), Value::Float(b)) => Ok(self.compare_float(*a as f64, *b)),
            (Value::Float(a), Value::Integer(b)) => Ok(self.compare_float(*a, *b as f64)),

            // Text comparisons
            (Value::Text(a), Value::Text(b)) => Ok(self.compare_ord(a, b)),

            // Boolean comparisons (only equality)
            (Value::Boolean(a), Value::Boolean(b)) => match self {
                ComparisonOp::Eq => Ok(a == b),
                ComparisonOp::Ne => Ok(a != b),
                _ => Err(DbError::TypeMismatch(
                    "Booleans only support equality operators".into()
                )),
            },

            _ => Err(DbError::TypeMismatch(format!(
                "Cannot compare {} with {}",
                left.type_name(), right.type_name()
            ))),
        }
    }

    fn compare_ord<T: Ord>(&self, a: &T, b: &T) -> bool {
        match self {
            ComparisonOp::Eq => a == b,
            ComparisonOp::Ne => a != b,
            ComparisonOp::Lt => a < b,
            ComparisonOp::Le => a <= b,
            ComparisonOp::Gt => a > b,
            ComparisonOp::Ge => a >= b,
        }
    }

    fn compare_float(&self, a: f64, b: f64) -> bool {
        match self {
            ComparisonOp::Eq => (a - b).abs() < f64::EPSILON,
            ComparisonOp::Ne => (a - b).abs() >= f64::EPSILON,
            ComparisonOp::Lt => a < b,
            ComparisonOp::Le => a <= b,
            ComparisonOp::Gt => a > b,
            ComparisonOp::Ge => a >= b,
        }
    }
}

// ============================================================================
// PATTERN MATCHING - LIKE operator support
// ============================================================================

/// Convert SQL LIKE pattern to regex pattern
fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' if i + 1 < chars.len() => {
                // Escape sequence
                i += 1;
                regex.push_str(&regex::escape(&chars[i].to_string()));
            }
            c if ".*+?^${}()|[]\\".contains(c) => {
                // Escape regex special characters
                regex.push('\\');
                regex.push(c);
            }
            c => regex.push(c),
        }
        i += 1;
    }

    regex.push('$');
    regex
}

/// Evaluate LIKE pattern matching
pub fn eval_like(text: &str, pattern: &str, case_sensitive: bool) -> Result<bool> {
    let regex_pattern = like_to_regex(pattern);

    let result = if case_sensitive {
        regex::Regex::new(&regex_pattern)
            .map_err(|e| DbError::ExecutionError(format!("Invalid LIKE pattern: {}", e)))?
            .is_match(text)
    } else {
        regex::RegexBuilder::new(&regex_pattern)
            .case_insensitive(true)
            .build()
            .map_err(|e| DbError::ExecutionError(format!("Invalid LIKE pattern: {}", e)))?
            .is_match(text)
    };

    Ok(result)
}

// ============================================================================
// SCHEMA - Enhanced type system
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}

impl DataType {
    pub fn from_sql(sql_type: &SqlDataType) -> Result<Self> {
        match sql_type {
            SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::BigInt(_) => {
                Ok(DataType::Integer)
            }
            SqlDataType::Float(_) | SqlDataType::Double(_) | SqlDataType::Real => {
                Ok(DataType::Float)
            }
            SqlDataType::Text
            | SqlDataType::Varchar(_)
            | SqlDataType::Char(_)
            | SqlDataType::String(_) => Ok(DataType::Text),
            SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),
            _ => Err(DbError::TypeMismatch(format!(
                "Unsupported SQL type: {:?}",
                sql_type
            ))),
        }
    }

    pub fn is_compatible(&self, value: &Value) -> bool {
        match (self, value) {
            (_, Value::Null) => true,
            (DataType::Integer, Value::Integer(_)) => true,
            (DataType::Float, Value::Float(_)) => true,
            (DataType::Text, Value::Text(_)) => true,
            (DataType::Boolean, Value::Boolean(_)) => true,
            _ => false,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
        }
    }
}

// ============================================================================
// COLUMN - Enhanced with builder pattern support
// ============================================================================

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn validate(&self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            if !self.nullable {
                return Err(DbError::ConstraintViolation(format!(
                    "Column '{}' cannot be NULL",
                    self.name
                )));
            }
            return Ok(());
        }

        if !self.data_type.is_compatible(value) {
            return Err(DbError::TypeMismatch(format!(
                "Column '{}' expects type {}, got {}",
                self.name,
                self.data_type,
                value.type_name()
            )));
        }

        Ok(())
    }
}

pub type Row = Vec<Value>;

// ============================================================================
// TABLE - Enhanced with better encapsulation
// ============================================================================

#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Row>,
}

impl Table {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            columns,
            rows: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn insert(&mut self, row: Row) -> Result<()> {
        self.validate_row(&row)?;
        self.rows.push(row);
        Ok(())
    }

    fn validate_row(&self, row: &Row) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(DbError::ExecutionError(format!(
                "Expected {} columns, got {}",
                self.columns.len(),
                row.len()
            )));
        }

        for (column, value) in self.columns.iter().zip(row.iter()) {
            column.validate(value)?;
        }

        Ok(())
    }

    pub fn find_column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|col| col.name == name)
            .ok_or_else(|| DbError::ColumnNotFound(name.to_string(), self.name.clone()))
    }

    pub fn get_column(&self, name: &str) -> Result<&Column> {
        let idx = self.find_column_index(name)?;
        Ok(&self.columns[idx])
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

// ============================================================================
// QUERY RESULT - Enhanced display
// ============================================================================

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self { columns, rows }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn print(&self) {
        if self.columns.is_empty() {
            println!("Empty result set");
            return;
        }

        // Calculate column widths
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();

        for row in &self.rows {
            for (i, value) in row.iter().enumerate() {
                widths[i] = widths[i].max(value.to_string().len());
            }
        }

        // Print header
        let header: Vec<String> = self.columns
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{:width$}", col, width = widths[i]))
            .collect();

        println!("{}", header.join(" | "));

        let separator: String = widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("-+-");
        println!("{}", separator);

        // Print rows
        for row in &self.rows {
            let row_str: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, val)| format!("{:width$}", val, width = widths[i]))
                .collect();
            println!("{}", row_str.join(" | "));
        }

        println!("\n{} row(s)", self.rows.len());
    }
}

// ============================================================================
// EXPRESSION EVALUATION - Enhanced with LIKE and BETWEEN
// ============================================================================

pub struct ExpressionEvaluator<'a> {
    table: &'a Table,
}

impl<'a> ExpressionEvaluator<'a> {
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    pub fn evaluate(&self, expr: &Expr, row: &Row) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => self.eval_identifier(ident, row),
            Expr::Value(val) => self.eval_literal(&val.value),
            Expr::BinaryOp { left, op, right } => self.eval_binary_op(left, op, right, row),
            Expr::Like { negated, expr, pattern, escape_char, .. } => {
                self.eval_like(*negated, expr, pattern, escape_char, row)
            }
            Expr::Between { expr, negated, low, high } => {
                self.eval_between(expr, *negated, low, high, row)
            }
            _ => Err(DbError::UnsupportedOperation(format!(
                "Expression: {:?}",
                expr
            ))),
        }
    }

    pub fn evaluate_as_bool(&self, expr: &Expr, row: &Row) -> Result<bool> {
        let value = self.evaluate(expr, row)?;
        Ok(value.as_bool())
    }

    fn eval_identifier(&self, ident: &Ident, row: &Row) -> Result<Value> {
        let idx = self.table.find_column_index(&ident.value)?;
        Ok(row[idx].clone())
    }

    fn eval_literal(&self, val: &SqlValue) -> Result<Value> {
        match val {
            SqlValue::Number(n, _) => self.parse_number(n),
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::Text(s.clone()))
            }
            SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
            SqlValue::Null => Ok(Value::Null),
            _ => Err(DbError::UnsupportedOperation(format!(
                "Literal: {:?}",
                val
            ))),
        }
    }

    fn parse_number(&self, n: &str) -> Result<Value> {
        if let Ok(i) = n.parse::<i64>() {
            Ok(Value::Integer(i))
        } else if let Ok(f) = n.parse::<f64>() {
            Ok(Value::Float(f))
        } else {
            Err(DbError::TypeMismatch(format!("Invalid number: {}", n)))
        }
    }

    fn eval_binary_op(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        row: &Row,
    ) -> Result<Value> {
        match op {
            BinaryOperator::And => self.eval_and(left, right, row),
            BinaryOperator::Or => self.eval_or(left, right, row),
            _ => self.eval_comparison(left, op, right, row),
        }
    }

    fn eval_and(&self, left: &Expr, right: &Expr, row: &Row) -> Result<Value> {
        let left_val = self.evaluate(left, row)?;
        if !left_val.as_bool() {
            return Ok(Value::Boolean(false));
        }
        let right_val = self.evaluate(right, row)?;
        Ok(Value::Boolean(right_val.as_bool()))
    }

    fn eval_or(&self, left: &Expr, right: &Expr, row: &Row) -> Result<Value> {
        let left_val = self.evaluate(left, row)?;
        if left_val.as_bool() {
            return Ok(Value::Boolean(true));
        }
        let right_val = self.evaluate(right, row)?;
        Ok(Value::Boolean(right_val.as_bool()))
    }

    fn eval_comparison(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        row: &Row,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, row)?;
        let right_val = self.evaluate(right, row)?;
        let comp_op = ComparisonOp::from_binary_op(op)?;
        let result = comp_op.apply(&left_val, &right_val)?;
        Ok(Value::Boolean(result))
    }

    /// Evaluate LIKE expression
    fn eval_like(
        &self,
        negated: bool,
        expr: &Expr,
        pattern: &Expr,
        _escape_char: &Option<SqlValue>,
        row: &Row,
    ) -> Result<Value> {
        let text_val = self.evaluate(expr, row)?;
        let pattern_val = self.evaluate(pattern, row)?;

        let result = match (&text_val, &pattern_val) {
            (Value::Null, _) | (_, Value::Null) => false,
            (Value::Text(text), Value::Text(pattern)) => {
                eval_like(text, pattern, true)?
            }
            _ => {
                return Err(DbError::TypeMismatch(
                    "LIKE requires text operands".into()
                ));
            }
        };

        Ok(Value::Boolean(if negated { !result } else { result }))
    }

    /// Evaluate BETWEEN expression
    fn eval_between(
        &self,
        expr: &Expr,
        negated: bool,
        low: &Expr,
        high: &Expr,
        row: &Row,
    ) -> Result<Value> {
        let val = self.evaluate(expr, row)?;
        let low_val = self.evaluate(low, row)?;
        let high_val = self.evaluate(high, row)?;

        if matches!(val, Value::Null) {
            return Ok(Value::Boolean(false));
        }

        let ge_low = ComparisonOp::Ge.apply(&val, &low_val)?;
        let le_high = ComparisonOp::Le.apply(&val, &high_val)?;
        let result = ge_low && le_high;

        Ok(Value::Boolean(if negated { !result } else { result }))
    }
}

// ============================================================================
// QUERY PLANNER - Better structured planning
// ============================================================================

#[derive(Debug)]
pub struct QueryPlan {
    table_name: String,
    selected_columns: Vec<String>,
    filter: Option<Expr>,
}

impl QueryPlan {
    pub fn from_select(select: &Select) -> Result<Self> {
        if select.from.len() != 1 {
            return Err(DbError::UnsupportedOperation(
                "Multi-table queries not supported".into(),
            ));
        }

        let table_name = Self::extract_table_name(&select.from[0])?;
        let selected_columns = Self::extract_columns(&select.projection)?;
        let filter = select.selection.clone();

        Ok(Self {
            table_name,
            selected_columns,
            filter,
        })
    }

    fn extract_table_name(table_ref: &sqlparser::ast::TableWithJoins) -> Result<String> {
        match &table_ref.relation {
            TableFactor::Table { name, .. } => {
                name.0
                    .last()
                    .map(|ident| ident.to_string())
                    .ok_or_else(|| DbError::ParseError("Invalid table name".into()))
            }
            _ => Err(DbError::UnsupportedOperation("Table subqueries".into())),
        }
    }

    fn extract_columns(projection: &[SelectItem]) -> Result<Vec<String>> {
        let mut columns = Vec::new();

        for item in projection {
            match item {
                SelectItem::Wildcard(_) => return Ok(vec!["*".to_string()]),
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    columns.push(ident.value.clone());
                }
                _ => {
                    return Err(DbError::UnsupportedOperation(
                        "Complex projections not supported".into(),
                    ))
                }
            }
        }

        Ok(columns)
    }

    pub fn execute(&self, table: &Table) -> Result<QueryResult> {
        let filtered_rows = self.apply_filter(table)?;
        let result_columns = self.determine_columns(table);

        Ok(QueryResult::new(result_columns, filtered_rows))
    }

    fn apply_filter(&self, table: &Table) -> Result<Vec<Row>> {
        let evaluator = ExpressionEvaluator::new(table);
        let mut result = Vec::new();

        for row in table.rows() {
            if let Some(filter_expr) = &self.filter {
                if !evaluator.evaluate_as_bool(filter_expr, row)? {
                    continue;
                }
            }
            result.push(row.clone());
        }

        Ok(result)
    }

    fn determine_columns(&self, table: &Table) -> Vec<String> {
        if self.selected_columns.len() == 1 && self.selected_columns[0] == "*" {
            table.columns().iter().map(|c| c.name.clone()).collect()
        } else {
            self.selected_columns.clone()
        }
    }
}

// ============================================================================
// STATEMENT EXECUTORS - Plugin architecture
// ============================================================================

pub trait StatementExecutor: Send + Sync {
    fn can_execute(&self, stmt: &Statement) -> bool;
    fn execute(&self, context: &mut ExecutionContext, stmt: &Statement) -> Result<QueryResult>;
}

pub struct ExecutionContext {
    tables: HashMap<String, Table>,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn get_table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
    }

    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
    }

    pub fn insert_table(&mut self, table: Table) -> Result<()> {
        let name = table.name().to_string();
        if self.tables.contains_key(&name) {
            return Err(DbError::TableExists(name));
        }
        self.tables.insert(name, table);
        Ok(())
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

// ============================================================================
// CREATE TABLE EXECUTOR
// ============================================================================

pub struct CreateTableExecutor;

impl StatementExecutor for CreateTableExecutor {
    fn can_execute(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::CreateTable(_))
    }

    fn execute(&self, context: &mut ExecutionContext, stmt: &Statement) -> Result<QueryResult> {
        let Statement::CreateTable(create) = stmt else {
            return Err(DbError::ExecutionError("Expected CREATE TABLE".into()));
        };

        let table_name = extract_table_name(&create.name)?;
        let columns = parse_column_definitions(&create.columns)?;
        let table = Table::new(table_name, columns);

        context.insert_table(table)?;
        Ok(QueryResult::empty())
    }
}

// ============================================================================
// INSERT EXECUTOR
// ============================================================================

pub struct InsertExecutor;

impl StatementExecutor for InsertExecutor {
    fn can_execute(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Insert(_))
    }

    fn execute(&self, context: &mut ExecutionContext, stmt: &Statement) -> Result<QueryResult> {
        let Statement::Insert(insert) = stmt else {
            return Err(DbError::ExecutionError("Expected INSERT".into()));
        };

        let table_name = insert.table.to_string();
        let table = context.get_table_mut(&table_name)?;

        if let Some(source) = &insert.source {
            if let SetExpr::Values(values) = &*source.body {
                for row_values in &values.rows {
                    let row = parse_row_values(row_values, table)?;
                    table.insert(row)?;
                }
            }
        }

        Ok(QueryResult::empty())
    }
}

// ============================================================================
// SELECT EXECUTOR
// ============================================================================

pub struct SelectExecutor;

impl StatementExecutor for SelectExecutor {
    fn can_execute(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Query(_))
    }

    fn execute(&self, context: &mut ExecutionContext, stmt: &Statement) -> Result<QueryResult> {
        let Statement::Query(query) = stmt else {
            return Err(DbError::ExecutionError("Expected SELECT".into()));
        };

        let SetExpr::Select(select) = &*query.body else {
            return Err(DbError::UnsupportedOperation("Only SELECT queries".into()));
        };

        let plan = QueryPlan::from_select(select)?;
        let table = context.get_table(&plan.table_name)?;
        plan.execute(table)
    }
}

// ============================================================================
// PARSER UTILITIES
// ============================================================================

fn extract_table_name(name: &ObjectName) -> Result<String> {
    name.0
        .last()
        .map(|ident| ident.to_string())
        .ok_or_else(|| DbError::ParseError("Invalid table name".into()))
}

fn parse_column_definitions(columns: &[ColumnDef]) -> Result<Vec<Column>> {
    columns
        .iter()
        .map(|col| {
            let nullable = !col
                .options
                .iter()
                .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));

            Ok(Column {
                name: col.name.value.clone(),
                data_type: DataType::from_sql(&col.data_type)?,
                nullable,
            })
        })
        .collect()
}

fn parse_row_values(values: &[Expr], table: &Table) -> Result<Row> {
    values
        .iter()
        .enumerate()
        .map(|(i, expr)| parse_value(expr, &table.columns()[i].data_type))
        .collect()
}

fn parse_value(expr: &Expr, expected_type: &DataType) -> Result<Value> {
    let Expr::Value(v) = expr else {
        return Err(DbError::UnsupportedOperation(format!(
            "Non-literal expression: {:?}",
            expr
        )));
    };

    match &v.value {
        SqlValue::Number(n, _) => match expected_type {
            DataType::Integer => n
                .parse()
                .map(Value::Integer)
                .map_err(|_| DbError::TypeMismatch("Invalid integer".into())),
            DataType::Float => n
                .parse()
                .map(Value::Float)
                .map_err(|_| DbError::TypeMismatch("Invalid float".into())),
            _ => Err(DbError::TypeMismatch("Expected numeric type".into())),
        },
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
        SqlValue::Null => Ok(Value::Null),
        _ => Err(DbError::UnsupportedOperation(format!(
            "Unsupported value: {:?}",
            v
        ))),
    }
}

// ============================================================================
// DATABASE ENGINE - Clean interface
// ============================================================================

pub struct InMemoryDB {
    context: ExecutionContext,
    executors: Vec<Box<dyn StatementExecutor>>,
}

impl InMemoryDB {
    pub fn new() -> Self {
        Self {
            context: ExecutionContext::new(),
            executors: Self::default_executors(),
        }
    }

    fn default_executors() -> Vec<Box<dyn StatementExecutor>> {
        vec![
            Box::new(CreateTableExecutor),
            Box::new(InsertExecutor),
            Box::new(SelectExecutor),
        ]
    }

    /// Register a custom executor
    pub fn register_executor(&mut self, executor: Box<dyn StatementExecutor>) {
        self.executors.push(executor);
    }

    /// Execute a SQL statement
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let statements = self.parse_sql(sql)?;

        if statements.is_empty() {
            return Err(DbError::ParseError("No statement found".into()));
        }

        self.execute_statement(&statements[0])
    }

    fn parse_sql(&self, sql: &str) -> Result<Vec<Statement>> {
        let dialect = PostgreSqlDialect {};
        Parser::parse_sql(&dialect, sql).map_err(|e| DbError::ParseError(e.to_string()))
    }

    fn execute_statement(&mut self, stmt: &Statement) -> Result<QueryResult> {
        for executor in &self.executors {
            if executor.can_execute(stmt) {
                return executor.execute(&mut self.context, stmt);
            }
        }

        Err(DbError::UnsupportedOperation(format!(
            "Statement type: {:?}",
            stmt
        )))
    }

    /// Get a reference to a table
    pub fn get_table(&self, name: &str) -> Result<&Table> {
        self.context.get_table(name)
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<&str> {
        self.context.list_tables()
    }

    /// Check if a table exists
    pub fn table_exists(&self, name: &str) -> bool {
        self.context.table_exists(name)
    }

    /// Get table statistics
    pub fn table_stats(&self, name: &str) -> Result<TableStats> {
        let table = self.get_table(name)?;
        Ok(TableStats {
            name: table.name().to_string(),
            column_count: table.columns().len(),
            row_count: table.row_count(),
        })
    }
}

impl Default for InMemoryDB {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// STATISTICS
// ============================================================================

#[derive(Debug)]
pub struct TableStats {
    pub name: String,
    pub column_count: usize,
    pub row_count: usize,
}

impl fmt::Display for TableStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Table '{}': {} columns, {} rows",
            self.name, self.column_count, self.row_count
        )
    }
}

// ============================================================================
// BUILDER PATTERN FOR TABLE CREATION (Optional convenience API)
// ============================================================================

pub struct TableBuilder {
    name: String,
    columns: Vec<Column>,
}

impl TableBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    pub fn column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        self.columns.push(Column::new(name, data_type));
        self
    }

    pub fn column_not_null(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        self.columns.push(Column::new(name, data_type).not_null());
        self
    }

    pub fn build(self) -> Table {
        Table::new(self.name, self.columns)
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_db() -> InMemoryDB {
        let mut db = InMemoryDB::new();
        db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
            .unwrap();
        db
    }

    fn setup_products_db() -> InMemoryDB {
        let mut db = InMemoryDB::new();
        db.execute("CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (1, 'Laptop Pro', 1299.99)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (2, 'Mouse Wireless', 29.99)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (3, 'Keyboard RGB', 79.99)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (4, 'Monitor 4K', 499.99)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (5, 'Laptop Basic', 599.99)")
            .unwrap();
        db
    }

    #[test]
    fn test_basic_select() {
        let mut db = setup_test_db();
        let result = db.execute("SELECT * FROM users").unwrap();
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn test_where_clause() {
        let mut db = setup_test_db();
        let result = db.execute("SELECT * FROM users WHERE age > 26").unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_where_and() {
        let mut db = setup_test_db();
        let result = db
            .execute("SELECT * FROM users WHERE age > 26 AND age < 32")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn test_where_text_comparison() {
        let mut db = setup_test_db();
        let result = db
            .execute("SELECT * FROM users WHERE name = 'Alice'")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    // ========================================================================
    // LIKE OPERATOR TESTS
    // ========================================================================

    #[test]
    fn test_like_starts_with() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE name LIKE 'Laptop%'")
            .unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_like_ends_with() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE name LIKE '%RGB'")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn test_like_contains() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE name LIKE '%Pro%'")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn test_like_single_char() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE name LIKE 'Mouse _________'")
            .unwrap();
        println!("{:?}", result);
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn test_not_like() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE name NOT LIKE '%Laptop%'")
            .unwrap();
        assert_eq!(result.row_count(), 3);
    }

    // ========================================================================
    // BETWEEN OPERATOR TESTS
    // ========================================================================

    #[test]
    fn test_between_inclusive() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE price BETWEEN 50 AND 500")
            .unwrap();
        assert_eq!(result.row_count(), 2); // Keyboard (79.99) and Monitor (499.99)
    }

    #[test]
    fn test_between_integers() {
        let mut db = setup_test_db();
        let result = db
            .execute("SELECT * FROM users WHERE age BETWEEN 25 AND 30")
            .unwrap();
        assert_eq!(result.row_count(), 2); // Bob (25) and Alice (30)
    }

    #[test]
    fn test_not_between() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE price NOT BETWEEN 100 AND 1000")
            .unwrap();
        println!("{:?}", result);
        assert_eq!(result.row_count(), 3); // Mouse (29.99) and Keyboard (79.99)
    }

    #[test]
    fn test_between_with_and_condition() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE price BETWEEN 50 AND 500 AND name LIKE '%Keyboard%'")
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    // ========================================================================
    // COMBINED TESTS
    // ========================================================================

    #[test]
    fn test_like_and_between_combined() {
        let mut db = setup_products_db();
        let result = db
            .execute("SELECT * FROM products WHERE name LIKE '%ro%' AND price BETWEEN 500 AND 2000")
            .unwrap();
        assert_eq!(result.row_count(), 1); // Laptop Pro
    }

    #[test]
    fn test_table_exists() {
        let db = setup_test_db();
        assert!(db.table_exists("users"));
        assert!(!db.table_exists("products"));
    }

    #[test]
    fn test_table_stats() {
        let db = setup_test_db();
        let stats = db.table_stats("users").unwrap();
        assert_eq!(stats.column_count, 3);
        assert_eq!(stats.row_count, 3);
    }

    #[test]
    fn test_duplicate_table() {
        let mut db = InMemoryDB::new();
        db.execute("CREATE TABLE test (id INTEGER)").unwrap();
        let result = db.execute("CREATE TABLE test (id INTEGER)");
        assert!(matches!(result, Err(DbError::TableExists(_))));
    }

    #[test]
    fn test_not_null_constraint() {
        let mut db = InMemoryDB::new();
        db.execute("CREATE TABLE test (id INTEGER NOT NULL)")
            .unwrap();
        let result = db.execute("INSERT INTO test VALUES (NULL)");
        assert!(matches!(result, Err(DbError::ConstraintViolation(_))));
    }

    #[test]
    fn test_builder_pattern() {
        let table = TableBuilder::new("products")
            .column_not_null("id", DataType::Integer)
            .column("name", DataType::Text)
            .column("price", DataType::Float)
            .build();

        assert_eq!(table.name(), "products");
        assert_eq!(table.columns().len(), 3);
        assert!(!table.columns()[0].nullable);
        assert!(table.columns()[1].nullable);
    }

    #[test]
    fn test_like_pattern_conversion() {
        assert!(eval_like("Hello World", "Hello%", true).unwrap());
        assert!(eval_like("Hello World", "%World", true).unwrap());
        assert!(eval_like("Hello World", "%lo Wo%", true).unwrap());
        assert!(eval_like("Test", "T_st", true).unwrap());
        assert!(!eval_like("Test", "T__st", true).unwrap());
    }
}

// ============================================================================
// EXAMPLE USAGE
// ============================================================================

fn main() {
    println!("🚀 RustMemDB - Professional In-Memory SQL Database");
    println!("   ✨ Now with LIKE, NOT LIKE, BETWEEN, and NOT BETWEEN support!\n");
    println!("{}", "=".repeat(70));

    let mut db = InMemoryDB::new();

    // Create table
    println!("\n📝 Creating 'products' table...");
    db.execute(
        "CREATE TABLE products (
            id INTEGER,
            name TEXT,
            price FLOAT,
            category TEXT,
            in_stock BOOLEAN
        )",
    )
        .unwrap();
    println!("✅ Table created successfully");

    // Insert data
    println!("\n📥 Inserting sample data...");
    let inserts = vec![
        "INSERT INTO products VALUES (1, 'Laptop Pro 15', 1299.99, 'Electronics', true)",
        "INSERT INTO products VALUES (2, 'Laptop Basic', 599.99, 'Electronics', true)",
        "INSERT INTO products VALUES (3, 'Wireless Mouse', 29.99, 'Accessories', true)",
        "INSERT INTO products VALUES (4, 'Gaming Keyboard RGB', 129.99, 'Accessories', false)",
        "INSERT INTO products VALUES (5, '4K Monitor', 499.99, 'Electronics', true)",
        "INSERT INTO products VALUES (6, 'USB Cable', 9.99, 'Accessories', true)",
        "INSERT INTO products VALUES (7, 'Laptop Stand', 49.99, 'Accessories', true)",
    ];

    for insert in inserts {
        db.execute(insert).unwrap();
    }
    println!("✅ Inserted {} rows", 7);

    // Query 1: All products
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM products");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM products").unwrap();
    result.print();

    // Query 2: LIKE - Products starting with 'Laptop'
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM products WHERE name LIKE 'Laptop%'");
    println!("{}", "=".repeat(70));
    let result = db
        .execute("SELECT * FROM products WHERE name LIKE 'Laptop%'")
        .unwrap();
    result.print();

    // Query 3: LIKE - Products containing 'RGB'
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM products WHERE name LIKE '%RGB%'");
    println!("{}", "=".repeat(70));
    let result = db
        .execute("SELECT * FROM products WHERE name LIKE '%RGB%'")
        .unwrap();
    result.print();

    // Query 4: NOT LIKE - Products not in Electronics
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM products WHERE category NOT LIKE 'Electronics'");
    println!("{}", "=".repeat(70));
    let result = db
        .execute("SELECT * FROM products WHERE category NOT LIKE 'Electronics'")
        .unwrap();
    result.print();

    // Query 5: BETWEEN - Products priced between $50 and $500
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM products WHERE price BETWEEN 50 AND 500");
    println!("{}", "=".repeat(70));
    let result = db
        .execute("SELECT * FROM products WHERE price BETWEEN 50 AND 500")
        .unwrap();
    result.print();

    // Query 6: NOT BETWEEN - Products NOT in mid-price range
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM products WHERE price NOT BETWEEN 100 AND 1000");
    println!("{}", "=".repeat(70));
    let result = db
        .execute("SELECT * FROM products WHERE price NOT BETWEEN 100 AND 1000")
        .unwrap();
    result.print();

    // Query 7: Combined - LIKE and BETWEEN together
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: Laptops between $500 and $2000");
    println!("   SELECT * FROM products WHERE name LIKE 'Laptop%' AND price BETWEEN 500 AND 2000");
    println!("{}", "=".repeat(70));
    let result = db
        .execute("SELECT * FROM products WHERE name LIKE 'Laptop%' AND price BETWEEN 500 AND 2000")
        .unwrap();
    result.print();

    // Query 8: Complex query with multiple conditions
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: In-stock accessories under $100");
    println!("   SELECT * FROM products WHERE category LIKE 'Accessories'");
    println!("   AND in_stock = true AND price < 100");
    println!("{}", "=".repeat(70));
    let result = db
        .execute("SELECT * FROM products WHERE category LIKE 'Accessories' AND in_stock = true AND price < 100")
        .unwrap();
    result.print();

    // Table statistics
    println!("\n{}", "=".repeat(70));
    println!("📈 Database Statistics");
    println!("{}", "=".repeat(70));
    println!("Available tables: {:?}", db.list_tables());

    if let Ok(stats) = db.table_stats("products") {
        println!("{}", stats);
    }

    // Demonstrate pattern matching capabilities
    println!("\n{}", "=".repeat(70));
    println!("🎯 Pattern Matching Examples");
    println!("{}", "=".repeat(70));
    println!("✓ 'Laptop%'      → Starts with 'Laptop'");
    println!("✓ '%RGB%'        → Contains 'RGB'");
    println!("✓ '%Cable'       → Ends with 'Cable'");
    println!("✓ 'Laptop _____' → Matches 'Laptop' + 5 chars");
    println!("✓ BETWEEN 50 AND 500 → Values in range [50, 500]");

    println!("\n🎉 Demo completed successfully!");
    println!("💡 Tip: Run tests with 'cargo test' to see more examples!");
}