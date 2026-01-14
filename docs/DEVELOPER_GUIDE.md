# RustMemDB Developer Guide

## Table of Contents

- [Introduction](#introduction)
- [Architecture Overview](#architecture-overview)
- [Plugin System Deep Dive](#plugin-system-deep-dive)
- [Adding New SQL Operators/Functions](#adding-new-sql-operatorsfunctions)
- [Adding New Statement Types](#adding-new-statement-types)
- [Best Practices](#best-practices)
- [Common Pitfalls](#common-pitfalls)
- [Testing Guidelines](#testing-guidelines)
- [Examples](#examples)

---

## Introduction

This guide will help you understand how to extend RustMemoDB with new functionality. The database uses a **plugin-based architecture** that makes it easy to add new SQL operators, functions, and statement types without modifying the core engine.

### Two Plugin Systems

RustMemDB has **two distinct plugin systems** that work at different stages:

1. **Expression Conversion Plugins** (`src/plugins/`)
   - Convert external SQL AST → Internal AST
   - Used during **parsing phase**
   - Handles SQL syntax parsing

2. **Expression Evaluation Plugins** (`src/evaluator/plugins/`)
   - Evaluate internal AST expressions at runtime
   - Used during **execution phase**
   - Handles actual computation

**Important**: Most new features require implementing **BOTH** plugins!

---

## Architecture Overview

### SQL Execution Pipeline

```
┌─────────────────────────────────────────────────────┐
│  SQL String: "SELECT * FROM users WHERE age > 25"   │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│  PHASE 1: PARSING                                   │
│  ┌────────────────────────────────────────────┐    │
│  │ SqlParserAdapter                           │    │
│  │  - Uses sqlparser crate (external AST)     │    │
│  │  - Calls ExpressionConverter               │    │
│  └────────────────────┬───────────────────────┘    │
│                       │                             │
│  ┌────────────────────▼───────────────────────┐    │
│  │ ExpressionPluginRegistry                   │    │
│  │  - ArithmeticPlugin                        │    │
│  │  - ComparisonPlugin                        │    │
│  │  - LogicalPlugin                           │    │
│  │  - FunctionPlugin                          │    │
│  │  - [YOUR NEW PLUGIN HERE]                  │    │
│  └────────────────────┬───────────────────────┘    │
│                       │                             │
│                       ▼                             │
│              Internal AST (Expr enum)               │
└─────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│  PHASE 2: PLANNING                                  │
│  QueryPlanner → LogicalPlan                         │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│  PHASE 3: EXECUTION                                 │
│  ┌────────────────────────────────────────────┐    │
│  │ QueryExecutor                              │    │
│  │  - Scans rows, applies filters             │    │
│  │  - Calls evaluator for each expression     │    │
│  └────────────────────┬───────────────────────┘    │
│                       │                             │
│  ┌────────────────────▼───────────────────────┐    │
│  │ EvaluatorRegistry                          │    │
│  │  - ArithmeticEvaluator                     │    │
│  │  - ComparisonEvaluator                     │    │
│  │  - LogicalEvaluator                        │    │
│  │  - FunctionEvaluator                       │    │
│  │  - [YOUR NEW EVALUATOR HERE]               │    │
│  └────────────────────┬───────────────────────┘    │
│                       │                             │
│                       ▼                             │
│              Value (result)                         │
└─────────────────────────────────────────────────────┘
```

---

## Plugin System Deep Dive

### Expression Conversion Plugin (Phase 1: Parsing)

**Location**: `src/plugins/`

**Purpose**: Convert SQL syntax from external AST to internal AST

**Trait Definition**:
```rust
pub trait ExpressionPlugin: Send + Sync {
    fn name(&self) -> &'static str;

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool;

    fn convert(
        &self,
        expr: sql_ast::Expr,          // External sqlparser AST
        converter: &ExpressionConverter, // For recursive conversion
    ) -> Result<Expr>;                  // Internal AST
}
```

**Key Points**:
- Receives `sqlparser::ast::Expr` (external format)
- Returns `crate::parser::ast::Expr` (internal format)
- Use `converter.convert()` for recursive sub-expressions
- Must implement `Send + Sync` for thread safety

### Expression Evaluation Plugin (Phase 3: Execution)

**Location**: `src/evaluator/plugins/`

**Purpose**: Evaluate expressions at runtime to produce values

**Trait Definition**:
```rust
pub trait ExpressionEvaluator: Send + Sync {
    fn name(&self) -> &'static str;

    fn can_evaluate(&self, expr: &Expr) -> bool;

    fn evaluate(
        &self,
        expr: &Expr,                      // Internal AST
        row: &Row,                        // Current row data
        schema: &Schema,                  // Table schema
        context: &EvaluationContext,      // Registry for recursion
    ) -> Result<Value>;                   // Computed value
}
```

**Key Points**:
- Receives internal `Expr` enum
- Returns `Value` (Integer, Float, Text, Boolean, Null)
- Access row data via `schema.get_column_index()` + `row[index]`
- Use `context.evaluate()` for recursive sub-expressions

---

## Adding New SQL Operators/Functions

This is the most common type of extension. Let's walk through adding a new feature step by step.

### Example: Adding UPPER() String Function

#### Step 1: Update Internal AST (if needed)

**File**: `src/parser/ast.rs`

Check if the `Expr` enum already supports your feature:

```rust
pub enum Expr {
    Column(String),
    Value(Value),
    BinaryOp { left: Box<Expr>, op: BinaryOperator, right: Box<Expr> },
    UnaryOp { op: UnaryOperator, operand: Box<Expr> },
    Function { name: String, args: Vec<Expr> },  // ✅ Already supports functions!
    // ... others
}
```

For `UPPER()`, we can use the existing `Function` variant. If you need a new variant:

```rust
pub enum Expr {
    // ... existing variants
    YourNewVariant { field1: Type1, field2: Type2 },  // Add here
}
```

#### Step 2: Create Conversion Plugin

**File**: `src/plugins/string_functions.rs` (new file)

```rust
use crate::errors::Result;
use crate::parser::ast::Expr;
use crate::plugins::{ExpressionPlugin, ExpressionConverter};
use sqlparser::ast as sql_ast;

pub struct StringFunctionPlugin;

impl ExpressionPlugin for StringFunctionPlugin {
    fn name(&self) -> &'static str {
        "STRING_FUNCTION"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        match expr {
            sql_ast::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                matches!(name.as_str(), "UPPER" | "LOWER" | "LENGTH")
            }
            _ => false,
        }
    }

    fn convert(
        &self,
        expr: sql_ast::Expr,
        converter: &ExpressionConverter,
    ) -> Result<Expr> {
        match expr {
            sql_ast::Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();

                // Convert arguments recursively
                let mut args = Vec::new();
                for arg in func.args.iter() {
                    match arg {
                        sql_ast::FunctionArg::Unnamed(sql_ast::FunctionArgExpr::Expr(e)) => {
                            args.push(converter.convert(e.clone())?);
                        }
                        sql_ast::FunctionArg::Named { arg, .. } => {
                            if let sql_ast::FunctionArgExpr::Expr(e) = arg {
                                args.push(converter.convert(e.clone())?);
                            }
                        }
                        _ => {}
                    }
                }

                Ok(Expr::Function { name, args })
            }
            _ => Err(crate::errors::DbError::ParseError(
                "StringFunctionPlugin: expected function".to_string()
            )),
        }
    }
}
```

**Important Notes**:
- Always normalize function names to uppercase: `func.name.to_string().to_uppercase()`
- Handle both `Unnamed` and `Named` function arguments
- Use `converter.convert()` for recursive argument conversion
- Return proper error messages for debugging

#### Step 3: Register Conversion Plugin

**File**: `src/plugins/mod.rs`

```rust
mod string_functions;  // Add module declaration

use string_functions::StringFunctionPlugin;

impl ExpressionPluginRegistry {
    pub fn with_default_plugins() -> Self {
        let mut registry = Self::new();

        // ... existing plugins
        registry.register(Box::new(FunctionPlugin));
        registry.register(Box::new(StringFunctionPlugin));  // ✅ Add here

        registry
    }
}
```

#### Step 4: Create Evaluation Plugin

**File**: `src/evaluator/plugins/string_functions.rs` (new file)

```rust
use crate::errors::{DbError, Result};
use crate::evaluator::context::EvaluationContext;
use crate::evaluator::ExpressionEvaluator;
use crate::parser::ast::Expr;
use crate::storage::schema::Schema;
use crate::storage::value::{Row, Value};

pub struct StringFunctionEvaluator;

impl ExpressionEvaluator for StringFunctionEvaluator {
    fn name(&self) -> &'static str {
        "STRING_FUNCTION"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function { name, .. } => {
                matches!(name.as_str(), "UPPER" | "LOWER" | "LENGTH")
            }
            _ => false,
        }
    }

    fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext,
    ) -> Result<Value> {
        match expr {
            Expr::Function { name, args } => {
                match name.as_str() {
                    "UPPER" => {
                        // Validate argument count
                        if args.len() != 1 {
                            return Err(DbError::ExecutionError(
                                format!("UPPER() expects 1 argument, got {}", args.len())
                            ));
                        }

                        // Evaluate argument recursively
                        let value = context.evaluate(&args[0], row, schema)?;

                        // Handle the value
                        match value {
                            Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                            Value::Null => Ok(Value::Null),
                            _ => Err(DbError::TypeMismatch(
                                format!("UPPER() requires TEXT argument, got {:?}", value)
                            )),
                        }
                    }

                    "LOWER" => {
                        if args.len() != 1 {
                            return Err(DbError::ExecutionError(
                                format!("LOWER() expects 1 argument, got {}", args.len())
                            ));
                        }
                        let value = context.evaluate(&args[0], row, schema)?;
                        match value {
                            Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                            Value::Null => Ok(Value::Null),
                            _ => Err(DbError::TypeMismatch(
                                format!("LOWER() requires TEXT argument, got {:?}", value)
                            )),
                        }
                    }

                    "LENGTH" => {
                        if args.len() != 1 {
                            return Err(DbError::ExecutionError(
                                format!("LENGTH() expects 1 argument, got {}", args.len())
                            ));
                        }
                        let value = context.evaluate(&args[0], row, schema)?;
                        match value {
                            Value::Text(s) => Ok(Value::Integer(s.len() as i64)),
                            Value::Null => Ok(Value::Null),
                            _ => Err(DbError::TypeMismatch(
                                format!("LENGTH() requires TEXT argument, got {:?}", value)
                            )),
                        }
                    }

                    _ => Err(DbError::UnsupportedOperation(
                        format!("Unknown string function: {}", name)
                    )),
                }
            }
            _ => Err(DbError::ExecutionError(
                "StringFunctionEvaluator: expected function".to_string()
            )),
        }
    }
}
```

**Important Notes**:
- Always validate argument count
- Use `context.evaluate()` for recursive evaluation
- Handle `Value::Null` appropriately (usually propagate NULL)
- Provide clear error messages with function name and expected types
- Consider type coercion if appropriate

#### Step 5: Register Evaluation Plugin

**File**: `src/evaluator/plugins/mod.rs`

```rust
mod string_functions;  // Add module declaration

use string_functions::StringFunctionEvaluator;

impl EvaluatorRegistry {
    pub fn with_default_evaluators() -> Self {
        let mut registry = Self::new();

        // ... existing evaluators
        registry.register(Box::new(FunctionEvaluator));
        registry.register(Box::new(StringFunctionEvaluator));  // ✅ Add here

        registry
    }
}
```

#### Step 6: Write Tests

**File**: `tests/string_functions_tests.rs` (new file)

```rust
use rustmemodb::Client;

#[test]
fn test_upper_function() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_upper (id INTEGER, name TEXT)").unwrap();
    client.execute("INSERT INTO test_upper VALUES (1, 'alice'), (2, 'Bob')").unwrap();

    let result = client.query("SELECT UPPER(name) FROM test_upper ORDER BY id").unwrap();
    assert_eq!(result.row_count(), 2);

    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "ALICE");
    assert_eq!(rows[1][0].to_string(), "BOB");
}

#[test]
fn test_lower_function() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_lower (id INTEGER, name TEXT)").unwrap();
    client.execute("INSERT INTO test_lower VALUES (1, 'ALICE'), (2, 'Bob')").unwrap();

    let result = client.query("SELECT LOWER(name) FROM test_lower ORDER BY id").unwrap();

    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "alice");
    assert_eq!(rows[1][0].to_string(), "bob");
}

#[test]
fn test_length_function() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_length (text TEXT)").unwrap();
    client.execute("INSERT INTO test_length VALUES ('hello'), ('world!')").unwrap();

    let result = client.query("SELECT LENGTH(text) FROM test_length").unwrap();

    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "5");
    assert_eq!(rows[1][0].to_string(), "6");
}

#[test]
fn test_string_function_with_null() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_null (text TEXT)").unwrap();
    client.execute("INSERT INTO test_null VALUES (NULL)").unwrap();

    let result = client.query("SELECT UPPER(text) FROM test_null").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "NULL");
}

#[test]
fn test_nested_string_functions() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_nested (text TEXT)").unwrap();
    client.execute("INSERT INTO test_nested VALUES ('Hello')").unwrap();

    let result = client.query("SELECT UPPER(LOWER('HELLO')) FROM test_nested").unwrap();
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "HELLO");
}
```

**Test Coverage Checklist**:
- ✅ Basic functionality
- ✅ Multiple rows
- ✅ NULL handling
- ✅ Type errors (add test for wrong type)
- ✅ Argument count errors (add test for wrong arg count)
- ✅ Nested/recursive usage
- ✅ Integration with WHERE clause
- ✅ Integration with ORDER BY

#### Step 7: Run Tests

```bash
# Run all tests
cargo test

# Run only your new tests
cargo test test_upper_function
cargo test string_functions

# Run with output
cargo test test_upper_function -- --nocapture
```

---

## Adding New Statement Types

For features that aren't expressions (like `CREATE INDEX`, `GRANT`, etc.), you need to add a new executor.

### Example: Adding CREATE INDEX Statement

#### Step 1: Update AST

**File**: `src/parser/ast.rs`

```rust
pub enum Statement {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateIndex(CreateIndexStmt),  // ✅ Add new variant
}

#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub is_unique: bool,
}
```

#### Step 2: Update Parser Adapter

**File**: `src/parser/adapter.rs`

```rust
impl SqlParserAdapter {
    pub fn parse(&self, sql: &str) -> Result<Statement> {
        let statements = Parser::parse_sql(&self.dialect, sql)?;

        match &statements[0] {
            // ... existing cases
            sql_ast::Statement::CreateIndex {
                name,
                table_name,
                columns,
                unique,
                ..
            } => {
                self.convert_create_index(name, table_name, columns, *unique)
            }
            _ => Err(DbError::UnsupportedOperation(/* ... */)),
        }
    }

    fn convert_create_index(
        &self,
        name: &sql_ast::ObjectName,
        table_name: &sql_ast::ObjectName,
        columns: &[sql_ast::OrderByExpr],
        is_unique: bool,
    ) -> Result<Statement> {
        let index_name = name.0[0].value.clone();
        let table_name = table_name.0[0].value.clone();

        let column_names = columns
            .iter()
            .map(|col| {
                match &col.expr {
                    sql_ast::Expr::Identifier(ident) => Ok(ident.value.clone()),
                    _ => Err(DbError::ParseError("Expected column name".to_string())),
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Statement::CreateIndex(CreateIndexStmt {
            index_name,
            table_name,
            column_names,
            is_unique,
        }))
    }
}
```

#### Step 3: Add Storage Support

**File**: `src/storage/index.rs` (new file)

```rust
use std::collections::HashMap;
use crate::storage::value::Value;

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub is_unique: bool,
    // BTree or HashMap for index storage
    pub data: HashMap<Value, Vec<usize>>,  // value -> row indices
}

impl Index {
    pub fn new(name: String, table_name: String, column_names: Vec<String>, is_unique: bool) -> Self {
        Self {
            name,
            table_name,
            column_names,
            is_unique,
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: Value, row_index: usize) -> Result<()> {
        if self.is_unique && self.data.contains_key(&key) {
            return Err(DbError::ConstraintViolation(
                format!("UNIQUE constraint violation on index '{}'", self.name)
            ));
        }

        self.data.entry(key).or_insert_with(Vec::new).push(row_index);
        Ok(())
    }

    pub fn lookup(&self, key: &Value) -> Option<&Vec<usize>> {
        self.data.get(key)
    }
}
```

**File**: `src/storage/memory.rs` (update)

```rust
pub struct InMemoryStorage {
    tables: HashMap<String, Table>,
    indexes: HashMap<String, Index>,  // ✅ Add index storage
}

impl InMemoryStorage {
    pub fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        column_names: Vec<String>,
        is_unique: bool,
    ) -> Result<()> {
        // Validate table exists
        if !self.tables.contains_key(table_name) {
            return Err(DbError::TableNotFound(table_name.to_string()));
        }

        // Check if index already exists
        if self.indexes.contains_key(index_name) {
            return Err(DbError::ExecutionError(
                format!("Index '{}' already exists", index_name)
            ));
        }

        let mut index = Index::new(
            index_name.to_string(),
            table_name.to_string(),
            column_names.clone(),
            is_unique,
        );

        // Build index from existing data
        let table = self.tables.get(table_name).unwrap();
        for (row_idx, row) in table.rows().iter().enumerate() {
            // Extract key from row based on column_names
            // ... implementation
        }

        self.indexes.insert(index_name.to_string(), index);
        Ok(())
    }
}
```

#### Step 4: Create Executor

**File**: `src/executor/create_index.rs` (new file)

```rust
use crate::errors::Result;
use crate::executor::{ExecutionContext, Executor};
use crate::parser::ast::{CreateIndexStmt, Statement};
use crate::result::QueryResult;

pub struct CreateIndexExecutor;

impl Executor for CreateIndexExecutor {
    fn name(&self) -> &'static str {
        "CreateIndexExecutor"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::CreateIndex(_))
    }

    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        match stmt {
            Statement::CreateIndex(create_index) => {
                self.execute_create_index(create_index, ctx)
            }
            _ => unreachable!(),
        }
    }
}

impl CreateIndexExecutor {
    fn execute_create_index(
        &self,
        stmt: &CreateIndexStmt,
        ctx: &ExecutionContext,
    ) -> Result<QueryResult> {
        let mut storage = ctx.storage.write()
            .map_err(|e| DbError::LockError(e.to_string()))?;

        storage.create_index(
            &stmt.index_name,
            &stmt.table_name,
            stmt.column_names.clone(),
            stmt.is_unique,
        )?;

        Ok(QueryResult::empty())
    }
}
```

#### Step 5: Register Executor

**File**: `src/facade/database.rs`

```rust
use crate::executor::create_index::CreateIndexExecutor;

impl InMemoryDB {
    pub fn new() -> Self {
        let catalog = Arc::new(Catalog::new());
        let storage = Arc::new(RwLock::new(InMemoryStorage::new()));

        let mut pipeline = ExecutorPipeline::new();
        pipeline.register(Box::new(CreateTableExecutor::new(/* ... */)));
        pipeline.register(Box::new(DropTableExecutor::new(/* ... */)));
        pipeline.register(Box::new(CreateIndexExecutor));  // ✅ Add here
        // ... other executors

        Self { catalog, storage, executor_pipeline: pipeline, /* ... */ }
    }
}
```

#### Step 6: Write Tests

**File**: `tests/index_tests.rs` (new file)

```rust
#[test]
fn test_create_index() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
    client.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

    // Create index
    let result = client.execute("CREATE INDEX idx_users_id ON users (id)");
    assert!(result.is_ok());
}

#[test]
fn test_create_unique_index() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE users (id INTEGER, email TEXT)").unwrap();

    let result = client.execute("CREATE UNIQUE INDEX idx_email ON users (email)");
    assert!(result.is_ok());

    // Insert duplicate should fail
    client.execute("INSERT INTO users VALUES (1, 'test@example.com')").unwrap();
    let result = client.execute("INSERT INTO users VALUES (2, 'test@example.com')");
    assert!(result.is_err());
}
```

---

## Best Practices

### 1. Error Handling

**DO**:
```rust
// Provide context in error messages
return Err(DbError::TypeMismatch(
    format!("UPPER() requires TEXT argument, got {:?}", value)
));

// Include function/operator name
return Err(DbError::ExecutionError(
    format!("SQRT() expects 1 argument, got {}", args.len())
));
```

**DON'T**:
```rust
// Too vague
return Err(DbError::ExecutionError("Invalid argument".to_string()));

// No context
return Err(DbError::TypeMismatch("Wrong type".to_string()));
```

### 2. NULL Handling

**DO**:
```rust
match value {
    Value::Null => Ok(Value::Null),  // Propagate NULL
    Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
    _ => Err(DbError::TypeMismatch(/* ... */)),
}
```

Most SQL functions return NULL when given NULL input (SQL NULL propagation rule).

**Exceptions**: `IS NULL`, `IS NOT NULL`, `COALESCE`

### 3. Recursive Evaluation

**DO**:
```rust
// Use context/converter for sub-expressions
let left_value = context.evaluate(&left, row, schema)?;
let right_value = context.evaluate(&right, row, schema)?;
```

**DON'T**:
```rust
// Don't try to evaluate manually
match &left {
    Expr::Column(name) => { /* manual column lookup */ }
    Expr::Value(v) => { /* ... */ }
    // This breaks for nested expressions!
}
```

### 4. Type Coercion

When appropriate, allow type coercion:

```rust
fn add_values(left: Value, right: Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        // Coerce integer to float
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + b as f64)),
        _ => Err(DbError::TypeMismatch(/* ... */)),
    }
}
```

### 5. Thread Safety

All plugins must implement `Send + Sync`:

```rust
pub struct MyPlugin;  // ✅ No internal state = automatically Send + Sync

pub struct MyPluginWithState {
    cache: HashMap<String, Value>,  // ❌ Not thread-safe!
}

pub struct MyPluginWithState {
    cache: Arc<RwLock<HashMap<String, Value>>>,  // ✅ Thread-safe
}
```

### 6. Documentation

**DO**:
```rust
/// Evaluates string manipulation functions: UPPER, LOWER, LENGTH.
///
/// # Supported Functions
/// - `UPPER(text)` - Converts text to uppercase
/// - `LOWER(text)` - Converts text to lowercase
/// - `LENGTH(text)` - Returns length of text
///
/// # NULL Handling
/// Returns NULL if input is NULL.
///
/// # Examples
/// ```sql
/// SELECT UPPER('hello')  -- Returns 'HELLO'
/// SELECT LENGTH('world') -- Returns 5
/// ```
pub struct StringFunctionEvaluator;
```

---

## Common Pitfalls

### 1. Forgetting to Register Plugin

**Symptom**: "Unsupported operation" error at runtime

**Fix**: Check both registries:
- `ExpressionPluginRegistry::with_default_plugins()`
- `EvaluatorRegistry::with_default_evaluators()`

### 2. Wrong AST Type

**Symptom**: Conversion plugin matches but evaluator doesn't

**Problem**: Conversion plugin created wrong `Expr` variant

**Fix**: Ensure conversion plugin creates the exact `Expr` variant your evaluator expects:

```rust
// Conversion plugin
Ok(Expr::Function { name: "UPPER".to_string(), args })

// Evaluator
match expr {
    Expr::Function { name, .. } if name == "UPPER" => { /* ... */ }
    //          ^^^^^^^^ Must match exactly!
}
```

### 3. Not Handling NULL

**Symptom**: Panic or crash on NULL values

**Fix**: Always handle `Value::Null`:

```rust
match value {
    Value::Null => Ok(Value::Null),  // Add this!
    Value::Integer(n) => { /* ... */ }
    // ...
}
```

### 4. Infinite Recursion

**Symptom**: Stack overflow

**Problem**: Direct recursion instead of using context/converter

**Fix**:
```rust
// DON'T
fn evaluate(&self, expr: &Expr, ...) -> Result<Value> {
    self.evaluate(&sub_expr, ...)  // ❌ Infinite loop!
}

// DO
fn evaluate(&self, expr: &Expr, ..., context: &EvaluationContext) -> Result<Value> {
    context.evaluate(&sub_expr, ...)  // ✅ Uses registry
}
```

### 5. Modifying Global State

**Problem**: Race conditions and unpredictable behavior

**Fix**: Keep plugins stateless or use proper synchronization:

```rust
// BAD
pub struct MyPlugin {
    counter: usize,  // ❌ Mutable state without synchronization
}

// GOOD
pub struct MyPlugin;  // ✅ No state

// GOOD (if you really need state)
pub struct MyPlugin {
    cache: Arc<RwLock<HashMap<String, Value>>>,  // ✅ Synchronized
}
```

### 6. Case Sensitivity

**Problem**: Function names not matching due to case differences

**Fix**: Always normalize to uppercase:

```rust
let name = func.name.to_string().to_uppercase();
match name.as_str() {
    "UPPER" => { /* ... */ }  // Will match "upper", "UPPER", "Upper"
}
```

---

## Testing Guidelines

### Test Structure

```rust
#[test]
fn test_feature_basic() {
    // Setup
    let client = Client::connect("admin", "admin").unwrap();
    client.execute("CREATE TABLE ...").unwrap();

    // Action
    let result = client.query("SELECT ...").unwrap();

    // Assert
    assert_eq!(result.row_count(), expected_count);
    let rows: Vec<_> = result.iter().collect();
    assert_eq!(rows[0][0].to_string(), "expected_value");
}
```

### Test Coverage Checklist

For each new feature, write tests for:

1. **Basic functionality** - Does it work in the simplest case?
2. **Multiple rows** - Does it work with more than one row?
3. **NULL handling** - What happens with NULL inputs?
4. **Type errors** - Does it reject invalid types?
5. **Argument errors** - Does it validate argument count?
6. **Nested usage** - Can it be nested with other expressions?
7. **WHERE clause** - Can it be used in filters?
8. **ORDER BY** - Can it be used in sorting?
9. **Edge cases** - Empty strings, zero, negative numbers, etc.

### Running Tests

```bash
# All tests
cargo test

# Specific test file
cargo test --test string_functions_tests

# Specific test
cargo test test_upper_function

# With output
cargo test test_upper_function -- --nocapture

# Run in release mode (for performance tests)
cargo test --release test_performance
```

---

## Examples

### Example 1: Binary Operator (MODULO %)

See existing `ArithmeticPlugin` and `ArithmeticEvaluator` in:
- `src/plugins/arithmetic.rs`
- `src/evaluator/plugins/arithmetic.rs`

### Example 2: Unary Operator (ABS)

```rust
// Conversion plugin
match expr {
    sql_ast::Expr::Function(func) if func.name.to_string().to_uppercase() == "ABS" => {
        Ok(Expr::UnaryOp {
            op: UnaryOperator::Abs,
            operand: Box::new(converter.convert(args[0].clone())?),
        })
    }
}

// Evaluator
match expr {
    Expr::UnaryOp { op: UnaryOperator::Abs, operand } => {
        let value = context.evaluate(operand, row, schema)?;
        match value {
            Value::Integer(n) => Ok(Value::Integer(n.abs())),
            Value::Float(f) => Ok(Value::Float(f.abs())),
            Value::Null => Ok(Value::Null),
            _ => Err(DbError::TypeMismatch(/* ... */)),
        }
    }
}
```

### Example 3: Multi-argument Function (CONCAT)

```rust
"CONCAT" => {
    let mut result = String::new();
    for arg in args {
        let value = context.evaluate(arg, row, schema)?;
        match value {
            Value::Text(s) => result.push_str(&s),
            Value::Integer(n) => result.push_str(&n.to_string()),
            Value::Float(f) => result.push_str(&f.to_string()),
            Value::Null => {}, // Skip NULLs
            _ => return Err(DbError::TypeMismatch(/* ... */)),
        }
    }
    Ok(Value::Text(result))
}
```

### Example 4: Special Case (COALESCE)

```rust
"COALESCE" => {
    // Return first non-NULL value
    for arg in args {
        let value = context.evaluate(arg, row, schema)?;
        if !matches!(value, Value::Null) {
            return Ok(value);
        }
    }
    Ok(Value::Null)  // All were NULL
}
```

---

## Quick Reference

### File Locations

| Component | Directory |
|-----------|-----------|
| Internal AST | `src/parser/ast.rs` |
| Parser Adapter | `src/parser/adapter.rs` |
| Conversion Plugins | `src/plugins/` |
| Conversion Registry | `src/plugins/mod.rs` |
| Evaluation Plugins | `src/evaluator/plugins/` |
| Evaluation Registry | `src/evaluator/plugins/mod.rs` |
| Executors | `src/executor/` |
| Storage | `src/storage/` |
| Tests | `tests/` |

### Key Traits

```rust
// Phase 1: Parsing
pub trait ExpressionPlugin: Send + Sync {
    fn name(&self) -> &'static str;
    fn can_handle(&self, expr: &sql_ast::Expr) -> bool;
    fn convert(&self, expr: sql_ast::Expr, converter: &ExpressionConverter) -> Result<Expr>;
}

// Phase 3: Execution
pub trait ExpressionEvaluator: Send + Sync {
    fn name(&self) -> &'static str;
    fn can_evaluate(&self, expr: &Expr) -> bool;
    fn evaluate(&self, expr: &Expr, row: &Row, schema: &Schema, context: &EvaluationContext) -> Result<Value>;
}

// Executors
pub trait Executor: Send + Sync {
    fn name(&self) -> &'static str;
    fn can_handle(&self, stmt: &Statement) -> bool;
    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult>;
}
```

---

## Getting Help

1. **Read existing plugins** - Best way to learn is by example
2. **Check tests** - See how features are used
3. **GitHub Issues** - Ask questions or report bugs
4. **Code Review** - Submit PR early for feedback

---

## Contributing Your Plugin

Once you've implemented and tested your feature:

1. Format code: `cargo fmt`
2. Run linter: `cargo clippy -- -D warnings`
3. Run all tests: `cargo test`
4. Update README if user-facing feature
5. Add documentation comments
6. Submit Pull Request

---

**Happy coding! 🦀**