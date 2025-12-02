# RustMemDB

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

**A lightweight, in-memory SQL database engine written in pure Rust with a focus on educational clarity and extensibility.**

---

## 📖 Table of Contents

- [Overview](#overview)
- [Mission & Purpose](#mission--purpose)
- [Architecture](#architecture)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [API Documentation](#api-documentation)
- [Performance Characteristics](#performance-characteristics)
- [Design Patterns](#design-patterns)
- [Extensibility](#extensibility)
- [Limitations](#limitations)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Educational Resources](#educational-resources)
- [License](#license)

---

## 🎯 Overview

RustMemDB is an **educational in-memory SQL database** that demonstrates how modern relational databases work under the hood. Built entirely in Rust, it implements a complete SQL query execution pipeline from parsing to result generation, while maintaining clean architecture and extensible design.

Unlike production databases (PostgreSQL, MySQL), RustMemDB prioritizes:
- **Code Clarity** - Easy to understand implementation
- **Educational Value** - Learn database internals by reading/modifying code
- **Extensibility** - Plugin-based architecture for adding features
- **Type Safety** - Leveraging Rust's strong type system

### What Makes It Unique?

```rust
// Simple, clean API
let mut db = InMemoryDB::new();

db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")?;
db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")?;

let result = db.execute("SELECT * FROM users WHERE age > 25")?;
result.print();
```

Under the hood, this simple query goes through a **complete database pipeline**:
1. **SQL Parsing** → AST (Abstract Syntax Tree)
2. **Query Planning** → Logical execution plan
3. **Optimization** → (Future: predicate pushdown, join ordering)
4. **Execution** → Physical operators (scan, filter, project, sort)
5. **Result Formatting** → User-friendly output

---

## 🎯 Mission & Purpose

### Primary Mission

**"Make database internals accessible and understandable through clean, well-documented Rust code."**

### Target Audience

1. **Students & Educators**
   - Learn how SQL databases work internally
   - Understand query processing pipelines
   - Study classic database algorithms (sorting, filtering, etc.)

2. **Rust Developers**
   - See real-world application of design patterns
   - Learn concurrent data structure design
   - Understand plugin architectures

3. **Database Enthusiasts**
   - Prototype new database features
   - Experiment with query optimization algorithms
   - Build custom storage engines

4. **Embedded Systems**
   - Lightweight SQL for resource-constrained environments
   - No external dependencies (pure Rust)
   - Small memory footprint

### What This Project Is For

✅ **Learning** - Study database architecture
✅ **Prototyping** - Test database algorithms quickly
✅ **Testing** - In-memory database for unit tests
✅ **Embedded SQL** - Simple queries in Rust applications
✅ **Research** - Academic database research projects

### What This Project Is NOT For

❌ **Production Databases** - Use PostgreSQL, MySQL, SQLite instead
❌ **Persistent Storage** - Data lost on shutdown (in-memory only)
❌ **High Performance** - Educational focus over optimization
❌ **Full SQL Compliance** - Subset of SQL features

---

## 🏗️ Architecture

RustMemDB follows the classic **three-stage database architecture** used by most relational databases:

```
┌─────────────────────────────────────────────────────────────┐
│                          SQL Query                          │
│              "SELECT * FROM users WHERE age > 25"           │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    PARSER LAYER                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │ SqlParserAdapter (Facade Pattern)                  │    │
│  │  - Uses sqlparser crate for SQL parsing            │    │
│  │  - Converts external AST → Internal AST            │    │
│  │  - Plugin-based expression conversion              │    │
│  └────────────────────────────────────────────────────┘    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼  Statement AST
┌─────────────────────────────────────────────────────────────┐
│                    PLANNER LAYER                            │
│  ┌────────────────────────────────────────────────────┐    │
│  │ QueryPlanner (Strategy Pattern)                    │    │
│  │  - AST → LogicalPlan transformation               │    │
│  │  - Logical operators: Scan, Filter, Project, Sort  │    │
│  │  - Future: Query optimization                      │    │
│  └────────────────────────────────────────────────────┘    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼  LogicalPlan
┌─────────────────────────────────────────────────────────────┐
│                   EXECUTOR LAYER                            │
│  ┌────────────────────────────────────────────────────┐    │
│  │ ExecutorPipeline (Chain of Responsibility)         │    │
│  │  ┌──────────────────────────────────────────────┐  │    │
│  │  │ DDL: CreateTableExecutor, DropTableExecutor  │  │    │
│  │  │ DML: InsertExecutor, UpdateExecutor,         │  │    │
│  │  │      DeleteExecutor                          │  │    │
│  │  │ DQL: QueryExecutor                           │  │    │
│  │  │      - TableScan → Filter → Aggregate/Sort   │  │    │
│  │  │      - Project → Limit                       │  │    │
│  │  └──────────────────────────────────────────────┘  │    │
│  └────────────────────────────────────────────────────┘    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER                            │
│  ┌───────────────────┐        ┌─────────────────────┐      │
│  │ Catalog           │        │ InMemoryStorage     │      │
│  │ (Copy-on-Write)   │        │ (Row-based)         │      │
│  │                   │        │                     │      │
│  │ - Table schemas   │        │ - Per-table RwLock  │      │
│  │ - Arc<HashMap>    │        │ - Concurrent access │      │
│  │ - Lock-free reads │        │ - Vec<Row> storage  │      │
│  └───────────────────┘        └─────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
                       │
                       ▼
               ┌───────────────┐
               │ QueryResult   │
               │  - Columns    │
               │  - Rows       │
               └───────────────┘
```

### Key Components

#### 1. Parser (`src/parser/`)
Converts SQL text into an Abstract Syntax Tree (AST).

- **SqlParserAdapter** - Facade over `sqlparser` crate
- **Plugin System** - Extensible expression conversion
- **AST Definition** - Internal representation optimized for our needs

#### 2. Planner (`src/planner/`)
Transforms AST into a logical execution plan.

- **QueryPlanner** - AST → LogicalPlan converter
- **LogicalPlan Nodes** - TableScan, Filter, Projection, Sort, Limit
- **Future** - Query optimization passes

#### 3. Executor (`src/executor/`)
Executes logical plans against storage.

- **ExecutorPipeline** - Chain of Responsibility pattern
- **Specialized Executors** - DDL, DML, DQL handlers
- **Physical Operators** - Actual data processing
- **EvaluatorRegistry** - Plugin-based expression evaluation

#### 4. Storage (`src/storage/`)
In-memory data storage with concurrent access.

- **Catalog** - Metadata (schemas) with lock-free reads
- **InMemoryStorage** - Actual row data with fine-grained locking
- **TableSchema** - Column definitions and constraints

#### 5. Evaluator (`src/evaluator/`)
Runtime expression evaluation system.

- **Plugin Architecture** - Extensible evaluators
- **Built-in Evaluators** - Arithmetic, comparison, logical, LIKE, BETWEEN, IS NULL
- **EvaluationContext** - Thread-safe expression evaluation

---

## ✨ Features

### Currently Implemented

#### SQL Support
- ✅ **DDL (Data Definition Language)**
  - `CREATE TABLE` with column types and constraints
  - `DROP TABLE` with `IF EXISTS` support
- ✅ **DML (Data Manipulation Language)**
  - `INSERT INTO` with multiple rows
  - `UPDATE` with `SET` and `WHERE` clauses
  - `DELETE FROM` with conditional filtering
- ✅ **DQL (Data Query Language)**
  - `SELECT` with full query capabilities
  - Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)

#### Query Capabilities
- ✅ **Projection** - `SELECT col1, col2` or `SELECT *`
- ✅ **Filtering** - `WHERE` with complex predicates and parentheses
- ✅ **Aggregation** - `COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`
- ✅ **Sorting** - `ORDER BY col1 ASC, col2 DESC` (multiple columns)
- ✅ **Limiting** - `LIMIT n` for result pagination
- ✅ **Expressions** - Full arithmetic and logical expressions in all clauses

#### Operators & Functions
- ✅ **Arithmetic** - `+`, `-`, `*`, `/`, `%`
- ✅ **Comparison** - `=`, `!=`, `<`, `<=`, `>`, `>=`
- ✅ **Logical** - `AND`, `OR`, `NOT` with parentheses support
- ✅ **Pattern Matching** - `LIKE`, `NOT LIKE` (with `%`, `_` wildcards)
- ✅ **Range** - `BETWEEN x AND y`
- ✅ **Null Checking** - `IS NULL`, `IS NOT NULL`
- ✅ **List Membership** - `IN (value1, value2, ...)`
- ✅ **Aggregate Functions** - `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

#### Data Types
- ✅ **INTEGER** - 64-bit signed integers
- ✅ **FLOAT** - 64-bit floating point
- ✅ **TEXT** - Variable-length strings
- ✅ **BOOLEAN** - true/false values
- ✅ **NULL** - Null value support with proper handling

#### Advanced Features
- ✅ **Multi-column sorting** with NULL handling
- ✅ **Expression evaluation** in WHERE, ORDER BY, SELECT, UPDATE
- ✅ **Concurrent access** - Fine-grained table locking with global singleton
- ✅ **Plugin system** - Extensible parsers and evaluators
- ✅ **Type coercion** - Automatic INTEGER ↔ FLOAT conversion
- ✅ **Client API** - PostgreSQL/MySQL-like connection interface
- ✅ **Connection pooling** - Efficient connection management
- ✅ **User management** - Authentication and authorization system

### Performance Features
- ✅ **Per-table locking** - Concurrent access to different tables
- ✅ **Lock-free catalog reads** - Copy-on-Write metadata
- ✅ **Stable sorting** - Predictable ORDER BY results
- ✅ **Efficient aggregation** - Single-pass aggregate computation
- ✅ **Global singleton** - Shared state for all connections

### Performance Metrics
```
Sequential UPDATE:     2.9M updates/sec (5,000 rows)
Mixed operations:      7,083 ops/sec (UPDATE + SELECT)
Concurrent access:     Stable with 4 threads
Aggregate functions:   Fast single-pass computation
```

---

## 🚀 Installation

### Prerequisites
- Rust 1.70 or higher
- Cargo (comes with Rust)

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/rustmemodb.git
cd rustmemodb

# Build the project
cargo build --release

# Run tests
cargo test

# Run the demo application
cargo run
```

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
rustmemodb = { path = "../rustmemodb" }  # or from crates.io when published
```

---

## ⚡ Quick Start

### Basic Example

```rust
use rustmemodb::InMemoryDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new database instance
    let mut db = InMemoryDB::new();

    // Create a table
    db.execute(
        "CREATE TABLE users (
            id INTEGER,
            name TEXT,
            age INTEGER
        )"
    )?;

    // Insert data
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")?;
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")?;

    // Query data
    let result = db.execute("SELECT * FROM users WHERE age > 26")?;
    result.print();

    Ok(())
}
```

Output:
```
┌────┬─────────┬─────┐
│ id │ name    │ age │
├────┼─────────┼─────┤
│ 1  │ Alice   │ 30  │
│ 3  │ Charlie │ 35  │
└────┴─────────┴─────┘
```

---

## 📚 Usage Examples

### Example 1: User Management System

```rust
use rustmemodb::InMemoryDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = InMemoryDB::new();

    // Create users table
    db.execute(
        "CREATE TABLE users (
            id INTEGER,
            username TEXT,
            email TEXT,
            age INTEGER,
            active BOOLEAN
        )"
    )?;

    // Insert users
    db.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com', 30, true)")?;
    db.execute("INSERT INTO users VALUES (2, 'bob', 'bob@example.com', 25, true)")?;
    db.execute("INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com', 35, false)")?;
    db.execute("INSERT INTO users VALUES (4, 'diana', 'diana@example.com', 28, true)")?;

    // Find active users over 26
    println!("\n=== Active users over 26 ===");
    let result = db.execute(
        "SELECT username, email, age
         FROM users
         WHERE active = true AND age > 26"
    )?;
    result.print();

    // Find users with email matching pattern
    println!("\n=== Users with 'example.com' email ===");
    let result = db.execute(
        "SELECT username, email
         FROM users
         WHERE email LIKE '%@example.com'"
    )?;
    result.print();

    // Top 3 oldest users
    println!("\n=== Top 3 oldest users ===");
    let result = db.execute(
        "SELECT username, age
         FROM users
         ORDER BY age DESC
         LIMIT 3"
    )?;
    result.print();

    Ok(())
}
```

### Example 2: Product Catalog

```rust
use rustmemodb::InMemoryDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = InMemoryDB::new();

    // Create products table
    db.execute(
        "CREATE TABLE products (
            id INTEGER,
            name TEXT,
            category TEXT,
            price FLOAT,
            stock INTEGER
        )"
    )?;

    // Insert products
    db.execute("INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 999.99, 10)")?;
    db.execute("INSERT INTO products VALUES (2, 'Mouse', 'Electronics', 29.99, 50)")?;
    db.execute("INSERT INTO products VALUES (3, 'Desk', 'Furniture', 299.99, 5)")?;
    db.execute("INSERT INTO products VALUES (4, 'Chair', 'Furniture', 199.99, 15)")?;
    db.execute("INSERT INTO products VALUES (5, 'Monitor', 'Electronics', 399.99, 8)")?;

    // Find expensive electronics
    println!("\n=== Electronics over $100 ===");
    let result = db.execute(
        "SELECT name, price, stock
         FROM products
         WHERE category = 'Electronics' AND price > 100
         ORDER BY price DESC"
    )?;
    result.print();

    // Products in price range
    println!("\n=== Products between $50 and $400 ===");
    let result = db.execute(
        "SELECT name, category, price
         FROM products
         WHERE price BETWEEN 50 AND 400
         ORDER BY price ASC"
    )?;
    result.print();

    // Low stock items
    println!("\n=== Low stock (< 10 items) ===");
    let result = db.execute(
        "SELECT name, stock
         FROM products
         WHERE stock < 10
         ORDER BY stock ASC"
    )?;
    result.print();

    Ok(())
}
```

### Example 3: Advanced Queries

```rust
use rustmemodb::InMemoryDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = InMemoryDB::new();

    db.execute(
        "CREATE TABLE employees (
            id INTEGER,
            name TEXT,
            department TEXT,
            salary FLOAT,
            years_employed INTEGER
        )"
    )?;

    // Insert data
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 95000.0, 5)")?;
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales', 75000.0, 3)")?;
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 110000.0, 8)")?;
    db.execute("INSERT INTO employees VALUES (4, 'Diana', 'Marketing', 80000.0, 4)")?;
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 105000.0, 6)")?;

    // Complex WHERE with multiple conditions
    println!("\n=== Senior Engineering employees ===");
    let result = db.execute(
        "SELECT name, salary, years_employed
         FROM employees
         WHERE department = 'Engineering'
           AND years_employed > 5
           AND salary > 100000
         ORDER BY salary DESC"
    )?;
    result.print();

    // Using expressions in SELECT
    println!("\n=== Salary after 10% raise ===");
    let result = db.execute(
        "SELECT name, department, salary * 1.1
         FROM employees
         ORDER BY salary DESC"
    )?;
    result.print();

    // Multi-level sorting
    println!("\n=== All employees by dept and salary ===");
    let result = db.execute(
        "SELECT name, department, salary
         FROM employees
         ORDER BY department ASC, salary DESC"
    )?;
    result.print();

    Ok(())
}
```

### Example 4: NULL Value Handling

```rust
use rustmemodb::InMemoryDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = InMemoryDB::new();

    db.execute(
        "CREATE TABLE contacts (
            id INTEGER,
            name TEXT,
            email TEXT,
            phone TEXT
        )"
    )?;

    // Some contacts have missing information
    db.execute("INSERT INTO contacts VALUES (1, 'Alice', 'alice@example.com', '555-1234')")?;
    db.execute("INSERT INTO contacts VALUES (2, 'Bob', NULL, '555-5678')")?;
    db.execute("INSERT INTO contacts VALUES (3, 'Charlie', 'charlie@example.com', NULL)")?;

    // Find contacts without email
    println!("\n=== Contacts without email ===");
    let result = db.execute(
        "SELECT name, phone
         FROM contacts
         WHERE email IS NULL"
    )?;
    result.print();

    // Find contacts with complete information
    println!("\n=== Contacts with complete info ===");
    let result = db.execute(
        "SELECT name, email, phone
         FROM contacts
         WHERE email IS NOT NULL AND phone IS NOT NULL"
    )?;
    result.print();

    Ok(())
}
```

### Example 5: UPDATE and DELETE Operations

```rust
use rustmemodb::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("admin", "admin")?;

    // Create table
    client.execute(
        "CREATE TABLE inventory (
            id INTEGER,
            product TEXT,
            quantity INTEGER,
            price FLOAT
        )"
    )?;

    // Insert initial data
    client.execute("INSERT INTO inventory VALUES (1, 'Laptop', 10, 999.99)")?;
    client.execute("INSERT INTO inventory VALUES (2, 'Mouse', 50, 29.99)")?;
    client.execute("INSERT INTO inventory VALUES (3, 'Keyboard', 30, 79.99)")?;
    client.execute("INSERT INTO inventory VALUES (4, 'Monitor', 15, 399.99)")?;

    // Update prices (10% discount)
    println!("\n=== Applying 10% discount ===");
    let result = client.execute("UPDATE inventory SET price = price * 0.9")?;
    println!("Updated {} products", result.affected_rows().unwrap_or(0));

    // Update specific item
    println!("\n=== Restocking mice ===");
    let result = client.execute("UPDATE inventory SET quantity = 100 WHERE product = 'Mouse'")?;
    println!("Updated {} rows", result.affected_rows().unwrap_or(0));

    // Delete low stock items
    println!("\n=== Removing low stock items ===");
    let result = client.execute("DELETE FROM inventory WHERE quantity < 20")?;
    println!("Deleted {} items", result.affected_rows().unwrap_or(0));

    // View remaining inventory
    println!("\n=== Current Inventory ===");
    let result = client.query("SELECT * FROM inventory ORDER BY product")?;
    result.print();

    Ok(())
}
```

### Example 6: Aggregate Functions

```rust
use rustmemodb::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect("admin", "admin")?;

    // Create sales table
    client.execute(
        "CREATE TABLE sales (
            id INTEGER,
            product TEXT,
            quantity INTEGER,
            revenue FLOAT
        )"
    )?;

    // Insert sales data
    client.execute("INSERT INTO sales VALUES (1, 'Laptop', 5, 4999.95)")?;
    client.execute("INSERT INTO sales VALUES (2, 'Mouse', 50, 1499.50)")?;
    client.execute("INSERT INTO sales VALUES (3, 'Keyboard', 30, 2399.70)")?;
    client.execute("INSERT INTO sales VALUES (4, 'Monitor', 10, 3999.90)")?;

    // Get comprehensive statistics
    println!("\n=== Sales Statistics ===");
    let result = client.query(
        "SELECT COUNT(*), SUM(revenue), AVG(revenue), MIN(revenue), MAX(revenue)
         FROM sales"
    )?;
    result.print();

    // Count total items sold
    println!("\n=== Total Items Sold ===");
    let result = client.query("SELECT SUM(quantity) FROM sales")?;
    result.print();

    // Find highest revenue
    println!("\n=== Highest Single Sale ===");
    let result = client.query("SELECT MAX(revenue) FROM sales")?;
    result.print();

    // Average quantity per order
    println!("\n=== Average Order Size ===");
    let result = client.query("SELECT AVG(quantity) FROM sales")?;
    result.print();

    Ok(())
}
```

### Example 7: Database Statistics

```rust
use rustmemodb::InMemoryDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = InMemoryDB::new();

    // Create multiple tables
    db.execute("CREATE TABLE users (id INTEGER, name TEXT)")?;
    db.execute("CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)")?;

    // Insert data
    for i in 1..=100 {
        db.execute(&format!("INSERT INTO users VALUES ({}, 'user_{}')", i, i))?;
    }

    for i in 1..=50 {
        db.execute(&format!(
            "INSERT INTO products VALUES ({}, 'product_{}', {})",
            i, i, i as f64 * 10.0
        ))?;
    }

    // Get database statistics
    println!("\n=== Database Statistics ===");
    println!("Tables: {:?}", db.list_tables());

    if let Ok(stats) = db.table_stats("users") {
        println!("{}", stats);
    }

    if let Ok(stats) = db.table_stats("products") {
        println!("{}", stats);
    }

    Ok(())
}
```

---

## 📖 API Documentation

### Core Types

#### `InMemoryDB`

The main database facade providing a simple API.

```rust
pub struct InMemoryDB { /* private fields */ }

impl InMemoryDB {
    /// Create a new empty database
    pub fn new() -> Self;

    /// Get the global database instance (singleton)
    pub fn global() -> &'static Arc<RwLock<InMemoryDB>>;

    /// Execute a SQL statement (returns QueryResult for all statement types)
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult>;

    /// Check if a table exists
    pub fn table_exists(&self, name: &str) -> bool;

    /// List all table names
    pub fn list_tables(&self) -> Vec<String>;

    /// Get statistics for a table
    pub fn table_stats(&self, name: &str) -> Result<TableStats>;
}
```

#### `Client`

PostgreSQL/MySQL-style client API with connection pooling.

```rust
pub struct Client { /* private fields */ }

impl Client {
    /// Connect with username and password
    pub fn connect(username: &str, password: &str) -> Result<Self>;

    /// Connect using connection URL
    /// Format: "rustmemodb://username:password@localhost"
    pub fn connect_url(url: &str) -> Result<Self>;

    /// Execute a SQL statement (UPDATE/DELETE/INSERT/CREATE/DROP)
    pub fn execute(&self, sql: &str) -> Result<QueryResult>;

    /// Execute a query (SELECT)
    pub fn query(&self, sql: &str) -> Result<QueryResult>;

    /// Get the authentication manager
    pub fn auth_manager(&self) -> Arc<AuthManager>;
}
```

#### `QueryResult`

Result of a query execution.

```rust
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Row>,
    affected_rows: Option<usize>,
}

impl QueryResult {
    /// Get column names
    pub fn columns(&self) -> &[String];

    /// Get rows
    pub fn rows(&self) -> &[Row];

    /// Get number of rows
    pub fn row_count(&self) -> usize;

    /// Get number of affected rows (for UPDATE/DELETE)
    pub fn affected_rows(&self) -> Option<usize>;

    /// Print formatted result to stdout
    pub fn print(&self);
}
```

#### `Value`

Represents a SQL value.

```rust
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}
```

#### `DataType`

Column data type.

```rust
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}
```

### Error Handling

All operations return `Result<T, DbError>`:

```rust
pub enum DbError {
    ParseError(String),
    TableExists(String),
    TableNotFound(String),
    ColumnNotFound(String, String),
    TypeMismatch(String),
    ConstraintViolation(String),
    ExecutionError(String),
    UnsupportedOperation(String),
    LockError(String),
}
```

---

## ⚡ Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| CREATE TABLE | O(n) | Clones entire catalog (n = tables) |
| DROP TABLE | O(1) | HashMap removal |
| INSERT | O(1) | Amortized vector push |
| UPDATE | O(n) | n = rows in table (full scan) |
| DELETE | O(n + m log m) | n = scan, m = matches to delete |
| SELECT (full scan) | O(n) | n = rows in table |
| SELECT (with WHERE) | O(n) | No indexes yet |
| SELECT (with ORDER BY) | O(n log n) | Stable sort |
| SELECT (with LIMIT) | O(n) | Must scan before limiting |
| SELECT (with aggregates) | O(n) | Single-pass computation |

### Space Complexity

| Structure | Space | Notes |
|-----------|-------|-------|
| Row | O(columns) | Vector of values |
| Table | O(rows × columns) | Vector of rows |
| Catalog | O(tables × columns) | Metadata only |

### Concurrency

- **Catalog Reads**: Lock-free (Copy-on-Write via Arc)
- **Table Reads**: Multiple concurrent readers (RwLock)
- **Table Writes**: Exclusive lock per table
- **Cross-Table**: Different tables can be accessed concurrently

### Benchmark Results

```
Concurrent reads (different tables): ~145ms
Operations: 800 SELECTs
Throughput: ~5,500 ops/sec

Mixed read/write (different tables): ~85ms
Operations: 400 SELECTs + 100 INSERTs
```

*Note: Benchmarks run on M1 Mac, results vary by hardware*

---

## 🎨 Design Patterns

RustMemDB demonstrates several classic software design patterns:

### 1. **Facade Pattern** (`InMemoryDB`)
Provides a simple interface to a complex subsystem.

```rust
// Simple facade hides parser, planner, executor complexity
db.execute("SELECT * FROM users")?;
```

### 2. **Chain of Responsibility** (`ExecutorPipeline`)
Each executor decides if it can handle a statement.

```rust
for executor in &self.executors {
    if executor.can_handle(stmt) {
        return executor.execute(stmt, ctx);
    }
}
```

### 3. **Strategy Pattern** (`QueryPlanner`, Executors)
Different strategies for different statement types.

### 4. **Plugin/Registry Pattern** (Expression Evaluators)
Extensible evaluation system.

```rust
registry.register(Box::new(ArithmeticEvaluator));
registry.register(Box::new(ComparisonEvaluator));
// Users can add custom evaluators
```

### 5. **Adapter Pattern** (`SqlParserAdapter`)
Adapts external sqlparser API to internal AST.

### 6. **Copy-on-Write** (`Catalog`)
Immutable data structure for lock-free reads.

### 7. **Builder Pattern** (Logical Plan construction)
Composable query plans.

---

## 🔧 Extensibility

### Adding Custom Expression Evaluators

```rust
use rustmemodb::evaluator::{ExpressionEvaluator, EvaluatorRegistry};

struct MyCustomEvaluator;

impl ExpressionEvaluator for MyCustomEvaluator {
    fn name(&self) -> &'static str {
        "CUSTOM"
    }

    fn can_evaluate(&self, expr: &Expr) -> bool {
        // Check if this evaluator handles the expression
        matches!(expr, Expr::Function { name, .. } if name == "MY_FUNC")
    }

    fn evaluate(
        &self,
        expr: &Expr,
        row: &Row,
        schema: &Schema,
        context: &EvaluationContext,
    ) -> Result<Value> {
        // Your custom evaluation logic
        todo!()
    }
}

// Register your evaluator
let mut registry = EvaluatorRegistry::new();
registry.register(Box::new(MyCustomEvaluator));
```

### Adding Custom Expression Converters

```rust
use rustmemodb::plugins::{ExpressionPlugin, ExpressionPluginRegistry};

struct MyCustomPlugin;

impl ExpressionPlugin for MyCustomPlugin {
    fn name(&self) -> &'static str {
        "CUSTOM_PLUGIN"
    }

    fn can_handle(&self, expr: &sql_ast::Expr) -> bool {
        // Check if this plugin handles the SQL expression
        todo!()
    }

    fn convert(
        &self,
        expr: sql_ast::Expr,
        converter: &ExpressionConverter,
    ) -> Result<Expr> {
        // Convert SQL AST to internal AST
        todo!()
    }
}
```

---

## ⚠️ Limitations

### Current Limitations

❌ **No persistent storage** - All data lost on shutdown
❌ **No transactions** - ACID not guaranteed
❌ **No indexes** - All queries do full table scans
❌ **No JOINs** - Single table queries only
❌ **No GROUP BY/HAVING** - Aggregates work on full result set only
❌ **No constraints** - No PRIMARY KEY, FOREIGN KEY, UNIQUE enforcement
❌ **No views** - No CREATE VIEW
❌ **Limited SQL** - Subset of SQL-92
❌ **No query optimization** - Plans not optimized
❌ **Single process** - No client-server architecture
❌ **Security** - Passwords stored in plaintext (see PRODUCTION_READINESS_ANALYSIS.md)

### Known Issues

See [CODE_REVIEW_REPORT.md](CODE_REVIEW_REPORT.md) for detailed issue analysis.

**Critical:**
- Float comparison uses fixed epsilon (incorrect for large numbers)
- Benchmarks use write locks instead of read locks
- Silent error swallowing in sort comparisons

**High:**
- No index support causes O(n) queries
- Catalog clones entire HashMap on schema changes
- Transaction system exists but not integrated

---

## 🗺️ Roadmap

### Phase 1: Stability (Current)
- [x] Basic SELECT, INSERT, CREATE TABLE
- [x] WHERE clause with complex predicates
- [x] ORDER BY with multiple columns
- [x] Plugin-based architecture
- [x] DROP TABLE support
- [x] UPDATE and DELETE statements
- [x] Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- [x] Client API and connection pooling
- [x] User management system
- [x] Comprehensive test coverage (71+ passing tests)
- [x] Performance benchmarks (load tests)
- [ ] Fix critical security issue (plaintext passwords)
- [ ] Fix remaining bugs from code review

### Phase 2: Core Features
- [ ] Transaction support (BEGIN, COMMIT, ROLLBACK)
- [ ] Basic indexes (B-Tree for PRIMARY KEY)
- [ ] GROUP BY and HAVING
- [ ] Subqueries
- [ ] Password hashing (bcrypt/argon2)

### Phase 3: Advanced Features
- [ ] INNER JOIN support
- [ ] LEFT/RIGHT JOIN support
- [ ] Query optimizer (predicate pushdown, join ordering)
- [ ] Secondary indexes
- [ ] Views (CREATE VIEW)
- [ ] Constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE)

### Phase 4: Production Readiness
- [ ] Persistent storage backend
- [ ] Write-ahead logging (WAL)
- [ ] MVCC (Multi-Version Concurrency Control)
- [ ] Query caching
- [ ] Connection pooling
- [ ] SQL-92 compliance

### Phase 5: Ecosystem
- [ ] Client-server architecture
- [ ] Wire protocol
- [ ] Language bindings (Python, JavaScript)
- [ ] SQL shell/REPL
- [ ] Migration tools
- [ ] Performance profiling tools

---

## 🤝 Contributing

Contributions are welcome! This is an educational project, so clear, well-documented code is more valuable than clever optimizations.

### How to Contribute

1. **Fork the repository**
2. **Create a feature branch** (`git checkout -b feature/amazing-feature`)
3. **Write tests** for your changes
4. **Ensure all tests pass** (`cargo test`)
5. **Run clippy** (`cargo clippy -- -D warnings`)
6. **Format code** (`cargo fmt`)
7. **Commit changes** (`git commit -m 'Add amazing feature'`)
8. **Push to branch** (`git push origin feature/amazing-feature`)
9. **Open a Pull Request**

### Development Guidelines

- **Code Clarity** > Performance (unless critical path)
- **Add tests** for all new features
- **Document public APIs** with `///` comments
- **Follow Rust conventions** (cargo fmt, clippy)
- **Update README** if adding user-facing features
- **Reference issues** in commits when applicable

### Good First Issues

Looking to contribute? Try these:

- **CRITICAL**: Implement password hashing (bcrypt/argon2) to replace plaintext storage
- Add missing documentation comments
- Implement GROUP BY and HAVING clauses
- Add more expression evaluators (string functions, date functions)
- Improve error messages
- Add more integration tests
- Implement basic indexes (B-Tree)
- Fix issues from CODE_REVIEW_REPORT.md

---

## 📚 Educational Resources

### Understanding the Code

1. **Start Here**: Read `src/main.rs` for a complete example
2. **Architecture**: Review the architecture diagram above
3. **Query Flow**: Follow a query through parser → planner → executor
4. **Tests**: Read tests in `src/executor/query.rs` for examples

### Learning Database Internals

**Recommended Reading:**
- "Database Internals" by Alex Petrov
- "Database System Concepts" by Silberschatz, Korth, Sudarshan
- "Architecture of a Database System" (Hellerstein, Stonebraker, Hamilton)
- CMU Database Systems Course (free online)

**Related Projects:**
- [SQLite](https://www.sqlite.org/) - Simple, embedded SQL database
- [DuckDB](https://duckdb.org/) - In-process OLAP database
- [ToyDB](https://github.com/erikgrinaker/toydb) - Educational distributed SQL database in Rust

### Rust Resources

- [The Rust Book](https://doc.rust-lang.org/book/)
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/)
- [Rust Design Patterns](https://rust-unofficial.github.io/patterns/)

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **sqlparser-rs** - SQL parsing library
- **Rust Community** - Excellent documentation and tools
- **Database Research** - Decades of academic research in database systems

---

## 📧 Contact

- **GitHub Issues**: For bugs and feature requests
- **Discussions**: For questions and ideas
- **Pull Requests**: For contributions

---

## ⭐ Star History

If you find this project useful for learning, please consider giving it a star!

---

**Built with ❤️ in Rust**
