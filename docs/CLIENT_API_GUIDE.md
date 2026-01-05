# RustMemDB Client API Guide

**Complete guide to using the PostgreSQL/MySQL-like client API**

---

## Table of Contents

- [Quick Start](#quick-start)
- [Connection Methods](#connection-methods)
- [Basic Operations](#basic-operations)
- [Transaction Support](#transaction-support)
- [Connection Pooling](#connection-pooling)
- [User Management](#user-management)
- [Advanced Usage](#advanced-usage)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)
- [API Reference](#api-reference)

---

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rustmemodb = { path = "../rustmemodb" }
```

### Hello World Example

```rust
use rustmemodb::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to database
    let client = Client::connect("admin", "admin")?;

    // Create table
    client.execute(
        "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"
    )?;

    // Insert data
    client.execute("INSERT INTO users VALUES (1, 'Alice', 30)")?;

    // Query data
    let result = client.query("SELECT * FROM users")?;
    result.print();

    Ok(())
}
```

---

## Connection Methods

### Method 1: Simple Connection (Recommended)

```rust
use rustmemodb::Client;

let client = Client::connect("username", "password")?;
```

**Use when:** You want quick setup with sensible defaults.

**Defaults:**
- Min connections: 1
- Max connections: 10
- Connection timeout: 30 seconds
- Idle timeout: 10 minutes

---

### Method 2: Custom Configuration

```rust
use rustmemodb::{Client, ConnectionConfig};
use std::time::Duration;

let config = ConnectionConfig::new("admin", "password")
    .database("production")
    .max_connections(20)
    .min_connections(5)
    .connect_timeout(Duration::from_secs(60))
    .idle_timeout(Duration::from_secs(300));

let client = Client::connect_with_config(config)?;
```

**Use when:** You need specific pool settings or timeouts.

---

### Method 3: Connection URL

```rust
use rustmemodb::Client;

let client = Client::connect_url(
    "rustmemodb://admin:secret@localhost:5432/production"
)?;
```

**Format:** `rustmemodb://username:password@host:port/database`

**Use when:** You have connection strings (e.g., from environment variables).

```rust
use std::env;

let url = env::var("DATABASE_URL")?;
let client = Client::connect_url(&url)?;
```

---

## Basic Operations

### CREATE TABLE

```rust
client.execute(
    "CREATE TABLE products (
        id INTEGER,
        name TEXT,
        price FLOAT,
        in_stock BOOLEAN
    )"
)?;
```

### INSERT

```rust
client.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99, true)")?;
client.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99, true)")?;
```

### SELECT

```rust
// All rows
let result = client.query("SELECT * FROM products")?;

// With WHERE clause
let result = client.query(
    "SELECT name, price FROM products WHERE price > 100"
)?;

// With ORDER BY and LIMIT
let result = client.query(
    "SELECT * FROM products ORDER BY price DESC LIMIT 5"
)?;
```

### Working with Results

```rust
let result = client.query("SELECT * FROM products")?;

// Get row count
println!("Found {} products", result.row_count());

// Iterate over rows
for row in result.rows() {
    println!("{:?}", row);
}

// Get column names
println!("Columns: {:?}", result.columns());

// Pretty print
result.print();
```

---

## Transaction Support

### Basic Transaction

```rust
// Get a connection from the pool
let mut conn = client.get_connection()?;

// Start transaction
conn.begin()?;

// Execute multiple statements
conn.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.0)")?;
conn.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.0)")?;

// Commit transaction
conn.commit()?;
```

### Rollback on Error

```rust
let mut conn = client.get_connection()?;

conn.begin()?;

match conn.execute("INSERT INTO accounts VALUES (...)") {
    Ok(_) => conn.commit()?,
    Err(e) => {
        conn.rollback()?;
        return Err(e.into());
    }
}
```

### Auto-Rollback

Transactions are automatically rolled back when connection is dropped:

```rust
{
    let mut conn = client.get_connection()?;
    conn.begin()?;
    conn.execute("INSERT ...")?;

    // Connection dropped here - transaction rolled back!
}
```

### Transaction Helper Pattern

```rust
fn transfer_money(
    client: &Client,
    from: i64,
    to: i64,
    amount: f64
) -> Result<()> {
    let mut conn = client.get_connection()?;

    conn.begin()?;

    // Deduct from sender
    // Note: UPDATE not implemented yet
    // conn.execute(&format!("UPDATE accounts SET balance = balance - {} WHERE id = {}", amount, from))?;

    // Add to receiver
    // conn.execute(&format!("UPDATE accounts SET balance = balance + {} WHERE id = {}", amount, to))?;

    conn.commit()?;
    Ok(())
}
```

---

## Connection Pooling

### Understanding the Pool

```rust
let client = Client::connect("admin", "admin")?;

// Check pool statistics
let stats = client.stats();
println!("{}", stats);
// Output: Pool Stats: 0/1 active, 1 available, max 10
```

### Pool Configuration

```rust
use rustmemodb::ConnectionConfig;
use std::time::Duration;

let config = ConnectionConfig::new("admin", "admin")
    // Pre-create 5 connections
    .min_connections(5)

    // Maximum 20 connections
    .max_connections(20)

    // Close idle connections after 5 minutes
    .idle_timeout(Duration::from_secs(300))

    // Recycle connections after 30 minutes
    .max_lifetime(Duration::from_secs(1800));

let client = Client::connect_with_config(config)?;
```

### Getting Connections

```rust
// Get a connection from the pool
let mut conn = client.get_connection()?;

// Use the connection
conn.execute("SELECT * FROM users")?;

// Connection automatically returned to pool when dropped
drop(conn);

// Or just let it go out of scope
{
    let mut conn = client.get_connection()?;
    conn.execute("...")?;
} // Returned to pool here
```

### Concurrent Access

```rust
use std::thread;
use std::sync::Arc;

let client = Arc::new(Client::connect("admin", "admin")?);

let mut handles = vec![];

for i in 0..10 {
    let client_clone = Arc::clone(&client);

    let handle = thread::spawn(move || {
        let mut conn = client_clone.get_connection().unwrap();
        conn.execute(&format!("INSERT INTO logs VALUES ({})", i)).unwrap();
    });

    handles.push(handle);
}

for handle in handles {
    handle.join().unwrap();
}
```

### Pool Exhaustion Handling

```rust
let config = ConnectionConfig::new("admin", "admin")
    .max_connections(5)
    .connect_timeout(Duration::from_secs(10));

let client = Client::connect_with_config(config)?;

// Acquire all connections
let conns: Vec<_> = (0..5)
    .map(|_| client.get_connection().unwrap())
    .collect();

// This will timeout after 10 seconds
match client.get_connection() {
    Ok(_) => println!("Got connection"),
    Err(e) => println!("Timeout: {}", e),
}
```

---

## User Management

### Creating Users

```rust
let client = Client::connect("admin", "admin")?;
let auth = client.auth_manager();

// Read-only user
auth.create_user(
    "reader",
    "password123",
    vec![Permission::Select]
)?;

// Read-write user
auth.create_user(
    "writer",
    "password456",
    vec![
        Permission::Select,
        Permission::Insert,
        Permission::Update,
        Permission::Delete,
    ]
)?;

// Admin user
auth.create_user(
    "superuser",
    "password789",
    vec![Permission::Admin]
)?;
```

### Permission Types

```rust
use rustmemodb::Permission;

// Data Query Language (DQL)
Permission::Select

// Data Manipulation Language (DML)
Permission::Insert
Permission::Update
Permission::Delete

// Data Definition Language (DDL)
Permission::CreateTable
Permission::DropTable

// Administrative
Permission::Admin  // Has all permissions
```

### Authentication

```rust
// Authenticate user
let user = auth.authenticate("alice", "password123")?;

// Check permissions
if user.has_permission(&Permission::Insert) {
    println!("User can insert data");
}

if user.is_admin() {
    println!("User is admin");
}
```

### Managing Permissions

```rust
// Grant permission
auth.grant_permission("alice", Permission::Insert)?;

// Revoke permission
auth.revoke_permission("alice", Permission::Delete)?;

// Check current permissions
let user = auth.get_user("alice")?;
for permission in &user.permissions {
    println!("{:?}", permission);
}
```

### Password Management

```rust
// Update password
auth.update_password("alice", "new_secure_password")?;

// Verify new password works
let user = auth.authenticate("alice", "new_secure_password")?;
```

### User Administration

```rust
// List all users
let users = auth.list_users()?;
for username in users {
    println!("User: {}", username);
}

// Get user details
let user = auth.get_user("alice")?;
println!("Username: {}", user.username);
println!("Admin: {}", user.is_admin());

// Delete user
auth.delete_user("alice")?;
```

### Connecting as Different Users

```rust
// Connect as admin
let admin_client = Client::connect("admin", "admin")?;

// Create a read-only user
let auth = admin_client.auth_manager();
auth.create_user("reader", "pass", vec![Permission::Select])?;

// Connect as read-only user
let reader_client = Client::connect("reader", "pass")?;

// This works
reader_client.query("SELECT * FROM users")?;

// This would fail (INSERT permission not granted)
// reader_client.execute("INSERT INTO users VALUES (...)")?;
```

---

## Advanced Usage

### Prepared Statements (Future)

```rust
// Currently placeholder - not fully implemented
let stmt = conn.prepare("SELECT * FROM users WHERE age > ?")?;
let result = stmt.execute(&[&25])?;
```

### Multiple Databases

```rust
// Production database
let prod_client = Client::connect_url(
    "rustmemodb://admin:prod_pass@prod-server:5432/production"
)?;

// Staging database
let staging_client = Client::connect_url(
    "rustmemodb://admin:staging_pass@staging-server:5432/staging"
)?;

// Test database
let test_client = Client::connect("admin", "admin")?;
```

### Connection Lifecycle

```rust
// Get connection
let mut conn = client.get_connection()?;
println!("Connection ID: {}", conn.connection().id());
println!("Username: {}", conn.connection().username());

// Check state
if conn.connection().is_active() {
    println!("Connection is active");
}

if conn.connection().is_in_transaction() {
    println!("Transaction active");
}

// Explicitly close
conn.connection().close()?;
```

---

## Error Handling

### Error Types

```rust
use rustmemodb::DbError;

match client.execute("INVALID SQL") {
    Ok(result) => println!("Success"),
    Err(DbError::ParseError(msg)) => println!("SQL parse error: {}", msg),
    Err(DbError::TableNotFound(table)) => println!("Table {} not found", table),
    Err(DbError::ColumnNotFound(col, table)) => {
        println!("Column {} not found in {}", col, table)
    }
    Err(DbError::TypeMismatch(msg)) => println!("Type error: {}", msg),
    Err(DbError::ExecutionError(msg)) => println!("Execution error: {}", msg),
    Err(e) => println!("Other error: {}", e),
}
```

### Retry Logic

```rust
use std::thread;
use std::time::Duration;

fn execute_with_retry(client: &Client, sql: &str, retries: u32) -> Result<QueryResult> {
    for attempt in 0..retries {
        match client.execute(sql) {
            Ok(result) => return Ok(result),
            Err(DbError::LockError(_)) if attempt < retries - 1 => {
                // Wait and retry on lock errors
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Err(DbError::ExecutionError("Max retries exceeded".into()))
}
```

---

## Best Practices

### 1. Use Connection Pooling

**DO:**
```rust
// Create one client per application
let client = Client::connect("admin", "admin")?;

// Reuse for all queries
client.execute("INSERT ...")?;
client.query("SELECT ...")?;
```

**DON'T:**
```rust
// Don't create new client for each query
for i in 0..100 {
    let client = Client::connect("admin", "admin")?;  // ❌ BAD
    client.execute("INSERT ...")?;
}
```

### 2. Handle Errors Properly

**DO:**
```rust
match client.execute(sql) {
    Ok(result) => {
        println!("Success: {} rows", result.row_count());
    }
    Err(e) => {
        eprintln!("Database error: {}", e);
        // Handle error appropriately
    }
}
```

**DON'T:**
```rust
client.execute(sql).unwrap();  // ❌ Panics on error
```

### 3. Use Transactions for Multi-Step Operations

**DO:**
```rust
let mut conn = client.get_connection()?;

conn.begin()?;
conn.execute("INSERT INTO table1 VALUES (...)")?;
conn.execute("INSERT INTO table2 VALUES (...)")?;
conn.commit()?;
```

**DON'T:**
```rust
// Without transaction - partial failure leaves inconsistent state
client.execute("INSERT INTO table1 VALUES (...)")?;
client.execute("INSERT INTO table2 VALUES (...)")?;  // ❌ If this fails, table1 still has data
```

### 4. Close Connections When Done

**DO:**
```rust
{
    let mut conn = client.get_connection()?;
    conn.execute("...")?;
}  // Auto-closed here
```

**DON'T:**
```rust
let mut conn = client.get_connection()?;
// ... lots of code ...
// Connection held for too long
```

### 5. Use Appropriate Permission Levels

**DO:**
```rust
// Least privilege principle
auth.create_user("app_reader", "pass", vec![Permission::Select])?;

let reader_client = Client::connect("app_reader", "pass")?;
```

**DON'T:**
```rust
// Don't use admin for everything
let client = Client::connect("admin", "admin")?;  // ❌ Too much power
```

---

## API Reference

### Client

#### Creation Methods

| Method | Description |
|--------|-------------|
| `Client::connect(username, password)` | Simple connection with defaults |
| `Client::connect_with_config(config)` | Custom configuration |
| `Client::connect_url(url)` | Connection string |

#### Query Methods

| Method | Description |
|--------|-------------|
| `client.execute(sql)` | Execute any SQL statement |
| `client.query(sql)` | Alias for execute |
| `client.get_connection()` | Get pooled connection |
| `client.stats()` | Get pool statistics |
| `client.auth_manager()` | Get authentication manager |

### Connection

#### Methods

| Method | Description |
|--------|-------------|
| `conn.execute(sql)` | Execute SQL |
| `conn.begin()` | Start transaction |
| `conn.commit()` | Commit transaction |
| `conn.rollback()` | Rollback transaction |
| `conn.is_active()` | Check if active |
| `conn.is_in_transaction()` | Check transaction state |
| `conn.close()` | Close connection |

### ConnectionConfig

#### Builder Methods

| Method | Default | Description |
|--------|---------|-------------|
| `database(name)` | "rustmemodb" | Database name |
| `host(host)` | "localhost" | Server host |
| `port(port)` | 5432 | Server port |
| `max_connections(n)` | 10 | Max pool size |
| `min_connections(n)` | 1 | Min pool size |
| `connect_timeout(duration)` | 30s | Connection timeout |
| `idle_timeout(duration)` | 10m | Idle timeout |
| `max_lifetime(duration)` | 30m | Max connection lifetime |

### AuthManager

#### Methods

| Method | Description |
|--------|-------------|
| `create_user(username, password, permissions)` | Create user |
| `authenticate(username, password)` | Verify credentials |
| `delete_user(username)` | Delete user |
| `update_password(username, password)` | Change password |
| `grant_permission(username, permission)` | Add permission |
| `revoke_permission(username, permission)` | Remove permission |
| `list_users()` | Get all usernames |
| `get_user(username)` | Get user details |

---

## Complete Example

```rust
use rustmemodb::{Client, ConnectionConfig, Permission};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure connection pool
    let config = ConnectionConfig::new("admin", "admin")
        .max_connections(20)
        .connect_timeout(Duration::from_secs(30));

    let client = Client::connect_with_config(config)?;

    // Create schema
    client.execute(
        "CREATE TABLE users (
            id INTEGER,
            username TEXT,
            email TEXT,
            created_at INTEGER
        )"
    )?;

    // Create application user
    let auth = client.auth_manager();
    auth.create_user(
        "app_user",
        "secure_password",
        vec![Permission::Select, Permission::Insert]
    )?;

    // Use transaction
    let mut conn = client.get_connection()?;
    conn.begin()?;

    conn.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com', 1234567890)")?;
    conn.execute("INSERT INTO users VALUES (2, 'bob', 'bob@example.com', 1234567891)")?;

    conn.commit()?;

    // Query data
    let result = client.query(
        "SELECT username, email FROM users ORDER BY id"
    )?;

    println!("Users:");
    result.print();

    // Check pool stats
    println!("\n{}", client.stats());

    Ok(())
}
```

---

## Migration from Other Databases

### From PostgreSQL

**PostgreSQL:**
```rust
let client = postgres::Client::connect("postgresql://user:pass@localhost/db", postgres::NoTls)?;
let rows = client.query("SELECT * FROM users", &[])?;
```

**RustMemDB:**
```rust
let client = Client::connect_url("rustmemodb://user:pass@localhost:5432/db")?;
let result = client.query("SELECT * FROM users")?;
```

### From MySQL

**MySQL:**
```rust
let pool = mysql::Pool::new("mysql://user:pass@localhost/db")?;
let mut conn = pool.get_conn()?;
let result = conn.query("SELECT * FROM users")?;
```

**RustMemDB:**
```rust
let client = Client::connect_url("rustmemodb://user:pass@localhost:3306/db")?;
let result = client.query("SELECT * FROM users")?;
```

---

**Last Updated:** 2025-12-01
**Version:** 0.1.0
