# 🚀 RustMemoDB Quickstart Guide

**RustMemoDB** is a lightning-fast, MVCC-powered in-memory database written in Rust that brings PostgreSQL-style features to your application without the operational overhead. Perfect for development, testing, caching, and high-performance applications that need full SQL support with transactional guarantees.

## Why RustMemoDB?

- ⚡ **Blazing Fast**: In-memory storage with zero network latency
- 🔒 **ACID Compliant**: Full MVCC transaction support with snapshot isolation
- 🎯 **Drop-in PostgreSQL-like SQL**: Familiar syntax, zero learning curve
- 🔄 **Connection Pooling**: Built-in, production-ready connection management
- 💾 **Optional Persistence**: WAL + snapshots for durability when you need it
- 🔐 **User Management**: Built-in authentication and authorization
- 📊 **Advanced Features**: Joins, aggregations, indexes, GROUP BY, ORDER BY
- 🦀 **Pure Rust**: Memory-safe, concurrent, and blazingly fast

---

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rustmemodb = "0.1"
tokio = { version = "1", features = ["full"] }
```

---

## Quick Start: 60 Seconds to Your First Query

```rust
use rustmemodb::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to the database
    let client = Client::connect("admin", "adminpass").await?;
    
    // Create a table
    client.execute(
        "CREATE TABLE users (
            id INTEGER,
            name TEXT,
            email TEXT,
            age INTEGER
        )"
    ).await?;
    
    // Insert data
    client.execute(
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30)"
    ).await?;
    
    // Query data
    let result = client.query("SELECT * FROM users WHERE age > 25").await?;
    result.print();
    
    Ok(())
}
```

**Output:**
```
id | name  | email             | age
---+-------+-------------------+----
1  | Alice | alice@example.com | 30
```

---

## Core Recipes

### 1. **Basic CRUD Operations**

```rust
use rustmemodb::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("admin", "adminpass").await?;
    
    // CREATE
    client.execute("CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)").await?;
    
    // INSERT
    client.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99)").await?;
    client.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99)").await?;
    
    // READ
    let result = client.query("SELECT * FROM products WHERE price < 100").await?;
    for row in result.rows() {
        println!("{:?}", row);
    }
    
    // UPDATE
    client.execute("UPDATE products SET price = 899.99 WHERE id = 1").await?;
    
    // DELETE
    client.execute("DELETE FROM products WHERE id = 2").await?;
    
    Ok(())
}
```

---

### 2. **Transactions: Banking Transfer Example**

```rust
use rustmemodb::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("admin", "adminpass").await?;
    
    // Setup accounts
    client.execute(
        "CREATE TABLE accounts (id INTEGER, name TEXT, balance FLOAT)"
    ).await?;
    
    client.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.0)").await?;
    client.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.0)").await?;
    
    // Transfer $200 from Alice to Bob (atomic!)
    let mut conn = client.get_connection().await?;
    
    conn.begin().await?;
    
    // Debit Alice
    conn.execute(
        "UPDATE accounts SET balance = balance - 200.0 WHERE name = 'Alice'"
    ).await?;
    
    // Credit Bob
    conn.execute(
        "UPDATE accounts SET balance = balance + 200.0 WHERE name = 'Bob'"
    ).await?;
    
    // Commit the transaction
    conn.commit().await?;
    
    // Verify balances
    let result = client.query("SELECT * FROM accounts ORDER BY id").await?;
    result.print();
    // Alice: 800.0, Bob: 700.0
    
    Ok(())
}
```

---

### 3. **Advanced Queries: Joins & Aggregations**

```rust
use rustmemodb::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("admin", "adminpass").await?;
    
    // Setup tables
    client.execute(
        "CREATE TABLE employees (id INTEGER, name TEXT, dept_id INTEGER, salary FLOAT)"
    ).await?;
    
    client.execute(
        "CREATE TABLE departments (id INTEGER, name TEXT)"
    ).await?;
    
    // Insert data
    client.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 75000)").await?;
    client.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 65000)").await?;
    client.execute("INSERT INTO employees VALUES (3, 'Charlie', 2, 80000)").await?;
    
    client.execute("INSERT INTO departments VALUES (1, 'Engineering')").await?;
    client.execute("INSERT INTO departments VALUES (2, 'Sales')").await?;
    
    // JOIN query
    let result = client.query(
        "SELECT e.name, d.name, e.salary 
         FROM employees e 
         JOIN departments d ON e.dept_id = d.id
         WHERE e.salary > 70000
         ORDER BY e.salary DESC"
    ).await?;
    
    result.print();
    
    // Aggregation query
    let result = client.query(
        "SELECT d.name, COUNT(*), AVG(e.salary)
         FROM employees e
         JOIN departments d ON e.dept_id = d.id
         GROUP BY d.name"
    ).await?;
    
    result.print();
    
    Ok(())
}
```

---

### 4. **Indexing for Performance**

```rust
use rustmemodb::{Client, Result};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("admin", "adminpass").await?;
    
    client.execute(
        "CREATE TABLE products (id INTEGER, sku TEXT, name TEXT, price FLOAT)"
    ).await?;
    
    // Insert 10,000 products
    for i in 0..10000 {
        client.execute(&format!(
            "INSERT INTO products VALUES ({}, 'SKU-{}', 'Product {}', {})",
            i, i, i, i as f64 * 1.5
        )).await?;
    }
    
    // Query without index
    let start = Instant::now();
    client.query("SELECT * FROM products WHERE sku = 'SKU-5000'").await?;
    println!("Without index: {:?}", start.elapsed());
    
    // Create index
    client.execute("CREATE INDEX ON products (sku)").await?;
    
    // Query with index (much faster!)
    let start = Instant::now();
    client.query("SELECT * FROM products WHERE sku = 'SKU-5000'").await?;
    println!("With index: {:?}", start.elapsed());
    
    Ok(())
}
```

---

### 5. **Connection Pooling**

```rust
use rustmemodb::{Client, ConnectionConfig, Result};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure connection pool
    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(2)
        .max_connections(10)
        .idle_timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(5));
    
    let client = Client::connect_with_config(config).await?;
    
    // Simulate concurrent requests
    let mut handles = vec![];
    
    for i in 0..20 {
        let client_clone = client.fork().await?;
        
        let handle = tokio::spawn(async move {
            let result = client_clone.query(&format!(
                "SELECT {} as request_id", i
            )).await;
            
            println!("Request {} completed", i);
            result
        });
        
        handles.push(handle);
    }
    
    // Wait for all requests
    for handle in handles {
        handle.await.unwrap()?;
    }
    
    // Check pool stats
    let stats = client.stats().await;
    println!("{}", stats);
    
    Ok(())
}
```

---

### 6. **User Management & Permissions**

```rust
use rustmemodb::{Client, Permission, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let admin = Client::connect("admin", "adminpass").await?;
    let auth = admin.auth_manager();
    
    // Create users with different permissions
    auth.create_user(
        "analyst",
        "analyst_pass",
        vec![Permission::Select]
    ).await?;
    
    auth.create_user(
        "developer",
        "dev_pass",
        vec![
            Permission::Select,
            Permission::Insert,
            Permission::Update,
            Permission::Delete,
        ]
    ).await?;
    
    // Setup table
    admin.execute("CREATE TABLE sales (id INTEGER, amount FLOAT)").await?;
    admin.execute("INSERT INTO sales VALUES (1, 1000.0)").await?;
    
    // Analyst can read
    let analyst = Client::connect("analyst", "analyst_pass").await?;
    let result = analyst.query("SELECT * FROM sales").await?;
    result.print(); // ✓ Works
    
    // But cannot write
    match analyst.execute("INSERT INTO sales VALUES (2, 500.0)").await {
        Ok(_) => println!("Unexpected success"),
        Err(e) => println!("Permission denied: {}", e), // ✓ Expected
    }
    
    // Developer can write
    let dev = Client::connect("developer", "dev_pass").await?;
    dev.execute("INSERT INTO sales VALUES (2, 500.0)").await?; // ✓ Works
    
    // List all users
    let users = auth.list_users().await?;
    println!("Users: {:?}", users);
    
    Ok(())
}
```

---

### 7. **Persistence: Durability with WAL**

```rust
use rustmemodb::{InMemoryDB, DurabilityMode, Result};
use std::fs;

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = "./my_database";
    
    // Create database with persistence
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(data_dir, DurabilityMode::Sync).await?;
        
        db.execute("CREATE TABLE users (id INTEGER, name TEXT)").await?;
        db.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
        db.execute("INSERT INTO users VALUES (2, 'Bob')").await?;
        
        // Force checkpoint
        db.checkpoint().await?;
        
        println!("Data written to disk");
    } // Database dropped
    
    // Recover from disk
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(data_dir, DurabilityMode::Sync).await?;
        
        // Data is automatically recovered!
        let result = db.execute("SELECT * FROM users").await?;
        result.print();
        
        assert_eq!(result.row_count(), 2);
        println!("Data recovered successfully!");
    }
    
    // Cleanup
    fs::remove_dir_all(data_dir).ok();
    
    Ok(())
}
```

---

### 8. **JSON Document Storage**

```rust
use rustmemodb::{JsonStorageAdapter, InMemoryDB, Result};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<()> {
    let db = Arc::new(RwLock::new(InMemoryDB::new()));
    let adapter = JsonStorageAdapter::new(db);
    
    // Create a collection with JSON documents
    let users_json = r#"[
        {
            "id": "1",
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30,
            "active": true
        },
        {
            "id": "2",
            "name": "Bob",
            "email": "bob@example.com",
            "age": 25,
            "active": true
        }
    ]"#;
    
    adapter.create("users", users_json).await?;
    
    // Query with SQL
    let result = adapter.read(
        "users",
        "SELECT name, email FROM users WHERE age > 26"
    ).await?;
    
    println!("{}", result);
    
    // Update documents
    let update_json = r#"[
        {
            "id": "1",
            "name": "Alice Smith",
            "email": "alice.smith@example.com",
            "age": 31,
            "active": true
        }
    ]"#;
    
    adapter.update("users", update_json).await?;
    
    // Delete by ID
    adapter.delete("users", "2").await?;
    
    Ok(())
}
```

---

### 9. **Database Forking (Copy-on-Write)**

```rust
use rustmemodb::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let main_client = Client::connect("admin", "adminpass").await?;
    
    // Setup main database
    main_client.execute("CREATE TABLE config (key TEXT, value TEXT)").await?;
    main_client.execute("INSERT INTO config VALUES ('env', 'production')").await?;
    
    // Fork for testing
    let test_client = main_client.fork().await?;
    
    // Modify test database
    test_client.execute("UPDATE config SET value = 'testing' WHERE key = 'env'").await?;
    test_client.execute("INSERT INTO config VALUES ('debug', 'true')").await?;
    
    // Check main database (unchanged)
    let main_result = main_client.query("SELECT * FROM config").await?;
    println!("Main database:");
    main_result.print();
    // env = 'production'
    
    // Check test database (modified)
    let test_result = test_client.query("SELECT * FROM config").await?;
    println!("\nTest database:");
    test_result.print();
    // env = 'testing', debug = 'true'
    
    Ok(())
}
```

---

### 10. **Pattern Matching & Complex Filters**

```rust
use rustmemodb::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("admin", "adminpass").await?;
    
    client.execute(
        "CREATE TABLE products (id INTEGER, name TEXT, category TEXT, price FLOAT)"
    ).await?;
    
    client.execute("INSERT INTO products VALUES (1, 'Laptop Pro 15', 'Electronics', 1299.99)").await?;
    client.execute("INSERT INTO products VALUES (2, 'Laptop Basic', 'Electronics', 599.99)").await?;
    client.execute("INSERT INTO products VALUES (3, 'Wireless Mouse', 'Accessories', 29.99)").await?;
    client.execute("INSERT INTO products VALUES (4, 'USB Cable', 'Accessories', 9.99)").await?;
    
    // LIKE pattern matching
    let result = client.query(
        "SELECT * FROM products WHERE name LIKE 'Laptop%'"
    ).await?;
    println!("Laptops:");
    result.print();
    
    // BETWEEN
    let result = client.query(
        "SELECT * FROM products WHERE price BETWEEN 50 AND 1000"
    ).await?;
    println!("\nMid-range products:");
    result.print();
    
    // Complex conditions
    let result = client.query(
        "SELECT * FROM products 
         WHERE (category = 'Electronics' AND price > 1000)
            OR (category = 'Accessories' AND price < 50)
         ORDER BY price DESC"
    ).await?;
    println!("\nFiltered products:");
    result.print();
    
    Ok(())
}
```

---

## Advanced Features

### Vacuum (Garbage Collection)

```rust
let client = Client::connect("admin", "adminpass").await?;
let db = InMemoryDB::global().read().await;

// After many transactions, clean up old versions
let freed = db.vacuum().await?;
println!("Freed {} old row versions", freed);
```

### Custom Connection URLs

```rust
// Connect with URL
let client = Client::connect_url(
    "rustmemodb://admin:adminpass@localhost:5432/production"
).await?;

// Or use config
let config = ConnectionConfig::from_url(
    "rustmemodb://admin:adminpass@localhost:5432/production"
)?;
let client = Client::connect_with_config(config).await?;
```

### Isolated Databases (Testing)

```rust
// Each test gets its own isolated database
let client = Client::connect_local("admin", "adminpass").await?;

// Changes don't affect other connections
client.execute("CREATE TABLE test_data (id INTEGER)").await?;
```

---

## Performance Tips

1. **Use Indexes** for frequently queried columns
2. **Enable Connection Pooling** for concurrent workloads
3. **Batch Inserts** when possible
4. **Use Transactions** for multiple related operations
5. **Run VACUUM** periodically for long-running applications
6. **Choose Durability Mode** based on your needs:
    - `DurabilityMode::None` - Fastest (no persistence)
    - `DurabilityMode::Async` - Fast (async writes)
    - `DurabilityMode::Sync` - Safe (sync writes)

---

## Comparison with Other Solutions

| Feature | RustMemoDB | SQLite | PostgreSQL | Redis |
|---------|-----------|--------|------------|-------|
| In-Memory | ✅ | ⚠️ (WAL mode) | ❌ | ✅ |
| Full SQL | ✅ | ✅ | ✅ | ❌ |
| MVCC Transactions | ✅ | ⚠️ (limited) | ✅ | ❌ |
| Joins & Aggregations | ✅ | ✅ | ✅ | ❌ |
| Async/Await | ✅ | ⚠️ (blocking) | ✅ | ✅ |
| Connection Pooling | ✅ Built-in | Manual | Manual | Manual |
| Zero Config | ✅ | ✅ | ❌ | ⚠️ |

---

## Common Patterns

### Testing Database Layer

```rust
#[tokio::test]
async fn test_user_creation() {
    let client = Client::connect_local("admin", "adminpass").await.unwrap();
    
    client.execute("CREATE TABLE users (id INTEGER, name TEXT)").await.unwrap();
    client.execute("INSERT INTO users VALUES (1, 'Test User')").await.unwrap();
    
    let result = client.query("SELECT * FROM users WHERE id = 1").await.unwrap();
    assert_eq!(result.row_count(), 1);
}
```

### API Response Caching

```rust
use rustmemodb::{Client, Result};

async fn cache_api_response(client: &Client, key: &str, data: &str) -> Result<()> {
    client.execute(&format!(
        "INSERT INTO cache VALUES ('{}', '{}', {})",
        key, data, chrono::Utc::now().timestamp()
    )).await
}

async fn get_cached_response(client: &Client, key: &str) -> Result<Option<String>> {
    let result = client.query(&format!(
        "SELECT data FROM cache WHERE key = '{}' AND timestamp > {}",
        key, chrono::Utc::now().timestamp() - 3600
    )).await?;
    
    Ok(result.rows().first().and_then(|row| row[0].as_str().map(String::from)))
}
```

---

## Error Handling

```rust
use rustmemodb::{Client, Result, DbError};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("admin", "adminpass").await?;
    
    match client.execute("SELECT * FROM nonexistent_table").await {
        Ok(result) => println!("Success: {:?}", result),
        Err(DbError::TableNotFound(name)) => {
            println!("Table '{}' doesn't exist, creating it...", name);
            client.execute("CREATE TABLE nonexistent_table (id INTEGER)").await?;
        }
        Err(e) => return Err(e),
    }
    
    Ok(())
}
```

---

## Next Steps

- 📚 **[Full API Documentation](https://docs.rs/rustmemodb)**
- 🔧 **[GitHub Repository](https://github.com/maxBogovick/rustmemodb)**
- 🐛 **[Report Issues](https://github.com/maxBogovick/rustmemodb/issues)**

---

## License

MIT License - See LICENSE file for details

---

**Made with ❤️ in Rust**