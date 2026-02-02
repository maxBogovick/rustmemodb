# 🦀 RustMemDB

[![Crates.io](https://img.shields.io/crates/v/rustmemodb.svg)](https://crates.io/crates/rustmemodb)
[![Documentation](https://docs.rs/rustmemodb/badge.svg)](https://docs.rs/rustmemodb)
[![Build Status](https://img.shields.io/github/actions/workflow/status/maxBogovick/rustmemodb/ci.yml)](https://github.com/maxBogovick/rustmemodb/actions)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**The logic-first, in-memory SQL engine designed for high-performance testing and rapid prototyping.**

> *"Postgres is for production. SQLite is for files. **RustMemDB is for code.**"*

---

## 📖 Table of Contents

- [⚡ Why RustMemDB?](#-why-rustmemdb)
- [🚀 Killer Feature: Instant Forking](#-killer-feature-instant-forking-cow)
- [📊 Benchmarks](#-benchmarks)
- [✅ SQL Support Matrix](#-sql-support-matrix)
- [🧩 Extensibility & Plugins](#-extensibility--plugins)
- [🎯 Ideal Use Cases](#-ideal-use-cases)
- [👩‍💻 Developer Experience (DX)](#-developer-experience-dx)
- [🛡️ Safety & Reliability](#-safety--reliability)
- [🔌 The "Drop-In" Architecture](#-the-drop-in-architecture)
- [💾 Persistence & Durability](#-persistence--durability)
- [🧩 Extensibility & Plugins](#-extensibility--plugins)
- [⚙️ Engineering Internals](#-engineering-internals)
- [❓ FAQ](#-faq)
- [📦 Installation](#-installation)

---
## 📚 Documentation

- [Quickstart](documentations/QUICKSTART.md)
- [Database Implementation](documentations/SHORT_DOCUMENTATION.md)

## ⚡ Why RustMemDB?

Integration testing in Rust usually forces a painful tradeoff:
1.  **Mocking:** Fast, but fake. You aren't testing SQL logic.
2.  **SQLite:** Fast, but typeless and behaves differently than Postgres/MySQL.
3.  **Docker (Testcontainers):** Accurate, but **slow**. Spinning up a container takes seconds; running parallel tests requires heavy resource management.

**RustMemDB is the Third Way.**

It is a pure Rust SQL engine with **MVCC** and **Snapshot Isolation** that introduces a paradigm shift in testing: **Instant Database Forking**.

---

## 🎮 Interactive CLI

RustMemDB now includes a modern, terminal-based user interface (TUI) powered by `ratatui`.

**Run the Interactive Terminal:**
```bash
cargo run -- cli
# Or simply (default):
cargo run
```

**Run the Postgres Server:**
```bash
cargo run -- server --host 127.0.0.1 --port 5432
```

**Features:**
*   🖥️ **Split View**: SQL Editor + Result Table side-by-side.
*   📝 **Smart Editor**: Multi-line input with **Autocomplete** (Keywords & Tables).
*   📜 **Scrollable History**: View past query results.
*   ⌨️ **Shortcuts**: `Ctrl+E` to execute, `Tab` for autocomplete, `Esc` to quit.

---

### ⚔️ Comparison Matrix

| Feature | RustMemDB 🦀 | SQLite :floppy_disk: | Docker (Postgres) 🐳 |
| :--- | :---: | :---: | :---: |
| **Startup Time** | **< 1ms** | ~10ms | 1s - 5s |
| **Test Isolation** | **Instant Fork (O(1))** | File Copy / Rollback | New Container / Truncate |
| **Parallelism** | ✅ **Safe & Fast** | ❌ Locking Issues | ⚠️ High RAM Usage |
| **Type Safety** | ✅ **Strict** | ❌ Loose / Dynamic | ✅ Strict |
| **Dependencies** | **Zero** (Pure Rust) | C Bindings | Docker Daemon |

---

## 🚀 Killer Feature: Instant Forking (COW)

Stop seeding your database for every test function.

RustMemDB uses **Persistent Data Structures (Copy-On-Write)** via the `im` crate to clone the entire database state instantly.

**The "Seed Once, Test Anywhere" Workflow:**

```text
Step 1: Setup (Runs once)
[ Master DB ] <--- Create Tables, Insert 50k Seed Rows (Heavy)
      |
      +------------------------+------------------------+
      | (Microseconds)         | (Microseconds)         |
      ▼                        ▼                        ▼
[ Fork: Test A ]         [ Fork: Test B ]         [ Fork: Test C ]
Delete ID=1              Update ID=2              Select Count(*)
(Isolated Change)        (Isolated Change)        (Sees Original Data)
```

### Code Example

```rust
use rustmemodb::Client;

#[tokio::test]
async fn test_parallel_logic() -> anyhow::Result<()> {
    // 1. Heavy Initialization (Done once per suite)
    let master = Client::connect_local("admin", "pass").await?;
    master.execute("CREATE TABLE users (id INT, name TEXT)").await?;
    // ... imagine inserting 10,000 rows here ...

    // 2. Create an O(1) Fork for this specific test
    let db = master.fork().await?; 
    
    // 3. Mutate safely. Master and other tests are unaffected.
    db.execute("DELETE FROM users").await?;
    assert_eq!(db.query("SELECT count(*) FROM users").await?.row_count(), 0);
    
    Ok(())
}
```

---

## 📊 Benchmarks

Time taken to create an isolated database environment ready for a test:

```text
RustMemDB (Forking):  [=] < 1ms 🚀
SQLite (In-Memory):   [==] 10ms
Docker (Postgres):    [==================================================] 2500ms+
```

*RustMemDB is approximately **2500x faster** than spinning up a Docker container for isolation.*

---

## ✅ SQL Support Matrix

We support a rich subset of SQL-92, focusing on the features most used in application logic.

| Category | Supported Features |
| :--- | :--- |
| **Data Types** | `INTEGER`, `FLOAT`, `TEXT`, `BOOLEAN`, `NULL`, **`TIMESTAMP`**, **`DATE`**, **`UUID`** |
| **Operators** | `+`, `-`, `*`, `/`, `%` |
| **Comparisons** | `=`, `!=`, `<`, `>`, `<=`, `>=` (Optimized Range Scans) |
| **Logic** | `AND`, `OR`, `NOT`, Parentheses `( )` |
| **Predicates** | `LIKE` (Pattern matching), `BETWEEN`, `IS NULL`, `IS NOT NULL`, `IN (list/subquery)`, `EXISTS` |
| **Aggregates** | `COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)` |
| **Constraints** | `PRIMARY KEY`, `UNIQUE`, **`FOREIGN KEY (REFERENCES)`** |
| **Statements** | `CREATE/DROP TABLE`, `CREATE/DROP VIEW`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE`, `SELECT`, **`EXPLAIN`** |
| **Clauses** | `WHERE`, `ORDER BY` (Multi-column), `LIMIT`, **`FROM (subquery)`** |
| **Transactions** | `BEGIN`, `COMMIT`, `ROLLBACK` |

---

## 🧩 Extensibility & Plugins

RustMemDB is built for Rust developers. Expanding the database with custom functions is trivial compared to C-based databases (SQLite/Postgres).

**Goal:** Add a custom `SCRAMBLE(text)` function.

```rust
use rustmemodb::core::{Value, Result};
use rustmemodb::evaluator::ExpressionEvaluator;

struct ScrambleFn;

impl ExpressionEvaluator for ScrambleFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        let text = args[0].as_str().unwrap();
        let scrambled: String = text.chars().rev().collect();
        Ok(Value::Text(scrambled))
    }
}

// Register and use immediately
db.register_function("SCRAMBLE", Box::new(ScrambleFn));
let result = db.query("SELECT SCRAMBLE('hello')"); // Returns 'olleh'
```

---

## 👩‍💻 Developer Experience (DX)

We believe databases should be a joy to use, not a black box.

### 1. Pure Rust & Async-Native
No C compilers, no `libsqlite3-dev` dependencies, no FFI overhead. RustMemDB is `async` from the ground up, built on `tokio`.

### 2. Helpful Error Messages
Stop guessing why your query failed.

```text
Error: TypeMismatch
  => Column 'age' expects INTEGER, got TEXT ('twenty')
```

### 3. Strict Type Safety
Unlike SQLite, RustMemDB enforces types. If you define an `INTEGER` column, you cannot insert a string. This catches bugs in your application logic **before** they hit production.

---

## 🎯 Ideal Use Cases

RustMemDB isn't trying to replace PostgreSQL in production. It excels where others fail:

### 1. High-Concurrency CI/CD
Run 1000s of integration tests in parallel without managing Docker containers or worrying about port conflicts.
*   **Benefit:** Reduce CI times from minutes to seconds.

### 2. Rapid Prototyping
Drafting a schema or an API? Don't waste time setting up `docker-compose.yml`. Just `cargo run` and you have a SQL engine.
*   **Benefit:** Zero-config development environment.

### 3. Embedded Logic Engine
Need to query internal application state using SQL? Use RustMemDB as an embedded library to store configuration or session data.
*   **Benefit:** Query your app's memory with `SELECT * FROM sessions WHERE inactive > 1h`.

---

## 🛡️ Safety & Reliability

Built on Rust's guarantees.

*   **Memory Safety:** Zero `unsafe` blocks in core logic. Immune to buffer overflows and use-after-free bugs that plague C-based databases.
*   **Thread Safety:** The compiler guarantees that our MVCC implementation is free of Data Races.
*   **Atomic Transactions:** If a transaction isn't committed, it's rolled back. No partial writes, ever.

---

## 🔌 The "Drop-In" Architecture

RustMemDB provides a standardized `DatabaseClient` trait. Write your business logic once, and run it against **RustMemDB in tests** and **Postgres in production**.

**Your Service:**
```rust
use rustmemodb::{DatabaseClient, Result};

pub struct UserService<D: DatabaseClient> {
    db: D
}

impl<D: DatabaseClient> UserService<D> {
    pub async fn create(&self, name: &str) -> Result<()> {
        self.db.execute(&format!("INSERT INTO users (name) VALUES ('{}')", name)).await?;
        Ok(())
    }
}
```

**Production (`main.rs`):**
```rust
// Wrapper for tokio-postgres
let pg_client = PostgresAdapter::connect("postgres://...").await?;
let service = UserService::new(pg_client);
```

**Testing (`tests.rs`):**
```rust
// Works instantly!
let mem_client = rustmemodb::Client::connect_local("admin", "pass").await?;
let service = UserService::new(mem_client);
```

---

## 💾 Persistence & Durability

"In-memory" doesn't mean "data loss". RustMemDB supports full persistence via **Write-Ahead Logging (WAL)**.

```rust
use rustmemodb::{InMemoryDB, DurabilityMode};

async fn persistence_example() -> anyhow::Result<()> {
    let mut db = InMemoryDB::new();
    
    // Enable WAL persistence to ./data directory
    db.enable_persistence("./data", DurabilityMode::Wal).await?;
    
    // Changes are now fsync'd to disk
    db.execute("INSERT INTO important_data VALUES (1)")?;
    
    // On restart, just call enable_persistence again to recover!
    Ok(())
}
```

---

## 🧩 Extensibility & Plugins

RustMemDB is written in Rust, for Rust developers. It exposes a powerful Plugin API.
Want to add a custom function? You don't need C-extensions.

```rust
// Define a custom evaluator
struct UpperEvaluator;

impl ExpressionEvaluator for UpperEvaluator {
    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        match &args[0] {
            Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
            _ => Err(DbError::TypeMismatch("Expected text".into()))
        }
    }
}

// Register it
db.register_function("UPPER", Box::new(UpperEvaluator));

// Use it immediately
db.query("SELECT UPPER(name) FROM users");
```

---

## ⚙️ Engineering Internals

We take engineering seriously. This is not just a `Vec<Row>`.

*   **MVCC (Multi-Version Concurrency Control):**
    *   Writers never block readers.
    *   Readers never block writers.
    *   Full Snapshot Isolation support.
*   **Persistent Data Structures:**
    *   Uses `im-rs` for O(1) cloning and efficient memory usage.
    *   Tables are structural-shared trees, not flat arrays.
*   **Indexing:**
    *   B-Tree backed indexes for `PRIMARY KEY` and `UNIQUE` constraints.
    *   Lookup time is `O(log n)`, not `O(n)`.
*   **Lock-Free Catalog:**
    *   Schema metadata is accessed via `Arc` and `Copy-On-Write`, eliminating read contention on the catalog.

---

## ❓ FAQ

**Q: Can I use this in production?**
A: Use Postgres or MySQL for critical production data storage. Use RustMemDB for testing, prototyping, or embedded scenarios where Postgres is overkill.

**Q: Is it faster than `HashMap`?**
A: No. A `HashMap` is O(1). A SQL engine handles Parsing, Planning, and Transactions. Use RustMemDB when you need *Relational Logic* (Joins, Where clauses, transactions), not just Key-Value storage.

**Q: Does it support the Postgres Wire Protocol?**
A: **Yes!** You can start the standalone server with `cargo run -- server`. It binds to `127.0.0.1:5432` by default and accepts connections from standard clients like `psql` or DBeaver.

---

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustmemodb = "0.1.2"
```

---

## 🤝 Contributing

We are building the best testing database for the Rust ecosystem.

*   **Found a bug?** Open an issue.
*   **Want to build a feature?** Check [developer guide](documentations/DEVELOPER_GUIDE.md).

## 📄 License

MIT. Use it freely in your OSS or commercial projects.

---

**Built with ❤️ in Rust**