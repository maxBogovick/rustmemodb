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
- [🧱 Persist Macros](#-persist-macros-experimental)
- [🧠 Persist Entity Runtime](#-persist-entity-runtime)
- [⚙️ Engineering Internals](#-engineering-internals)
- [❓ FAQ](#-faq)
- [📦 Installation](#-installation)

---
## 📚 Documentation

- [Quickstart](documentations/QUICKSTART.md)
- [Database Implementation](documentations/SHORT_DOCUMENTATION.md)
- [Metrics & Profiling](docs/metrics.md)
- [Persist Roadmap](PERSIST_ROADMAP.md)
- [Persist Command-First RFC](docs/persist/RFC_COMMAND_FIRST_PERSIST.md)
- [Managed Examples Index](examples/managed/README.md)
- [Persist Showcase Example](examples/managed/persist_showcase/README.md)
- [Advanced Examples Index](examples/advanced/README.md)
- [Persist Runtime Showcase Example](examples/advanced/persist_runtime_showcase/README.md)
- [Sentinel Core Example](examples/advanced/sentinel_core/README.md)
- [Todo Persist Runtime Example](examples/managed/todo_persist_runtime/README.md)
- [LedgerCore Example](examples/ledger_core/README.md)
- [No-DB API Example](examples/no_db_api/README.md)
- [AgileBoard Example](examples/agile_board/README.md)

## ✨ Autonomous REST DX (New)

You can now define domain logic once and let `persist` generate:
1. REST command/query routes.
2. Automatic idempotent command replay via `Idempotency-Key` (enabled by default for generated commands).
3. OpenAPI document at `GET /_openapi.json`.
4. Domain error mapping with explicit HTTP status/code.
5. `204 No Content` automatically for command/query/view methods returning `()` / `Result<(), E>`.
6. Generic nested JSON persistence via `PersistJson<T>` (no local wrapper + no manual `PersistValue` impl).
7. Smart DTO inference: for methods with one DTO argument, request payload binding is inferred automatically (no mandatory `input = ...`).

```rust
use rustmemodb::prelude::dx::*;

#[derive(Autonomous)]
pub struct LedgerBook { /* domain state */ }

#[derive(ApiError)]
pub enum LedgerError {
    #[api_error(status = 404, code = "account_not_found")]
    AccountNotFound,
}

#[expose_rest]
impl LedgerBook {
    #[command]
    pub fn create_transfer(&mut self, input: CreateTransferInput) -> Result<TransferView, LedgerError> {
        /* business logic only */
    }

    #[query]
    pub fn balance(&self, query: BalanceQuery) -> Result<BalanceView, LedgerError> {
        /* business logic only */
    }
}

let app = PersistApp::open_auto("./data").await?;
let api = app.serve_autonomous_model::<LedgerBook>("ledgers").await?;
```

Reference implementations:
1. `examples/ledger_core` - generated REST + idempotency + OpenAPI.
2. `examples/agile_board` - generated REST only from domain model (`src/model.rs`) and tiny bootstrap in `src/main.rs` (no manual `api.rs`/`store.rs`).

DX contract for showcase examples:
1. Runtime path should be only `model.rs` + `main.rs`.
2. `main.rs` should mount only `PersistApp::serve_autonomous_model::<Model>(...)`.
3. No manual `api.rs`, `store.rs`, repository adapters in the active runtime flow.

## ⚡ Why RustMemDB?

Integration testing in Rust usually forces a painful tradeoff:
1.  **Mocking:** Fast, but fake. You aren't testing SQL logic.
2.  **SQLite:** Fast, but typeless and behaves differently than Postgres/MySQL.
3.  **Docker (Testcontainers):** Accurate, but **slow**. Spinning up a container takes seconds; running parallel tests requires heavy resource management.

**RustMemDB is the Third Way.**

It is a pure Rust SQL engine with MVCC-based storage and Snapshot Isolation that introduces a paradigm shift in testing: **Instant Database Forking**.
Note: read-only queries run under a shared lock, while writes are exclusive, so parallelism is limited under write-heavy workloads.

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

**Run the Synthetic Load Test:**
```bash
cargo run -- load-test --duration-secs 10 --concurrency 4 --rows 10000 --read-ratio 80
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
| **Parallelism** | ⚠️ **Limited (shared read / exclusive write)** | ❌ Locking Issues | ⚠️ High RAM Usage |
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
| **JSON** | `->` (Get as JSON), `->>` (Get as Text) |
| **Functions** | `UPPER`, `LOWER`, `LENGTH`, `COALESCE`, `NOW` |
| **Predicates** | `LIKE` (Pattern matching), `BETWEEN`, `IS NULL`, `IS NOT NULL`, `IN (list/subquery)`, `EXISTS` |
| **Aggregates** | `COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)` (Support **`DISTINCT`**) |
| **Window Functions** | **`ROW_NUMBER()`**, **`RANK()`** with `OVER (PARTITION BY ... ORDER BY ...)` |
| **Constraints** | `PRIMARY KEY`, `UNIQUE`, **`FOREIGN KEY (REFERENCES)`** |
| **Statements** | `CREATE/DROP TABLE`, `CREATE/DROP VIEW`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE`, `SELECT`, **`EXPLAIN`** |
| **Alter Table** | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, **`RENAME TABLE`** |
| **Clauses** | `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `FROM (subquery)`, `DISTINCT`, **`WITH (Recursive CTEs)`** |
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

### 4. Prepared Statements (Basic)
The client API supports simple parameter binding via `PreparedStatement::execute` (numeric/boolean/NULL parsing; everything else treated as text).

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
*   **Transaction Semantics:** Uncommitted changes are invisible; recovery replays only committed WAL entries.

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
    db.enable_persistence("./data", DurabilityMode::Sync).await?;
    
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

## 🧱 Persist Macros (Experimental)

RustMemDB now includes a persistence-oriented object layer:

- `persist_struct!` generates an object that owns state and can persist itself.
- `persist_vec!` manages collections of those objects.
- `persist_vec!(hetero ...)` supports mixed object types in one collection.
- `PersistApp` manages snapshot/recovery/replication lifecycle for collections.
- `PersistApp::open_auto(...)` enables zero-thinking persistence defaults (auto recovery + auto snapshot policy).

Command-first direction and API freeze are tracked in:
- [PERSIST_ROADMAP.md](PERSIST_ROADMAP.md)
- [docs/persist/RFC_COMMAND_FIRST_PERSIST.md](docs/persist/RFC_COMMAND_FIRST_PERSIST.md)

Recommended for application code: use `PersistApp::open_auto(...)` + managed `open_vec`.
Snippets that use `PersistSession` below are advanced low-level API and are kept for internals/migration scenarios.

### 0. UniStructGen Integration (Optional)

If you want compile-time model generation from JSON/OpenAPI/SQL/GraphQL/env, enable
the optional `unistructgen` feature and use re-exported macros directly from `rustmemodb`.

```toml
[dependencies]
rustmemodb = { version = "0.1.2", features = ["unistructgen"] }
```

```rust
use rustmemodb::generate_struct_from_json;

generate_struct_from_json! {
    name = "SignupPayload",
    json = r#"{"email":"alice@example.com","active":true}"#,
    serde = true
}
```

Re-exported macros:
- `generate_struct_from_json!`
- `openapi_to_rust!`
- `generate_struct_from_sql!`
- `generate_struct_from_graphql!`
- `generate_struct_from_env!`
- `json_struct` (attribute)
- `struct_from_external_api!`

### 0.1 Zero-Code BaaS from `schemas/*.json`

`PersistApp` can now mount generic CRUD REST directly from JSON Schema files in a folder:

```rust
use rustmemodb::PersistApp;

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let dynamic_router = app.serve_json_schema_dir("./schemas").await?;
let api = axum::Router::new().nest("/api", dynamic_router);
# let _ = api;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

Generated routes per schema file:
- `GET /api/:collection`
- `POST /api/:collection`
- `GET /api/:collection/:id`
- `PATCH /api/:collection/:id`
- `DELETE /api/:collection/:id`
- `GET /api/_openapi.json`

Notes:
- collection name is derived from schema filename (`users.json` -> `users`),
- payloads are validated against JSON Schema field types,
- persistence, table bootstrap and CRUD SQL are hidden under the hood,
- schema changes are hot-reloaded from disk without restart,
- newly added schema fields are auto-migrated via internal `ALTER TABLE ... ADD COLUMN`,
- field names currently must match SQL-safe identifier pattern `[A-Za-z_][A-Za-z0-9_]*`.

### 1. Typed Mode (`struct` input, Advanced Low-Level API)

```rust
use rustmemodb::{InMemoryDB, PersistSession, PersistEntity, persist_struct};

persist_struct! {
    pub struct UserState {
        name: String,
        score: i64,
        active: bool,
    }
}

# tokio_test::block_on(async {
let session = PersistSession::new(InMemoryDB::new());
let mut user = UserState::new("Alice".to_string(), 10, true);
user.set_score(15);        // marks field as dirty
user.save(&session).await?; // INSERT on first save, UPDATE on next save
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

### 2. Dynamic Mode from DDL

```rust
use rustmemodb::{PersistCommandModel, Value, persist_struct};

persist_struct! {
    pub struct DdlNote from_ddl =
        "CREATE TABLE source_note (title TEXT NOT NULL, score INTEGER, active BOOLEAN)"
}

let mut note = DdlNote::new()?;
note.set_field("title", Value::Text("Hello".into()))?;
note.set_field("score", Value::Integer(42))?;

// Command-first API is also generated for dynamic entities.
let draft = DdlNoteDraft::new()
    .with_field("title", Value::Text("From draft".into()))?
    .with_field("score", Value::Integer(1))?;
let mut from_draft = <DdlNote as PersistCommandModel>::try_from_draft(draft)?;
from_draft.apply(DdlNoteCommand::set("active", Value::Boolean(true)))?;
from_draft.patch(
    DdlNotePatch::new().with_field("score", Value::Integer(2))?
)?;
```

### 3. Dynamic Mode from JSON Schema

```rust
use rustmemodb::{PersistCommandModel, Value, persist_struct};

persist_struct! {
    pub struct JsonNote from_json_schema = r#"{
      "type": "object",
      "properties": {
        "title": { "type": "string" },
        "count": { "type": "integer" },
        "flag": { "type": "boolean" }
      },
      "required": ["title"]
    }"#
}

let mut note = JsonNote::new()?;
note.set_field("title", Value::Text("Item".into()))?;

// The same Draft/Patch/Command trio exists in JSON Schema mode.
let draft = JsonNoteDraft::new().with_field("title", Value::Text("json".into()))?;
let mut item = <JsonNote as PersistCommandModel>::try_from_draft(draft)?;
item.apply(JsonNoteCommand::set("count", Value::Integer(3)))?;
```

### 4. Heterogeneous Collection (Advanced Low-Level API)

```rust
use rustmemodb::{
    InMemoryDB, PersistSession, PersistEntityFactory, SnapshotMode,
    RestoreConflictPolicy, persist_vec
};

persist_vec!(hetero pub MixedPersistVec);

# tokio_test::block_on(async {
let session = PersistSession::new(InMemoryDB::new());
let mut mixed = MixedPersistVec::new("mixed");
mixed.register_type::<UserState>();
mixed.register_type::<DdlNote>();

mixed.add_one(UserState::new("A".into(), 1, true))?;
mixed.add_one(DdlNote::new()?)?;
mixed.save_all(&session).await?;

let snapshot = mixed.snapshot(SnapshotMode::WithData);

let mut restored = MixedPersistVec::new("restored");
restored.register_type::<UserState>();
restored.register_type::<DdlNote>();
restored
    .restore_with_policy(snapshot, &session, RestoreConflictPolicy::OverwriteExisting)
    .await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

### 5. Managed Collection via `PersistApp` (Recommended)

```rust
use rustmemodb::{PersistApp, persist_struct, persist_vec};

persist_struct! {
    pub struct TodoItem {
        title: String,
        done: bool,
    }
}
persist_vec!(pub TodoVec, TodoItem);

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let mut todos = app.open_vec::<TodoVec>("todos").await?;

let todo_id = todos
    .create_from_draft(TodoItemDraft::new("Write RFC".to_string(), false))
    .await?;

todos
    .patch(
        &todo_id,
        TodoItemPatch {
            done: Some(true),
            ..Default::default()
        },
    )
    .await?;

todos
    .apply_command(&todo_id, TodoItemCommand::SetTitle("Ship RFC".to_string()))
    .await?;

let items = todos.list_page(0, 50);
assert_eq!(items.len(), 1);

todos.delete(&todo_id).await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

Managed write semantics for `PersistApp` collections:
- `create/update/delete` and `create_many/apply_many/delete_many` are atomic.
- Batch operations are `all-or-nothing`: on any write error, in-memory and DB state are rolled back.
- Optimistic lock / write-write / unique-key failures are surfaced as explicit conflicts.
- Retry policy is configured once in `PersistAppPolicy::conflict_retry`; app handlers should not implement retry loops.

Declarative constraints and indexing in typed `persist_struct!`:

```rust
use rustmemodb::{persist_struct, persist_vec, PersistApp};

persist_struct! {
    pub struct UserProfile {
        #[persist(unique)]
        email: String,
        #[persist(index)]
        team_id: String,
        active: bool,
    }
}
persist_vec!(pub UserProfileVec, UserProfile);

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let mut users = app.open_vec::<UserProfileVec>("users").await?;

users
    .create(UserProfile::new(
        "alice@example.com".to_string(),
        "team-a".to_string(),
        true,
    ))
    .await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

- `#[persist(unique)]` enforces uniqueness in managed writes and creates a `UNIQUE INDEX`.
- `#[persist(index)]` creates an index automatically during table bootstrap.
- Repository code no longer needs technical claim-table workarounds for common uniqueness patterns.

`PersistApp` now also exposes a transaction entrypoint for advanced orchestration:

```rust
use rustmemodb::{DbError, PersistApp};

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
app.transaction(|tx| async move {
    tx.execute("CREATE TABLE tx_demo (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
        .await?;
    tx.execute("INSERT INTO tx_demo (id, note) VALUES (1, 'committed')")
        .await?;
    Ok::<(), DbError>(())
})
.await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

Global retry behavior for transaction-level write-write conflicts is policy-driven:

```rust
use rustmemodb::{PersistApp, PersistAppPolicy, PersistConflictRetryPolicy};

# tokio_test::block_on(async {
let mut policy = PersistAppPolicy::default();
policy.conflict_retry = PersistConflictRetryPolicy {
    max_attempts: 3,
    base_backoff_ms: 5,
    max_backoff_ms: 25,
    retry_write_write: true,
};
let _app = PersistApp::open("./.persist_data", policy).await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

- This is the Phase-2 foundation for cross-collection atomic flows with explicit `PersistTx`.
- `PersistApp::open_domain::<...>(...)` is the quick-start entrypoint and returns `PersistDomainStore` (alias over autonomous aggregate).
- `PersistApp::open_autonomous::<...>(...)` remains the explicit optimistic-lock entrypoint when product code wants manual `expected_version`.
- `PersistApp::open_aggregate::<...>(...)` remains available as an advanced/explicit orchestration layer.

Canonical Part B DX route (recommended):

1. Bootstrap with `PersistApp::open_auto(...)` + `open_domain::<...>(...)`.
2. Write business actions via `intent(...)`, `intent_many(...)`, `patch(...)`, `remove(...)`, `workflow_with_create(...)`.
3. Keep handler parsing/mapping in `persist::web` helpers (`parse_if_match_header`, `normalize_idempotency_key`, `normalize_request_id`, `map_conflict_problem`).
4. Prefer domain-outcome methods (`create_one`, `intent_one`, `patch_one`, `remove_one`) to avoid app-layer `Option + DbError` branching.
5. For closure-style business mutations, prefer `mutate_one_with(...)` / `mutate_many_with(...)` to keep domain/business errors typed and avoid `DbError` plumbing.

For product/lesson app code, avoid direct `execute_intent_if_match_auto_audit(...)` usage when the domain route above is sufficient.

- maintainability layout: `src/persist/app.rs` keeps contracts/types; implementations are split into `src/persist/app/{autonomous,aggregate_store,managed_vec,legacy_adapter}.rs`.
- maintainability layout: `src/persist/app.rs` is an entrypoint split into `src/persist/app/{collection_contracts,policies_and_conflicts,app_open,store_types}.rs` plus focused impl modules.
- maintainability layout: `src/persist/app/app_open.rs` is an entrypoint split into `src/persist/app/app_open/{types_and_tx,constructors_and_retry,open_collections,transactions}.rs`.
- maintainability layout: `src/persist/app/aggregate_store.rs` is an entrypoint split into `src/persist/app/aggregate_store/{core,indexed_crud_query,command_audit_workflow}.rs`.
- maintainability layout: `src/persist/app/aggregate_store/command_audit_workflow.rs` is an entrypoint split into `src/persist/app/aggregate_store/command_audit_workflow/{intent_and_audit,command_and_delete,workflow_ops}.rs`.
- maintainability layout: `src/persist/app/autonomous.rs` is an entrypoint split into `src/persist/app/autonomous/{core_read,conflict_and_apply,high_level_convenience,workflow_and_compat}.rs`.
- maintainability layout: `src/persist/app/managed_vec.rs` is an entrypoint split into `src/persist/app/managed_vec/{base_collection,indexed_crud,command_model,optimistic_workflows,io_utils}.rs`.
- maintainability layout: `src/persist/app/managed_vec/indexed_crud.rs` is an entrypoint split into `src/persist/app/managed_vec/indexed_crud/{validation_and_reads,create_paths,update_paths,delete_paths}.rs`.
- maintainability layout: `src/persist/mod.rs` keeps contracts/types; core behavior is split into `src/persist/core/{session_impl,migration_impl,persist_vec_impl,hetero_vec_impl,persist_value_impls,schema_utils}.rs`.
- maintainability layout: `src/persist/mod.rs` is an entrypoint split into `src/persist/core/{api_version,session_and_metadata,descriptors_and_state,dynamic_schema_contracts,snapshots_and_migrations,entity_contracts,containers_and_values}.rs` plus focused impl modules.
- maintainability layout: `src/persist/core/migration_impl.rs` is an entrypoint split into `src/persist/core/migration_impl/{step_builder_and_debug,plan_basics_and_validation,plan_execution}.rs`.
- maintainability layout: `src/persist/core/persist_vec_impl.rs` is an entrypoint split into `src/persist/core/persist_vec_impl/{basics_and_io,invoke_and_prune,snapshot_and_restore}.rs`.
- maintainability layout: `src/persist/core/hetero_vec_impl.rs` is an entrypoint split into `src/persist/core/hetero_vec_impl/{basics_and_registration,collection_mutations,runtime_ops,snapshot_restore}.rs`.
- maintainability layout: `src/persist/core/schema_utils.rs` is an entrypoint split into `src/persist/core/schema_utils/{naming_and_sql,ddl_schema,json_schema}.rs`.
- maintainability layout: `src/persist/runtime.rs` keeps runtime contracts/types; `src/persist/runtime/runtime_support.rs` is a support entrypoint split into `src/persist/runtime/support/{helpers,worker,compat}.rs`; main runtime implementation entrypoint is `src/persist/runtime/runtime_impl.rs`.
- maintainability layout: runtime contracts/types are grouped by domain in `src/persist/runtime/types/{handlers_and_envelope,policy,entity_and_journal,projection,stats_and_registry}.rs`.
- maintainability layout: `src/persist/runtime/types/projection.rs` is an entrypoint split into `src/persist/runtime/types/projection/{contracts,table_and_undo,mailbox}.rs`.
- maintainability layout: `src/persist/runtime/types/handlers_and_envelope.rs` is an entrypoint split into `src/persist/runtime/types/handlers_and_envelope/{handler_types,envelope_and_side_effects,payload_schema}.rs`.
- maintainability layout: `src/persist/runtime/runtime_impl.rs` is split into `src/persist/runtime/runtime_impl/{api_registry_and_crud,command_and_lifecycle,storage_and_projection,internals}.rs`; `api_registry_and_crud.rs` is further split into `src/persist/runtime/runtime_impl/api_registry_and_crud/{open_and_stats,registry_and_projection,entity_crud_and_outbox}.rs`; `command_and_lifecycle.rs` is further split into `src/persist/runtime/runtime_impl/command_and_lifecycle/{deterministic_command,runtime_closure,lifecycle_snapshot}.rs`; `internals.rs` is further split into `src/persist/runtime/runtime_impl/internals/{entity_and_tombstones,journal_and_snapshot,replication_and_io,recovery_and_backpressure}.rs`.
- maintainability layout: `src/persist/runtime/runtime_impl/api_registry_and_crud/registry_and_projection.rs` is an entrypoint split into `src/persist/runtime/runtime_impl/api_registry_and_crud/registry_and_projection/{deterministic_registry,migration_registry,runtime_closure_and_projection}.rs`.
- maintainability layout: `src/persist/runtime/runtime_impl/storage_and_projection.rs` is an entrypoint split into `src/persist/runtime/runtime_impl/storage_and_projection/{disk_and_journal,projections,mailboxes}.rs`.
- maintainability layout: `src/persist/cluster.rs` is an entrypoint split into `src/persist/cluster/{routing,policy_and_trait,node,in_memory_forwarder}.rs`.
- maintainability layout: `src/persist/cluster/routing.rs` is an entrypoint split into `src/persist/cluster/routing/{types,membership,routing_table,shard_hash}.rs`.
- maintainability layout: `src/persist/cluster/routing/routing_table.rs` is an entrypoint split into `src/persist/cluster/routing/routing_table/{construct_and_validate,mutations,lookups}.rs`.
- maintainability layout: `src/persist/macros.rs` is a stable entrypoint and macro bodies are split into `src/persist/macros/{attr_helpers,persist_struct,persist_vec}.rs`.
- maintainability layout: `src/persist/macros.rs` remains the stable entrypoint; macro implementations are kept in `src/persist/macros/persist_struct.rs` and `src/persist/macros/persist_vec.rs` (internal helper split deferred until a lint-safe dispatch strategy is finalized).
- docs contract: generated public APIs from `persist_struct!` and `persist_vec!` include rustdoc comments so consumers can use methods from docs without reading macro internals.
- web adapter helpers expose:
  - `parse_if_match_header(...)`
  - `normalize_idempotency_key(...)`
  - `normalize_request_id(...)`
  - `map_conflict_problem(...)`
  - `PersistServiceError` (shared service-layer error envelope)
  - `PersistServiceError::from_domain_for(...)`
  - `PersistServiceError::from_mutation_for(...)`
- Managed collections expose `*_with_tx(...)` as the primary transaction-scoped mutation path.
- Managed collections expose `atomic_with(...)` to hide cross-collection snapshot/rollback choreography.
- Managed command flows expose:
  - `execute_command_if_match(...)`
  - `execute_patch_if_match(...)`
  - `execute_command_if_match_with_create(...)`
  - `execute_workflow_if_match_with_create(...)`
  - `execute_workflow_for_many_with_create_many(...)`
- Aggregate auto-audit helpers expose:
  - `execute_command_if_match_with_audit(...)`
  - `execute_command_for_many_with_audit(...)`
  - `execute_intent_if_match_auto_audit(...)` using intent-to-command and intent-to-audit mapping functions
  - `execute_intent_for_many_auto_audit(...)` using intent-to-command and intent-to-audit mapping functions
  - built-in `PersistAuditRecord` / `PersistAuditRecordVec`
  - no entity-level audit trait or extra entity annotation is required in product code
- Autonomous aggregate facade exposes:
  - `intent(...)`, `intent_many(...)`, `patch(...)`, `remove(...)` for high-level product code without explicit version plumbing
  - `create_one(...)`, `intent_one(...)`, `patch_one(...)`, `remove_one(...)` for high-level product code with explicit domain outcomes and no low-level error classification
  - `workflow_with_create(...)` for high-level cross-collection workflow execution without explicit version plumbing
  - `apply(...)` and `apply_many(...)` for intent-only command flow with audit fully under the hood
  - `mutate_one_with(...)` and `mutate_many_with(...)` for closure updates with typed business errors (no `DbError` bridge in product code)
  - `patch_if_match(...)` and `delete_if_match(...)`
  - `list_audits_for(...)` for audit history projection without opening a separate audit store
  - `#[derive(PersistAutonomousIntent)]` + `#[persist_intent(...)]` to avoid manual `impl PersistAutonomousCommand<...>`
  - `#[derive(Autonomous)]` + `PersistApp::open_autonomous_model::<Model>(...)` for source-model-first DX without manual `persist_vec!` wiring in app code
  - `#[autonomous_impl]` + `#[rustmemodb::command]` to auto-generate model-specific high-level handle methods (`<Model>AutonomousOps`) instead of handwritten `mutate_one_with(...)` wrappers
  - `#[expose_rest]` + `#[rustmemodb::command]`/`#[rustmemodb::query]`/`#[rustmemodb::view]` to auto-generate axum routers, DTOs and handlers from model methods
  - `#[derive(ApiError)]` + `#[api_error(status = ...)]` to auto-map domain errors into `PersistServiceError`/HTTP semantics
  - `PersistApp::serve_autonomous_model::<Model>(...)` to mount generated REST without manual `api.rs`/`store.rs`
  - `PersistApp::serve_json_schema_dir(...)` to mount generic CRUD REST from runtime `schemas/*.json` without handwritten handlers
  - create endpoint auto-derives DTO from `new(...)` constructor arguments (fallback: full model payload)
  - `#[rustmemodb::query]` supports typed query arguments (`GET /:id/<query>?...`) without manual DTO wiring
  - `#[rustmemodb::view(input = "body")]` supports typed body DTOs (`POST /:id/<view>`) without manual handlers
  - generated REST includes built-in audit endpoint `GET /:id/_audits` (no separate audit API layer)
  - generated command endpoints support `Idempotency-Key` replay (same status/body, no duplicate mutation on retry)
  - single-id APIs accept `impl AsRef<str>` to avoid repetitive `&id`/`to_string()` adapters in app services/handlers
- Aggregate query helpers expose:
  - `find_first(...)`
  - `query_page_filtered_sorted(...)`
- Managed delete flows expose:
  - `execute_delete_if_match(...)`
- `*_with_session(...)` remains available for low-level migration/internal scenarios.
- Recommended app-layer shape: keep `workspace/service` as a thin business-intent facade and call these managed helpers directly from intent methods; do not add extra repository/store/session orchestration layers in product code.

```rust
use rustmemodb::{persist_struct, persist_vec, DbError, PersistApp};

persist_struct! {
    pub struct SignupUser {
        #[persist(unique)]
        email: String,
        active: bool,
    }
}
persist_vec!(pub SignupUserVec, SignupUser);

persist_struct! {
    pub struct SignupMetric {
        category: String,
        value: i64,
    }
}
persist_vec!(pub SignupMetricVec, SignupMetric);

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let mut users = app.open_aggregate::<SignupUserVec>("users").await?;
let mut metrics = app.open_aggregate::<SignupMetricVec>("metrics").await?;

let user = SignupUser::new("atomic@example.com".to_string(), true);

users
    .atomic_with(&mut metrics, move |tx, users, metrics| {
        Box::pin(async move {
            users.create_with_tx(&tx, user).await?;
            metrics
                .create_with_tx(&tx, SignupMetric::new("signup".to_string(), 1))
                .await?;
            Ok::<(), DbError>(())
        })
    })
    .await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

```rust
use rustmemodb::{PersistApp, persist_struct, persist_vec};

persist_struct! {
    pub struct Profile {
        name: String,
        active: bool,
    }
}
persist_vec!(pub ProfileVec, Profile);

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let mut profiles = app.open_aggregate::<ProfileVec>("profiles").await?;
let profile = Profile::new("Alice".to_string(), true);
let profile_id = profile.persist_id().to_string();
profiles.create(profile).await?;

let updated = profiles
    .execute_command_if_match(
        &profile_id,
        1,
        ProfileCommand::SetActive(false),
    )
    .await?
    .expect("profile must exist");
assert_eq!(*updated.active(), false);
let deleted = profiles.execute_delete_if_match(&profile_id, 2).await?;
assert!(deleted);
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

```rust
use rustmemodb::{PersistApp, persist_struct, persist_vec};

persist_struct! {
    pub struct QueryTodo {
        title: String,
        done: bool,
    }
}
persist_vec!(pub QueryTodoVec, QueryTodo);

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let mut todos = app.open_aggregate::<QueryTodoVec>("query_todos").await?;
todos
    .create_many(vec![
        QueryTodo::new("Gamma".to_string(), true),
        QueryTodo::new("Alpha".to_string(), true),
        QueryTodo::new("Zeta".to_string(), false),
    ])
    .await?;

let page = todos.query_page_filtered_sorted(
    1,
    2,
    |todo| *todo.done(),
    |left, right| left.title().cmp(right.title()),
);
assert_eq!(page.total, 2);
assert_eq!(page.items.len(), 2);
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

```rust
use rustmemodb::{PersistApp, PersistAutonomousIntent, persist_struct, persist_vec};

persist_struct! {
    pub struct AuditUser {
        name: String,
        active: bool,
    }
}
persist_vec!(pub AuditUserVec, AuditUser);

#[derive(Clone, Copy, PersistAutonomousIntent)]
#[persist_intent(model = AuditUser)]
enum AuditUserIntent {
    #[persist_case(command = AuditUserCommand::SetActive(false))]
    Deactivate,
}

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let mut users = app.open_domain::<AuditUserVec>("audit_users").await?;
let user = AuditUser::new("Alice".to_string(), true);
let user_id = user.persist_id().to_string();
users.create(user).await?;

users.intent(&user_id, AuditUserIntent::Deactivate).await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

Source-model autonomous derive (no manual `persist_struct!` / `persist_vec!` in app code):

```rust
use rustmemodb::{Autonomous, PersistApp};

#[derive(Debug, Clone, Autonomous)]
#[persist_model(table = "board_model", schema_version = 1)]
struct Board {
    name: String,
    active: bool,
}

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let boards = app
    .open_autonomous_model::<Board>("boards")
    .await?;

let created = boards
    .create_one(Board {
        name: "Platform".to_string(),
        active: true,
    })
    .await?;

let (updated, board_name) = boards
    .mutate_one_with_result(&created.persist_id, |board| {
        board.active = false;
        Ok::<String, std::convert::Infallible>(board.name().to_string())
    })
    .await?;

assert!(!updated.model.active);
assert_eq!(board_name, "Platform");
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

You can auto-generate model-specific high-level handle methods from domain methods:

```rust
use rustmemodb::autonomous_impl;

#[autonomous_impl]
impl Board {
    #[rustmemodb::command]
    fn deactivate(&mut self) -> bool {
        self.active = false;
        self.active
    }
}

# tokio_test::block_on(async {
# let app = rustmemodb::PersistApp::open_auto("./.persist_data").await?;
# let boards = app.open_autonomous_model::<Board>("boards").await?;
# let created = boards.create_one(Board { name: "P".into(), active: true }).await?;
let active = boards.deactivate(&created.persist_id).await?;
assert!(!active);
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

JSON field boilerplate can be derived directly (no local wrapper type needed):

```rust
use rustmemodb::{PersistJsonValue, persist_struct};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PersistJsonValue)]
enum LobbyState {
    Waiting,
    InProgress,
}

persist_struct! {
    pub struct Lobby {
        state: LobbyState,
    }
}
```

Trait-mapped workflow helper (no inline closure plumbing in app service code):

```rust
use rustmemodb::{PersistApp, PersistWorkflowCommandModel, persist_struct, persist_vec};

persist_struct! {
    pub struct WorkflowUser {
        name: String,
        active: bool,
    }
}
persist_vec!(pub WorkflowUserVec, WorkflowUser);

persist_struct! {
    pub struct WorkflowAudit {
        user_id: String,
        event_type: String,
        resulting_version: i64,
    }
}
persist_vec!(pub WorkflowAuditVec, WorkflowAudit);

#[derive(Clone, Copy)]
struct DeactivateWorkflow;

impl PersistWorkflowCommandModel<DeactivateWorkflow, WorkflowAudit> for WorkflowUser {
    fn to_persist_command(_: &DeactivateWorkflow) -> Self::Command {
        WorkflowUserCommand::SetActive(false)
    }

    fn to_related_record(
        _: &DeactivateWorkflow,
        updated: &Self,
    ) -> rustmemodb::Result<WorkflowAudit> {
        Ok(WorkflowAudit::new(
            updated.persist_id().to_string(),
            "deactivate".to_string(),
            updated.metadata().version,
        ))
    }
}

# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let mut users = app.open_aggregate::<WorkflowUserVec>("workflow_users").await?;
let mut audits = app.open_aggregate::<WorkflowAuditVec>("workflow_audits").await?;
let user = WorkflowUser::new("Alice".to_string(), true);
let user_id = user.persist_id().to_string();
users.create(user).await?;

users
    .execute_workflow_if_match_with_create(&mut audits, &user_id, 1, DeactivateWorkflow)
    .await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

Temporary migration adapter for old vector-style code:

```rust
use rustmemodb::{PersistApp, RestoreConflictPolicy, SnapshotMode};
# use rustmemodb::{persist_struct, persist_vec};
# persist_struct! {
#   pub struct TodoItem {
#       title: String,
#       done: bool,
#   }
# }
# persist_vec!(pub TodoVec, TodoItem);
# tokio_test::block_on(async {
let app = PersistApp::open_auto("./.persist_data").await?;
let mut legacy = app.open_vec_legacy::<TodoVec>("todos").await?;

legacy.add_one(TodoItem::new("Legacy style".to_string(), false));
legacy.save_all().await?;

let snapshot = legacy.snapshot(SnapshotMode::WithData);
legacy
    .restore_with_policy(snapshot, RestoreConflictPolicy::OverwriteExisting)
    .await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

```rust
use rustmemodb::{classify_managed_conflict, ManagedConflictKind};
# use rustmemodb::{PersistApp, persist_struct, persist_vec};
# persist_struct! {
#   pub struct TodoItem {
#       title: String,
#       done: bool,
#   }
# }
# persist_vec!(pub TodoVec, TodoItem);
# tokio_test::block_on(async {
# let app = PersistApp::open_auto("./.persist_data").await?;
# let mut todos = app.open_vec::<TodoVec>("todos").await?;
# let todo_id = todos
#     .create_from_draft(TodoItemDraft::new("Write RFC".to_string(), false))
#     .await?;
# let mut stale = app.open_vec::<TodoVec>("todos").await?;
# todos
#     .patch(
#         &todo_id,
#         TodoItemPatch {
#             done: Some(true),
#             ..Default::default()
#         },
#     )
#     .await?;
let err = stale
    .apply_command(&todo_id, TodoItemCommand::SetDone(true))
    .await
    .expect_err("stale update should conflict");

assert_eq!(
    classify_managed_conflict(&err),
    Some(ManagedConflictKind::OptimisticLock)
);
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

### 6. Auto-Persist With Bound Session (Advanced Low-Level API)

```rust
use rustmemodb::{InMemoryDB, PersistSession, PersistEntity, persist_struct};

persist_struct! {
    pub struct LiveUser {
        name: String,
        score: i64,
        active: bool,
    }
}

# tokio_test::block_on(async {
let session = PersistSession::new(InMemoryDB::new());
let mut user = LiveUser::new("Mia".into(), 1, true);
user.bind_session(session.clone());
user.set_auto_persist(true)?;
user.set_score_persisted(2).await?; // changes + save automatically
user.mutate_persisted(|u| u.set_active(false)).await?; // batch mutate + auto-save
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

### 7. Existing Struct via Derive (`PersistModel`)

```rust
use rustmemodb::{PersistModel, PersistEntity, persist_struct};

#[derive(PersistModel)]
#[persist_model(schema_version = 2)]
struct Task {
    title: String,
    done: bool,
    attempts: i64,
}

persist_struct!(pub struct PersistedTask from_struct = Task);

# tokio_test::block_on(async {
let mut task = PersistedTask::from_draft(PersistedTaskDraft::new(
    "Write tests".into(),
    false,
    0,
));
task.apply(PersistedTaskCommand::SetAttempts(1))?;
task.patch(PersistedTaskPatch {
    done: Some(true),
    ..Default::default()
})?;

// Alias generated from existing struct:
let _alias = PersistedTask::from_parts("Ship".into(), false, 1);
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

### 8. Attribute DSL (`#[persistent]`, `#[persistent_impl]`, `#[command]`)

```rust
use rustmemodb::{InMemoryDB, PersistEntityRuntime, PersistSession, RuntimeOperationalPolicy};

#[rustmemodb::persistent(schema_version = 2, table = "wallets")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct Wallet {
    #[sql(index)]
    owner: String,
    #[sql]
    balance: i64,
}

#[rustmemodb::persistent_impl]
impl Wallet {
    #[rustmemodb::command]
    fn deposit(&mut self, amount: i64) -> rustmemodb::Result<i64> {
        self.balance += amount;
        Ok(self.balance)
    }
}

# tokio_test::block_on(async {
let session = PersistSession::new(InMemoryDB::new());
let mut wallet = Wallet { owner: "A".into(), balance: 10 }.into_persisted();
wallet.bind_session(session);
wallet.save_bound().await?;

let output = wallet
    .apply_domain_command_persisted(WalletPersistentCommand::Deposit { amount: 5 })
    .await?;
assert_eq!(output.as_i64(), Some(15));

let command = WalletPersistentCommand::Deposit { amount: 3 };
let envelope = wallet.domain_command_envelope_with_expected_version(&command)?;
assert_eq!(envelope.command_name, "deposit");
assert_eq!(envelope.payload_json["amount"], serde_json::json!(3));

// Optional bridge: register all #[command] methods as runtime deterministic handlers.
let mut runtime = PersistEntityRuntime::open(
    "./tmp_wallet_runtime",
    RuntimeOperationalPolicy::default(),
).await?;
WalletPersisted::try_register_domain_commands_in_runtime(&mut runtime)?;

// Projection helpers are generated for indexed #[sql(index)] fields.
let wallet_id = runtime
    .create_entity(
        "Wallet",
        "wallet_state",
        serde_json::json!({"owner": "A", "balance": 15}),
        1,
    )
    .await?;
let ids = WalletPersisted::find_projection_ids_by_owner(&runtime, "A".to_string())?;
assert_eq!(ids, vec![wallet_id]);
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

Full end-to-end usage is shown in `examples/managed/crm_no_sql.rs`.

Projection mapping rules:
- if no field has `#[sql(...)]`, all model fields are projected by default;
- if at least one field has `#[sql(...)]`, only explicitly annotated fields are projected.

### 9. Schema Versioning and Migrations (Stable API)

```rust
use rustmemodb::{
    InMemoryDB, PersistMigrationPlan, PersistMigrationStep, PersistSession,
    RestoreConflictPolicy, SnapshotMode
};

# use rustmemodb::{persist_struct, persist_vec, PersistEntity};
# persist_struct! {
#   pub struct UserState {
#       name: String,
#       score: i64,
#       active: bool,
#   }
# }
# persist_vec!(pub UserStateVec, UserState);
# tokio_test::block_on(async {
let source_session = PersistSession::new(InMemoryDB::new());
let mut users = UserStateVec::new("users");
users.add_one(UserState::new("A".into(), 10, true));
users.save_all(&source_session).await?;

let mut snapshot = users.snapshot(SnapshotMode::WithData);
for state in &mut snapshot.states {
    state.metadata.schema_version = 1;
}

let mut plan = PersistMigrationPlan::new(2);
plan.add_step(
    PersistMigrationStep::new(1, 2).with_state_migrator(|state| {
        let fields = state.fields_object_mut()?;
        let score = fields.get("score").and_then(|v| v.as_i64()).unwrap_or(0);
        fields.insert("score".to_string(), serde_json::json!(score * 10));
        Ok(())
    }),
)?;

let restore_session = PersistSession::new(InMemoryDB::new());
let mut restored = UserStateVec::new("users-restored");
restored
    .restore_with_custom_migration_plan(
        snapshot,
        &restore_session,
        RestoreConflictPolicy::FailFast,
        plan,
    )
    .await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

`restore_with_policy` supports:
- `FailFast` (default)
- `SkipExisting`
- `OverwriteExisting`

Persist macros and migration contracts above are exposed as stable public API.

### 10. Public API Version Contract

`persist` public API is explicitly versioned:

- `PERSIST_PUBLIC_API_VERSION_STRING`
- `PERSIST_PUBLIC_API_VERSION_MAJOR`
- `PERSIST_PUBLIC_API_VERSION_MINOR`
- `PERSIST_PUBLIC_API_VERSION_PATCH`
- `persist_public_api_version()`

CLI helper:

```bash
cargo run --bin persist_tool -- api-version
```

### 11. Legacy -> Managed Migration Path

Migration path is first-class and integration-tested:

1. Open old vec style through `open_vec_legacy`.
2. Persist existing snapshot/state with `legacy.save_all()`.
3. Re-open the same vec via `open_vec` and continue with command-first managed API.

Reference integration tests:
- `tests/persist_app_tests.rs::persist_app_legacy_adapter_supports_old_vector_style_flow`
- `tests/persist_app_tests.rs::managed_collection_continues_from_legacy_snapshot_without_manual_restore`

---

## 🧠 Persist Entity Runtime

For long-running autonomous entities (event-sourcing style), use `PersistEntityRuntime`.
It gives:

- deterministic command registry (replay-safe handlers),
- envelope-first command API (`RuntimeCommandEnvelope`),
- command migration registry for legacy envelopes (`register_command_migration`/`register_command_alias`),
- deterministic context API for sanctioned handlers (`RuntimeDeterministicContext`),
- consistency profiles (`RuntimeConsistencyMode::{Strong, LocalDurable, Eventual}`),
- expected-version CAS checks on write,
- scoped idempotency deduplication (`entity_type:entity_id:command:idempotency_key`),
- strict payload contracts for command input (`RuntimeCommandPayloadSchema`),
- deterministic side-effects to durable outbox records,
- projection contracts (`RuntimeProjectionContract`) with synchronous write path,
- indexed projection lookups (`find_projection_*`) for `#[sql(index)]` fields,
- projection rebuild from loaded snapshot+journal state (`rebuild_registered_projections`),
- durable JSONL journal + crash recovery,
- snapshot scheduler + compaction,
- optional background snapshot worker (`spawn_runtime_snapshot_worker`),
- replication shipping for journal/snapshot (sync or async best-effort),
- lifecycle passivation/resurrection/GC,
- explicit tombstone retention policy with TTL (`RuntimeTombstonePolicy`) for delete/GC behavior,
- mailbox-backed lifecycle safety (busy entities are excluded from passivate/GC),
- tracing spans/events for envelope flow and outbox dispatch (OpenTelemetry-friendly via `tracing`),
- SLO runtime stats (`durability_lag_ms`, `projection_lag_entities`, `lifecycle_churn_total`) via `stats()`/`slo_metrics()`,
- compatibility diagnostics for persisted artifacts (`runtime_snapshot_compat_check`/`runtime_journal_compat_check`),
- strict/eventual durability policies with retry/backpressure controls,
- optional non-serializable runtime closures for local behavior.

```rust
use rustmemodb::{
    PersistEntityRuntime, RuntimeCommandEnvelope, RuntimeConsistencyMode,
    RuntimeDeterminismPolicy, RuntimeOperationalPolicy, RuntimeSideEffectSpec,
    RuntimeTombstonePolicy,
};
use serde_json::json;

# tokio_test::block_on(async {
let mut policy = RuntimeOperationalPolicy::default();
policy.consistency = RuntimeConsistencyMode::Strong;
policy.determinism = RuntimeDeterminismPolicy::StrictContextOnly;
policy.tombstone = RuntimeTombstonePolicy {
    ttl_ms: 300_000,
    retain_for_lifecycle_gc: true,
};

let mut rt = PersistEntityRuntime::open("./runtime_data", policy).await?;
rt.register_deterministic_context_command("Counter", "increment", std::sync::Arc::new(|state, payload, ctx| {
    let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
    let fields = state.fields_object_mut()?;
    let current = fields.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
    fields.insert("count".to_string(), json!(current + delta));
    state.metadata.version = state.metadata.version.saturating_add(1);
    Ok(vec![RuntimeSideEffectSpec {
        effect_type: "counter.incremented".to_string(),
        payload_json: json!({
            "delta": delta,
            "event_id": ctx.deterministic_uuid("counter.incremented")
        }),
    }])
}));

let id = rt.create_entity("Counter", "counter_table", json!({"count": 0}), 1).await?;
let envelope = RuntimeCommandEnvelope::new("Counter", &id, "increment", json!({"delta": 2}))
    .with_expected_version(1)
    .with_idempotency_key("inc-1");
let applied = rt.apply_command_envelope(envelope).await?;
for event in applied.outbox {
    rt.mark_outbox_dispatched(&event.outbox_id).await?;
}
let stats = rt.stats();
assert_eq!(stats.projection_lag_entities, 0);
let _tombstones = rt.list_tombstones();
let slo = rt.slo_metrics();
assert_eq!(slo.projection_lag_entities, 0);
rt.register_command_alias("Counter", "increment", 1, "increment").unwrap();
rt.force_snapshot_and_compact().await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

See full scenario in:
- [examples/managed](examples/managed/README.md)
- [examples/advanced/persist_runtime_showcase](examples/advanced/persist_runtime_showcase/README.md)
- [examples/advanced/sentinel_core](examples/advanced/sentinel_core/README.md)
- [examples/managed/todo_persist_runtime](examples/managed/todo_persist_runtime/README.md)
- [examples/ledger_core](examples/ledger_core/README.md)
- CLI tooling: `cargo run --bin persist_tool -- --help` (`compat-check --snapshot ... --journal ...`)

### Cluster Bootstrap (Shard Routing + Forwarding + Quorum Baseline)

```rust
use rustmemodb::{
    InMemoryRuntimeForwarder, PersistEntityRuntime, RuntimeClusterNode,
    RuntimeClusterMembership, RuntimeClusterWritePolicy, RuntimeOperationalPolicy, RuntimeShardRoutingTable
};
use std::sync::Arc;
use tokio::sync::Mutex;

# tokio_test::block_on(async {
let forwarder = InMemoryRuntimeForwarder::new();
let remote = Arc::new(Mutex::new(
    PersistEntityRuntime::open("./cluster_remote", RuntimeOperationalPolicy::default()).await?
));
let mut remote_routing = RuntimeShardRoutingTable::new(16, "node-local")?;
remote_routing.set_shard_leader(0, "node-remote", 2)?;
forwarder
    .register_peer_with_routing("node-remote", remote.clone(), Some(remote_routing))
    .await?;

let mut routing = RuntimeShardRoutingTable::new(16, "node-local")?;
routing.set_shard_leader(0, "node-remote", 2)?;
routing.set_shard_followers(0, vec!["node-follower-b".to_string()])?;
routing.set_shard_quorum(0, 2)?;
let membership = RuntimeClusterMembership::new(vec![
    "node-local".to_string(),
    "node-remote".to_string(),
    "node-follower-b".to_string(),
])?;
let _movement = routing.move_shard_leader(0, "node-remote", Some(&membership))?;

let node = RuntimeClusterNode::new_with_policy(
    "node-local",
    routing,
    Arc::new(forwarder),
    RuntimeClusterWritePolicy {
        require_quorum: true,
        enforce_epoch_fencing: true,
    },
)?;
# let mut local_rt = PersistEntityRuntime::open("./cluster_local", RuntimeOperationalPolicy::default()).await?;
# let _ = node;
# let _ = local_rt;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

Integration scenarios for routing/quorum/fencing/failover are covered in:
- `tests/persist_cluster_tests.rs`

---

## ⚙️ Engineering Internals

We take engineering seriously. This is not just a `Vec<Row>`.

*   **MVCC (Multi-Version Concurrency Control):**
    *   Snapshot visibility rules are applied in storage.
    *   Connection-level execution is currently serialized (global lock).
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
