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
- [Persist Showcase Example](examples/persist_showcase/README.md)
- [Persist Runtime Showcase Example](examples/persist_runtime_showcase/README.md)
- [Todo Persist Runtime Example](examples/todo_persist_runtime/README.md)

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

### 1. Typed Mode (`struct` input)

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

### 4. Heterogeneous Collection

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

### 5. Managed Collection via `PersistApp`

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

### 5. Auto-Persist With Bound Session

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

### 6. Existing Struct via Derive (`PersistModel`)

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

### 7. Schema Versioning and Migrations (Stable API)

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

---

## 🧠 Persist Entity Runtime

For long-running autonomous entities (event-sourcing style), use `PersistEntityRuntime`.
It gives:

- deterministic command registry (replay-safe handlers),
- strict payload contracts for command input (`RuntimeCommandPayloadSchema`),
- durable JSONL journal + crash recovery,
- snapshot scheduler + compaction,
- optional background snapshot worker (`spawn_runtime_snapshot_worker`),
- replication shipping for journal/snapshot (sync or async best-effort),
- lifecycle passivation/resurrection/GC,
- strict/eventual durability policies with retry/backpressure controls,
- optional non-serializable runtime closures for local behavior.

```rust
use rustmemodb::{
    PersistEntityRuntime, RuntimeDurabilityMode, RuntimeOperationalPolicy
};
use serde_json::json;

# tokio_test::block_on(async {
let mut policy = RuntimeOperationalPolicy::default();
policy.durability = RuntimeDurabilityMode::Strict;

let mut rt = PersistEntityRuntime::open("./runtime_data", policy).await?;
rt.register_deterministic_command("Counter", "increment", std::sync::Arc::new(|state, payload| {
    let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
    let fields = state.fields_object_mut()?;
    let current = fields.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
    fields.insert("count".to_string(), json!(current + delta));
    Ok(())
}));

let id = rt.create_entity("Counter", "counter_table", json!({"count": 0}), 1).await?;
rt.apply_deterministic_command("Counter", &id, "increment", json!({"delta": 2})).await?;
rt.force_snapshot_and_compact().await?;
# Ok::<(), rustmemodb::DbError>(())
# })?;
```

See full scenario in:
- [examples/persist_runtime_showcase](examples/persist_runtime_showcase/README.md)
- [examples/todo_persist_runtime](examples/todo_persist_runtime/README.md)
- CLI tooling: `cargo run --bin persist_tool -- --help`

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
