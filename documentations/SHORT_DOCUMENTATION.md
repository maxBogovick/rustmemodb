# Complete documentation — Rust in-memory DB implementation

> NOTE: This document is outdated and may not reflect current behavior.
> Use `README.md` and `MEMORY_BANK.md` for the current feature set and limitations.

# Overview

This project implements an embeddable, in-memory SQL engine with a small SQL parser, an executor pipeline, a lightweight storage/catalog layer, a transaction manager, and some adapters (JSON storage adapter, client/connection pooling, auth). The main façade type exported for convenient usage is `InMemoryDB` (facade) and there is a `Client` wrapper providing pooled access for multi-threaded applications.

Key modules / responsibilities

* `core` — fundamental types and small helpers (error types, `Value`, `DataType`, `Column`, `Table`, `QueryResult`).
* `parser` / `planner` / `executor` — parse SQL, generate query plans and run statements using an executor pipeline of executors (DDL/DML/Query).
* `storage` — (InMemoryStorage) persistent layers, catalog/schema, indexing and persistence manager; used by `InMemoryDB`.
* `transaction` — MVCC transaction manager used by the storage/executor layer.
* `connection` — connection pool, `Client`, auth manager, config objects.
* `json` — JSON import / adapter to run SQL over JSON docs (adapter and converters).

---

# Core primitives

### `DbError` and `Result<T>`

`DbError` is the project error enum (parse errors, table not found, type mismatch, constraint violations, unsupported operation, etc.). `Result<T>` is an alias using that error. Use it for all API results.

### `Value` (enum)

Represents runtime cell values.

Variants:

* `Null`
* `Integer(i64)`
* `Float(f64)`
* `Text(String)`
* `Boolean(bool)`

Important methods:

* `type_name(&self) -> &'static str` — returns `"integer" / "float" / "text" / "boolean" / "null"`.
* `as_bool(&self) -> bool` — truthiness conversion used by boolean evaluation: `Null -> false`, booleans as themselves; numeric `!= 0` is true; text non-empty is true.
* `coerce_numeric(self, other: Value) -> Result<(NumericValue, NumericValue)>` — used for numeric comparisons/coercion between integer and float. Returns `NumericValue` internal helper.

`Display` is implemented to render values as strings for printing.

---

### `DataType`

Rust enum representing schema types: `Integer`, `Float`, `Text`, `Boolean`.

Key methods:

* `from_sql(sqlparser::ast::DataType) -> Result<DataType>` — maps SQL parser types to `DataType`. It supports `Int/Integer/BigInt -> Integer`, `Float/Double/Real -> Float`, `Text/Varchar/Char/String -> Text`, `Boolean/Bool -> Boolean`. Unsupported SQL types return a `TypeMismatch`/error.
* `is_compatible(&self, &Value) -> bool` — check whether a runtime `Value` is compatible with this declared `DataType` (special casing `Null` as compatible).

`Display` is implemented to present `INTEGER`, `FLOAT`, `TEXT`, `BOOLEAN`.

---

### `Column`

Represents a table column.

Fields:

* `name: String`
* `data_type: DataType`
* `nullable: bool`

Constructors and helpers:

* `Column::new(name, data_type) -> Column` — nullable by default.
* `Column::not_null(self) -> Column` — builder style to mark not nullable.
* `validate(&self, value: &Value) -> Result<()>` — checks `NULL` and type compatibility; returns `ConstraintViolation` if `NULL` on not nullable column, or `TypeMismatch` if types mismatch.

---

### `Row` and `Table`

* `type Row = Vec<Value>` — a row is a vector of `Value`.
* `struct Table` holds:

    * `name: String`
    * `columns: Vec<Column>`
    * `rows: Vec<Row>`

Important `Table` methods:

* `Table::new(name, columns) -> Table` — construct an empty table.
* `name(&self) -> &str`, `columns(&self) -> &[Column]`, `rows(&self) -> &[Row]` — accessors.
* `insert(&mut self, row: Row) -> Result<()>` — validates the row (column count + per-column `Column::validate`) and appends it to `rows`. Returns `ExecutionError` for column count mismatch, or other validation errors.
* `validate_row(&self, row: &Row) -> Result<()>` — internal check used by `insert`.
* `find_column_index(&self, name: &str) -> Result<usize>` — returns column index or `ColumnNotFound` error.
* `get_column(&self, name: &str) -> Result<&Column>` — get column metadata.
* `row_count(&self) -> usize` — number of rows.

---

### `QueryResult`

Result of a select/query execution.

Fields:

* `columns: Vec<String>`
* `rows: Vec<Row>`

Methods:

* `QueryResult::empty()` — empty result.
* `QueryResult::new(columns, rows)` — create with columns and rows.
* `row_count(&self) -> usize` — number of rows.
* `is_empty(&self) -> bool` — true if no rows.
* `print(&self)` — nice textual printer that computes column widths and prints rows (used heavily in examples). Note: printing logic computes column widths and formats rows; it returns early if no columns.

---

# Expression evaluation & query planning

### `ExpressionEvaluator<'a>`

Evaluates SQL expressions against a `Table` and a row.

Main functions:

* `new(table: &'a Table) -> Self` — create evaluator bound to a table schema.

* `evaluate(&self, expr: &Expr, row: &Row) -> Result<Value>` — evaluate any supported `sqlparser::ast::Expr` into a `Value`. Supported forms include:

    * `Identifier` — resolves to a column value via `find_column_index`.
    * `Value` (literal) — parse numbers/strings/boolean/null into `Value`.
    * `BinaryOp` — handles boolean `AND`/`OR` and comparison ops mapped into `ComparisonOp`.
    * `Like` — pattern matching implemented via regex transforming SQL LIKE `%/_` into regex; note both case sensitive and case insensitive support (but default in evaluator uses case sensitive). The `like_to_regex` and `eval_like` helpers implement pattern conversion and matching.
    * `Between` — implemented by evaluating low/high and using comparisons.

* `evaluate_as_bool(&self, expr: &Expr, row: &Row) -> Result<bool>` — evaluate expression and interpret its truthiness with `Value::as_bool`. Used for WHERE predicate evaluation.

Internal helpers:

* `eval_identifier`, `eval_literal`, `eval_binary_op` (with `eval_and`, `eval_or`, `eval_comparison`), `eval_like`, `eval_between`, `parse_number` and numeric coercion via `ComparisonOp`/`NumericValue`. All implement type checking and return `TypeMismatch`/`UnsupportedOperation` when needed.

### `ComparisonOp`

Enum for Eq/Ne/Lt/Le/Gt/Ge and conversion from `sqlparser::BinaryOperator`. Provides `apply(&self, left: &Value, right: &Value) -> Result<bool>` which performs proper type checks and numeric coercions.

### `QueryPlan`

A simplified plan produced from a parsed `SELECT`:

Fields:

* `table_name: String`
* `selected_columns: Vec<String>` (supports `*` wildcard)
* `filter: Option<Expr>`

Methods:

* `from_select(select: &Select) -> Result<QueryPlan>` — extracts table (only supports single table), selected columns (simple identifiers or wildcard). Returns `UnsupportedOperation` for complex projections or multi-table queries.
* `execute(&self, table: &Table) -> Result<QueryResult>` — applies filter across table rows (using `ExpressionEvaluator`) and projects the selected columns (or all columns for `*`), returning a `QueryResult`. The filter is applied row by row. Ordering/limit/grouping are not implemented in this simple plan (complex features appear unsupported).

---

# Statement execution pipeline

A pluggable executor pipeline is used; executors implement the `StatementExecutor` trait:

### `trait StatementExecutor`

* `fn can_execute(&self, stmt: &Statement) -> bool` — indicates whether this executor can handle a statement.
* `fn execute(&self, context: &mut ExecutionContext, stmt: &Statement) -> Result<QueryResult>` — run the statement using the provided execution context and return a `QueryResult`.

Provided executors (default set)

* `CreateTableExecutor` — handles `CREATE TABLE` statements.

    * Extracts table name and column definitions (via `parse_column_definitions`), constructs `Table` and inserts into execution context. Errors: `TableExists` if exists.
* `InsertExecutor` — handles `INSERT` (values) statements.

    * Parses literal values for each row and calls `Table::insert`. Only supports `INSERT INTO ... VALUES (...)` where values are literal expressions.
* `SelectExecutor` (query executor) — handles `SELECT` queries by creating a `QueryPlan::from_select` and executing it. Supports only single-table selects and simple projections (identifiers or `*`).

Executors for other operations (in other project areas) exist—e.g. pipeline in the more advanced `InMemoryDB` implementation shows registration of `Begin/Commit/Rollback/Insert/Delete/Update/Query` etc.—the pipeline is extensible by `register(Box::new(...))`.

---

# `ExecutionContext`

Holds in-memory tables and provides table management functions.

Key methods:

* `ExecutionContext::new() -> ExecutionContext` — creates empty context.
* `get_table(&self, name: &str) -> Result<&Table>` — fetch table read-only or `TableNotFound`.
* `get_table_mut(&mut self, name: &str) -> Result<&mut Table>` — get mutable table reference.
* `insert_table(&mut self, table: Table) -> Result<()>` — insert a new table; if name exists returns `TableExists`.
* `list_tables(&self) -> Vec<&str>` — list table names.
* `table_exists(&self, name: &str) -> bool` — check presence.

---

# `InMemoryDB` façade (high-level API)

There are two layers in your repo: a small `facade::InMemoryDB` exported publicly and a more advanced `InMemoryDB` implementation with parser/executor/storage/tx manager. The facade type exposed via `pub use facade::InMemoryDB;` is the primary entrypoint used by examples. Both are documented below (the repo contains multiple variants — the main, full featured one lives in `src` with parser/executor/storage).

## Key constructors

* `InMemoryDB::new()` — create a fresh instance with default pipeline and empty storage/catalog/transaction manager. The full implementation registers default executors (begin/commit/rollback/create/drop/insert/delete/update/query).
* `InMemoryDB::global()` — (lazy_static) global `Arc<RwLock<InMemoryDB>>` for shared global DB in some uses.

## Query/DDL API (async)

* `pub async fn execute(&mut self, sql: &str) -> Result<QueryResult>` — high level execute method that calls `execute_with_transaction(sql, None)`. It parses SQL and runs statement(s) through the executor pipeline; returns a `QueryResult`. Examples use this method from sync code via `tokio` or from `Client`.
* `execute_with_transaction(&mut self, sql: &str, transaction_id: Option<TransactionId>) -> Result<QueryResult>` — executes SQL within optional explicit transaction id (internal API used by transaction manager and worker threads).

## Internal helpers

* `parse_sql(&self, sql: &str) -> Result<Vec<Statement>>` — uses `sqlparser` with `PostgreSqlDialect` to parse SQL; returns parse error if invalid.
* `execute_statement(&mut self, stmt: &Statement) -> Result<QueryResult>` — routes a statement to the first executor in the pipeline that `can_execute` it; returns `UnsupportedOperation` if none match.

## Catalog / Storage / Transactions

The full `InMemoryDB` contains:

* `storage: InMemoryStorage` — actual table storage and persistence helpers.
* `catalog: Catalog` — schema metadata, used by query executor.
* `transaction_manager: Arc<TransactionManager>` — MVCC transaction manager.
* `persistence: Option<Arc<Mutex<PersistenceManager>>>` — optional persistence (WAL/checkpoint) support and durability modes. Methods like `enable_persistence`, `checkpoint`, `vacuum`, etc. are present in examples and storage module (see persistence demo/example).

## Utilities

* `table_stats(&self, name: &str) -> Result<TableStats>` — returns `TableStats { name, column_count, row_count }`. `TableStats` implements `Display`.
* `list_tables`, `table_exists`, `get_table` — proxies to execution context.

**Executor pipeline registration**
`InMemoryDB::new()` registers default executors: `BeginExecutor`, `CommitExecutor`, `RollbackExecutor`, `CreateTableExecutor`, `DropTableExecutor`, `InsertExecutor`, `DeleteExecutor`, `UpdateExecutor`, and `QueryExecutor`. The pipeline is extensible via `register`.

---

# `TableBuilder`

Builder helper for programmatic table creation.

* `TableBuilder::new(name)` — create builder.
* `column(name, data_type)` / `column_not_null(name, data_type)` — add column.
* `build()` — returns `Table` ready to be inserted into an execution context.

---

# Client / Connection layer

A `Client` wrapper provides pooled connections and higher-level async API for multi-threaded use. It implements the exported `DatabaseClient` trait for generic use.

`struct Client { pool: ConnectionPool }` (pool implementation lives in `connection::pool`)

Important `Client` methods (facade in `src/lib.rs`):

* `pub async fn connect(username: &str, password: &str) -> Result<Client>` — create a pool using `ConnectionConfig::new(username, password)` and `ConnectionPool::new(config)`.
* `pub async fn connect_with_config(config: ConnectionConfig) -> Result<Client>` — create client with provided config.
* `pub async fn connect_url(url: &str) -> Result<Client>` — parse URL via `ConnectionConfig::from_url` and create pool.
* `pub async fn connect_local(username: &str, password: &str) -> Result<Client>` — create an isolated pool (useful for tests/examples).
* `pub async fn query(&self, sql: &str) -> Result<QueryResult>` — gets a pool connection, calls `conn.execute(sql).await` and returns result. `execute()` is an alias to `query`.
* `pub async fn get_connection(&self) -> Result<PoolGuard>` — acquire a connection guard for transaction usage or multiple statements without returning to pool.
* `pub async fn stats(&self) -> PoolStats` — returns pool statistics.
* `pub fn auth_manager(&self) -> &Arc<AuthManager>` — get reference to the auth manager used by the pool; allows calling user management methods.
* `pub async fn fork(&self) -> Result<Self>` — produce a client with a forked/new pool (pool-level fork).

`Client` implements `interface::DatabaseClient` trait (async) and provides `ping()` convenience that executes `SELECT 1`.

---

### `ConnectionConfig`

Configuration builder for client/pool.

Fields include:

* `host`, `port`, `database`, `username`, `password`
* `connect_timeout: Duration`
* `query_timeout: Option<Duration>`
* `max_connections`, `min_connections`
* `idle_timeout`, `max_lifetime`

Constructors & fluent setters:

* `ConnectionConfig::new(username, password)` — default host `localhost`, default port `5432`, default database name `"rustmemodb"`, sensible timeouts & min/max connections.
* builder style setters: `database()`, `host()`, `port()`, `connect_timeout()`, `query_timeout()`, `max_connections()`, `min_connections()`, `idle_timeout()`, `max_lifetime()`.
* `from_url(url: &str) -> Result<ConnectionConfig, String>` — parse `rustmemodb://` or `postgres://` style url into config (returns parse error string on failure).

---

# Authentication / User management — `AuthManager`

`AuthManager` wraps an in-memory `HashMap<String, User>` protected by `tokio::sync::RwLock` and supports secure password hashing (bcrypt) and permission management. A global singleton `GLOBAL_AUTH_MANAGER` is provided.

`Permission` enum: `Select, Insert, Update, Delete, CreateTable, DropTable, Admin`.

`User` struct:

* `username: String`
* `password_hash: String` (bcrypt)
* `permissions: Vec<Permission>`

`User` helpers:

* `new(username, password_hash, permissions)` — constructor.
* `username(&self) -> &str`, `permissions(&self) -> &[Permission]`.
* `has_permission(permission)` — returns true if user is admin or explicitly has permission.
* `is_admin(&self) -> bool`
* internal mutators: `set_password_hash`, `add_permission`, `remove_permission`.

`AuthManager` methods:

* `AuthManager::new()` / `with_admin(username, password)` — creates manager with an admin user (defaults: admin/adminpass). `global()` returns `&'static Arc<AuthManager>` singleton. Passwords are hashed with bcrypt.
* `authenticate(&self, username, password) -> Result<User>` — reads `users` with RwLock, verifies password using bcrypt, and returns the cloned `User` on success; returns `ExecutionError` otherwise.
* `create_user(&self, username, password, permissions) -> Result<()>` — validates username & password (non empty, length limits), checks uniqueness, inserts new user with hashed password. Errors: `ExecutionError` if user exists or validation fails.
* `delete_user(&self, username) -> Result<()>` — disallow deleting last admin, otherwise remove user.
* `update_password(&self, username, new_password) -> Result<()>` — validate password, update hashed password.
* `grant_permission(&self, username, permission) -> Result<()>` — add perm (no-op if already present).
* `revoke_permission(&self, username, permission) -> Result<()>` — revoke perm; disallow revoking Admin from the last admin user.
* `list_users(&self) -> Result<Vec<String>>` — return sorted usernames.
* `get_user(&self, username) -> Result<User>` — get cloned `User` or `ExecutionError`.
* `user_exists()` and `user_count()` helpers.

Concurrency: `users: RwLock<HashMap<...>>` — reads use `read().await`, writes use `write().await`. Password verification uses bcrypt synchronous APIs (could block; typical to offload if under heavy load).

---

# JSON storage adapter (`json::JsonStorageAdapter`)

A convenience adapter to load JSON documents into the DB and expose create/read/update/delete semantics over JSON collections.

From examples (`examples/json_storage_demo.rs`) we see methods used:

* `JsonStorageAdapter::new(db: Arc<RwLock<InMemoryDB>>) -> JsonStorageAdapter` — wrap a DB.
* `adapter.create(collection_name, json_doc).await?` — create collection and insert JSON docs (adapter uses converter/statement builders to generate CREATE TABLE / INSERT SQL from JSON).
* `adapter.read(collection_name, "SELECT * FROM collection").await?` — run SQL against collection and return `QueryResult`. `read` supports arbitrary SQL but the adapter may validate queries if configured.
* `adapter.update(collection_name, json_doc).await?` — update by id or provided key(s).
* `adapter.delete(collection_name, id).await?` — delete document by id.
* `adapter.drop_collection("grades").await?` and `adapter.list_collections()` — management helpers.

Configuration:
`JsonStorageConfig` exposes `insert_batch_size`, `auto_create_table`, and `validate_queries` with defaults. The adapter includes schema inference and conversion modules (converter, schema_inference, validator, converter helpers).

Notes: Adapter includes validation to prevent SQL injection or invalid collection names (validator includes `validate_collection_name` rules). Examples also show the adapter handles attempted injection strings gracefully in examples (calls result and handles errors).

---

# Storage, persistence & MVCC (high level)

The repository contains a more advanced storage implementation (`InMemoryStorage`) and MVCC features referenced in `src` files. Relevant features:

* MVCC rows and visibility checks (`is_visible`, `is_committed`, `is_version_live`) and vacuum implementation to free obsolete versions. These functions use transaction snapshots and the transaction manager to determine visibility and to implement `vacuum` to free storage from old transactions.
* Indexing: per-table indexes and functions `create_index`, `get_index`, and `update_indexes` to maintain simple value → row id mappings (`OrdMap` used). Unique constraint checks consult indexes and visible version checks before insert (`check unique` logic).
* Persistence manager with durability modes (`DurabilityMode::Sync`, `Async`, `None`) and checkpoint/WAL support. Examples show `enable_persistence(data_dir, DurabilityMode::Async).await` and `checkpoint().await`. Persistence is optional and integrated with storage and the DB object's `persistence` field. See `examples/managed/persistence_demo.rs` for usage patterns.

---

# Examples & typical usage

See the `examples` folder for many ready examples:

* `quickstart.rs` — create tables, insert, select, update and transactions via `Client` and `InMemoryDB`.
* `persistence_demo.rs` — enabling persistence, checkpoints and verifying data restored across runs.
* `json_storage_demo.rs` — using `JsonStorageAdapter` to import JSON arrays and run SQL queries over them.
* `connection_pooling.rs` — demonstrates `ConnectionConfig`, connection pooling, concurrent inserts and pool behavior.
* `user_management.rs` — show user creation, permission operations and authentication flows using the `AuthManager` via a client.

Examples demonstrate common error handling patterns (match on `Result`, assert expectations in tests, print results with `result.print()`).

---

# Error model & returned errors

Functions return `Result<T, DbError>`. `DbError` variants include:

* `ParseError(String)` — SQL parsing or config URL parsing.
* `TableExists(String)` / `TableNotFound(String)` / `ColumnNotFound(String, String)`
* `TypeMismatch(String)` — incompatible types.
* `ConstraintViolation(String)` — constraint/unique/nullable violation.
* `ExecutionError(String)` — general runtime errors (invalid username/password, etc).
* `UnsupportedOperation(String)` — when feature not implemented (multi-table queries, complex projections, unsupported SQL constructs).

---

# Threading & concurrency considerations

* `InMemoryDB` in the full implementation is stored inside `Arc<RwLock<InMemoryDB>>` for sharing (`InMemoryDB::global()`), and `JsonStorageAdapter` holds `Arc<RwLock<InMemoryDB>>`. Many APIs are `async` to play nicely with tokio.
* `AuthManager` uses `tokio::sync::RwLock<HashMap<...>>` for safe concurrent user modifications. Password hashing uses bcrypt (synchronous), which may block; consider offloading hashing/verify to blocking threadpool for heavy loads.
* `Client` and `ConnectionPool` provide multi-connection pooling and `PoolGuard` semantics for borrowing a connection; examples show concurrency patterns and pool cloning for concurrent insert tasks. See `examples/connection_pooling.rs`.

---

# Limitations & unsupported SQL features (as implemented)

* Multi-table queries (joins) are not supported by `QueryPlan::from_select` — the plan errors if `FROM` contains more than one relation. Complex projections (expressions, aggregates, functions) are flagged as `UnsupportedOperation`. Order/limit/grouping are not implemented in the simple pipeline. These are intentional simplifications.
* `INSERT` only supports `VALUES` literal form; no `INSERT ... SELECT`, no parameterized/prepared statements in the core examples.
* `UPDATE` / `DELETE` executors exist in the extended pipeline but their feature set depends on the executor implementation; check `executor/dml` and `executor/update/delete` modules for exact semantics.
* Transaction complexity and MVCC correctness: a full MVCC system is present in the storage code (visibility checks, snapshots, vacuuming) but you should review `transaction` and `storage` modules carefully for edge cases if you plan to use heavy concurrency.

---

# Where to look (source locations)

For direct reading of the implementations mentioned above, consult these source snippets in the uploaded bundle:

* Main `InMemoryDB` and executor pipeline: `src/backup_main.rs` / `src/*` (full implementation excerpts).
* Table/Column/Value/DataType/QueryResult/QueryPlan/ExpressionEvaluator: core types in `src/backup_main.rs` and `src/core` snippets.
* Client and connection-level API: `src/lib.rs` and client implementation.
* Auth manager and users: `src/connection/auth.rs`.
* ConnectionConfig: `src/connection/config.rs`.
* JSON adapter + config & converters: `src/json/adapter.rs` and supporting modules.
* Examples: `examples/*.rs` — quickstart, persistence demo, json storage demo, connection pooling, transactions and user management.

(Each section above includes inline citations pointing to the precise file snippets extracted from your upload. Follow those citations to see the exact code.)

---

# Quick reference: important function signatures (summary)

Here are the most commonly used function signatures and short descriptions:

Core / façade

* `InMemoryDB::new() -> InMemoryDB` — create DB.
* `async fn execute(&mut self, sql: &str) -> Result<QueryResult>` — run SQL.
* `fn table_stats(&self, name: &str) -> Result<TableStats>` — table stats.

Client / connection

* `async fn Client::connect(username: &str, password: &str) -> Result<Client>` — create pooled client.
* `async fn Client::connect_with_config(config: ConnectionConfig) -> Result<Client>` — create with config.
* `async fn Client::query(&self, sql: &str) -> Result<QueryResult>` — run SQL via pool.
* `async fn Client::get_connection(&self) -> Result<PoolGuard>` — get connection for transactions.

Auth manager

* `async fn AuthManager::authenticate(&self, username: &str, password: &str) -> Result<User>` — verify credentials.
* `async fn AuthManager::create_user(&self, username: &str, password: &str, permissions: Vec<Permission>) -> Result<()>` — add user.
* `async fn AuthManager::update_password(&self, username: &str, new_password: &str) -> Result<()>` — change password.
* `async fn AuthManager::grant_permission(&self, username: &str, permission: Permission) -> Result<()>` — grant permission.

JSON adapter

* `JsonStorageAdapter::new(db: Arc<RwLock<InMemoryDB>>) -> JsonStorageAdapter` — wrap db.
* `async fn adapter.create(collection, json_doc) -> JsonResult<()>` — import JSON array.
* `async fn adapter.read(collection, sql) -> JsonResult<QueryResult>` — run SQL.
