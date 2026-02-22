# RFC: Command-First Persist API

Status: `ACCEPTED`  
Date: `2026-02-11`  
Scope: `persist_struct!`, `persist_vec!`, `PersistApp`

## 1. Problem

Current persistence usage still leaks infrastructure concerns into application code:
- manual session wiring,
- manual save/snapshot/recovery flow,
- runtime internals visible in CRUD handlers.

Target: developers should work with domain entities and collections, not persistence plumbing.

## 2. Goals

1. Command-first mutation model for deterministic replay.
2. `PersistApp` as the single lifecycle owner (recovery/snapshot/replication).
3. `persist_vec!` as the main application collection API.
4. Stable, versioned public contracts.
5. Migration path from existing API without sudden breakage.

## 3. Non-goals

1. Replacing all existing low-level APIs immediately.
2. Forcing one persistence mode for every workload.
3. Hard deprecation in the same release as introduction.

## 4. Public API Contract (M0 Freeze)

## 4.1 PersistApp

```rust
pub struct PersistApp;

impl PersistApp {
    pub async fn open(root: impl Into<PathBuf>, policy: PersistAppPolicy) -> Result<Self>;
    pub async fn open_domain<V>(&self, name: impl Into<String>) -> Result<PersistDomainStore<V>>
    where
        V: PersistCollection;
    pub async fn open_autonomous_model<M>(
        &self,
        name: impl Into<String>,
    ) -> Result<PersistAutonomousModelHandle<M>>
    where
        M: PersistAutonomousModel;
    pub async fn serve_json_schema_dir(
        &self,
        schemas_dir: impl AsRef<Path>,
    ) -> Result<axum::Router>;
    pub async fn open_autonomous<V>(&self, name: impl Into<String>) -> Result<PersistAutonomousAggregate<V>>
    where
        V: PersistCollection;
    pub async fn open_aggregate<V>(&self, name: impl Into<String>) -> Result<PersistAggregateStore<V>>
    where
        V: PersistCollection;
    pub async fn open_vec<V>(&self, name: impl Into<String>) -> Result<ManagedPersistVec<V>>
    where
        V: PersistIndexedCollection;
    pub async fn transaction<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(PersistTx) -> Fut,
        Fut: Future<Output = Result<T>>;
}
```

`PersistApp` owns lifecycle responsibilities:
- startup recovery,
- snapshot scheduling/compaction,
- replication shipping,
- operational policy enforcement.

Application code must not use low-level lifecycle calls directly.
For multi-collection orchestration, one shared `PersistTx` context is the canonical path.
`PersistSession` remains available as a low-level advanced API for compatibility and internals.

## 4.2 Collection Contract (`persist_vec!`)

Generated collection API must support:
- `create/get/list/patch/delete`,
- `create_many/apply_many/delete_many`,
- aggregate query helpers for app-facing filtering/pagination (`find_first(...)`, `query_page_filtered_sorted(...)`),
- `atomic_with(...)` for cross-collection atomic orchestration without app-level snapshot/session plumbing,
- `execute_command_if_match(...)` for optimistic command execution without manual version pre-checks,
- `execute_patch_if_match(...)` for optimistic patch execution without manual read/version branching,
- `execute_command_if_match_with_create(...)` for command + related-record append in one atomic flow,
- aggregate auto-audit helpers (`execute_command_if_match_with_audit(...)`, `execute_command_for_many_with_audit(...)`) using built-in `PersistAuditRecord`,
- aggregate intent-based auto-audit helpers (`execute_intent_if_match_auto_audit(...)`, `execute_intent_for_many_auto_audit(...)`) where app code passes business intent + command/audit mapping functions,
- autonomous aggregate facade (`PersistAutonomousAggregate`) with intent-only methods:
  - `apply(...)` (single command + auto audit),
  - `apply_many(...)` (bulk command + auto audit),
  - `patch_if_match(...)` / `delete_if_match(...)`,
  - quick-start convenience methods without explicit version plumbing:
    - `intent(...)`,
    - `intent_many(...)`,
    - `patch(...)`,
    - `remove(...)`,
    - `workflow_with_create(...)`,
  - domain-outcome convenience methods without `Option + DbError` branching:
    - `create_one(...)`,
    - `intent_one(...)`,
    - `patch_one(...)`,
    - `remove_one(...)`,
  - typed closure-mutation convenience methods:
    - `mutate_one_with(...)`,
    - `mutate_many_with(...)`,
  - audit reads via `list_audits_for(...)` when product code needs history projection,
- `execute_workflow_if_match_with_create(...)` for trait-mapped domain workflows without inline closure plumbing,
- `execute_workflow_for_many_with_create_many(...)` for bulk workflow + related-record append without manual tx loops,
- `execute_delete_if_match(...)` for optimistic delete without manual version pre-checks,
- deterministic command application,
- atomic batch semantics (`all-or-nothing` per batch call).

Preferred app-facing access:
- `PersistApp::open_domain(...)` returns high-level `PersistDomainStore<_>` for fast-start app code with minimal persistence ceremony.
- `PersistApp::open_domain_handle(...)` returns `PersistDomainHandle<_>` for shared app state without app-layer `Arc<Mutex<...>>` boilerplate.
- `PersistApp::open_autonomous_model::<Model>(...)` returns `PersistAutonomousModelHandle<Model>` for source-model-first flows generated by `#[derive(Autonomous)]`.
- default mutation vocabulary in app code: `intent(...)`, `intent_many(...)`, `patch(...)`, `remove(...)`, `workflow_with_create(...)`.
- default app-facing error model for domain store is `PersistDomainError` (instead of direct `DbError` pattern matching).
- `PersistApp::open_autonomous(...)` returns `PersistAutonomousAggregate<_>` as the default zero-thinking command/audit surface.
- `PersistApp::open_aggregate(...)` returns `PersistAggregateStore<_>` as the default product/lesson surface.
- `PersistApp::open_vec(...)` remains as compatibility/advanced layer.
- direct `execute_*_if_match*` usage is compatibility/advanced and should not be the first-choice shape in Part B lesson/product app modules.
- `persist::web` provides framework-agnostic handler primitives for DX-first HTTP layers:
  - `parse_if_match_header(...)`
  - `normalize_idempotency_key(...)`
  - `normalize_request_id(...)`
  - `map_conflict_problem(...)`
  - `PersistServiceError` as shared app/service error model
  - `PersistServiceError::from_domain_for(...)` for contextual not-found mapping
  - `PersistServiceError::from_mutation_for(...)` for domain + user-mutation mapping
- high-level single-id APIs (`PersistDomainHandle`, `PersistAutonomousModelHandle`) accept `impl AsRef<str>` to remove app-layer id adapter noise.
- internal maintainability contract: keep `app.rs` as public contracts/types and place behavior impls in `src/persist/app/*` modules to prevent monolithic files.
- internal maintainability contract: keep `app.rs` as entrypoint and split app contracts/policy/open/store types in `src/persist/app/{collection_contracts,policies_and_conflicts,app_open,store_types}.rs`.
- internal maintainability contract: keep `aggregate_store.rs` as app-aggregate entrypoint and split detailed behavior in `src/persist/app/aggregate_store/*`.
- internal maintainability contract: keep `command_audit_workflow.rs` as app-aggregate command entrypoint and split intent/audit, command/delete, and workflow concerns in `src/persist/app/aggregate_store/command_audit_workflow/*`.
- internal maintainability contract: keep `autonomous.rs` as autonomous entrypoint and split intent/conflict/high-level/workflow concerns in `src/persist/app/autonomous/*`.
- internal maintainability contract: keep `managed_vec.rs` as managed-collection entrypoint and split detailed behavior in `src/persist/app/managed_vec/*`.
- internal maintainability contract: keep `indexed_crud.rs` as managed indexed-CRUD entrypoint and split read/create/update/delete concerns in `src/persist/app/managed_vec/indexed_crud/*`.
- internal maintainability contract: keep `app_open.rs` as `PersistApp` lifecycle/opening entrypoint and split constructors/retry/open-collection/transaction concerns in `src/persist/app/app_open/*`.
- internal maintainability contract: keep `mod.rs` as public contracts/types and place core behavior impls in `src/persist/core/*` modules to prevent monolithic files.
- internal maintainability contract: keep `mod.rs` as entrypoint and split contracts/types by domain in `src/persist/core/{api_version,session_and_metadata,descriptors_and_state,dynamic_schema_contracts,snapshots_and_migrations,entity_contracts,containers_and_values}.rs`.
- internal maintainability contract: keep `migration_impl.rs` as migration entrypoint and split step-builder/debug, plan validation, and plan execution concerns in `src/persist/core/migration_impl/*`.
- internal maintainability contract: keep `persist_vec_impl.rs` as typed-collection entrypoint and split basics/I-O, invoke-prune, and snapshot-restore concerns in `src/persist/core/persist_vec_impl/*`.
- internal maintainability contract: keep `hetero_vec_impl.rs` as core heterogeneous-collection entrypoint and split registration/mutation/runtime/snapshot concerns in `src/persist/core/hetero_vec_impl/*`.
- internal maintainability contract: keep `schema_utils.rs` as core schema util entrypoint and split naming/sql, DDL parsing, and JSON-schema parsing concerns in `src/persist/core/schema_utils/*`.
- internal maintainability contract: keep `runtime.rs` as public runtime contracts/types and place helper/worker/compat + main runtime behavior impl in `src/persist/runtime/*` modules to prevent monolithic files.
- internal maintainability contract: keep `macros.rs` as stable entrypoint and place macro bodies in `src/persist/macros/*` modules to prevent monolithic files.
- documentation contract: generated public APIs from `persist_vec!` and `persist_struct!` must carry rustdoc comments so users can adopt API from docs without reading macro internals.
- internal maintainability contract: keep `runtime_impl.rs` as runtime impl entrypoint and split implementation domains in `src/persist/runtime/runtime_impl/*`.
- internal maintainability contract: keep runtime contract/types grouped by domain in `src/persist/runtime/types/*` and included by `runtime.rs`.
- internal maintainability contract: keep `projection.rs` as runtime projection-type entrypoint and split contract/row, table+undo, and mailbox concerns in `src/persist/runtime/types/projection/*`.
- internal maintainability contract: keep `handlers_and_envelope.rs` as runtime type entrypoint and split handler aliases, envelope/outbox types, and payload schema concerns in `src/persist/runtime/types/handlers_and_envelope/*`.
- internal maintainability contract: keep `runtime_support.rs` as support entrypoint and split helper/worker/compat logic in `src/persist/runtime/support/*`.
- internal maintainability contract: keep `api_registry_and_crud.rs` as runtime API entrypoint and split its concerns in `src/persist/runtime/runtime_impl/api_registry_and_crud/*`.
- internal maintainability contract: keep `registry_and_projection.rs` as runtime registry/projection entrypoint and split deterministic registry, migration registry, and projection lookup concerns in `src/persist/runtime/runtime_impl/api_registry_and_crud/registry_and_projection/*`.
- internal maintainability contract: keep `command_and_lifecycle.rs` as runtime command/lifecycle entrypoint and split its concerns in `src/persist/runtime/runtime_impl/command_and_lifecycle/*`.
- internal maintainability contract: keep `storage_and_projection.rs` as runtime storage/projection entrypoint and split disk/journal, projection, and mailbox concerns in `src/persist/runtime/runtime_impl/storage_and_projection/*`.
- internal maintainability contract: keep `internals.rs` as runtime infra entrypoint and split its concerns in `src/persist/runtime/runtime_impl/internals/*`.
- internal maintainability contract: keep `cluster.rs` as runtime-cluster entrypoint and split its concerns in `src/persist/cluster/*`.
- internal maintainability contract: keep `routing.rs` as routing entrypoint and split routing types/membership/table/hash concerns in `src/persist/cluster/routing/*`.
- internal maintainability contract: keep `routing_table.rs` as routing-table entrypoint and split constructor/validation, mutation, and lookup concerns in `src/persist/cluster/routing/routing_table/*`.
- internal maintainability contract: keep `macros.rs` as a stable macro entrypoint and keep macro implementations isolated in `src/persist/macros/{persist_struct.rs,persist_vec.rs}`; do not require crate-level lint allowances for public macro dispatch.

Product-application layering target:
- app facade modules (`workspace/service`) should expose business intent methods only,
- app code should call managed helper APIs directly instead of adding extra repository/store layers that re-introduce persistence plumbing.
- conflict retry behavior must be configured once in `PersistAppPolicy::conflict_retry` (infrastructure concern), not implemented with ad-hoc retry loops in handlers/services.
- default retry scope is transient `write_write` conflicts; business `if-match` conflicts (`optimistic_lock`) are returned immediately.

## 4.3 Entity Contract (`persist_struct!`)

Generated entity API must expose:
- typed state (`from_struct`),
- typed `Draft`,
- typed `Patch`,
- typed `Command` enum for deterministic mutations,
- optional sugar methods mapped to commands (non-primary path).

Intent-command API must expose:
- derive-based bridge to autonomous command flow without manual trait impls:
  - `#[derive(PersistAutonomousIntent)]`
  - `#[persist_intent(model = <Entity>, to_command = <method>)]` (method-based mapping)
  - `#[persist_intent(model = <Entity>)]` + variant-level `#[persist_case(...)]` (no helper methods / no impl block)
- derive-based source-model bridge to autonomous persistence without manual collection wiring:
  - `#[derive(Autonomous)]` + `#[persist_model(table = \"...\", schema_version = N)]`
  - generated `<Model>Persisted` + `<Model>AutonomousVec`
  - generated contracts implementing `PersistBackedModel<Model>` and `PersistAutonomousModel`
  - stable high-level import path: `use rustmemodb::prelude::dx::*;`
  - low-level escape hatch path is explicit: `rustmemodb::prelude::advanced::*` / `rustmemodb::persist::*`
  - `#[autonomous_impl]` + `#[rustmemodb::command]` on model impl generates `<Model>AutonomousOps` trait for high-level domain methods on `PersistAutonomousModelHandle<Model>`
  - generated `<Model>AutonomousOps` methods append system audit events by default (no manual app-layer audit wiring)
  - `#[expose_rest]` on model impl generates axum router + command DTOs + handlers from `#[rustmemodb::command]`/`#[rustmemodb::query]`/`#[rustmemodb::view]`
  - `PersistApp::serve_autonomous_model::<Model>(...)` mounts generated REST router directly
- derive-based API error mapping for domain errors:
  - `#[derive(ApiError)]`
  - variant-level `#[api_error(status = <u16>, code = "...")]`
  - generated `From<DomainError> for PersistServiceError` (no manual app-layer error mapping impl)
  - generated mapping preserves explicit status+code in `PersistServiceError::Custom`
  - `Magic REST` MVP constraints:
  - `#[rustmemodb::query]` supports typed query arguments (`GET /:id/<query>?...`),
  - when command/query/view method has exactly one DTO-like argument, generated handlers infer direct payload binding automatically (no required `input = <Type>` override),
  - `#[rustmemodb::query(input = <Type>, output = <Type>)]` binds typed request/response contracts,
  - `#[rustmemodb::command(input = <Type>, output = <Type>)]` binds typed command payload/response contracts,
  - `#[rustmemodb::view]` remains available for explicit view routes,
  - `#[rustmemodb::view(input = "body")]` supports typed body DTOs (`POST /:id/<view>`),
  - `#[rustmemodb::view(input_type = <Type>, output = <Type>)]` binds explicit view contracts,
  - generated command/query/view handlers return `204 No Content` when method success type is `()` / `Result<(), E>`,
  - generated create endpoint (`POST /`) uses constructor-derived DTO when `new(...) -> Self` is detected,
  - fallback create behavior accepts full source-model payload when constructor is not detected,
  - generated router includes built-in aggregate audit projection endpoint (`GET /:id/_audits`),
  - generated command endpoints enable `Idempotency-Key` replay by default (duplicate key returns same status/body and does not re-apply mutation),
  - `#[command(idempotent = false)]` is available as an explicit opt-out,
  - generated autonomous router exposes `GET /_openapi.json` built from command/query/view registry,
  - schema-first runtime path `PersistApp::serve_json_schema_dir(...)` builds generic CRUD router directly from `schemas/*.json` without app-layer handler code.
  - schema-first router exposes `GET /_openapi.json` (mounted path aware, e.g. `/api/_openapi.json`).
  - schema-first router hot-reloads schema files and reconciles new fields to storage automatically.
- derive-based JSON field persistence for domain value types without wrapper boilerplate:
  - `#[derive(PersistJsonValue)]` on local struct/enum types used in `persist_struct!` fields
- generic JSON wrapper for nested data:
  - `PersistJson<T>` built into core (no local wrapper struct + no manual `PersistValue` impl),
  - intended for fields like `PersistJson<Vec<Child>>` in autonomous/domain models.
- optional method mapping overrides in `#[persist_intent(...)]`:
  - `event_type = <method>`
  - `event_message = <method>`
  - `bulk_event_type = <method>`
  - `bulk_event_message = <method>`
- variant-level mapping contract:
  - required: `command = <expr>`
  - optional overrides: `event_type = \"...\"`, `event_message = \"...\"`, `bulk_event_type = \"...\"`, `bulk_event_message = \"...\"`
  - when overrides are omitted, persist generates system defaults from command name (`set_active`, `bulk_set_active`, etc.)
- `#[api_service]` ergonomics contract:
  - supports service-return aliases with names ending in `Result` (example: `ApiResult<T>`) as `Result<T, E>` in generated handlers

Field-level persistence annotations in typed mode:
- `#[persist(unique)]` for declarative uniqueness.
- `#[persist(index)]` for declarative indexing.

Runtime expectations for these annotations:
1. `#[persist(unique)]` is enforced in managed write flow and mapped to conflict-class errors.
2. `#[persist(unique)]` emits bootstrap `UNIQUE INDEX` DDL automatically.
3. `#[persist(index)]` emits bootstrap `INDEX` DDL automatically.

Primary mutation API:

```rust
todo.apply(TodoCommand::Complete).await?;
todos.patch(id, TodoPatch { priority: Some(3), ..Default::default() }).await?;
```

## 5. Deterministic Command Envelope

Persisted mutation record format (versioned envelope):

```json
{
  "format_version": 1,
  "seq": 123,
  "ts_unix_ms": 1738800000000,
  "entity_type": "Todo",
  "entity_id": "todo-42",
  "schema_version": 2,
  "command_type": "Complete",
  "payload": {},
  "expected_version": 4,
  "new_version": 5,
  "idempotency_key": "..."
}
```

Rules:
1. Reducers are deterministic.
2. Envelope is append-only.
3. Replay order is strictly `seq`.
4. Unknown `format_version` is a hard compatibility error.

## 6. Error Model

Stable error classes:
- `NotFound`
- `Conflict` (version mismatch / optimistic lock)
- `Validation`
- `Timeout`
- `Backpressure`
- `Compatibility`
- `Internal`

All public APIs map internal errors into these classes.

## 7. Compatibility Matrix

| Runtime | Snapshot/Journal | Support |
|---|---|---|
| N | N | FULL |
| N | N-1 | FULL |
| N | N-2 | BEST-EFFORT (explicitly documented per release) |
| N-1 | N | READ only if forward-compatible envelope/schema rules allow |
| N-2 | N | NOT GUARANTEED |

Compatibility rules:
1. Schema migrations are monotonic by version.
2. Command migrations are explicit and tested.
3. No silent downgrade of unknown fields/commands.

## 8. Anti-Leak DX Policy

New examples and guides MUST NOT:
- create or pass `PersistSession` directly in app handlers,
- prefer `PersistTx` + `*_with_tx(...)` over `*_with_session(...)` in application code,
- duplicate `If-Match`/idempotency parsing logic in handlers when `persist::web` primitives cover the use case,
- use `execute_intent_if_match_auto_audit(...)` directly from Part B application modules when `PersistDomainStore::intent(...)` path is available,
- classify low-level conflicts in app code via `classify_managed_conflict(...)` when `PersistDomainStore::*_one(...)` APIs are sufficient,
- call `snapshot_for_external_transaction`, `restore_snapshot_for_external_transaction`, `shared_session`, or `on_external_mutation_committed` from application-layer lesson/product code,
- call manual lifecycle methods (`restore`, `force_snapshot`, replication internals),
- manually orchestrate recovery in user code,
- implement manual technical claim-table patterns for uniqueness when `#[persist(unique)]` fits,
- add custom repository/store adapter layers whose main job is to proxy managed `persist` calls.
- for showcase/autonomous examples, keep runtime path to `model.rs` + `main.rs` and mount generated router via `PersistApp::serve_autonomous_model::<Model>(...)` without manual `api.rs`/`store.rs` layers.

Enforcement:
1. DX integration tests.
2. Example lint checklist.
3. Release checklist gate.

## 9. Migration Path

1. Introduce `PersistApp` + new generated APIs.
2. Keep legacy API with adapter layer for transition window.
3. Mark legacy low-level flow as deprecated in docs.
4. Remove legacy path only after compatibility window and migration cookbook.

## 10. Acceptance Criteria for M0

- [x] Public API signatures frozen in RFC.
- [x] Command envelope format frozen in RFC.
- [x] Stable error model frozen in RFC.
- [x] Compatibility matrix frozen in RFC.
- [x] Anti-leak policy defined.

## 11. Reference DX Examples

The following examples are maintained as living DX references for this RFC:

- `examples/agile_board`:
  - source-model autonomous derive + generated REST (`serve_autonomous_model`).
  - focus: nested domain mutations with zero manual API/store adapter layers in runtime path.
- `examples/ledger_core`:
  - personal finance ledger (double-entry, multi-currency transfer, balance reports).
  - focus: atomic business mutations and in-memory reporting without SQL orchestration in handlers/services.
