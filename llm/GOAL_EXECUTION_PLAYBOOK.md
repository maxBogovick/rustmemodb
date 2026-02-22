# Goal Execution Playbook: Path to Autonomous Structures

Status: ACTIVE
Last Updated: 2026-02-21
Depends On: `/Users/maxim/RustroverProjects/rustmemodb/llm/GOAL_ALIGNMENT_CHARTER.md`
Guardrails: `/Users/maxim/RustroverProjects/rustmemodb/llm/AUTONOMY_DX_CONTRACT.md`
Active DX Simplification Plan: `/Users/maxim/RustroverProjects/rustmemodb/llm/PERSIST_DX_SIMPLIFICATION_EXECUTION.md`

## 1. Purpose

This playbook translates the charter into executable work.
It defines phases, concrete deliverables, acceptance gates, and risk controls.

Primary objective:

Move from "managed persistence API" to "application-level autonomous persistence platform"
without breaking existing users.

## 2. Current Gap Summary (Starting Point)

Current strengths already available:

- `PersistApp` lifecycle abstraction.
- Managed collection APIs (`create`, `update`, `patch`, `apply_command`, `apply_many`).
- `PersistAggregateStore` app-facing wrapper via `PersistApp::open_aggregate(...)`.
- Runtime envelope path with idempotency and outbox support.
- Runtime projection contracts and indexed lookup.

Current gaps versus charter target:

- Legacy app/example code still orchestrates persistence operations manually.
- Multi-collection atomic domain operations are available at managed layer, but aggregate-level abstractions are still incomplete.
- Phase 1 constraints/indexing are now available for typed `persist_struct!`, but broader migration is still in progress.
- Audit and outbox behavior are not declarative defaults at app level.
- Web handlers still parse and map storage-related semantics manually.

## 2.1 Execution Delta (2026-02-21)

Closed in this execution wave:

1. Autonomous router generation now includes full command/query/view route surface plus `GET /_openapi.json`.
2. `ApiError` derive now supports explicit status+code mapping and removes manual HTTP-code wiring in domain examples.
3. Generated command handlers include idempotency replay and advertise it in OpenAPI.
4. Typed request/response binding added to macro surface:
   - `#[command(input = <Type>, output = <Type>)]`
   - `#[query(input = <Type>, output = <Type>)]`
   - `#[view(input_type = <Type>, output = <Type>)]`
5. `examples/ledger_core` migrated to typed autonomous flow using `unistructgen`-generated DTOs for command/query payload contracts.
6. `examples/agile_board` migrated to generated autonomous REST runtime path:
   - removed manual runtime API/store adapter layers from active flow;
   - mounted only `PersistApp::serve_autonomous_model::<Board>(...)`;
   - domain errors mapped automatically via `#[derive(ApiError)]`.
7. `#[expose_rest]` semantics aligned for no-content operations:
   - command/query/view methods with `()` / `Result<(), E>` now return `204 No Content`;
   - generated OpenAPI success status now matches runtime behavior.
8. Core `PersistJson<T>` shipped and adopted in showcase examples:
   - nested vectors/maps can be persisted without local wrapper boilerplate.
9. Added executable DX contract test:
   - `tests/dx_contract_examples_tests.rs` verifies showcase examples keep generated-router runtime path and avoid manual `api/store` layers.
10. Stable DX prelude path added:
    - `rustmemodb::prelude::dx::*` is now the canonical high-level import surface for showcase/product code.
11. Explicit advanced escape hatch path added:
    - `rustmemodb::prelude::advanced::*` centralizes low-level persistence internals to avoid accidental leakage into default app code.
12. Showcase examples aligned to prelude route:
    - `examples/agile_board` and `examples/ledger_core` now import runtime bootstrap via `prelude::dx::PersistApp`,
    - model methods use concise `#[command]` / `#[query]` / `#[view]` markers.
13. Idempotency-by-default proved in generated REST:
    - `agile_board` integration test verifies replay behavior (same `Idempotency-Key` => same response, no duplicate mutation),
    - OpenAPI assertion verifies `Idempotency-Key` header is documented on generated command route.
14. Smart DTO inference for generated REST handlers:
    - for single DTO-like method arguments in `#[command]`/`#[query]`/`#[view]`, generated handlers infer direct payload binding automatically,
    - showcase `ledger_core` now works without explicit `input = <Type>` overrides for those methods.

## 3. Delivery Strategy

Delivery model:

- Preserve backward compatibility by adding new first-class APIs.
- Migrate examples and lessons to prove value before hard deprecation.
- Keep low-level escape hatches for advanced users.
- Treat education artifacts as product acceptance tests for DX.

Hard gate:

- For Part B apps, any app-layer manual persistence orchestration is a release blocker.
- Green tests do not override this blocker.

## 3.1 Active Execution Lock (2026-02-18)

Until Stage 1 is accepted by the user, implementation scope is locked to DX-first items only.

Allowed work:

- High-level domain API path (`open_domain` + `intent/intent_many/patch/remove/workflow_with_create`).
- Zero-thinking defaults (no mandatory manual audit/event wiring).
- `persist_web` adapter primitives for handler-level parsing/mapping deduplication.
- DX guardrails and contract tests (`AUTONOMY_DX_CONTRACT`) for product/lesson app layers.
- Documentation and lesson updates that reinforce this path.

Disallowed work (unless directly required to unblock allowed scope):

- New low-level runtime/cluster/storage feature expansion.
- API shape changes that push app code back to `PersistSession`/`PersistTx`/manual expected-version wiring.
- Refactors not tied to DX-first acceptance criteria.

## 3.2 Canonical DX Route (Locked)

Part B app code should follow only this path:

1. Bootstrap: `PersistApp::open_auto(...)` + `app.open_domain::<...>(...)`.
2. Business mutations: `intent(...)`, `intent_many(...)`, `patch(...)`, `remove(...)`, `workflow_with_create(...)`.
3. HTTP adapter plumbing: `parse_if_match_header(...)`, `normalize_idempotency_key(...)`, `normalize_request_id(...)`, `map_conflict_problem(...)`.
4. Showcase runtime contract: keep runtime path to `model.rs` + `main.rs`; mount only generated router (`serve_autonomous_model`) and do not add manual `api/store` adapters.

Allowed only as advanced/internal escape hatch (not default lesson/product app code):

1. `open_autonomous(...)`, `open_aggregate(...)`, `open_vec(...)`.
2. `execute_*_if_match*` helpers.
3. `PersistTx` and `*_with_tx(...)` APIs.

Forbidden in Part B app layer while Stage 1 is active:

1. `execute_intent_if_match_auto_audit(...)` usage directly from application modules.
2. Manual audit label/message plumbing for common intent flows.
3. Any explicit session/snapshot/retry choreography in handlers/services/workspaces.

## 4. Phased Roadmap

## Phase 0: Baseline and Instrumentation

Objective:

Define measurable baseline for persistence leakage and boilerplate.

Deliverables:

1. Baseline report for current examples (LOC and leakage metrics).
2. Static checklist identifying persistence primitives in app code.
3. Initial dashboard section in docs for charter metrics.

Acceptance criteria:

- Baseline captured for `education/habit-hero-ab/lesson4`.
- Metrics can be re-run automatically.

Exit artifact:

- `llm/BASELINE_AUTONOMY_METRICS.md`

## Phase 1: Declarative Constraints and Indices

Objective:

Eliminate app-level manual uniqueness and index structures.

Deliverables:

1. Field-level attributes in macro layer:
- `#[persist(unique)]`
- `#[persist(index)]`
2. Runtime enforcement integrated into managed write path.
3. Stable error classification for constraint violations.

Acceptance criteria:

- No claim-table workaround required for unique email pattern.
- Constraint violations surfaced as deterministic conflict class.
- Tests cover restart and multi-instance consistency behavior.

Key tests:

- unique insert conflict under concurrent create
- unique behavior across reopen and replication mode variations

Status update (2026-02-16):

- Typed macro support shipped for `#[persist(unique)]` and `#[persist(index)]`.
- Managed write path now enforces declarative uniqueness and reports conflict class for unique violations.
- Lesson4 product flow removed manual email claim-table workaround and uses `#[persist(unique)]`.
- Phase 2 foundation shipped:
  - `PersistApp::transaction(...)` entrypoint,
  - explicit `PersistTx` transaction context type,
  - transaction-scoped `ManagedPersistVec` mutation helpers (`*_with_tx(...)` primary, `*_with_session(...)` compatibility),
  - `ManagedPersistVec::atomic_with(...)` helper for cross-collection atomic flows without app-level snapshot choreography,
  - `ManagedPersistVec::execute_command_if_match(...)` for optimistic command execution without manual pre-check plumbing,
  - `ManagedPersistVec::execute_patch_if_match(...)` for optimistic patch execution without manual read/version branching,
  - `ManagedPersistVec::execute_command_if_match_with_create(...)` for command + related-record append in one atomic call,
  - `ManagedPersistVec::execute_workflow_if_match_with_create(...)` for trait-mapped domain workflow execution without inline closure plumbing,
  - `ManagedPersistVec::execute_workflow_for_many_with_create_many(...)` for bulk workflow + related-record append without manual tx loops,
  - `ManagedPersistVec::execute_delete_if_match(...)` for optimistic delete without manual pre-check plumbing,
  - lesson4 lifecycle + audit flow moved into managed workflow helpers with one atomic commit path,
  - failure-injection rollback test proving no partial user/event writes.
- Lesson4 Part B hardening shipped:
  - public composition no longer exposes `PersistUserStore`/`UserRepository`,
  - lifecycle flow no longer uses `shared_session`/snapshot restore primitives in app code.
  - `application/user_workspace.rs` now follows canonical thin-facade syntax in a single module file (no extra repository/store adapter file).
  - explicit `expected_version` plumbing removed from product app layer (`users_handler` -> `user_service` -> `user_workspace`) for update/delete/lifecycle paths.
  - lesson4 product mutation paths now use high-level domain APIs directly: `patch(...)`, `remove(...)`, `intent(...)`, `intent_many(...)`.
  - lesson4 product contract suite moved to DX-specific shared tests (`run_write_users_contract_dx`, `run_commands_contract_dx`) while keeping strict `If-Match` contracts for classic API.
  - domain outcome API shipped for app-facing error semantics:
    - `PersistDomainError` introduced as high-level persistence outcome type,
    - `PersistDomainStore::{create_one,intent_one,patch_one,remove_one}` added to avoid app-layer `Option + DbError` branching,
    - lesson4 product workspace now maps `PersistDomainError` -> `DomainError` without `classify_managed_conflict` plumbing.
- Phase 3 foundation shipped:
  - `PersistApp::open_aggregate(...)` entrypoint,
  - `PersistAggregateStore` app-facing API over managed runtime helpers,
  - aggregate query helpers (`find_first`, `query_page_filtered_sorted`) for app-facing filter/sort/page without hand-written pagination loops,
  - lesson4 product workspace migrated from `ManagedPersistVec` state fields to `PersistAggregateStore`.
- Phase 4 foundation shipped:
  - built-in `PersistAuditRecord` / `PersistAuditRecordVec`,
  - aggregate auto-audit helpers (`execute_command_if_match_with_audit`, `execute_command_for_many_with_audit`),
  - intent-based aggregate helpers (`execute_intent_if_match_auto_audit`, `execute_intent_for_many_auto_audit`),
  - autonomous aggregate facade (`PersistApp::open_autonomous(...)`, `PersistAutonomousAggregate`) with intent-only methods (`apply`, `apply_many`) and hidden audit store wiring,
  - quick-start high-level entrypoint and methods (`PersistApp::open_domain(...)`, `PersistDomainStore::intent/intent_many/patch/remove/workflow_with_create`) to hide explicit optimistic version plumbing in app code while preserving low-level `*_if_match` APIs,
  - domain-handle DX layer shipped:
    - `PersistApp::open_domain_handle(...)`,
    - `PersistDomainHandle` (`&self` API, internal lock management),
    - `PersistAutonomousAggregate::{mutate_one,mutate_many}` for closure-based business updates without patch wiring,
    - typed closure mutation path `PersistAutonomousAggregate::{mutate_one_with,mutate_many_with}` and `PersistDomainHandle::{mutate_one_with,mutate_many_with}` to keep business errors typed (no `DbError` string bridge),
  - derive-based intent bridge (`#[derive(PersistAutonomousIntent)]`, `#[persist_intent(...)]`, `#[persist_case(...)]`) to remove manual `impl PersistAutonomousCommand<...>` boilerplate and helper mapping methods,
  - source-model autonomous derive bridge shipped:
    - `#[derive(Autonomous)]` + `#[persist_model(...)]`,
    - generated `<Model>Persisted` + `<Model>AutonomousVec`,
    - generated `PersistBackedModel` / `PersistAutonomousModel` contracts,
    - `PersistApp::open_autonomous_model::<Model>(...)` + `PersistAutonomousModelHandle` (`create_one/get_one/list/mutate_one_with/mutate_one_with_result/remove_one`),
    - `#[autonomous_impl]` + `#[rustmemodb::command]` generates `<Model>AutonomousOps` (domain methods on handle, no handwritten store-side mutate wrappers),
    - integration coverage in `tests/persist_autonomous_derive_tests.rs`,
  - adoption proofs moved to product/examples:
    - `education/habit-hero-ab/lesson4/product-api` now opens user store via `open_domain(...)`,
    - `examples/gamemaster` now opens player store via `open_domain(...)`,
    - `examples/agile_board` now runs through generated autonomous REST (`serve_autonomous_model`) with no manual runtime `api/store` adapter layer.
  - lesson4 lifecycle audit flow migrated to autonomous apply paths (no custom audit record type / workflow trait in app layer, no entity-level audit traits, no explicit mapper wiring in `user_workspace`).
  - maintainability refactor: `src/persist/app.rs` split into `src/persist/app/{aggregate_store,autonomous,managed_vec,legacy_adapter}.rs` so future DX work lands in focused modules instead of one monolith.
  - maintainability refactor: `src/persist/app.rs` converted into entrypoint with focused files in `src/persist/app/{collection_contracts,policies_and_conflicts,app_open,store_types}.rs`.
  - maintainability refactor: `src/persist/app/app_open.rs` converted into entrypoint with focused files in `src/persist/app/app_open/{types_and_tx,constructors_and_retry,open_collections,transactions}.rs`.
  - maintainability refactor: `src/persist/app/aggregate_store.rs` converted into entrypoint with focused files in `src/persist/app/aggregate_store/{core,indexed_crud_query,command_audit_workflow}.rs`.
  - maintainability refactor: `src/persist/app/aggregate_store/command_audit_workflow.rs` converted into entrypoint with focused files in `src/persist/app/aggregate_store/command_audit_workflow/{intent_and_audit,command_and_delete,workflow_ops}.rs`.
  - maintainability refactor: `src/persist/app/autonomous.rs` converted into entrypoint with focused files in `src/persist/app/autonomous/{core_read,conflict_and_apply,high_level_convenience,workflow_and_compat}.rs`.
  - maintainability refactor: `src/persist/app/managed_vec.rs` converted into entrypoint with focused files in `src/persist/app/managed_vec/{base_collection,indexed_crud,command_model,optimistic_workflows,io_utils}.rs`.
  - maintainability refactor: `src/persist/app/managed_vec/indexed_crud.rs` converted into entrypoint with focused files in `src/persist/app/managed_vec/indexed_crud/{validation_and_reads,create_paths,update_paths,delete_paths}.rs`.
  - maintainability refactor: `src/persist/mod.rs` split into `src/persist/core/{session_impl,migration_impl,persist_vec_impl,hetero_vec_impl,persist_value_impls,schema_utils}.rs` while keeping public contracts in `mod.rs`.
  - maintainability refactor: `src/persist/mod.rs` converted into entrypoint with focused contract/type files in `src/persist/core/{api_version,session_and_metadata,descriptors_and_state,dynamic_schema_contracts,snapshots_and_migrations,entity_contracts,containers_and_values}.rs`.
  - maintainability refactor: `src/persist/core/migration_impl.rs` converted into entrypoint with focused files in `src/persist/core/migration_impl/{step_builder_and_debug,plan_basics_and_validation,plan_execution}.rs`.
  - maintainability refactor: `src/persist/core/persist_vec_impl.rs` converted into entrypoint with focused files in `src/persist/core/persist_vec_impl/{basics_and_io,invoke_and_prune,snapshot_and_restore}.rs`.
  - maintainability refactor: `src/persist/core/hetero_vec_impl.rs` converted into entrypoint with focused files in `src/persist/core/hetero_vec_impl/{basics_and_registration,collection_mutations,runtime_ops,snapshot_restore}.rs`.
  - maintainability refactor: `src/persist/core/schema_utils.rs` converted into entrypoint with focused files in `src/persist/core/schema_utils/{naming_and_sql,ddl_schema,json_schema}.rs`.
  - maintainability refactor: `src/persist/runtime.rs` split into runtime contracts/types, `runtime_support.rs` for helper/worker/compat logic, and `runtime_impl.rs` for main runtime behavior implementation.
  - maintainability refactor: runtime contracts/types split by domain into `src/persist/runtime/types/{handlers_and_envelope,policy,entity_and_journal,projection,stats_and_registry}.rs`.
  - maintainability refactor: `src/persist/runtime/types/projection.rs` converted into entrypoint with focused files in `src/persist/runtime/types/projection/{contracts,table_and_undo,mailbox}.rs`.
  - maintainability refactor: `src/persist/runtime/types/handlers_and_envelope.rs` converted into entrypoint with focused files in `src/persist/runtime/types/handlers_and_envelope/{handler_types,envelope_and_side_effects,payload_schema}.rs`.
  - maintainability refactor: `src/persist/runtime/runtime_support.rs` converted into entrypoint with focused support files in `src/persist/runtime/support/{helpers,worker,compat}.rs`.
  - maintainability refactor: `src/persist/runtime/runtime_impl.rs` split into focused runtime domains (`api_registry_and_crud`, `command_and_lifecycle`, `storage_and_projection`, `internals`).
  - maintainability refactor: `src/persist/runtime/runtime_impl/api_registry_and_crud.rs` converted into entrypoint with focused files in `src/persist/runtime/runtime_impl/api_registry_and_crud/{open_and_stats,registry_and_projection,entity_crud_and_outbox}.rs`.
  - maintainability refactor: `src/persist/runtime/runtime_impl/api_registry_and_crud/registry_and_projection.rs` converted into entrypoint with focused files in `src/persist/runtime/runtime_impl/api_registry_and_crud/registry_and_projection/{deterministic_registry,migration_registry,runtime_closure_and_projection}.rs`.
  - maintainability refactor: `src/persist/runtime/runtime_impl/command_and_lifecycle.rs` converted into entrypoint with focused files in `src/persist/runtime/runtime_impl/command_and_lifecycle/{deterministic_command,runtime_closure,lifecycle_snapshot}.rs`.
  - maintainability refactor: `src/persist/runtime/runtime_impl/storage_and_projection.rs` converted into entrypoint with focused files in `src/persist/runtime/runtime_impl/storage_and_projection/{disk_and_journal,projections,mailboxes}.rs`.
  - maintainability refactor: `src/persist/runtime/runtime_impl/internals.rs` converted into entrypoint with focused files in `src/persist/runtime/runtime_impl/internals/{entity_and_tombstones,journal_and_snapshot,replication_and_io,recovery_and_backpressure}.rs`.
  - maintainability refactor: `src/persist/cluster.rs` converted into entrypoint with focused files in `src/persist/cluster/{routing,policy_and_trait,node,in_memory_forwarder}.rs`.
  - maintainability refactor: `src/persist/cluster/routing.rs` converted into entrypoint with focused files in `src/persist/cluster/routing/{types,membership,routing_table,shard_hash}.rs`.
  - maintainability refactor: `src/persist/cluster/routing/routing_table.rs` converted into entrypoint with focused files in `src/persist/cluster/routing/routing_table/{construct_and_validate,mutations,lookups}.rs`.
  - maintainability refactor: `src/persist/macros.rs` kept as entrypoint and split into `src/persist/macros/{attr_helpers,persist_struct,persist_vec}.rs` for focused macro evolution.
  - maintainability refactor: macro entrypoint remains `src/persist/macros.rs`, and implementations are isolated in `src/persist/macros/{persist_struct,persist_vec}.rs` without crate-level lint allowances for dispatcher forwarding.
  - documentation refactor: generated public methods in `persist_vec!` and `persist_struct!` now include rustdoc comments so API discovery does not require reading macro expansion internals.
- retry hardening update:
  - conflict retries are policy-driven (`PersistAppPolicy::conflict_retry`) and stay below app service/handler layer,
  - default retry scope targets transient `write_write` conflicts only,
  - business `optimistic_lock` (`If-Match`) conflicts are not auto-retried.
- stress hardening update:
  - added `tests/persist_app_stress_tests.rs` coverage for write-write race behavior with and without retry policy,
  - validated under load that optimistic-lock conflicts remain non-retry business responses,
  - validated repeated rollback + replay paths (`atomic_with` and autonomous apply failure injection) leave no partial state and keep version/audit consistency.
- MVCC hardening shipped for rollback path:
  - `Table::update` now ignores aborted tail versions and avoids false write-write conflicts,
  - regression test added for post-rollback update continuation.
- Remaining hardening focus for this phase:
  - explicit replication mode matrix for unique behavior.

## Phase 2: Multi-Collection Atomic Scope (`PersistTx`)

Objective:

Provide first-class cross-collection atomic operations.

Deliverables:

1. New API surface:
- `PersistApp::transaction(...)`
- explicit `PersistTx` context object
- transaction-scoped collection mutation helpers (`*_with_tx(...)`)
- cross-collection helper `ManagedPersistVec::atomic_with(...)`
- command/delete helpers `ManagedPersistVec::execute_command_if_match(...)`, `ManagedPersistVec::execute_patch_if_match(...)`, `ManagedPersistVec::execute_command_if_match_with_create(...)`, `ManagedPersistVec::execute_workflow_if_match_with_create(...)`, `ManagedPersistVec::execute_workflow_for_many_with_create_many(...)`, and `ManagedPersistVec::execute_delete_if_match(...)`
2. Atomic commit/rollback across touched collections.
3. Conflict semantics and retry integration.

Acceptance criteria:

- user + audit + index update can be committed atomically from one closure.
- no manual compensation logic in app code.

Key tests:

- fail-after-first-write rollback correctness
- failpoint rollback leaves no partial user/audit writes
- commit ordering durability under crash replay

## Phase 3: Aggregate Store API (`PersistAggregateStore<T>`)

Objective:

Replace handwritten repository plumbing with generated or framework-owned aggregate API.

Deliverables:

1. Aggregate-centric API with commands and optimistic concurrency.
2. Built-in CRUD and command handlers for common patterns.
3. Stable mapping between aggregate errors and public API error classes.

Proposed high-level usage:

- `store.create(draft)`
- `store.execute(id, command).if_match(version)`
- `store.delete(id).if_match(version)`

Acceptance criteria:

- handlers/services do not directly use `ManagedPersistVec`.
- repository boilerplate for common aggregates reduced to near-zero.

Key tests:

- command replay determinism
- stale version conflict semantics
- not-found semantics unchanged from existing contract

## Phase 4: Declarative Audit and Outbox Policies

Objective:

Make audit records and side-effect queueing built-in behavior, not app code.

Deliverables:

1. Declarative policy options:
- intent-driven audit mapping in aggregate helpers (`execute_intent_if_match_auto_audit`, `execute_intent_for_many_auto_audit`)
- autonomous intent surface (`PersistAutonomousCommand` + `PersistAutonomousAggregate::apply/apply_many`) as default app-layer DX
- derive/attribute surface for intent enums (`PersistAutonomousIntent`, `persist_intent`) as default path for app teams
- derive/attribute surface for source models (`Autonomous` + `persist_model`) with `open_autonomous_model::<Model>(...)` as the no-boilerplate domain entrypoint
- auto-generated system audit labels/messages from command names when teams do not provide overrides
- `#[persist(outbox = ...)]`
2. Auto-generated audit records from successful commands.
3. Outbox dispatch integration contract and state transitions.

Acceptance criteria:

- command endpoint no longer writes audit entity manually.
- outbox records appear without app-level envelope crafting.

Key tests:

- exactly-once logical behavior with idempotency key
- outbox status transitions survive restart

## Phase 5: Query and Projection DSL

Objective:

Eliminate manual list/filter/sort/page logic in repositories.

Deliverables:

1. Typed query DSL with pagination and sorting.
2. Projection registration conventions per aggregate.
3. Indexed lookup API over registered projections.

Acceptance criteria:

- app code does not iterate full list for standard queries.
- pagination and filtering are declarative at API layer.

Key tests:

- projection rebuild correctness
- index lookup parity with full-scan semantics

## Phase 6: Web Adapter Layer (`persist_web`)

Objective:

Remove repetitive HTTP-layer plumbing around persistence semantics.

Deliverables:

1. Adapter utilities for:
- `If-Match` parsing and validation
- `Idempotency-Key` support
- problem-details mapping
- correlation-id propagation
2. Optional route generation for standard aggregate endpoints.

Acceptance criteria:

- handler code for standard endpoints becomes orchestration-only.
- repeated parsing/mapping helpers deleted from example apps.

Key tests:

- end-to-end contract parity with shared tests
- consistent error payload shapes across adapters

## Phase 7: Education Refactor and Demonstration

Objective:

Prove visible improvement through the A/B course artifacts.

Deliverables:

1. Refactor `education/habit-hero-ab` lessons to new API.
2. Add explicit before/after code-size and complexity deltas.
3. Update markdown-first lessons with new patterns.

Acceptance criteria:

- Part B shows structural reduction, not just renamed layers.
- Shared contract tests remain green.
- lesson narrative clearly communicates deleted complexity categories.

## Phase 8: Hardening and Compatibility Finalization

Objective:

Lock production readiness and migration path.

Deliverables:

1. Compatibility matrix for old and new app APIs.
2. Migration cookbook for existing users.
3. Deprecation schedule for legacy convenience layers.

Acceptance criteria:

- migration path is tested and documented.
- no forced breaking change without migration assistant.

## 5. Workstream Breakdown

Parallel workstreams to reduce delivery risk:

1. Runtime and transaction kernel
2. Macro and generated API surface
3. Web adapter and contract tooling
4. Documentation and education migration
5. Test platform and chaos validation

Each phase should map to at least two workstreams to avoid serial bottlenecks.

## 6. Detailed Backlog by Capability

## 6.1 Declarative Constraints

Tasks:

1. Extend macro metadata parser for field attributes.
2. Add constraint descriptors in generated contracts.
3. Enforce constraints in managed write commit path.
4. Integrate constraint metadata into diagnostics and stats.

## 6.2 Multi-Collection Transactions

Tasks:

1. Introduce transaction context with collection registry.
2. Implement write-set tracking and atomic finalize.
3. Wire rollback snapshots and conflict propagation.
4. Add metrics for tx attempts, commits, rollbacks.

## 6.3 Aggregate Store

Tasks:

1. Define aggregate trait and command execution contract.
2. Generate default store implementation from model metadata.
3. Add optional extension hooks for custom business rules.
4. Integrate with declarative audit/outbox and query DSL.

## 6.4 Web Adapter

Tasks:

1. Standard extractors for conditional headers and idempotency.
2. Unified problem-details mapper for domain/storage errors.
3. Route helpers for aggregate CRUD and command endpoints.
4. Correlation-id propagation policy defaults.

## 7. Acceptance Gates Per Pull Request

Every PR in these phases must include:

1. Charter alignment note.
2. Updated tests (unit plus integration where relevant).
3. API docs updates if public surface changes.
4. Education impact note if example patterns change.
5. Migration note for behavioral or signature changes.

## 8. Risk Register and Mitigation

Risk 1: Hidden complexity moves into generated code but not reduced.

Mitigation:

- enforce code-size and usage metrics in examples,
- require explicit deleted-boilerplate statement per milestone.

Risk 2: Feature growth breaks determinism or replay safety.

Mitigation:

- mandatory replay and crash tests for command-path changes,
- no merge without runtime chaos suite green.

Risk 3: API fragmentation between low-level and high-level paths.

Mitigation:

- define one recommended path per use case,
- mark all alternatives as advanced and document tradeoffs.

Risk 4: Educational drift from product reality.

Mitigation:

- generate lessons from runnable source snippets where possible,
- treat lesson contract tests as release gate.

Risk 5: Backward compatibility burden slows progress.

Mitigation:

- time-box adapter support,
- publish explicit deprecation windows and migration tools.

## 9. Migration Strategy for Existing Examples

1. Introduce new APIs side-by-side with legacy usage.
2. Migrate one canonical example first.
3. Capture quantitative delta report.
4. Migrate education lessons in sequence.
5. Deprecate old patterns after evidence-backed transition.

Priority migration order:

1. `education/habit-hero-ab/lesson4`
2. `examples/scheduler`
3. `examples/managed/todo_persist_runtime`

## 10. Operating Cadence

Weekly rhythm:

1. Monday: phase planning and risk review.
2. Mid-week: implementation and test progress sync.
3. Friday: charter compliance review with metric snapshot.

Bi-weekly:

- roadmap adjustment based on measured boilerplate reduction and user friction.

## 11. Definition of Done (Program Level)

Program is complete when all are true:

1. Product-mode apps can be built without handwritten persistence repositories for standard flows.
2. Uniqueness, indexing, audit, and outbox are declarative and automatic.
3. Cross-collection business operations are atomic through first-class API.
4. Web endpoints no longer manually parse and map persistence semantics repeatedly.
5. Education Part B consistently demonstrates major boilerplate deletion while preserving contracts.
6. Recovery, idempotency, and observability guarantees remain validated by tests.
7. Part B code passes `AUTONOMY_DX_CONTRACT` red-flag checks (no app-layer persistence orchestration primitives).

## 12. Immediate Next 3 Execution Steps

1. Expand `PersistTx` ergonomics beyond raw SQL/session:
- typed collection access patterns for multi-collection orchestration (beyond current `atomic_with(...)`),
- explicit retry hooks for conflict-classified failures.
2. Add stress-level transaction test matrix:
- concurrent writers on shared entities,
- rollback + replay behavior under repeated conflict conditions.
3. Publish updated Phase 2 metrics and guidance:
- boilerplate delta,
- autonomy red-flag elimination delta (`AUTONOMY_DX_CONTRACT` check),
- conflict semantics parity,
- rollback correctness evidence.

Status update (2026-02-17):
- Step 1 completed via policy-driven retry in `PersistAppPolicy::conflict_retry` (no app-layer retry loops required).
- Step 2 completed via `tests/persist_app_stress_tests.rs`.
- Step 3 completed via reproducible report pipeline:
  - `scripts/collect_autonomy_metrics.sh --run-gates`
  - output artifact: `llm/BASELINE_AUTONOMY_METRICS.md`

## 12.1 Next Execution Plan (Pending Approval)

Execution protocol for all stages below:

1. agree scope and acceptance criteria with user first,
2. implement only after approval,
3. re-sync docs and metrics after each stage.

### Stage 1: Part B DX Hardening (Primary)

Goal:
- keep app code on autonomous/aggregate path only.

Scope:
- enforce `open_domain` + `intent/intent_many/patch/remove/workflow_with_create` as the only default app shape,
- keep `open_autonomous/open_aggregate` as advanced/explicit escape hatch,
- remove direct app-layer usage of `execute_intent_if_match_auto_audit` in lesson/product modules,
- no app-layer retry loops, no low-level transaction/session orchestration in product handlers/services.

Acceptance:
- `AUTONOMY_DX_CONTRACT` red-flag count remains zero in Part B app layer.

### Stage 2: `persist_web` Adapter Layer

Goal:
- remove persistence-semantic plumbing from HTTP handlers.

Scope:
- standardized `If-Match` handling,
- persistence conflict to problem-details mapping,
- idempotency-key normalization entrypoint.

Acceptance:
- `lesson4/product-api` handlers no longer carry repeated persistence mapping code,
- API contracts stay unchanged.

### Stage 3: Expand Guard Coverage Beyond Lesson4

Goal:
- make autonomy guardrails global for all Part B artifacts.

Scope:
- add guard script for `education/**/product-api/**`,
- extend metrics collection with per-lesson red-flag and boilerplate deltas.

Acceptance:
- one command shows PASS/FAIL for all Part B lessons.

### Stage 4: Course Artifact Packaging

Goal:
- publish "before/after" educational evidence tied to measured deltas.

Scope:
- update `education/habit-hero-ab/lessons-md` with:
  - deleted boilerplate categories,
  - measured autonomy delta,
  - conflict/rollback guarantees evidence links.

Acceptance:
- each lesson has objective "what was removed" and "what is now automatic".

### Stage 5: Release Gate Finalization

Goal:
- lock process so future iterations do not regress into persistence thinking.

Scope:
- run full gate set via `scripts/collect_autonomy_metrics.sh --run-gates`,
- update execution + progress docs from the gate artifact.

Acceptance:
- all gates PASS and docs reflect the same artifact snapshot.

## 12.2 Latest Executed Increment (2026-02-21)

Completed:

1. Built `examples/ledger_core` as a DX-first autonomous showcase.
2. Kept app-layer code business-only (no manual session/transaction/version choreography in handlers/services).
3. Used generated surfaces:
   - `#[derive(Autonomous)]` + `#[expose_rest]` domain model + REST API from one impl block.
   - `PersistApp::serve_autonomous_model::<Model>(...)` as the only router mount path.
4. Added integration tests proving:
   - successful same-currency transfer flow,
   - cross-currency validation + recovery flow,
   - no-partial-write behavior on insufficient funds.
5. Removed additional persistence leakage in framework layer:
   - `PersistAutonomousModelHandle` now appends system audit records for create/mutate flows.
   - `#[autonomous_impl]` generated methods now pass explicit operation name for audit event generation.
   - `#[api_service]` now accepts `Result` aliases ending with `Result` (for example `ApiResult<T>`), reducing service trait noise.
6. Added “magic REST” path directly from domain model:
   - `#[expose_rest]` generates router/DTO/handlers from command/query/view methods,
   - `#[derive(ApiError)]` + `#[api_error(status=...)]` removes manual domain->HTTP mapping impls,
   - `PersistApp::serve_autonomous_model::<Model>(...)` mounts generated REST in one line,
   - create DTO auto-derives from `new(...)` constructor args,
   - `#[rustmemodb::query]` supports typed GET query arguments without manual DTO files,
   - `#[rustmemodb::view(input = "body")]` supports typed body DTOs for POST views,
   - generated REST includes `GET /:id/_audits` so audit projection does not require manual store/api code.
7. Removed remaining manual boilerplate in `examples/ledger_core`:
   - deleted `src/api.rs` and `src/store.rs`,
   - updated integration tests to hit only generated endpoints,
   - kept all business assertions (atomicity, conflict semantics, consistency) on the generated surface.
8. Added generated REST command idempotency (no app-layer code):
   - `Idempotency-Key` is parsed/normalized by generated handlers,
   - duplicate command requests replay stored status/body,
   - domain mutation is executed exactly once for the same key/scope (prevents double-spend on retry),
   - covered by `examples/ledger_core` HTTP integration test for transfer retry behavior.
9. Added schema-first runtime REST mount for "zero-handler BaaS" flow:
   - `PersistApp::serve_json_schema_dir(...)` loads `schemas/*.json` and mounts generic CRUD routes,
   - app code no longer needs to implement REST handlers for schema-defined resources,
   - field-level validation and CRUD persistence are executed by framework internals,
   - schema changes are hot-reloaded without restart, with automatic `ADD COLUMN` reconciliation,
   - generated schema-first OpenAPI document is available at `GET /_openapi.json`,
   - covered by integration test `tests/persist_schema_rest_tests.rs`,
   - adopted in `examples/no_db_api` by removing manual `api/store/model` layers.
10. Migrated `examples/agile_board` to generated autonomous REST:
   - removed manual runtime `api.rs` / `api_new.rs` / `store.rs` path from the example;
   - runtime now mounts only `PersistApp::serve_autonomous_model::<Board>(...)`;
   - kept nested business logic in domain model and validated with HTTP integration tests.

Why this aligns with the charter:

- It demonstrates that developers can implement non-trivial domain logic (double-entry + FX + reporting)
  through high-level persist APIs while staying focused on business intent.

## 13. One-Line Execution Rule

If a milestone does not remove real application-layer persistence thinking, it is not progress.
