# Persist Implementation Progress

## Source of Truth

- Execution roadmap: `PERSIST_ROADMAP.md`
- Progress tracker (this file): `PERSIST_PROGRESS.md`
- Autonomy DX guardrails: `llm/AUTONOMY_DX_CONTRACT.md`

## Latest Update (2026-02-21)

Delivered in this iteration:

1. `#[command]/#[query]/#[view] -> router` generation finalized in `#[expose_rest]` flow.
2. `#[derive(ApiError)]` extended with `status + code`:
   - `#[api_error(status = <u16>, code = "...")]`
   - maps into `PersistServiceError::Custom { status, code, message }`.
3. Generated command handlers include idempotency replay integration (via `Idempotency-Key`) and now expose it in generated OpenAPI metadata.
4. `unistructgen` input/output binding integrated in autonomous macros:
   - `#[command(input = <Type>, output = <Type>)]`
   - `#[query(input = <Type>, output = <Type>)]`
   - `#[view(input_type = <Type>, output = <Type>)]`.
5. Generated autonomous router now serves OpenAPI from command/query registry:
   - `GET /_openapi.json`.
6. `examples/ledger_core` migrated to typed input flow with generated REST + OpenAPI + API error codes.
7. `examples/agile_board` migrated to generated autonomous REST path:
   - runtime path now uses `PersistApp::serve_autonomous_model::<Board>("boards")`;
   - manual API/store modules removed from active example flow;
   - domain errors now use `#[derive(ApiError)]` for automatic HTTP status/code mapping.
8. `#[expose_rest]` no-content semantics hardened:
   - methods returning `()` or `Result<(), E>` now produce `204 No Content` (runtime + generated OpenAPI status).
9. Core `PersistJson<T>` shipped:
   - generic JSON-backed `PersistValue` wrapper with `Deref/DerefMut`,
   - removes need for local wrapper structs and manual JSON `PersistValue` implementations.
10. Examples simplified with `PersistJson<T>`:
   - `examples/agile_board` switched `columns` field to `PersistJson<Vec<Column>>`,
   - `examples/ledger_core` switched `accounts/transactions` to `PersistJson<Vec<_>>`.
11. Stable DX prelude added:
   - `rustmemodb::prelude::dx::*` introduced as canonical high-level app path,
   - `rustmemodb::prelude::advanced::*` introduced as explicit low-level escape hatch.
12. Showcase examples moved to DX prelude route:
   - `examples/agile_board` and `examples/ledger_core` now import `PersistApp` from `prelude::dx`,
   - model impls switched to concise `#[command]` / `#[query]` / `#[view]` markers (no `rustmemodb::` prefix noise).
13. Idempotency-by-default hardened with executable proof:
   - added `agile_board` HTTP test that replays the same command with the same `Idempotency-Key`,
   - asserts response replay and no duplicate domain mutation,
   - asserts generated OpenAPI includes `Idempotency-Key` header for command routes.
14. DX contract guard expanded:
   - `tests/dx_contract_examples_tests.rs` now also verifies showcase examples use `prelude::dx` and do not leak low-level persistence orchestration primitives.
15. Smart DTO inference shipped in `#[expose_rest]`:
   - command/query/view methods with one DTO-like argument now auto-bind request payload directly,
   - `input = <Type>` is no longer required for the common single-DTO method shape.
16. `examples/ledger_core` cleaned from manual input overrides:
   - removed `input = ...` from `open_account`, `create_transfer`, `account_balance`, `account_balance_body`,
   - HTTP contract and tests remain unchanged and green.

Verification:

1. `cargo test -p rustmemodb_derive --offline`
2. `cargo test --lib --offline`
3. `cargo test --manifest-path examples/ledger_core/Cargo.toml --offline`
4. `cargo test --manifest-path examples/no_db_api/Cargo.toml --offline`
5. `cargo test --manifest-path examples/agile_board/Cargo.toml --offline`
6. `cargo clippy --manifest-path examples/agile_board/Cargo.toml --offline --all-targets -- -D warnings`
7. `cargo clippy -p rustmemodb_derive --offline -- -D warnings`
8. `cargo clippy --manifest-path examples/ledger_core/Cargo.toml --offline --all-targets -- -D warnings`
9. `cargo test --test dx_contract_examples_tests --offline`
10. `cargo fmt --all`
11. `cargo test --manifest-path examples/agile_board/Cargo.toml --offline`
12. `cargo test --manifest-path examples/ledger_core/Cargo.toml --offline`

## Active Scope Lock (2026-02-18)

Until Stage 1 acceptance, implementation scope is locked to DX-first only:

1. High-level app path: `open_domain` + `intent/intent_many/patch/remove/workflow_with_create`.
2. Zero-thinking defaults: no mandatory manual audit/event wiring.
3. `persist_web` adapter primitives for repeated HTTP persistence plumbing.
4. Guardrail expansion and documentation aligned to `AUTONOMY_DX_CONTRACT`.

Out of scope unless strictly required to unblock the items above:

1. New low-level runtime/cluster feature expansion.
2. API changes that push app code back to session/transaction/version plumbing.

## Canonical DX Route (Locked)

For Part B applications and lessons, implementation must use only:

1. `PersistApp::open_auto(...)` + `PersistApp::open_domain::<...>(...)`.
2. Domain mutations via `intent(...)`, `intent_many(...)`, `patch(...)`, `remove(...)`, `workflow_with_create(...)`.
3. HTTP persistence plumbing via `persist::web` helpers:
   `parse_if_match_header(...)`, `normalize_idempotency_key(...)`, `normalize_request_id(...)`, `map_conflict_problem(...)`.

Not allowed as default app-layer shape during Stage 1:

1. Direct app usage of `execute_intent_if_match_auto_audit(...)`.
2. Manual audit label/message mapping for standard intent flows.
3. Manual session/snapshot/retry orchestration in handlers/services/workspaces.

## Goal

Implement `persist_struct!` and `persist_vec!` so persistent objects can:

1. Own state and persist themselves to `InMemoryDB`.
2. Auto-create their tables.
3. Auto-`INSERT` / auto-`UPDATE` on changed fields.
4. Report available functions and support selective invocation in collections.
5. Snapshot/restore with and without data.
6. Support schema sources from:
   - Rust struct fields
   - DDL input
   - JSON Schema input

And provide a production-leaning runtime layer with:

1. Deterministic command registry (+ runtime closures).
2. Durable journal + crash recovery.
3. Snapshot scheduler + compaction.
4. Lifecycle manager (passivation/resurrection/GC).
5. Operational policy (strict/eventual, retry, backpressure).
6. CLI tooling for generation and compatibility checks.

## Done

- [x] Command-first RFC freeze (`M0`)
  - [x] `docs/persist/RFC_COMMAND_FIRST_PERSIST.md`
  - [x] public API contract (`PersistApp`, `open_aggregate`, `open_vec`, generated `Command/Patch/Draft`)
  - [x] deterministic command envelope spec
  - [x] stable error model
  - [x] compatibility matrix + anti-leak DX policy

  - [x] `PersistApp` kernel foundation (`M1` completed)
  - [x] `src/persist/app.rs`
    - [x] `PersistApp`
    - [x] `PersistAppPolicy`
    - [x] `PersistAppAutoPolicy`
    - [x] entrypoint split in `src/persist/app/`:
      - [x] `collection_contracts.rs`
      - [x] `policies_and_conflicts.rs`
      - [x] `app_open.rs`
      - [x] `app_open.rs` domain split in `src/persist/app/app_open/`:
        - [x] `types_and_tx.rs`
        - [x] `constructors_and_retry.rs`
        - [x] `open_collections.rs`
        - [x] `transactions.rs`
      - [x] `store_types.rs`
    - [x] maintainability split (contracts/types in `app.rs`, impls in `src/persist/app/`):
      - [x] `aggregate_store.rs`
      - [x] domain split in `src/persist/app/aggregate_store/`:
        - [x] `core.rs`
        - [x] `indexed_crud_query.rs`
        - [x] `command_audit_workflow.rs`
        - [x] domain split in `src/persist/app/aggregate_store/command_audit_workflow/`:
          - [x] `intent_and_audit.rs`
          - [x] `command_and_delete.rs`
          - [x] `workflow_ops.rs`
      - [x] `autonomous.rs`
      - [x] domain split in `src/persist/app/autonomous/`:
        - [x] `core_read.rs`
        - [x] `conflict_and_apply.rs`
        - [x] `high_level_convenience.rs`
        - [x] `workflow_and_compat.rs`
      - [x] `managed_vec.rs`
      - [x] domain split in `src/persist/app/managed_vec/`:
        - [x] `base_collection.rs`
        - [x] `indexed_crud.rs`
        - [x] domain split in `src/persist/app/managed_vec/indexed_crud/`:
          - [x] `validation_and_reads.rs`
          - [x] `create_paths.rs`
          - [x] `update_paths.rs`
          - [x] `delete_paths.rs`
        - [x] `command_model.rs`
        - [x] `optimistic_workflows.rs`
        - [x] `io_utils.rs`
      - [x] `legacy_adapter.rs`
  - [x] `PersistApp::open_auto(...)`
  - [x] `PersistApp::open_aggregate(...)` app-facing aggregate entrypoint (`PersistAggregateStore`)
  - [x] `PersistApp::open_autonomous(...)` zero-thinking aggregate entrypoint (`PersistAutonomousAggregate`)
  - [x] `PersistApp::transaction(...)` foundation for shared transactional orchestration (`PersistTx`)
  - [x] `ManagedPersistVec`
    - [x] `PersistCollection` trait
    - [x] `PersistIndexedCollection` trait
    - [x] Managed CRUD APIs:
      - [x] `create/get/list/update/delete`
      - [x] `create_many/apply_many/delete_many`
      - [x] `list_page/list_filtered/list_sorted_by`
  - [x] `persist_vec!` wrappers implement `PersistCollection`
  - [x] `persist_vec!` wrappers implement `PersistIndexedCollection`
  - [x] public re-exports in `src/lib.rs`
  - [x] `examples/managed/todo_persist_runtime` CRUD handlers migrated to managed collection API (`PersistApp::open_auto + ManagedPersistVec`)
  - [x] `examples/managed/persist_showcase` migrated to managed collection API (`PersistApp::open_auto + ManagedPersistVec`, no manual `PersistSession::new`)
  - [x] `examples/managed/persistence_demo.rs` migrated from manual engine persistence calls to managed `PersistApp` flow
  - [x] snapshot/recovery lifecycle hidden from REST handlers (no manual snapshot endpoint usage in happy path)
  - [x] todo runtime shutdown path no longer forces manual snapshot flush
  - [x] delete semantics fixed to avoid re-insert resurrection after `DELETE`
  - [x] transactional managed write path in `ManagedPersistVec` (begin/commit/rollback via `PersistSession`)
  - [x] atomic rollback for managed writes (`create/update/delete/create_many/apply_many/delete_many`)
  - [x] explicit managed conflict classification (`ManagedConflictKind`, `classify_managed_conflict`)
  - [x] declarative constraint/index enforcement in managed writes:
    - [x] field-level unique validation before commit (`PersistEntity::unique_fields`)
    - [x] conflict classifier recognizes engine `unique index` violations
  - [x] external transaction helpers on managed collections:
    - [x] shared session accessor (`shared_session`)
    - [x] snapshot/restore hooks for cross-collection rollback (`snapshot_for_external_transaction`, `restore_snapshot_for_external_transaction`)
    - [x] commit hook for snapshot policy integration (`on_external_mutation_committed`)
    - [x] `*_with_session(...)` mutation methods for transaction-scoped writes
    - [x] `*_with_tx(...)` mutation methods as primary app-level transaction API
    - [x] high-level `atomic_with(...)` orchestration helper that hides snapshot/rollback choreography for two managed collections
    - [x] high-level command orchestration helpers:
      - [x] `execute_command_if_match(...)`
      - [x] `execute_patch_if_match(...)`
      - [x] `execute_command_if_match_with_create(...)`
      - [x] `execute_command_if_match_with_audit(...)`
      - [x] `execute_command_for_many_with_audit(...)`
      - [x] `execute_intent_if_match_auto_audit(...)`
      - [x] `execute_intent_for_many_auto_audit(...)`
      - [x] autonomous command helpers:
        - [x] `apply(...)`
        - [x] `apply_many(...)`
        - [x] `patch_if_match(...)`
        - [x] `delete_if_match(...)`
      - [x] `execute_workflow_if_match_with_create(...)`
      - [x] `execute_workflow_for_many_with_create_many(...)`
      - [x] `execute_delete_if_match(...)`
      - [x] built-in audit record types (`PersistAuditRecord`, `PersistAuditRecordVec`)
  - [x] `PersistAggregateStore`
    - [x] app-facing CRUD/query helper delegations over managed runtime
    - [x] app-facing command/workflow helper delegations (`execute_patch_if_match`, `execute_delete_if_match`, workflow helpers)
    - [x] app-facing query helpers (`find_first`, `query_page_filtered_sorted`)
    - [x] app-facing auto-audit helpers (`execute_command_if_match_with_audit`, `execute_command_for_many_with_audit`)
    - [x] intent-based auto-audit helpers (`execute_intent_if_match_auto_audit`, `execute_intent_for_many_auto_audit`)
  - [x] `PersistAutonomousAggregate`
    - [x] business-intent-first command APIs (`apply`, `apply_many`) with audit hidden under the hood
    - [x] quick-start high-level APIs (`intent`, `intent_many`, `patch`, `remove`, `workflow_with_create`) without explicit expected-version plumbing in app code
    - [x] closure-mutation high-level APIs (`mutate_one`, `mutate_many`) for business updates without patch wiring
    - [x] typed closure-mutation APIs (`mutate_one_with`, `mutate_many_with`) that preserve business mutator errors without `DbError` string bridges
    - [x] audit projection helper (`list_audits_for`) without separate app-managed audit store
    - [x] compatibility bridges for optimistic patch/delete (`patch_if_match`/`delete_if_match`)
  - [x] `PersistApp::open_domain(...)` quick-start entrypoint (high-level API) while preserving low-level (`open_aggregate`/`open_vec`) access
  - [x] `PersistApp::open_domain_handle(...)` (`PersistDomainHandle`) to remove app-level `Arc<Mutex<...>>` orchestration
  - [x] `PersistDomainHandle` `&self` outcome path:
    - [x] `create/create_one/create_many`
    - [x] `get_one/list/find_first/query_page_filtered_sorted/list_audits_for`
    - [x] `intent/intent_one/patch/patch_one/remove/remove_one`
    - [x] `mutate_one/mutate_many`
    - [x] `mutate_one_with/mutate_many_with`
  - [x] quick-start adoption in examples/education:
    - [x] `education/habit-hero-ab/lesson4/product-api` workspace opened via `open_domain(...)`
    - [x] `examples/gamemaster` player store opened via `open_domain(...)`
    - [x] `examples/agile_board` migrated from app-level `Arc<Mutex<PersistDomainStore<_>>>` to `PersistDomainHandle` and typed `mutate_one_with(...)` flow (no `DbError` string parsing in app layer)
  - [x] `persist_web` high-level adapter primitives:
    - [x] `parse_if_match_header(...)`
    - [x] `normalize_idempotency_key(...)`
    - [x] `normalize_request_id(...)`
    - [x] `map_conflict_problem(...)`
    - [x] stable validation message constants exported from `rustmemodb`
    - [x] integration tests in `tests/persist_web_tests.rs`
  - [x] autonomous intent derive/attribute (`rustmemodb_derive`):
    - [x] `#[derive(PersistAutonomousIntent)]`
    - [x] `#[persist_intent(model = <Entity>, to_command = <method>)]`
    - [x] variant-level mapping without helper methods (`#[persist_intent(model = <Entity>)]` + `#[persist_case(...)]`)
    - [x] optional method overrides for audit labels/messages (`event_type`, `event_message`, `bulk_event_type`, `bulk_event_message`)
    - [x] default system audit labels/messages generated automatically from command name when overrides are omitted
    - [x] `#[derive(PersistJsonValue)]` for local struct/enum JSON fields (no custom `PersistValue` wrapper boilerplate)
  - [x] autonomous source-model derive/handle (`rustmemodb_derive` + app layer):
    - [x] `#[derive(Autonomous)]` for source-model-first persistence
    - [x] generated persisted bridge (`<Model>Persisted`) with `PersistBackedModel`
    - [x] generated collection wrapper (`<Model>AutonomousVec`)
    - [x] `PersistAutonomousModel` trait contract
    - [x] `PersistAutonomousModelHandle` API (`create_one/get_one/list/mutate_one_with/mutate_one_with_result/remove_one`)
    - [x] `#[autonomous_impl]` + `#[rustmemodb::command]` generates model-specific extension trait (`<Model>AutonomousOps`) with high-level methods over `PersistAutonomousModelHandle`
    - [x] `PersistApp::open_autonomous_model::<Model>(...)` entrypoint
    - [x] integration tests in `tests/persist_autonomous_derive_tests.rs`
  - [x] MVCC conflict regression hardening:
    - [x] `Table::update` no longer reports false write-write conflict when newest tail version belongs to an aborted transaction
    - [x] regression test `update_ignores_aborted_tail_version_and_allows_next_writer`

- [x] Runtime module `src/persist/mod.rs`
  - [x] entrypoint split in `src/persist/core/`:
    - [x] `api_version.rs`
    - [x] `session_and_metadata.rs`
    - [x] `descriptors_and_state.rs`
    - [x] `dynamic_schema_contracts.rs`
    - [x] `snapshots_and_migrations.rs`
    - [x] `entity_contracts.rs`
    - [x] `containers_and_values.rs`
  - [x] maintainability split (contracts/types in `mod.rs`, impls in `src/persist/core/`):
    - [x] `session_impl.rs`
    - [x] `migration_impl.rs`
      - [x] domain split in `src/persist/core/migration_impl/`:
        - [x] `step_builder_and_debug.rs`
        - [x] `plan_basics_and_validation.rs`
        - [x] `plan_execution.rs`
    - [x] `persist_vec_impl.rs`
      - [x] domain split in `src/persist/core/persist_vec_impl/`:
        - [x] `basics_and_io.rs`
        - [x] `invoke_and_prune.rs`
        - [x] `snapshot_and_restore.rs`
    - [x] `hetero_vec_impl.rs`
      - [x] domain split in `src/persist/core/hetero_vec_impl/`:
        - [x] `basics_and_registration.rs`
        - [x] `collection_mutations.rs`
        - [x] `runtime_ops.rs`
        - [x] `snapshot_restore.rs`
    - [x] `persist_value_impls.rs`
    - [x] `schema_utils.rs`
      - [x] domain split in `src/persist/core/schema_utils/`:
        - [x] `naming_and_sql.rs`
        - [x] `ddl_schema.rs`
        - [x] `json_schema.rs`
  - [x] cluster routing internals maintainability split:
    - [x] `src/persist/cluster/routing/routing_table.rs` entrypoint
    - [x] `src/persist/cluster/routing/routing_table/construct_and_validate.rs`
    - [x] `src/persist/cluster/routing/routing_table/mutations.rs`
    - [x] `src/persist/cluster/routing/routing_table/lookups.rs`
  - [x] `PersistSession`
  - [x] Traits: `PersistEntity`, `PersistEntityFactory`
  - [x] declarative field metadata hooks on `PersistEntity`:
    - [x] `unique_fields()`
    - [x] `indexed_fields()`
  - [x] deterministic index naming helper (`default_index_name`)
  - [x] Trait: `PersistModelExt` (for existing structs)
  - [x] Command-first trait/contracts:
    - [x] `PersistCommandModel`
    - [x] `try_from_draft(...)` default path for fallible models
    - [x] `validate_draft_payload(...)` hook
    - [x] `PersistPatchContract`
    - [x] `PersistCommandContract`
    - [x] `PersistCommandFieldContract`
  - [x] Versioned schema metadata (`schema_version`) for states/snapshots
  - [x] Migration API:
    - [x] `PersistMigrationStep`
    - [x] `PersistMigrationPlan`
    - [x] schema registry table (`__persist_schema_versions`)
    - [x] table migration hooks + state migration hooks
  - [x] `PersistVec` container
  - [x] `PersistState`, descriptors, invoke outcomes
  - [x] Snapshot types (`SnapshotMode`, `PersistVecSnapshot`)
  - [x] `PersistValue` typed mapping
  - [x] Dynamic schema support:
    - [x] `DynamicSchema`, `DynamicFieldDef`
    - [x] `dynamic_schema_from_ddl(...)`
    - [x] `dynamic_schema_from_json_schema(...)`
    - [x] value compatibility and SQL literal helpers

- [x] Macros `src/persist/macros.rs`
  - [x] maintainability split (entrypoint in `macros.rs`, bodies in `src/persist/macros/`):
    - [x] `attr_helpers.rs`
    - [x] `persist_struct.rs`
    - [x] `persist_vec.rs`
  - [x] macro implementations are currently consolidated in `persist_struct.rs` and `persist_vec.rs` to keep expansion behavior stable without crate-level lint allowances
  - [x] generated `persist_vec!` public APIs include rustdoc comments for discoverability and contract clarity
  - [x] generated `persist_struct!` public APIs (typed and dynamic) include rustdoc comments for discoverability and contract clarity
  - [x] `persist_struct!` typed mode (`struct {...}`)
  - [x] typed field attributes:
    - [x] `#[persist(unique)]`
    - [x] `#[persist(index)]`
    - [x] mixed attribute parsing (`#[persist(unique, index)]` order-independent)
  - [x] `persist_struct!` dynamic mode from DDL
  - [x] `persist_struct!` dynamic mode from JSON Schema
  - [x] `persist_struct!` from existing struct alias (`from_struct = ...`)
  - [x] `persist_vec!` wrapper macro
  - [x] `persist_vec!(hetero ...)` mixed-type wrapper macro
  - [x] Bound-session auto-persist API for typed and dynamic entities
  - [x] `mutate_persisted(...)` batch mutation with one auto-save flush
  - [x] typed bootstrap auto-index DDL generation:
    - [x] `CREATE UNIQUE INDEX IF NOT EXISTS` for `#[persist(unique)]`
    - [x] `CREATE INDEX IF NOT EXISTS` for `#[persist(index)]`
  - [x] Command-first generation for typed `persist_struct!`:
    - [x] `<Entity>Draft`
    - [x] `<Entity>Patch`
    - [x] `<Entity>Command`
    - [x] `from_draft/patch/apply` reducers (+ persisted variants)
    - [x] patch/command payload contracts exposed via `PersistCommandModel`
  - [x] `from_struct` alias companion type aliases:
    - [x] `<Alias>Draft`
    - [x] `<Alias>Patch`
    - [x] `<Alias>Command`
  - [x] Command-first generation for dynamic `persist_struct!`:
    - [x] `<Entity>Draft`
    - [x] `<Entity>Patch`
    - [x] `<Entity>Command`
    - [x] dynamic draft/patch/command payload validation
    - [x] `PersistCommandModel` implementation for `from_ddl` / `from_json_schema`
  - [x] Typed vec restore with custom migration plan
  - [x] Hetero type registration with custom migration plan

  - [x] Derive crate `rustmemodb_derive`
  - [x] `#[derive(PersistModel)]` for existing named-field structs
  - [x] `#[derive(PersistAutonomousIntent)]` for intent enums
  - [x] `#[derive(Autonomous)]` for source-model-first autonomous persistence
  - [x] DSL attribute macros:
    - [x] `#[persistent(...)]` to auto-attach `PersistModel`
    - [x] `#[persistent_impl]` to generate domain command bridge on `<Model>Persisted`
    - [x] `#[command]` marker for domain methods (+ optional `name = "...")`
    - [x] runtime payload/schema/envelope helpers generated from `#[command]` methods
    - [x] auto-registration helper for runtime deterministic handlers (`register_domain_commands_in_runtime`)
  - [x] Generated persisted wrapper (`<StructName>Persisted`)
  - [x] Conversion helpers: `into_persisted`, `into_persisted_with_table`, `from_parts`
  - [x] `#[persist_model(schema_version = ...)]` support
  - [x] generated command-first companion types:
    - [x] `<StructName>PersistedDraft`
    - [x] `<StructName>PersistedPatch`
    - [x] `<StructName>PersistedCommand`
  - [x] generated reducers + contracts:
    - [x] `from_draft/patch/apply` (+ persisted variants)
    - [x] `PersistCommandModel` implementation

- [x] Public exports in `src/lib.rs`
- [x] Warning cleanup for default workspace build
  - [x] deprecated chrono conversion usage replaced in `src/core/types.rs`
  - [x] WAL writer dead code removed in `src/storage/persistence.rs`
  - [x] unused evaluator/catalog helpers removed in `src/evaluator/mod.rs` and `src/facade/database.rs`

- [x] Tests `tests/persist_macros_tests.rs`
  - [x] Typed save/update
  - [x] Typed auto-persist with bound session
  - [x] Function introspection + invoke
  - [x] Vec snapshot/restore + prune
  - [x] DDL-based entity persistence
  - [x] Dynamic auto-persist with bound session
  - [x] JSON-schema-based entity persistence
  - [x] heterogeneous mixed-type container behavior
  - [x] Derive + from_struct alias flow
  - [x] custom migration plan for typed vec restore
  - [x] per-type migration plan in heterogeneous restore
- [x] DSL tests `tests/persistent_dsl_tests.rs`
  - [x] generated domain command contract and command names
  - [x] generated runtime payload/schema/envelope metadata
  - [x] generated runtime handler registration path
  - [x] persisted command application and save path

- [x] Runtime layer `src/persist/runtime.rs`
  - [x] maintainability split:
    - [x] contracts/types in `runtime.rs`
    - [x] contracts/types domain split in `src/persist/runtime/types/`:
      - [x] `handlers_and_envelope.rs`
      - [x] domain split in `src/persist/runtime/types/handlers_and_envelope/`:
        - [x] `handler_types.rs`
        - [x] `envelope_and_side_effects.rs`
        - [x] `payload_schema.rs`
      - [x] `policy.rs`
      - [x] `entity_and_journal.rs`
      - [x] `projection.rs`
      - [x] domain split in `src/persist/runtime/types/projection/`:
        - [x] `contracts.rs`
        - [x] `table_and_undo.rs`
        - [x] `mailbox.rs`
      - [x] `stats_and_registry.rs`
    - [x] support entrypoint in `src/persist/runtime/runtime_support.rs`:
      - [x] `src/persist/runtime/support/helpers.rs`
      - [x] `src/persist/runtime/support/worker.rs`
      - [x] `src/persist/runtime/support/compat.rs`
    - [x] main impl entrypoint in `src/persist/runtime/runtime_impl.rs`
  - [x] runtime impl split (entrypoint in `runtime_impl.rs`, internals in `src/persist/runtime/runtime_impl/`):
    - [x] `api_registry_and_crud.rs`
      - [x] domain split in `src/persist/runtime/runtime_impl/api_registry_and_crud/`:
        - [x] `open_and_stats.rs`
        - [x] `registry_and_projection.rs`
          - [x] domain split in `src/persist/runtime/runtime_impl/api_registry_and_crud/registry_and_projection/`:
            - [x] `deterministic_registry.rs`
            - [x] `migration_registry.rs`
            - [x] `runtime_closure_and_projection.rs`
        - [x] `entity_crud_and_outbox.rs`
    - [x] `command_and_lifecycle.rs`
      - [x] domain split in `src/persist/runtime/runtime_impl/command_and_lifecycle/`:
        - [x] `deterministic_command.rs`
        - [x] `runtime_closure.rs`
        - [x] `lifecycle_snapshot.rs`
    - [x] `storage_and_projection.rs`
      - [x] domain split in `src/persist/runtime/runtime_impl/storage_and_projection/`:
        - [x] `disk_and_journal.rs`
        - [x] `projections.rs`
        - [x] `mailboxes.rs`
    - [x] `internals.rs`
      - [x] domain split in `src/persist/runtime/runtime_impl/internals/`:
        - [x] `entity_and_tombstones.rs`
        - [x] `journal_and_snapshot.rs`
        - [x] `replication_and_io.rs`
        - [x] `recovery_and_backpressure.rs`
  - [x] `PersistEntityRuntime::open(...)`
  - [x] deterministic command registry (`register_deterministic_command`)
  - [x] envelope-first command registry (`register_deterministic_envelope_command[_with_schema]`)
  - [x] deterministic context registry (`register_deterministic_context_command[_with_schema]`)
  - [x] determinism policy (`RuntimeDeterminismPolicy::StrictContextOnly`) for sanctioned command path
  - [x] `RuntimeCommandEnvelope` + envelope validation
  - [x] expected-version (CAS) enforcement in envelope path
  - [x] scoped idempotency key deduplication and replay-safe receipts
  - [x] durable outbox records + dispatch status tracking
  - [x] panic-safe deterministic command execution with state rollback on failure
  - [x] runtime closure registry (`register_runtime_closure`)
  - [x] entity operations (`create_entity`, `upsert_state`, `delete_entity`)
  - [x] command execution with retry/backoff
  - [x] backpressure semaphore + timeout
  - [x] durable JSONL journal and replay
  - [x] snapshot export/load and compaction
  - [x] lifecycle maintenance:
    - [x] passivation
    - [x] resurrection on access
    - [x] stale GC
    - [x] guard against same-cycle passivate+GC deletion
  - [x] runtime stats/paths + compatibility check
  - [x] payload-schema validation for deterministic commands
  - [x] background snapshot worker API (`spawn_runtime_snapshot_worker`)
  - [x] replication policy + journal/snapshot shipping
  - [x] consistency mode mapping (`RuntimeConsistencyMode::{Strong, LocalDurable, Eventual}`)
  - [x] mailbox-backed lifecycle safety guards (skip passivate/GC for busy entities)
  - [x] tracing spans/events for envelope flow and outbox dispatch
  - [x] SLO stats in runtime metrics (durability lag, projection lag, lifecycle churn)
  - [x] direct SLO export surface (`RuntimeSloMetrics`)
  - [x] explicit tombstone retention policy (`RuntimeTombstonePolicy`)
  - [x] tombstone snapshots + TTL pruning
  - [x] projection contract registry (`RuntimeProjectionContract`, `RuntimeProjectionField`)
  - [x] sync projection write path with rollback-safe journal coupling
  - [x] projection index lookups + generated runtime query helpers support
  - [x] projection rebuild API from loaded snapshot+journal state (`rebuild_registered_projections`)

- [x] Runtime tests `tests/persist_runtime_tests.rs`
  - [x] crash recovery replay
  - [x] snapshot compaction
  - [x] envelope CAS/idempotency/outbox recovery integration test
  - [x] strict context determinism policy enforcement
  - [x] deterministic context helper usage + panic rollback safety
  - [x] projection sync/index/rebuild/reopen integration test (`runtime_projection_sync_index_lookup_and_rebuild`)
  - [x] lifecycle passivate/resurrect/GC
  - [x] consistency mode normalization test (`runtime_consistency_mode_normalizes_operational_policy`)
  - [x] runtime SLO/lifecycle churn stats test (`runtime_stats_expose_slo_metrics_and_lifecycle_churn`)
  - [x] chaos crash/recovery+lifecycle integration test (`runtime_chaos_crash_recovery_with_lifecycle_preserves_state`)
  - [x] non-serializable runtime closures path
  - [x] `tests/persist_app_tests.rs` for `PersistApp` recovery + sync replication
  - [x] managed CRUD helper contract test (`managed_persist_vec_crud_helpers_work_for_typed_collections`)
  - [x] `open_auto` zero-thinking contract test (`persist_app_open_auto_hides_snapshot_lifecycle_from_handlers`)
  - [x] declarative constraints/index tests:
    - [x] `managed_unique_field_constraint_blocks_duplicate_create_and_patch`
    - [x] `managed_indexed_field_is_declared_and_saves_without_manual_index_sql`
  - [x] managed atomic batch/conflict tests:
    - [x] `managed_create_many_is_atomic_on_conflict`
    - [x] `managed_apply_many_is_atomic_on_mutator_error`
    - [x] `managed_update_exposes_explicit_optimistic_conflict_for_stale_collection`
  - [x] managed command-first API test:
    - [x] `managed_command_first_api_works_with_draft_patch_and_command`
    - [x] `managed_command_first_api_works_for_dynamic_entities`
    - [x] `managed_execute_patch_if_match_updates_entity_without_manual_version_checks`
    - [x] `managed_execute_workflow_for_many_with_create_many_hides_bulk_plumbing`
    - [x] `persist_aggregate_store_auto_audit_helpers_append_records_without_manual_workflow_types`
  - [x] macro/derive command-first tests:
    - [x] `persist_struct_command_first_api_works_for_typed_entities`
    - [x] `persist_struct_dynamic_command_first_api_works`
    - [x] `persist_model_derive_exposes_command_first_api_through_from_struct_alias`

- [x] Developer tooling
  - [x] `src/bin/persist_tool.rs`
  - [x] `generate entity`
  - [x] `generate migration`
  - [x] `compat-check` for runtime/typed/hetero snapshots
  - [x] `api-version` for stable persist public API contract

- [x] Practical examples
  - [x] `examples/managed/crm_no_sql.rs` migrated to attribute DSL (`#[persistent]`, `#[persistent_impl]`, `#[command]`)
  - [x] `examples/managed/persist_showcase/` (macros/migrations/hetero vec)
  - [x] `examples/advanced/persist_runtime_showcase/` (runtime durability/recovery/lifecycle)
  - [x] `examples/advanced/sentinel_core/` (domain monitoring agent on top of managed persist runtime)
  - [x] `examples/managed/README.md` added as primary managed DX index
  - [x] `examples/managed/todo_persist_runtime/` (REST CRUD on `PersistApp::open_auto` + managed API, recovery, snapshot replication, admin stats)
  - [x] `examples/managed/todo_persist_runtime` macro showcase bin (`todo_macro_showcase`) for `PersistModel + persist_struct! + persist_vec!`
  - [x] application examples use `PersistApp` managed API (no direct `PersistSession` in `todo_persist_runtime`)
  - [x] command-first usage in todo example:
    - [x] create via `create_from_draft`
    - [x] patch via generated `<Entity>Patch`
    - [x] macro showcase mutation via generated `<Entity>Command`
  - [x] Integration HTTP smoke-test for todo runtime via `tower::ServiceExt` (no real socket bind)
  - [x] restart recovery test no longer depends on explicit app shutdown snapshot flush
  - [x] Second-node failover demo script (`examples/managed/todo_persist_runtime/scripts/failover_demo.sh`)

- [x] Education integration
  - [x] `education/habit-hero-ab/lesson4/product-api`:
    - [x] removed manual email-claim technical collection
    - [x] switched to declarative `#[persist(unique)]` on `User.email`
    - [x] preserved contract behavior (`email_uniqueness_across_instances` remains green)
    - [x] lifecycle command + audit append now executed in one shared DB transaction context (no split-write window between collections)
    - [x] failpoint-based rollback proof for lifecycle+audit transaction (no partial write on injected failure)
    - [x] removed explicit `PersistUserStore`/`UserRepository` layer from public app composition:
      - [x] `UserService::open(...)` is the composition entrypoint
      - [x] persistence runtime is internal to `application` module (no infrastructure store type in `main`/HTTP tests)
    - [x] no app-layer manual transaction choreography in lifecycle flows:
      - [x] removed `snapshot_for_external_transaction/shared_session/restore_snapshot_for_external_transaction` from lesson product code
      - [x] switched lifecycle flows to managed workflow helpers (`execute_workflow_if_match_with_create(...)`, `execute_workflow_for_many_with_create_many(...)`)
      - [x] switched single lifecycle command flow to managed `execute_workflow_if_match_with_create(...)`
      - [x] switched update flow to managed `execute_patch_if_match(...)`
      - [x] switched delete flow to managed `execute_delete_if_match(...)`
      - [x] removed service-layer delete pre-read (`get_by_id + delete`) in favor of single command-intent call
      - [x] removed service-layer pre-read for update/lifecycle (`get_by_id + operation`) in favor of single command-intent calls
      - [x] moved lifecycle orchestration under direct `ManagedPersistVec` helpers so workspace methods stay intent-only
      - [x] replaced inline lifecycle closure plumbing with trait-mapped workflow helpers
      - [x] achieved canonical single-file `application/user_workspace.rs` shape without extra repository/store module
      - [x] migrated workspace state from `ManagedPersistVec` to `PersistAggregateStore` (`PersistApp::open_aggregate(...)`) as app-facing syntax baseline
      - [x] migrated list/find flows to aggregate query helpers (`find_first`, `query_page_filtered_sorted`) to remove manual pagination boilerplate
      - [x] migrated lifecycle audit flow to built-in `PersistAuditRecordVec` + aggregate auto-audit helpers (removed lesson-local audit record type/workflow trait)
      - [x] switched lifecycle calls to autonomous aggregate APIs (`users.apply(...)`, `users.apply_many(...)`) with no explicit mapper wiring in `user_workspace`
      - [x] switched workspace state to `PersistAutonomousAggregate<UserVec>` (single aggregate handle, no separate `user_events` field)
      - [x] switched `UserLifecycleCommand` from manual `impl PersistAutonomousCommand<User>` to `#[derive(PersistAutonomousIntent)] + #[persist_intent(...)]`
      - [x] removed helper mapping methods (`to_user_command/event_type/...`) via variant-level derive mapping (`#[persist_case(command = ...)]`)
      - [x] made audit labels/messages optional for intent DX: app code can provide only `command = ...` and rely on system-generated defaults from command names
      - [x] moved conflict retry control under infrastructure policy (`PersistAppPolicy::conflict_retry`) so handlers do not implement retry loops
      - [x] scoped automatic retries to transient `write_write` conflicts; `optimistic_lock` conflicts remain immediate business responses
      - [x] switched repeated handler parsing to `persist_web` helpers (`parse_if_match_header`, `normalize_request_id`)
      - [x] expanded lesson4 guard script to fail on direct `execute_intent_if_match_auto_audit(...)`/`execute_intent_for_many_auto_audit(...)` usage in app layer
      - [x] removed explicit `expected_version` plumbing from lesson4 Part B app flow:
        - [x] workspace mutation path now uses `users.patch(...)`, `users.remove(...)`, `users.intent(...)`
        - [x] service signatures no longer accept `expected_version` for update/delete/lifecycle
        - [x] HTTP handlers no longer parse/require `If-Match` for product Part B flow
      - [x] introduced domain outcome API in `persist` to remove `DbError` branching from app code:
        - [x] new `PersistDomainError` classification (`NotFound`, `ConflictConcurrent`, `ConflictUnique`, `Validation`, `Internal`)
        - [x] `PersistDomainStore` high-level outcome methods:
          - [x] `create_one(...)`
          - [x] `intent_one(...)`
          - [x] `patch_one(...)`
          - [x] `remove_one(...)`
        - [x] lesson4 product workspace now uses `*_one(...)` and no longer contains manual `classify_managed_conflict`/`DbError` mapping
        - [x] integration test coverage added in `tests/persist_app_tests.rs` (`persist_domain_store_outcome_api_returns_domain_errors_without_db_leaks`)
      - [x] added DX-specific lesson4 product contract paths in `lesson4/shared-tests`:
        - [x] `run_write_users_contract_dx(...)`
        - [x] `run_commands_contract_dx(...)`
        - [x] product-api contract test switched to DX variants while classic keeps strict `If-Match` contract
      - [x] added stress matrix for transaction conflicts/rollback-replay (`tests/persist_app_stress_tests.rs`):
        - [x] write-write race without retry surfaces conflicts
        - [x] write-write race with policy retry converges to full success
        - [x] optimistic lock under concurrent stale-if-match load is not auto-retried
        - [x] repeated `atomic_with` failures keep both collections free of partial writes
        - [x] rollback-then-replay keeps versions/audit counts consistent
      - [x] published reproducible autonomy metrics artifact:
        - [x] added `scripts/collect_autonomy_metrics.sh` (supports `--run-gates`)
        - [x] generated `llm/BASELINE_AUTONOMY_METRICS.md` with:
          - [x] boilerplate delta (classic vs product lesson4)
          - [x] AUTONOMY_DX_CONTRACT red-flag delta
          - [x] conflict semantics parity evidence
          - [x] rollback correctness evidence
          - [x] mandatory gate status snapshot (PASS/FAIL)

## Verified

- [x] `cargo fmt --all`
  - result: formatting passed
- [x] `scripts/collect_autonomy_metrics.sh --run-gates`
  - result: metrics report generated, all mandatory gates PASS
- [x] `cargo test --test persist_app_stress_tests`
  - result: `5 passed, 0 failed`
- [x] `cargo test --test persist_app_tests`
  - result: `30 passed, 0 failed`
- [x] `cargo test --manifest-path education/habit-hero-ab/lesson4/product-api/Cargo.toml`
  - result: all lesson4 product-api tests passed (`8` unit + `1` contract + `1` uniqueness reopen)
- [x] `cargo test --test persistent_dsl_tests`
  - result: `4 passed, 0 failed`
- [x] `cargo test --test persist_macros_tests -- --nocapture`
  - result: `17 passed, 0 failed`
- [x] `cargo test --test persist_runtime_tests -- --nocapture`
  - result: `20 passed, 0 failed`
- [x] `cargo test --test persist_cluster_tests -- --nocapture`
  - result: `9 passed, 0 failed`
- [x] `cargo test --test persist_app_tests`
  - result: `27 passed, 0 failed`
- [x] `cargo test --manifest-path education/habit-hero-ab/lesson4/product-api/Cargo.toml`
  - result: all lesson4 product-api tests passed (contract + uniqueness reopen + rollback failpoint)
- [x] `cargo test --test persistent_dsl_tests -- --nocapture`
  - result: `4 passed, 0 failed`
- [x] `cargo test`
  - result: full suite passed (including new `persist_app_tests`)
- [x] `cargo test --manifest-path examples/managed/todo_persist_runtime/Cargo.toml`
  - result: todo runtime smoke test passed (`http_smoke_crud_snapshot_and_recovery_without_socket`)
- [x] `cargo test --test persist_app_tests -- --nocapture`
  - result: `26 passed, 0 failed`
- [x] `cargo run --example crm_no_sql`
  - result: runs on attribute DSL + domain commands with persisted recovery

## Remaining

- [x] Complete milestones from `PERSIST_ROADMAP.md` (current phase: `M5` completed).
- [x] End-to-end migration cookbook docs for rename/nullability/backfill patterns.
- [x] Global success criteria in `PERSIST_ROADMAP.md` are fully closed.

## Next (Pending Approval)

Execution rule:
- implementation starts only after explicit user approval for each stage.

Planned stages:

1. Part B DX hardening (primary):
- keep app shape on `open_domain` + `intent/intent_many/patch/remove/workflow_with_create`;
- no app-layer retry/session orchestration.

2. `persist_web` adapter layer:
- remove repeated persistence HTTP plumbing (`If-Match`, conflict mapping, idempotency normalization) from handlers.

3. Guard expansion:
- extend autonomy guard + metrics coverage from lesson4 to all `education/**/product-api/**`.

4. Education packaging:
- publish measured "before/after" deltas in `education/habit-hero-ab/lessons-md`.

5. Release gate finalization:
- run `scripts/collect_autonomy_metrics.sh --run-gates` and sync docs from generated artifact.

## Cluster Bootstrap (Phase E1)

- [x] Add shard routing contract:
  - [x] `RuntimeShardRoutingTable`
  - [x] `RuntimeShardLeader`
  - [x] `RuntimeShardRoute`
  - [x] `stable_shard_for(...)` deterministic shard function
- [x] Add leader forwarding abstraction:
  - [x] `RuntimeClusterForwarder` trait
  - [x] `RuntimeClusterNode` routing + local/forward execution
  - [x] `InMemoryRuntimeForwarder` for local multi-node tests
  - [x] maintainability split:
    - [x] entrypoint `src/persist/cluster.rs`
    - [x] `src/persist/cluster/routing.rs`
    - [x] domain split in `src/persist/cluster/routing/`:
      - [x] `types.rs`
      - [x] `membership.rs`
      - [x] `routing_table.rs`
      - [x] `shard_hash.rs`
    - [x] `src/persist/cluster/policy_and_trait.rs`
    - [x] `src/persist/cluster/node.rs`
    - [x] `src/persist/cluster/in_memory_forwarder.rs`
- [x] Add cluster tests:
  - [x] routing determinism + leader mapping
  - [x] remote leader forwarding command execution
  - [x] leader quorum replication with follower apply
  - [x] stale leader epoch fencing (failover baseline)
  - [x] quorum preflight reject when replicas are insufficient
  - [x] membership + shard leader movement primitives
  - [x] failover continuation: new leader applies writes after movement; stale leader fenced

## Cluster Quorum/Fencing Baseline (Phase E2)

- [x] Add shard replica/quorum contracts:
  - [x] `RuntimeShardRoutingTable::set_shard_followers(...)`
  - [x] `RuntimeShardRoutingTable::set_shard_quorum(...)`
  - [x] `RuntimeShardRoutingTable::replica_nodes_for_shard(...)`
  - [x] `RuntimeShardRoutingTable::write_quorum_for_shard(...)`
- [x] Add cluster write policy + quorum status:
  - [x] `RuntimeClusterWritePolicy`
  - [x] `RuntimeClusterQuorumStatus`
  - [x] `RuntimeClusterApplyResult`
- [x] Add leader quorum apply path:
  - [x] local preflight probe before commit
  - [x] follower replication + ACK accounting
  - [x] idempotency key auto-fill for envelope retry safety
- [x] Add epoch fence checks in transport:
  - [x] `RuntimeClusterForwarder::probe_replica(...)`
  - [x] `RuntimeClusterForwarder::replicate_command(...)`
  - [x] `InMemoryRuntimeForwarder::register_peer_with_routing(...)`
- [x] Add membership/shard movement primitives:
  - [x] `RuntimeClusterMembership`
  - [x] `RuntimeShardMovement`
  - [x] `RuntimeShardRoutingTable::move_shard_leader(...)`

## Compatibility Increment (Roadmap M4)

- [x] Add command migration contracts in runtime:
  - [x] `register_command_migration(...)` (legacy command+payload-version -> canonical command+payload-version)
  - [x] `register_command_alias(...)` (identity payload adapter helper)
  - [x] `list_command_migrations(...)`
- [x] Apply migration resolution in envelope execution path before deterministic handler lookup.
- [x] Extend runtime stats with migration observability:
  - [x] `RuntimeStats::registered_command_migrations`
- [x] Add integration tests:
  - [x] legacy envelope migration rewrite + idempotent replay
  - [x] legacy envelope rejection when no migration exists
- [x] Add runtime snapshot/journal compatibility checks:
  - [x] `runtime_snapshot_compat_check(...)` (existing)
  - [x] `runtime_journal_compat_check(...)` (new)
  - [x] integration test for version mismatch detection across snapshot+journal
- [x] Add temporary adapter layer for old API migration:
  - [x] `PersistApp::open_vec_legacy(...)`
  - [x] `LegacyPersistVecAdapter` (`add_one`, `add_many`, `remove_by_persist_id`, `save_all`, `restore_with_policy`)
  - [x] integration test for old vector-style flow on top of `PersistApp`

## Hardening Increment (Roadmap M5)

- [x] Add failover continuity integration test:
  - [x] shard leader movement + continued writes on new leader
  - [x] stale old leader epoch fencing after movement
- [x] Add async replication lag/eventual-recovery test:
  - [x] `RuntimeReplicationMode::AsyncBestEffort` eventually ships journal state to replica
- [x] Add explicit tombstone + GC retention controls:
  - [x] `RuntimeTombstonePolicy { ttl_ms, retain_for_lifecycle_gc }`
  - [x] runtime tombstone stats + lifecycle report (`tombstones`, `tombstones_pruned`, `tombstones_pruned_total`)
  - [x] snapshot/compaction keeps active tombstones and prunes expired tombstones by TTL
  - [x] integration tests:
    - [x] `runtime_tombstones_survive_compaction_until_ttl_expires`
    - [x] `runtime_lifecycle_gc_can_skip_tombstones_by_policy`
- [x] Harden async journal shipping line writes to avoid interleaved JSONL records.

## DX Showcase Increment (2026-02-19)

- [x] Added new example `examples/ledger_core` (LedgerCore: personal finance ledger).
- [x] Demonstrated source-model-first autonomous flow:
  - [x] `#[derive(Autonomous)]` aggregate (`LedgerBook`)
  - [x] `#[autonomous_impl]` + `#[rustmemodb::command]` domain commands
  - [x] `PersistApp::open_autonomous_model::<LedgerBook>(...)`
- [x] Demonstrated generated HTTP API surface:
  - [x] `#[api_service]` + `#[async_trait]` + generated router
  - [x] store error mapping via `From<StoreError> for WebError`
- [x] Domain capabilities demonstrated in API and tests:
  - [x] double-entry postings for opening balance and account-to-account transfer
  - [x] multi-currency transfer with explicit target amount
  - [x] atomic rollback on business failures (insufficient funds / invalid FX input)
  - [x] instant balance report computed from in-memory transaction history
- [x] Integration test coverage:
  - [x] happy-path same-currency transfer
  - [x] cross-currency transfer with failure-then-success flow
  - [x] insufficient-funds no-partial-write guarantee

## DX Acceleration Increment (2026-02-19, v2)

- [x] Autonomous source-model path now emits system audit records by default:
  - [x] `PersistAutonomousModelHandle::create_one(...)` appends audit event
  - [x] `PersistAutonomousModelHandle::mutate_one_with(...)` appends audit event
  - [x] `PersistAutonomousModelHandle::mutate_one_with_result_named(...)` added for explicit operation naming
  - [x] `#[autonomous_impl]` generated methods now call `mutate_one_with_result_named(...)` with method name as audit operation key
  - [x] regression coverage in `tests/persist_autonomous_derive_tests.rs` asserts generated command events are present in audit stream
- [x] `PersistDomainHandle::append_audit_for(...)` added as internal/high-level audit append primitive.
- [x] `#[api_service]` output parser now supports `Result` aliases whose type name ends with `Result` (example: `ApiResult<T>`),
      reducing signature verbosity in service traits.

## DX Moduleization Increment (2026-02-19, v3)

- [x] Added shared service-layer error model in `persist::web`:
  - [x] `PersistServiceError` (`NotFound/Conflict/Validation/Internal`)
  - [x] `PersistServiceError::from_domain_for(...)`
  - [x] `PersistServiceError::from_mutation_for(...)`
- [x] Added built-in HTTP mappings in `web::WebError`:
  - [x] `From<PersistDomainError> for WebError`
  - [x] `From<PersistDomainMutationError<E>> for WebError` (when `E: Into<PersistServiceError>`)
  - [x] `From<PersistServiceError> for WebError`
- [x] Refactored showcase apps to use shared moduleized errors (removed duplicated local `StoreError` plumbing):
  - [x] `examples/agile_board`
  - [x] `examples/ledger_core`
- [x] Reduced ID-conversion noise in high-level handles:
  - [x] `PersistDomainHandle` single-id APIs now accept `impl AsRef<str>`
  - [x] `PersistAutonomousModelHandle` single-id APIs now accept `impl AsRef<str>`
  - [x] API/store layers in `examples/agile_board` and `examples/ledger_core` now pass ids directly (no repetitive `&id` adapters)
- [x] Added `persist::web` unit coverage for new service-error mapping helpers.

## Magic REST Increment (2026-02-20)

- [x] Added derive/attribute API surface for zero-boilerplate REST:
  - [x] `#[expose_rest]` (impl-level autonomous + REST generation)
  - [x] `#[rustmemodb::view]` marker for read views
  - [x] `#[derive(ApiError)]` + `#[api_error(status = ...)]` for domain->service error mapping
- [x] Added app-level mount entrypoint:
  - [x] `PersistApp::serve_autonomous_model::<Model>(...)`
  - [x] new `PersistAutonomousRestModel` contract
- [x] Generated REST model path now includes:
  - [x] CRUD endpoints for autonomous model records
  - [x] command endpoints from `#[rustmemodb::command]` with auto-generated DTOs
  - [x] view endpoints from `#[rustmemodb::view]`
- [x] `Magic REST` improvements:
  - [x] generated create DTO is derived from `new(...)` constructor arguments when available
  - [x] `#[rustmemodb::view]` now supports typed arguments via query DTO (`GET /:id/<view>?...`)
  - [x] fallback behavior: if constructor is not detected, create endpoint accepts full source-model payload
- [x] Added `From<Infallible> for PersistServiceError` for zero-friction error conversion in generated handlers.
- [x] Updated `examples/ledger_core` main boot flow to mount generated router through `serve_autonomous_model::<LedgerBook>(...)`.

## Magic REST Increment (2026-02-20, v2)

- [x] Added built-in generated audit route to `#[expose_rest]` routers:
  - [x] `GET /:id/_audits` returns persisted audit stream for the aggregate id.
  - [x] no manual store/api layer required for audit projection in app examples.
- [x] `examples/ledger_core` fully removed manual REST/service boilerplate:
  - [x] deleted `examples/ledger_core/src/api.rs`
  - [x] deleted `examples/ledger_core/src/store.rs`
  - [x] `examples/ledger_core/src/lib.rs` now exports only `model`
  - [x] `examples/ledger_core/tests/http_api.rs` migrated to pure generated REST surface (`serve_autonomous_model`).
- [x] `examples/ledger_core` docs updated to reflect fully generated endpoint surface (including `/:id/_audits`).

## Magic REST Increment (2026-02-20, v3)

- [x] Added first-class read-only `#[rustmemodb::query]` attribute in `rustmemodb_derive`.
  - [x] `#[query]` routes are generated as GET endpoints with query-string DTO extraction.
  - [x] `#[query]` is integrated into `#[expose_rest]` method discovery (same high-level router generation flow as `#[view]`).
  - [x] `#[query]` enforces query-input semantics (no body mode).
- [x] Migrated `examples/ledger_core` read endpoints to `#[rustmemodb::query]` for canonical read-only DX.

## Magic REST Increment (2026-02-20, v4)

- [x] Added built-in command idempotency for generated `#[expose_rest]` command endpoints.
  - [x] Handlers parse optional `Idempotency-Key` header and normalize it via `persist::web` rules.
  - [x] `PersistAutonomousModelHandle::execute_rest_command_with_idempotency(...)` executes command once and stores replay receipt under scoped key `<aggregate_id>:<operation>:<idempotency_key>`.
  - [x] Duplicate requests with the same key replay stored HTTP status + response body without re-running domain mutation.
- [x] Added internal persist collection for REST idempotency receipts:
  - [x] `PersistRestIdempotencyRecord` / `PersistRestIdempotencyRecordVec`
  - [x] wired into `PersistApp::open_autonomous(...)` as `<aggregate_name>__rest_idempotency`.
- [x] Added end-to-end coverage in `examples/ledger_core/tests/http_api.rs`:
  - [x] retrying transfer with the same `Idempotency-Key` does not double-spend and returns replayed payload.

## UniStructGen Integration Increment (2026-02-20, v1)

- [x] Added optional dependency wiring for `unistructgen-macro` in `rustmemodb`:
  - [x] new cargo feature `unistructgen`
  - [x] no impact on default build/profile (feature is opt-in)
- [x] Re-exported UniStructGen schema-to-code macros from `rustmemodb` under `feature = "unistructgen"`:
  - [x] `generate_struct_from_json!`
  - [x] `openapi_to_rust!`
  - [x] `generate_struct_from_sql!`
  - [x] `generate_struct_from_graphql!`
  - [x] `generate_struct_from_env!`
  - [x] `json_struct` attribute
  - [x] `struct_from_external_api!`
- [x] Updated `README.md` with integration section and enablement snippet.

## Magic REST Increment (2026-02-20, v5)

- [x] Added schema-first zero-handler REST mount in `PersistApp`:
  - [x] `PersistApp::serve_json_schema_dir(...)`
  - [x] scans `*.json` files in a directory and derives collection names from filenames.
- [x] Added generic CRUD router over runtime JSON Schema collections:
  - [x] `GET /:collection`
  - [x] `POST /:collection`
  - [x] `GET /:collection/:id`
  - [x] `PATCH /:collection/:id`
  - [x] `DELETE /:collection/:id`
- [x] Added runtime validation + persistence internals under the hood:
  - [x] schema file parsing via `dynamic_schema_from_json_schema(...)`
  - [x] automatic table bootstrap and schema registry write
  - [x] payload validation by field/type against loaded schema
  - [x] SQL-safe identifier guardrails for dynamic schema field names.
- [x] Added integration test:
  - [x] `tests/persist_schema_rest_tests.rs` validates end-to-end create/list/get/patch/delete flow and validation errors without handwritten handlers.
- [x] Migrated `examples/no_db_api` to schema-first router:
  - [x] removed handwritten `api/store/model` layer
  - [x] now boots via `PersistApp::serve_json_schema_dir(...)` + `schemas/users.json`.

## Magic REST Increment (2026-02-21, v6)

- [x] Added hot-reload for schema-first router (`PersistApp::serve_json_schema_dir(...)`):
  - [x] runtime tracks schema directory signature and reloads collection metadata without restart.
  - [x] schema reload is throttled (`reload_check_interval`) to avoid per-request heavy I/O.
- [x] Added automatic schema-to-table reconciliation on reload:
  - [x] missing fields are migrated with `ALTER TABLE ... ADD COLUMN` under the hood.
  - [x] duplicate-column cases are handled safely (idempotent reload behavior).
- [x] Added OpenAPI endpoint for schema-first mode:
  - [x] `GET /_openapi.json` in mounted router (e.g., `/api/_openapi.json`).
  - [x] generates path + component schemas for all discovered collections.
- [x] Added example-level tests first, then implementation:
  - [x] `examples/no_db_api/tests/http_api.rs` covers CRUD, hot-reload, and OpenAPI generation.
  - [x] all `examples/no_db_api` tests pass offline.

## UniStructGen Integration Increment (2026-02-21, v2)

- [x] Completed real (non-declarative) integration usage:
  - [x] `rustmemodb` now re-exports UniStructGen macros behind `feature = "unistructgen"`.
  - [x] `unistructgen-macro` dependency made optional and wired to dedicated cargo feature.
- [x] Added practical usage in `examples/no_db_api`:
  - [x] enabled `features = ["unistructgen"]` for example dependency on `rustmemodb`.
  - [x] request DTOs in `examples/no_db_api/tests/http_api.rs` are generated via `generate_struct_from_json!` instead of handwritten structs / raw map boilerplate.
