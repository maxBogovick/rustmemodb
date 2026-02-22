# Persist Macros Progress

## Scope

- `persist_struct!` for typed persistent entities over `InMemoryDB`.
- `persist_vec!` for collections of `persist_struct!` entities.
- Runtime support: session, traits, state/snapshot model, invoke catalog.

## Checklist

- [x] Create runtime module `persist` with:
  - [x] `PersistSession`
  - [x] `PersistEntity` / `PersistEntityFactory`
  - [x] `PersistVec`
  - [x] `PersistState`, `ObjectDescriptor`, `FunctionDescriptor`
  - [x] `SnapshotMode`, `PersistVecSnapshot`
  - [x] `PersistValue` type mapping and SQL literal conversion helpers
- [x] Implement `persist_struct!` macro (typed mode):
  - [x] Struct generation + metadata
  - [x] Auto table DDL generation
  - [x] `save()` insert/update flow with dirty tracking
  - [x] Built-in function introspection (`state/save/delete/touch`)
  - [x] Runtime custom function registry per object
- [x] Implement `persist_vec!` macro:
  - [x] add one / add many
  - [x] states/descriptors/functions catalog
  - [x] save all / ensure all tables / invoke supported
  - [x] prune stale
  - [x] snapshot / restore
- [x] Add integration tests for `persist_struct!` and `persist_vec!`.
- [ ] Validate restore semantics on clean DB and with existing rows conflict policy.
- [ ] Document API usage in `README.md`.

## Current State

- Runtime + macros implemented.
- Integration tests added and passing (`tests/persist_macros_tests.rs`: 4/4 green).
- Next immediate step: finalize restore conflict policy and write user-facing README section.
