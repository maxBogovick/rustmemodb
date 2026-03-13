# RFC: PersistView MVP (Automatic Materialized Views for DX-First Domain Code)

Status: `IN_PROGRESS`  
Date: `2026-02-22`  
Scope: `persist runtime`, `derive/macros`, generated REST

## 1. Problem

Application examples still drift into manual projection code:

1. domain structs add cache fields (`stats`, `channel_stats`),
2. command handlers call manual `recompute_*`,
3. read endpoints use repeated scans or hand-maintained denormalized state.

This violates the product goal: developers write business logic, not storage/projection plumbing.

## 2. Goals

1. Remove manual cache fields and recompute logic from app/domain code.
2. Provide first-class, transactional materialized views in `persist`.
3. Keep view updates deterministic, idempotent, and rollback-safe.
4. Auto-expose views via generated REST/OpenAPI without extra app code.
5. Preserve low-level escape hatch for advanced optimization.

## 3. Non-goals (MVP)

1. Full SQL-like query planner.
2. Arbitrary join engine across unrelated aggregates.
3. Cross-domain global analytics with eventual consistency tuning.

## 4. Public API (MVP)

## 4.1 View declaration

```rust
use rustmemodb::prelude::dx::*;

#[derive(PersistView)]
#[persist_view(model = PulseWorkspace, name = "workspace_dashboard")]
pub struct WorkspaceDashboardView {
    #[view_metric(kind = "count", source = "channels")]
    pub channels_total: i64,

    #[view_metric(kind = "sum", source = "campaigns", field = "spent_minor")]
    pub spent_total_minor: i64,

    #[view_metric(kind = "group_by", source = "campaigns", by = "status")]
    pub campaigns_by_status: std::collections::BTreeMap<String, i64>,
}
```

Notes:

1. Supported `view_metric` options in MVP:
   - `kind = "copy"` + `source`
   - `kind = "count"` + `source`
   - `kind = "sum"` + `source` + `field`
   - `kind = "group_by"` + `source` + `by` (+ optional `op = "count" | "sum"` and `field` for `sum`).
2. `#[persist_view(compute = ...)]` remains available for fully custom view projection logic.

## 4.2 Runtime registration

```rust
let app = PersistApp::open_auto(data_dir).await?;
let workspaces = app.open_autonomous_model::<PulseWorkspace>("workspaces").await?;
let dashboard = app.register_view::<PulseWorkspace, WorkspaceDashboardView>(&workspaces);
```

For generated REST, no manual registration is required:

```rust
#[api(views(WorkspaceDashboardView))]
impl PulseWorkspace {
    // commands/queries...
}
```

## 4.3 Generated REST

When `#[api]`/`#[expose_rest]` is used, runtime exposes:

1. `GET /:id/views/<view_name>`
2. OpenAPI schema for `<view_name>` output
3. error mapping through existing `DomainError` contract

No manual query wrapper is required in app code.

## 5. Runtime Semantics

1. View updates are executed inside the same command transaction.
2. Command failure rolls back model and view updates together.
3. Idempotent command replay does not re-apply view deltas.
4. Recovery/snapshot reconstructs view state from durable model state + journal.

## 6. Internal Contracts

1. Add `PersistViewDescriptor` to command/query registry.
2. Add `ViewDelta` envelope to runtime mutation pipeline.
3. Persist view blobs in deterministic namespace:
   - table key: `<aggregate>__view__<name>`
4. Keep view logic out of user model files.

## 7. Migration Plan

## Phase A

1. Ship `PersistView` derive and runtime registration.
2. Keep existing manual query methods fully compatible.

## Phase B

1. Enable auto-mount in generated routers (`serve_domain!` path).
2. Add docs + examples replacing manual recompute caches.

## Phase C

1. Add built-in secondary index helpers for nested collections.
2. Add metric filters (`filter = ...`) for derived views.
3. Add optional incremental materialization mode hints.

## 8. PulseStudio Target Shape

Before:

1. `PulseWorkspace` has `stats/channel_stats` fields.
2. every mutating command calls `recompute_counters()`.

After:

1. `PulseWorkspace` keeps only business state (`channels/campaigns/activity`).
2. dashboard/overview are served from `PersistView` outputs.
3. model code stays focused on invariants (`budget`, `status`, `active channel`).

## 9. Acceptance Criteria

1. Showcase model files no longer contain manual cache fields.
2. Showcase model files no longer contain manual `recompute_*` functions.
3. Generated REST includes view endpoints and OpenAPI docs.
4. `cargo test` and `cargo clippy -D warnings` pass for updated examples.
