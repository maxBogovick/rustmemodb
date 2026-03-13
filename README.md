# RustMemoDB

[![Crates.io](https://img.shields.io/crates/v/rustmemodb.svg)](https://crates.io/crates/rustmemodb)
[![Documentation](https://docs.rs/rustmemodb/badge.svg)](https://docs.rs/rustmemodb)
[![Build Status](https://img.shields.io/github/actions/workflow/status/maxBogovick/rustmemodb/ci.yml)](https://github.com/maxBogovick/rustmemodb/actions)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

RustMemoDB is an embedded persistence and runtime toolkit for Rust applications that want business-logic-first APIs, generated product surfaces, and fewer infrastructure layers in the first serious phase of a product.

The recommended surface today is:

```rust
use rustmemodb::prelude::dx::*;
```

## What You Get

- `#[domain]` for persisted domain models.
- `#[api]` and `#[expose_rest]` for generated REST from model methods.
- `PersistApp::open_auto(...)` for durable embedded storage.
- `serve_domain!(...)` for mounting generated routers.
- `PersistJson<T>` for nested aggregate state without local wrapper boilerplate.
- Built-in `_audits`, generated `_openapi.json`, and `Idempotency-Key`.
- Typed `PersistView` read models exposed at `/views/<name>`.
- Query DSL and nested graph mutation helpers on the high-level runtime path.
- AI session memory and replay via `AgentSessionRuntime`.

## Best Fit

RustMemoDB is strongest when:

- the app and its persistence belong together
- the domain model should stay close to the API contract
- the team wants one Rust service, not an early database-and-glue stack
- audits, retries, OpenAPI, and typed views matter before the backend is "finished"

It is not the best first choice when you primarily need a shared external SQL database for many unrelated services, a SQL-first analytics platform, or cross-language writes as the main integration model.

## Quickstart Shape

```rust
use axum::Router;
use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};

#[domain(table = "boards", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Board {
    name: String,
    active: bool,
}

#[api]
impl Board {
    pub fn new(name: String) -> Self {
        Self {
            name: name.trim().to_string(),
            active: false,
        }
    }

    pub fn activate(&mut self) -> bool {
        self.active = true;
        self.active
    }
}

let app = PersistApp::open_auto("./data").await?;
let boards_router = rustmemodb::serve_domain!(app, Board, "boards")?;
let app = Router::new().nest("/api/boards", boards_router);
```

That shape is exercised by repository examples and tests that anchor the current product story.

## Why Teams Choose It

The main win is not "a clever storage engine."
The main win is that the product model, generated HTTP contract, read models, and persistence runtime stay much closer together.

That can reduce:

- handler and repository boilerplate
- drift between business logic and API contract
- infrastructure setup drag in the first product phase
- custom glue required before a backend is demoable

## Strong Starting Examples

- [AgileBoard](examples/agile_board/README.md)
  nested aggregate state, generated REST, list query DSL, nested graph mutation
- [LedgerCore](examples/ledger_core/README.md)
  command-heavy financial flow, conflict handling, audits, idempotent replay
- [PulseStudio](examples/pulse_studio/README.md)
  product dashboard surface, typed views, spend invariants, generated contracts
- [AgentOps Mission Control](examples/agentops_mission_control/README.md)
  AI operations control plane, workflow commands, reliability views, auditability
- [No-DB API](examples/no_db_api/README.md)
  schema-first CRUD generation and hot-reload path

## Read The Book

Core reading:

- [Book Welcome](docs/src/README.md)
- [Quickstart](docs/src/quickstart.md)
- [Architecture Overview](docs/src/architecture.md)
- [Tradeoffs and Limits](docs/src/tradeoffs.md)
- [Proof and Examples](docs/src/proof_and_examples.md)

Decision and rollout guides:

- [The Adoption Ladder](docs/src/adoption/the_adoption_ladder.md)
- [A 7-Day Pilot Plan](docs/src/adoption/a_7_day_pilot_plan.md)
- [How to Win the First Architecture Review](docs/src/briefings/how_to_win_the_first_architecture_review.md)
- [The 10-Minute Demo That Sells](docs/src/briefings/the_10_minute_demo_that_sells.md)
- [How to De-Risk the First Launch](docs/src/briefings/how_to_de_risk_the_first_launch.md)

Hands-on product guides:

- [PersistView Read Models](docs/src/guides/persistview_read_models.md)
- [Query DSL and Nested Graph Ops](docs/src/guides/query_dsl_and_nested_graph_ops.md)
- [Contracts, Audits, and Idempotency](docs/src/guides/contracts_audits_and_idempotency.md)
- [PulseStudio Walkthrough](docs/src/guides/pulse_studio_walkthrough.md)
- [AgentOps Walkthrough](docs/src/guides/agentops_mission_control_walkthrough.md)

## Verified In This Repository

The current public story is intentionally aligned to examples and tests that already run:

- generated REST from domain methods
- typed `PersistView` routes and generated OpenAPI
- declarative list query DSL and nested graph mutation helpers
- idempotent replay on showcase command endpoints
- durable reopen and restart behavior
- AI session workflow and replay usage tests

The most useful trust anchors are:

- `examples/agile_board/tests/http_api.rs`
- `examples/ledger_core/tests/http_api.rs`
- `examples/pulse_studio/tests/http_api.rs`
- `examples/agentops_mission_control/tests/http_api.rs`
- `tests/persist_dx_api_macros_tests.rs`
- `tests/persist_view_mvp_tests.rs`
- `tests/ai_memory_phase1_usage_tests.rs`

If the README and the code ever disagree, prefer the examples and tests.

## Installation

```bash
cargo add rustmemodb
```

Then start from:

```rust
use rustmemodb::prelude::dx::*;
```
