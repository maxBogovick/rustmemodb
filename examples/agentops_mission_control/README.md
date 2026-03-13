# AgentOps Mission Control

`agentops_mission_control` is a flagship modern showcase for the high-level DX path: `rustmemodb::prelude::dx::*`.

It models real AI operations work:
1. register agents,
2. create and activate missions,
3. run mission executions with step timeline,
4. handoff between agents,
5. incident lifecycle,
6. operational views for reliability.

Related reading:

- [`docs/src/guides/agentops_mission_control_walkthrough.md`](../../docs/src/guides/agentops_mission_control_walkthrough.md)
- [`docs/src/killer_features/native_ai_memory.md`](../../docs/src/killer_features/native_ai_memory.md)
- [`docs/src/briefings/what_success_looks_like_after_30_days.md`](../../docs/src/briefings/what_success_looks_like_after_30_days.md)

## What this example proves

1. You write domain methods, not DB plumbing.
2. REST is generated from model methods (`#[api]` + `#[command]`/`#[query]`).
3. Command idempotency is built-in (`Idempotency-Key`).
4. Audit trail is built-in (`GET /:id/_audits`).
5. List query DSL is built-in on generated list route:
   - `page`, `per_page`, `sort`, `field`, `field__op`.
6. Typed views are auto-mounted:
   - `GET /:id/views/ops_dashboard`
   - `GET /:id/views/reliability`
7. OpenAPI is generated:
   - `GET /api/workspaces/_openapi.json`
8. Command DTO validation is generated:
   - DTOs derive `Validate`,
   - command methods use `#[command(validate = true)]`,
   - invalid payloads return `422` without manual validator handlers.

## Core files

1. `src/model.rs`: domain model, commands, queries, views, errors.
2. `src/main.rs`: minimal runtime bootstrap via `serve_domain!`.
3. `tests/http_api.rs`: integration contract (idempotency, audit, query DSL, restart durability).

## Run

```bash
cargo run --manifest-path examples/agentops_mission_control/Cargo.toml
```

Environment:
1. `AGENTOPS_PORT` (default `3030`)
2. `AGENTOPS_DATA_DIR` (default `./agentops_data`)

## HTTP surface (generated)

1. `POST /api/workspaces`
2. `GET /api/workspaces`
3. `GET /api/workspaces/:id`
4. `DELETE /api/workspaces/:id`
5. `GET /api/workspaces/:id/_audits`
6. `GET /api/workspaces/_openapi.json`
7. Commands:
   - `register_agent`
   - `set_agent_active`
   - `create_mission`
   - `activate_mission`
   - `pause_mission`
   - `archive_mission`
   - `start_run`
   - `append_run_step`
   - `handoff_run`
   - `accept_handoff`
   - `finish_run`
   - `fail_run`
   - `cancel_run`
   - `retry_run`
   - `raise_incident`
   - `resolve_incident`
8. Queries:
   - `run_timeline`
   - `mission_health`
   - `agent_load`
   - `open_incidents`
9. Views:
   - `GET /api/workspaces/:id/views/ops_dashboard`
   - `GET /api/workspaces/:id/views/reliability`

## Demo scenario

```bash
BASE=http://127.0.0.1:3030

WORKSPACE_ID=$(curl -sX POST "$BASE/api/workspaces" \
  -H 'content-type: application/json' \
  -d '{"name":"AgentOps HQ"}' | jq -r .persist_id)

ALPHA_ID=$(curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/register_agent" \
  -H 'content-type: application/json' \
  -d '{"handle":"@alpha","model":"gpt-5"}' | jq -r .id)

BETA_ID=$(curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/register_agent" \
  -H 'content-type: application/json' \
  -d '{"handle":"@beta","model":"gpt-5-mini"}' | jq -r .id)

MISSION_ID=$(curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/create_mission" \
  -H 'content-type: application/json' \
  -d '{"title":"Stabilize triage","objective":"route incidents under 2 min","priority":5}' | jq -r .id)

curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/activate_mission" \
  -H 'content-type: application/json' \
  -d "{\"mission_id\":\"$MISSION_ID\",\"owner_agent_id\":\"$ALPHA_ID\"}"

RUN_ID=$(curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/start_run" \
  -H 'content-type: application/json' \
  -d "{\"mission_id\":\"$MISSION_ID\",\"assigned_agent_id\":\"$ALPHA_ID\"}" | jq -r .id)

curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/append_run_step" \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: run-step-1' \
  -d "{\"run_id\":\"$RUN_ID\",\"phase\":\"classify\",\"summary\":\"queued for db triage\",\"latency_ms\":640,\"token_cost\":200}" | jq

# same key replay => same response, no duplicate step
curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/append_run_step" \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: run-step-1' \
  -d "{\"run_id\":\"$RUN_ID\",\"phase\":\"classify\",\"summary\":\"queued for db triage\",\"latency_ms\":640,\"token_cost\":200}" | jq

curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/handoff_run" \
  -H 'content-type: application/json' \
  -d "{\"run_id\":\"$RUN_ID\",\"to_agent_id\":\"$BETA_ID\",\"note\":\"need db specialist\"}"

curl -sX POST "$BASE/api/workspaces/$WORKSPACE_ID/accept_handoff" \
  -H 'content-type: application/json' \
  -d "{\"run_id\":\"$RUN_ID\"}"

curl -s "$BASE/api/workspaces/$WORKSPACE_ID/views/ops_dashboard" | jq
curl -s "$BASE/api/workspaces/$WORKSPACE_ID/views/reliability" | jq
curl -s "$BASE/api/workspaces/$WORKSPACE_ID/_audits" | jq
```

## Generated list query DSL examples

```bash
# sort + pagination
curl -s "$BASE/api/workspaces?sort=name&page=1&per_page=2" | jq

# contains filter
curl -s "$BASE/api/workspaces?name__contains=ops" | jq
```

## Tests

```bash
cargo test --manifest-path examples/agentops_mission_control/Cargo.toml --offline
cargo clippy --manifest-path examples/agentops_mission_control/Cargo.toml --all-targets --offline -- -D warnings
```
