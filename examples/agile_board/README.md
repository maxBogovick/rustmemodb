# Agile Board API Example

`agile_board` is a modern REST example that demonstrates a no-DB workflow with generated API, generated DTOs, and domain-only business logic.

## What This Example Shows

- Full CRUD for boards, columns, and tasks.
- Generated REST router via `PersistApp::serve_autonomous_model::<Board>(...)`.
- Stable high-level imports via `rustmemodb::prelude::dx::*`.
- Generated DTOs/endpoints from `#[expose_rest]` + `#[command]`.
- Automatic domain-error -> HTTP mapping via `#[derive(ApiError)]`.
- Command idempotency via `Idempotency-Key` works by default (no manual handler logic).
- Nested data persistence via `PersistJson<Vec<...>>` (no local JSON wrapper boilerplate).
- Durable restart behavior (data survives process restart).
- Source model persistence via `#[derive(Autonomous)]`.

Core implementation files:

- `src/model.rs` - domain state + business commands + generated REST.
- `src/main.rs` - only mounts generated router (`/api/boards`).

## Run

```bash
cargo run --manifest-path examples/agile_board/Cargo.toml
```

Optional runtime config:

- `AGILE_BOARD_PORT` (default: `3002`)
- `AGILE_BOARD_DATA_DIR` (default: `./agile_board_data`)

Example:

```bash
AGILE_BOARD_PORT=3010 AGILE_BOARD_DATA_DIR=/tmp/agile_board_data \
  cargo run --manifest-path examples/agile_board/Cargo.toml
```

## HTTP API

- `POST /api/boards`
- `GET /api/boards`
- `GET /api/boards/:id`
- `DELETE /api/boards/:id`
- `POST /api/boards/:id/rename`
- `POST /api/boards/:id/add_column`
- `POST /api/boards/:id/rename_column`
- `POST /api/boards/:id/remove_column`
- `POST /api/boards/:id/add_task`
- `POST /api/boards/:id/update_task`
- `POST /api/boards/:id/remove_task`
- `POST /api/boards/:id/move_task`
- `GET /api/boards/:id/_audits`
- `GET /api/boards/_openapi.json`

## Quick Scenario

```bash
BASE=http://127.0.0.1:3002

BOARD_ID=$(curl -sX POST "$BASE/api/boards" \
  -H 'content-type: application/json' \
  -d '{"name":"Platform Team"}' | jq -r .persist_id)

COLUMN_ID=$(curl -sX POST "$BASE/api/boards/$BOARD_ID/add_column" \
  -H 'content-type: application/json' \
  -d '{"title":"Backlog"}' | jq -r .)

TASK_ID=$(curl -sX POST "$BASE/api/boards/$BOARD_ID/add_task" \
  -H 'content-type: application/json' \
  -d "{\"title\":\"Design API\",\"description\":\"v1\",\"column_id\":\"$COLUMN_ID\"}" | jq -r .)

curl -sX POST "$BASE/api/boards/$BOARD_ID/update_task" \
  -H 'content-type: application/json' \
  -d "{\"task_id\":\"$TASK_ID\",\"title\":\"Design API v2\",\"tags\":[\"persist\",\"dx\"]}"

curl -s "$BASE/api/boards/$BOARD_ID" | jq
```

## Tests

```bash
cargo test --manifest-path examples/agile_board/Cargo.toml
```
