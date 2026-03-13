# Agile Board API Example

`agile_board` is a modern REST example that demonstrates a no-DB workflow with generated API, generated DTOs, and domain-only business logic.

Related reading:

- [`docs/src/quickstart.md`](../../docs/src/quickstart.md)
- [`docs/src/guides/query_dsl_and_nested_graph_ops.md`](../../docs/src/guides/query_dsl_and_nested_graph_ops.md)
- [`docs/src/briefings/the_10_minute_demo_that_sells.md`](../../docs/src/briefings/the_10_minute_demo_that_sells.md)

## What This Example Shows

- Full CRUD for boards, columns, and tasks.
- Generated REST router via `serve_domain!(app, Board, "boards")`.
- Stable high-level imports via `rustmemodb::prelude::dx::*`.
- Generated DTOs/endpoints from `#[expose_rest]` + `#[command]`.
- Automatic domain-error -> HTTP mapping via `#[derive(DomainError)]`.
- Command idempotency via `Idempotency-Key` works by default (no manual handler logic).
- Nested data persistence via `PersistJson<Vec<...>>` (no local JSON wrapper boilerplate).
- Durable restart behavior (data survives process restart).
- Source model persistence via `#[domain(...)]`.
- Generated list endpoint query DSL (`page`, `per_page`, `sort`, `field`, `field__op`).
- High-level nested mutation API (`nested_push/patch/move/remove`) for aggregate graph updates without manual traversal.

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

## List Query DSL (Generated)

```bash
BASE=http://127.0.0.1:3002

# sort + pagination
curl -s "$BASE/api/boards?sort=name&page=1&per_page=2" | jq

# filter by field operator
curl -s "$BASE/api/boards?name__contains=alpha" | jq
```

`agile_board` integration tests validate this runtime behavior in
`tests/http_api.rs` (`generated_router_supports_list_query_params`).

## Nested Mutation API (High-Level)

The project also demonstrates high-level nested graph operations via autonomous handle APIs:
- `nested_push`
- `nested_patch_where_eq`
- `nested_move_where_eq`
- `nested_remove_where_eq`

These are validated in
`tests/http_api.rs` (`high_level_nested_mutation_api_updates_board_without_manual_traversal`).

## Tests

```bash
cargo test --manifest-path examples/agile_board/Cargo.toml
```
