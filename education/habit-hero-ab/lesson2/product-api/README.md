# Habit Hero Product API (Part B)

RustMemDB implementation for Lesson 1 with the same external contract:
- Axum HTTP layer
- controller/service/repository layering
- `PersistApp::open_auto(...)` for persistence lifecycle
- `persist_struct!` + `persist_vec!` for managed persistence

## Run

```bash
cargo run --manifest-path education/habit-hero-ab/product-api/Cargo.toml
```

## Endpoint

`POST /api/v1/users`

Example:

```bash
curl -sS -X POST http://127.0.0.1:18081/api/v1/users \
  -H "content-type: application/json" \
  -d '{"email":"alice@example.com","display_name":"Alice"}'
```
