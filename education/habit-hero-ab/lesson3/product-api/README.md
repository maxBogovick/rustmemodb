# Habit Hero Product API (Part B)

RustMemDB implementation for Lesson 3 with the same external contract:
- Axum HTTP layer
- controller/service/repository layering
- `PersistApp::open_auto(...)` for persistence lifecycle
- `persist_struct!` + `persist_vec!` for managed persistence

## Run

```bash
cargo run --manifest-path education/habit-hero-ab/lesson3/product-api/Cargo.toml
```

## Key Endpoints

- `POST /api/v1/users`
- `GET /api/v1/users/{id}`
- `GET /api/v1/users`
- `PATCH /api/v1/users/{id}` (requires `If-Match`)
- `DELETE /api/v1/users/{id}` (requires `If-Match`)
