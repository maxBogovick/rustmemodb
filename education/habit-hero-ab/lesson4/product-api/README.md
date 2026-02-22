# Habit Hero Product API (Part B)

RustMemDB implementation for Lesson 4 with the same external contract:
- Axum HTTP layer
- controller/service/repository layering
- `PersistApp::open_auto(...)` for persistence lifecycle
- `persist_struct!` + `persist_vec!` for managed persistence
- command-first mutation path via generated command enums

## Run

```bash
cargo run --manifest-path education/habit-hero-ab/lesson4/product-api/Cargo.toml
```

## Key Endpoints

- `POST /api/v1/users`
- `GET /api/v1/users/{id}`
- `GET /api/v1/users`
- `PATCH /api/v1/users/{id}`
- `DELETE /api/v1/users/{id}`
- `POST /api/v1/users/{id}/commands`
- `POST /api/v1/users/commands/bulk-lifecycle`
- `GET /api/v1/users/{id}/events`
