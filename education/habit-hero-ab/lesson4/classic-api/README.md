# Habit Hero Classic API (Part A)

Classic production-style implementation for Lesson 4:
- Axum HTTP layer
- controller/service/repository layering
- PostgreSQL via SQLx
- Docker Compose runtime for local DB
- SQL migrations on startup
- transactional user command + audit event flow

## Run

```bash
cd education/habit-hero-ab/lesson4/classic-api
docker compose up -d
cargo run --manifest-path Cargo.toml
```

## Key Endpoints

- `POST /api/v1/users`
- `GET /api/v1/users/{id}`
- `GET /api/v1/users`
- `PATCH /api/v1/users/{id}` (requires `If-Match`)
- `DELETE /api/v1/users/{id}` (requires `If-Match`)
- `POST /api/v1/users/{id}/commands` (requires `If-Match`)
- `POST /api/v1/users/commands/bulk-lifecycle`
- `GET /api/v1/users/{id}/events`
