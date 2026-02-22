# Habit Hero Classic API (Part A)

Classic production-style implementation for Lesson 3:
- Axum HTTP layer
- controller/service/repository layering
- PostgreSQL via SQLx
- Docker Compose runtime for local DB
- SQL migrations on startup

## Run

```bash
cd education/habit-hero-ab/lesson3/classic-api
docker compose up -d
cargo run --manifest-path Cargo.toml
```

## Key Endpoints

- `POST /api/v1/users`
- `GET /api/v1/users/{id}`
- `GET /api/v1/users`
- `PATCH /api/v1/users/{id}` (requires `If-Match`)
- `DELETE /api/v1/users/{id}` (requires `If-Match`)
