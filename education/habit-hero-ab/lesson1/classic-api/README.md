# Habit Hero Classic API (Part A)

Classic production-style implementation for Lesson 1:
- Axum HTTP layer
- controller/service/repository layering
- PostgreSQL via SQLx
- Docker Compose runtime for local DB
- SQL migrations on startup

## Run

```bash
cd education/habit-hero-ab/classic-api
docker compose up -d
cargo run --manifest-path Cargo.toml
```

## Endpoint

`POST /api/v1/users`

Example:

```bash
curl -sS -X POST http://127.0.0.1:18080/api/v1/users \
  -H "content-type: application/json" \
  -d '{"email":"alice@example.com","display_name":"Alice"}'
```
