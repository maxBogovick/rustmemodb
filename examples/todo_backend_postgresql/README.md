# Todo Backend (Rust + PostgreSQL)

Production-ready CRUD backend for Todo application built with:
- `axum` (HTTP API)
- `sqlx` (PostgreSQL access + migrations)
- `tokio` (async runtime)
- `docker compose` (local environment)

## Features

- Full Todo CRUD API
- Soft delete + restore
- Filtering, full-text search, sorting, pagination
- Validation for payload and query parameters
- Structured error responses
- SQL migrations embedded into the binary (`sqlx::migrate!`)
- In-memory repository for fast HTTP tests
- PostgreSQL repository test for end-to-end DB flow

## Project structure

```text
examples/todo_backend/
  src/
    app.rs
    config.rs
    error.rs
    handlers.rs
    models.rs
    repository.rs
    state.rs
    main.rs
    bin/
      migrate.rs
      seed.rs
  migrations/
    0001_init_schema.sql
    0002_indexes.sql
  tests/
    http_crud.rs
    postgres_repository.rs
  scripts/
    dev-up.sh
    dev-down.sh
    migrate-up.sh
    seed.sh
    test.sh
    test-with-postgres.sh
  docker-compose.yml
  Dockerfile
  .env.example
```

## Quick start (local)

```bash
cd examples/todo_backend
cp .env.example .env
cargo run
```

The server starts at `http://localhost:8080`.

## Quick start (Docker)

```bash
cd examples/todo_backend
./scripts/dev-up.sh
```

- API: `http://localhost:8080`
- PostgreSQL: `localhost:5432`

Stop stack:

```bash
./scripts/dev-down.sh
```

## Migrations

Apply migrations via binary:

```bash
cd examples/todo_backend
./scripts/migrate-up.sh
```

## Seed demo data

```bash
cd examples/todo_backend
./scripts/seed.sh
```

## Testing

Run quality gate (fmt + clippy + tests):

```bash
cd examples/todo_backend
./scripts/test.sh
```

Run tests with Docker PostgreSQL:

```bash
cd examples/todo_backend
./scripts/test-with-postgres.sh
```

## API

Base path: `/api/v1`

### Endpoints

- `GET /health`
- `POST /api/v1/todos`
- `GET /api/v1/todos`
- `GET /api/v1/todos/{id}`
- `PATCH /api/v1/todos/{id}`
- `PUT /api/v1/todos/{id}`
- `DELETE /api/v1/todos/{id}`
- `POST /api/v1/todos/{id}/restore`

### Create todo

```bash
curl -X POST http://localhost:8080/api/v1/todos \
  -H 'content-type: application/json' \
  -d '{
    "title": "Ship backend",
    "description": "Finalize CRUD",
    "priority": 4,
    "status": "in_progress"
  }'
```

### List todos (filters)

```bash
curl 'http://localhost:8080/api/v1/todos?page=1&per_page=20&status=pending&search=ship&sort_by=created_at&order=desc'
```

### Patch todo

```bash
curl -X PATCH http://localhost:8080/api/v1/todos/<id> \
  -H 'content-type: application/json' \
  -d '{"status":"completed"}'
```

### Delete and restore

```bash
curl -X DELETE http://localhost:8080/api/v1/todos/<id>
curl -X POST http://localhost:8080/api/v1/todos/<id>/restore
```

## Notes

- `DELETE` performs soft delete by setting `deleted_at`.
- `GET /api/v1/todos/{id}` hides soft-deleted records by default.
- Use `?include_deleted=true` to read deleted records.
- Status transitions set/reset `completed_at` automatically.
