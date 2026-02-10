# Todo Backend (Rust + RustMemDB/PostgreSQL)

Production-ready CRUD backend for Todo application built with:
- `axum` (HTTP API)
- `rustmemodb` (default backend for local run)
- `sqlx` + PostgreSQL (optional production-compatible mode)
- `tokio` (async runtime)
- `docker compose` (PostgreSQL stack)

## Why this setup

You can run the API **on RustMemDB without changing any code/config**:

```bash
cd examples/todo_backend
./scripts/run-rustmemodb.sh
```

This is the default demo path now.

## Features

- Full Todo CRUD API
- Soft delete + restore
- Filtering, search, sorting, pagination
- Validation for payload and query parameters
- Structured error responses
- Dual backend support:
  - `rustmemodb` (default)
  - `postgres` (via SQLx migrations)
- Full tests for all CRUD methods:
  - HTTP API tests
  - RustMemDB repository tests
  - PostgreSQL repository integration test

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
    rustmemodb_repository.rs
    postgres_repository.rs
  scripts/
    run-rustmemodb.sh
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

## Quick start (RustMemDB, no changes)

```bash
cd examples/todo_backend
./scripts/run-rustmemodb.sh
```

- API: `http://localhost:8080`
- DB backend: `rustmemodb`

## Quick start (PostgreSQL via Docker)

```bash
cd examples/todo_backend
./scripts/dev-up.sh
```

- API: `http://localhost:8080`
- PostgreSQL: `localhost:5432`
- DB backend: `postgres`

Stop stack:

```bash
./scripts/dev-down.sh
```

## Configuration

Main env variables:

- `DATABASE_BACKEND=rustmemodb|postgres`
- `DATABASE_URL` (default is RustMemDB URL)
- `RUSTMEMODB_USERNAME` (default `admin`)
- `RUSTMEMODB_PASSWORD` (default `adminpass`)
- `DB_MAX_CONNECTIONS`

See full defaults in `.env.example`.

## Migrations and seeding

```bash
cd examples/todo_backend
./scripts/migrate-up.sh
./scripts/seed.sh
```

`migrate` and `seed` work with the selected `DATABASE_BACKEND`.

## Testing

Run quality gate:

```bash
cd examples/todo_backend
./scripts/test.sh
```

Run tests against real PostgreSQL:

```bash
cd examples/todo_backend
./scripts/test-with-postgres.sh
```

## API

Base path: `/api/v1`

Endpoints:
- `GET /health`
- `POST /api/v1/todos`
- `GET /api/v1/todos`
- `GET /api/v1/todos/{id}`
- `PATCH /api/v1/todos/{id}`
- `PUT /api/v1/todos/{id}`
- `DELETE /api/v1/todos/{id}`
- `POST /api/v1/todos/{id}/restore`

Example create:

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
