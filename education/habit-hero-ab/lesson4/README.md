# Habit Hero A/B Lesson 4

## User Goal

Пользователь может:
- применять domain-команды `activate/deactivate` к профилю с optimistic concurrency
- массово переключать статус пользователей одним bulk-запросом
- смотреть audit trail по пользователю

## Scope

- `POST /api/v1/users`
- `GET /api/v1/users/{id}`
- `GET /api/v1/users?page=&per_page=&email_contains=&active=&sort_by=&order=`
- `PATCH /api/v1/users/{id}` + `If-Match`
- `DELETE /api/v1/users/{id}` + `If-Match`
- `POST /api/v1/users/{id}/commands` + `If-Match`
- `POST /api/v1/users/commands/bulk-lifecycle`
- `GET /api/v1/users/{id}/events?limit=`
- одинаковый OpenAPI-контракт и общий контрактный тестовый набор для Part A и Part B

## Part A (Classic)

Стек: Axum + SQLx + PostgreSQL + migrations.

Run:

```bash
cd education/habit-hero-ab/lesson4/classic-api
docker compose up -d
cargo run --manifest-path Cargo.toml
```

Default bind: `127.0.0.1:18080`

## Part B (Product)

Стек: Axum + RustMemDB persist runtime.

Run:

```bash
cargo run --manifest-path education/habit-hero-ab/lesson4/product-api/Cargo.toml
```

Default bind: `127.0.0.1:18081`

## Smoke Check

```bash
bash education/habit-hero-ab/lesson4/scripts/lesson4_smoke.sh http://127.0.0.1:18080
bash education/habit-hero-ab/lesson4/scripts/lesson4_smoke.sh http://127.0.0.1:18081
```

## Contract Tests

```bash
cargo test --manifest-path education/habit-hero-ab/lesson4/Cargo.toml --workspace --offline
```
