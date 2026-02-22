# Habit Hero A/B Lesson 3

## User Goal

Пользователь может:
- обновлять профиль (`display_name`, `active`) с optimistic concurrency
- удалять профиль с optimistic concurrency

## Scope

- `POST /api/v1/users`
- `GET /api/v1/users/{id}`
- `GET /api/v1/users?page=&per_page=&email_contains=&active=&sort_by=&order=`
- `PATCH /api/v1/users/{id}` + `If-Match`
- `DELETE /api/v1/users/{id}` + `If-Match`
- одинаковый OpenAPI-контракт и общий контрактный тестовый набор для Part A и Part B

## Part A (Classic)

Стек: Axum + SQLx + PostgreSQL + migrations.

Run:

```bash
cd education/habit-hero-ab/lesson3/classic-api
docker compose up -d
cargo run --manifest-path Cargo.toml
```

Default bind: `127.0.0.1:18080`

## Part B (Product)

Стек: Axum + RustMemDB persist runtime.

Run:

```bash
cargo run --manifest-path education/habit-hero-ab/lesson3/product-api/Cargo.toml
```

Default bind: `127.0.0.1:18081`

## Smoke Check

```bash
bash education/habit-hero-ab/lesson3/scripts/lesson3_smoke.sh http://127.0.0.1:18080
bash education/habit-hero-ab/lesson3/scripts/lesson3_smoke.sh http://127.0.0.1:18081
```

## Contract Tests

```bash
cargo test --manifest-path education/habit-hero-ab/lesson3/Cargo.toml --workspace --offline
```
