# Habit Hero A/B Lesson 2

## User Goal

Пользователь может:
- получить профиль по `id`
- посмотреть список пользователей с фильтрами, сортировкой и пагинацией

## Scope

- `POST /api/v1/users`
- `GET /api/v1/users/{id}`
- `GET /api/v1/users?page=&per_page=&email_contains=&active=&sort_by=&order=`
- одинаковый OpenAPI-контракт и общий контрактный тестовый набор для Part A и Part B

## Part A (Classic)

Стек: Axum + SQLx + PostgreSQL + migrations.

Run:

```bash
cd education/habit-hero-ab/lesson2/classic-api
docker compose up -d
cargo run --manifest-path Cargo.toml
```

Default bind: `127.0.0.1:18080`

## Part B (Product)

Стек: Axum + RustMemDB persist runtime.

Run:

```bash
cargo run --manifest-path education/habit-hero-ab/lesson2/product-api/Cargo.toml
```

Default bind: `127.0.0.1:18081`

## Smoke Check

```bash
bash education/habit-hero-ab/lesson2/scripts/lesson2_smoke.sh http://127.0.0.1:18080
bash education/habit-hero-ab/lesson2/scripts/lesson2_smoke.sh http://127.0.0.1:18081
```

## Contract Tests

```bash
cargo test --manifest-path education/habit-hero-ab/lesson2/Cargo.toml --workspace --offline
```
