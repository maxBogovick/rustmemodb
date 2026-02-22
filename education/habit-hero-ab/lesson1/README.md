# Habit Hero A/B Lesson 1

## User Goal

Пользователь может зарегистрироваться и сразу получить стабильный профиль `id`.

## Scope

- `POST /api/v1/users`
- структурированные ошибки `application/problem+json`
- одинаковый контракт и одинаковые контрактные тесты для Part A и Part B

## Part A (Classic)

Стек: Axum + SQLx + PostgreSQL + migrations.

Run:

```bash
cd education/habit-hero-ab/lesson1/classic-api
docker compose up -d
cargo run --manifest-path Cargo.toml
```

Default bind: `127.0.0.1:18080`

## Part B (Product)

Стек: Axum + RustMemDB persist runtime.

Run:

```bash
cargo run --manifest-path education/habit-hero-ab/lesson1/product-api/Cargo.toml
```

Default bind: `127.0.0.1:18081`

## Smoke Check

```bash
bash education/habit-hero-ab/lesson1/scripts/lesson1_smoke.sh http://127.0.0.1:18080
bash education/habit-hero-ab/lesson1/scripts/lesson1_smoke.sh http://127.0.0.1:18081
```

## Contract Tests

```bash
cargo test --manifest-path education/habit-hero-ab/lesson1/Cargo.toml --workspace --offline
```

## Cliffhanger to Lesson 2

Регистрация готова. Следующий шаг: как добавить чтение и список (`GET /users/{id}` + `GET /users`) с фильтрами, сортировкой и пагинацией без потери качества контракта.
