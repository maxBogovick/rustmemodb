# Habit Hero A/B Course

Курс разделен на независимые уроки. У каждого урока свой workspace, свой OpenAPI и свои контрактные тесты.
Параллельно есть markdown-first трек, где каждый урок оформлен как один большой `.md`
с блоками `Was (classic)` и `Now (product)`.

## Structure

```text
education/habit-hero-ab/
  lessons-md/
    lesson-01-user-registration.md
    lesson-02-read-model-pagination.md
    lesson-03-write-model-concurrency.md
    lesson-04-command-first-bulk-audit.md
    _lesson-template.md
  lesson1/
    openapi/
    shared-tests/
    classic-api/
    product-api/
    scripts/
  lesson2/
    openapi/
    shared-tests/
    classic-api/
    product-api/
    scripts/
  lesson3/
    openapi/
    shared-tests/
    classic-api/
    product-api/
    scripts/
  lesson4/
    openapi/
    shared-tests/
    classic-api/
    product-api/
    scripts/
```

## Markdown-First Lessons

Если цель - обучать без перегруза файлами, начинай отсюда:

`/Users/maxim/RustroverProjects/rustmemodb/education/habit-hero-ab/lessons-md/README.md`

## Lesson 1

Фокус: базовая ценность для пользователя - регистрация.

- User goal: пользователь регистрируется и сразу получает стабильный `id`
- API: `POST /api/v1/users`
- Why it matters: сравнение классического стека и RustMemDB на одинаковой бизнес-пользе

Start: `education/habit-hero-ab/lesson1/README.md`

## Lesson 2

Фокус: чтение и масштабирование API-контракта.

- User goal: получить пользователя по `id` и список пользователей с фильтрами/сортировкой/пагинацией
- API: `GET /api/v1/users/{id}`, `GET /api/v1/users`
- Why it matters: показываем, как растет сложность в классике и как выглядит тот же функционал через продукт

Start: `education/habit-hero-ab/lesson2/README.md`

## Lesson 3

Фокус: update/delete и optimistic concurrency.

- User goal: безопасно менять и удалять профиль без потери согласованности
- API: `PATCH /api/v1/users/{id}` + `If-Match`, `DELETE /api/v1/users/{id}` + `If-Match`
- Why it matters: показываем, как в обеих реализациях оформлять конфликт версий и избежать «тихих перезаписей»

Start: `education/habit-hero-ab/lesson3/README.md`

## Lesson 4

Фокус: command-first mutation, bulk operations и audit trail.

- User goal: применять `activate/deactivate` как доменные команды, выполнять bulk-команды и видеть историю изменений
- API: `POST /api/v1/users/{id}/commands`, `POST /api/v1/users/commands/bulk-lifecycle`, `GET /api/v1/users/{id}/events`
- Why it matters: показываем переход от «просто CRUD» к управляемым командам и наблюдаемым бизнес-событиям

Start: `education/habit-hero-ab/lesson4/README.md`
