# Persist Showcase

Практический мини-проект, который показывает **managed persist-flow** через `PersistApp` и `open_vec`.
Фокус примера: как писать прикладной код без ручного `PersistSession::new(...)`.

- `#[derive(PersistModel)]` + `persist_struct!(from_struct = ...)`
- managed CRUD/command/patch через `ManagedPersistVec`
- `versioned schema` и кастомные `state/sql migrations`
- динамическая схема из `JSON Schema`
- `heterogeneous persist_vec` с миграциями по типам

## Быстрый запуск

```bash
cargo run --offline --manifest-path examples/managed/persist_showcase/Cargo.toml
```

Если у вас есть сеть, можно без `--offline`.

## Что именно показывает пример

1. **Managed auto-persist + Derive model**
- обычная Rust-структура `CatalogProduct` превращается в persisted-объект;
- изменения сохраняются через `create_from_draft / apply_command / patch`, а snapshot lifecycle управляется `PersistApp::open_auto`.

2. **Versioned restore migration (typed vec)**
- делается snapshot с версией схемы `v1`;
- применяется migration-plan `v1 -> v2`:
  - SQL migration: `ALTER TABLE {table} ...`
  - state migration: трансформация полей в snapshot state.

3. **Dynamic JSON-schema entity**
- объект `AuditEvent` создается из JSON Schema;
- типобезопасная запись полей и auto-persist.

4. **Heterogeneous persist_vec**
- в одном контейнере живут разные типы;
- selective invoke вызывает функцию только у поддерживающих объектов;
- per-type migration-plan применяется на restore.

## Примечание

Пример остается демонстрационным: в разделах миграции и hetero-вектора используется `mutate_async(...)`, где managed runtime передает `session` внутрь операции автоматически. Пользовательский код не создает `PersistSession` вручную.

## Структура

```text
examples/managed/persist_showcase/
  Cargo.toml
  README.md
  src/
    main.rs
```

## Для адаптации под ваш проект

- замените `CatalogProduct`, `OpsTicket`, `AuditEvent` на ваши доменные модели;
- зафиксируйте стратегию версий через `schema_version` и `PersistMigrationPlan`;
- переносите миграции в отдельный модуль и тестируйте каждую версию snapshot отдельно.
