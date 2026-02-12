# Persist Showcase

Практический мини-проект, который показывает **современное и удобное** использование persist-слоя RustMemDB в реальном сценарии:

- `#[derive(PersistModel)]` + `persist_struct!(from_struct = ...)`
- `auto-persist` с `bind_session + set_auto_persist`
- `versioned schema` и кастомные `state/sql migrations`
- динамическая схема из `JSON Schema`
- `heterogeneous persist_vec` с миграциями по типам

## Быстрый запуск

```bash
cargo run --offline --manifest-path examples/persist_showcase/Cargo.toml
```

Если у вас есть сеть, можно без `--offline`.

## Что именно показывает пример

1. **Auto-persist + Derive model**
- обычная Rust-структура `CatalogProduct` превращается в persisted-объект;
- изменения сохраняются автоматически через async-setter и `mutate_persisted(...)`.

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

## Структура

```text
examples/persist_showcase/
  Cargo.toml
  README.md
  src/
    main.rs
```

## Для адаптации под ваш проект

- замените `CatalogProduct`, `OpsTicket`, `AuditEvent` на ваши доменные модели;
- зафиксируйте стратегию версий через `schema_version` и `PersistMigrationPlan`;
- переносите миграции в отдельный модуль и тестируйте каждую версию snapshot отдельно.
