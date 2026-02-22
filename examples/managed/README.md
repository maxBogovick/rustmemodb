# Managed Examples

Этот раздел содержит примеры с базовым DX-подходом:
- пользовательский код работает через `PersistApp` и managed-коллекции;
- runtime/snapshot/recovery детали максимально скрыты от прикладной логики.

Для низкоуровневых и runtime-архитектурных сценариев см. `/examples/advanced`.

## Рекомендуемый порядок

1. [`./persistence_demo.rs`](persistence_demo.rs) — минимальный managed поток: create/mutate/restart recovery.
2. [`./persist_showcase/`](persist_showcase/README.md) — `persist_struct!`, `persist_vec!`, миграции, hetero-вектор.
3. [`./crm_no_sql.rs`](crm_no_sql.rs) — прикладной DSL-пример с `#[persistent]` и command-first.
4. [`./todo_persist_runtime/`](todo_persist_runtime/README.md) — REST CRUD + recovery + snapshot/replication.

## Быстрый запуск

```bash
cargo run --offline --example persistence_demo
cargo run --offline --example crm_no_sql
cargo run --offline --manifest-path examples/managed/persist_showcase/Cargo.toml
cargo run --offline --manifest-path examples/managed/todo_persist_runtime/Cargo.toml
```
