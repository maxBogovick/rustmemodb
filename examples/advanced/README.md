# Advanced Examples

Этот раздел содержит продвинутые примеры для runtime/архитектурных сценариев.
Базовые managed DX-примеры остаются в `/examples`.

## Состав

- [`persist_runtime_showcase/`](persist_runtime_showcase/README.md) — deterministic runtime, journal/recovery, lifecycle, compaction.
- [`sentinel_core/`](sentinel_core/README.md) — прикладной monitoring-agent поверх `PersistApp`.

## Быстрый запуск

```bash
cargo run --offline --manifest-path examples/advanced/persist_runtime_showcase/Cargo.toml
cargo run --offline --manifest-path examples/advanced/sentinel_core/Cargo.toml
```
