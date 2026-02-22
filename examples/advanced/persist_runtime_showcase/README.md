# Persist Runtime Showcase

Практический пример, который показывает, как строить "самосохраняющиеся" доменные объекты без ручной SQL-рутины:

- deterministic command registry (вместо хаотичных update-скриптов);
- runtime closures (несериализуемая бизнес-логика в памяти);
- durable journal + crash recovery;
- snapshot + compaction для быстрого старта;
- lifecycle manager (passivation/resurrection/GC);
- strict/eventual operational policy.

## Какую боль решает

1. Не нужно вручную писать `INSERT/UPDATE` для каждой мутации.
2. После рестарта не теряется состояние сущностей.
3. Нет "бесконечного replay журнала": snapshot + compaction держат восстановление быстрым.
4. Можно отделить детерминированные команды (для воспроизведения) от runtime-замыканий (локальная логика).

## Быстрый запуск

```bash
cargo run --offline --manifest-path examples/advanced/persist_runtime_showcase/Cargo.toml
```

## Что делает пример

1. Поднимает runtime для `Incident`-сущностей с strict durability.
2. Создает инциденты, применяет deterministic команды и runtime closure.
3. Делает snapshot + compaction и проверяет schema compatibility.
4. Симулирует restart процесса и доказывает crash recovery.
5. Прогоняет lifecycle-циклы (passivation/resurrection/GC).
6. Отдельно показывает eventual mode для более дешевой fsync-стратегии.

## Структура

```text
examples/advanced/persist_runtime_showcase/
  Cargo.toml
  README.md
  src/
    main.rs
```
