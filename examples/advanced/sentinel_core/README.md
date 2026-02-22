# Sentinel Core (Advanced)

Демо monitoring-agent, построенный поверх `PersistApp` и managed `persist_vec` коллекций.

## Что показывает

- доменные persisted-модели для конфигурации, метрик и событий;
- auto-recovery состояния при рестарте;
- runtime-циклы телеметрии и event logging без ручного SQL слоя.

## Запуск

```bash
cargo run --offline --manifest-path examples/advanced/sentinel_core/Cargo.toml
```

## Дополнительно

Внутреннее устройство описано в:

- `examples/advanced/sentinel_core/INTERNALS.md`
