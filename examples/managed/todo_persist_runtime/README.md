# Todo Persist Runtime

Показательный REST CRUD-проект, в котором API-слой построен на:

- `#[derive(PersistModel)]`
- `#[persist_model(schema_version = 2)]`
- `persist_struct!(from_struct = TodoModel)`
- `persist_vec!(TodoVec, PersistedTodo)`

## Что демонстрирует этот пример

1. Todo-объекты сохраняются через `PersistedTodo` без ручного SQL `INSERT/UPDATE`.
2. CRUD API работает поверх `PersistApp + ManagedPersistVec` (`create/get/list/update/delete`) без ручных lifecycle-вызовов в хендлерах.
3. Snapshot (`PersistVecSnapshot`) пишется автоматически и восстанавливается автоматически при старте.
4. Snapshot можно реплицировать в `replica` директории (sync/async режимы).
5. Разработчику не нужно вызывать `restore`/`snapshot` вручную.

## Запуск REST сервиса

```bash
cargo run --offline --manifest-path examples/managed/todo_persist_runtime/Cargo.toml
```

Сервер по умолчанию слушает `127.0.0.1:8090`.

## Macro showcase (derive + persist_struct! + persist_vec!)

В проекте есть отдельный бинарник с чистой macro-демонстрацией:

```bash
cargo run --offline \
  --manifest-path examples/managed/todo_persist_runtime/Cargo.toml \
  --bin todo_macro_showcase
```

## Полезные переменные окружения

- `TODO_BIND_ADDR` (default: `127.0.0.1:8090`)
- `TODO_DATA_DIR` (default: `examples/managed/todo_persist_runtime/.data/primary`)
- `TODO_DURABILITY_MODE` (`strict` | `eventual`, default: `strict`)
- `TODO_EVENTUAL_SYNC_MS` (default: `250`)
- `TODO_SNAPSHOT_EVERY_OPS` (default: `1`, zero-thinking режим)
- `TODO_REPLICATION_MODE` (`sync` | `async`, default: `sync`)
- `TODO_REPLICA_DIRS` (comma-separated, например `examples/managed/todo_persist_runtime/.data/replica-a`)

## Проверка recovery

1. Создать todo:
```bash
curl -s -X POST http://127.0.0.1:8090/api/v1/todos \
  -H "content-type: application/json" \
  -d '{"title":"Buy milk","priority":2}'
```

2. Перезапустить сервер (`Ctrl+C`, затем снова `cargo run ...`).

3. Проверить список:
```bash
curl -s http://127.0.0.1:8090/api/v1/todos
```

## Failover demo второй ноды

Готовый сценарий переключения на реплику:

```bash
bash examples/managed/todo_persist_runtime/scripts/failover_demo.sh
```

Скрипт:

1. Поднимает primary-node с snapshot replication в replica data-dir.
2. Создаёт todo через REST.
3. Останавливает primary (симуляция падения).
4. Поднимает replica как новый primary на другом порту.
5. Проверяет, что todo восстановлен после failover.

Override переменные:

- `PRIMARY_PORT` (default `18090`)
- `FAILOVER_PORT` (default `18091`)
- `DATA_ROOT` (default `examples/managed/todo_persist_runtime/.data/failover_demo`)
