use rustmemodb::{
    PersistEntityRuntime, RuntimeCommandPayloadSchema, RuntimeDurabilityMode,
    RuntimeLifecyclePolicy, RuntimeOperationalPolicy, RuntimePayloadType, RuntimeReplicationMode,
    RuntimeSnapshotPolicy, Value, spawn_runtime_snapshot_worker,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Mutex;

fn count_from_state(state: &rustmemodb::PersistState) -> i64 {
    state
        .fields
        .as_object()
        .and_then(|fields| fields.get("count"))
        .and_then(|v| v.as_i64())
        .unwrap_or_default()
}

#[tokio::test]
async fn runtime_crash_recovery_replays_journal() {
    let dir = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.durability = RuntimeDurabilityMode::Strict;

    let persist_id = {
        let mut runtime = PersistEntityRuntime::open(dir.path(), policy.clone())
            .await
            .unwrap();

        runtime.register_deterministic_command(
            "Counter",
            "increment",
            std::sync::Arc::new(|state, payload| {
                let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
                let fields = state.fields_object_mut()?;
                let current = fields
                    .get("count")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                fields.insert("count".to_string(), json!(current + delta));
                Ok(())
            }),
        );

        let persist_id = runtime
            .create_entity("Counter", "runtime_counter", json!({"count": 0}), 1)
            .await
            .unwrap();

        runtime
            .apply_deterministic_command("Counter", &persist_id, "increment", json!({"delta": 1}))
            .await
            .unwrap();
        runtime
            .apply_deterministic_command("Counter", &persist_id, "increment", json!({"delta": 2}))
            .await
            .unwrap();

        let state_before = runtime.get_state("Counter", &persist_id).unwrap();
        assert_eq!(count_from_state(&state_before), 3);

        persist_id
    };

    let mut reopened = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();
    reopened.register_deterministic_command(
        "Counter",
        "increment",
        std::sync::Arc::new(|state, payload| {
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("count".to_string(), json!(current + delta));
            Ok(())
        }),
    );

    let recovered = reopened.get_state("Counter", &persist_id).unwrap();
    assert_eq!(count_from_state(&recovered), 3);

    reopened
        .apply_deterministic_command("Counter", &persist_id, "increment", json!({"delta": 5}))
        .await
        .unwrap();
    let updated = reopened.get_state("Counter", &persist_id).unwrap();
    assert_eq!(count_from_state(&updated), 8);
}

#[tokio::test]
async fn runtime_snapshot_scheduler_compacts_journal() {
    let dir = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.snapshot = RuntimeSnapshotPolicy {
        snapshot_every_ops: 2,
        compact_if_journal_exceeds_bytes: 1024 * 1024,
        background_worker_interval_ms: None,
    };

    let mut runtime = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();
    runtime.register_deterministic_command(
        "Counter",
        "increment",
        std::sync::Arc::new(|state, payload| {
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("count".to_string(), json!(current + delta));
            Ok(())
        }),
    );

    let id = runtime
        .create_entity("Counter", "runtime_counter", json!({"count": 0}), 1)
        .await
        .unwrap();

    for _ in 0..5 {
        runtime
            .apply_deterministic_command("Counter", &id, "increment", json!({"delta": 1}))
            .await
            .unwrap();
    }

    runtime.force_snapshot_and_compact().await.unwrap();

    let paths = runtime.paths();
    let journal = std::fs::read_to_string(paths.journal_file).unwrap_or_default();
    let non_empty_lines = journal
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
    assert_eq!(non_empty_lines, 0);

    assert!(paths.snapshot_file.exists());
}

#[tokio::test]
async fn runtime_payload_schema_validation_enforces_contract() {
    let dir = tempdir().unwrap();
    let mut runtime = PersistEntityRuntime::open(dir.path(), RuntimeOperationalPolicy::default())
        .await
        .unwrap();

    runtime.register_deterministic_command_with_schema(
        "Counter",
        "increment",
        RuntimeCommandPayloadSchema::object()
            .require_field("delta", RuntimePayloadType::Integer)
            .allow_extra_fields(false),
        Arc::new(|state, payload| {
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(0);
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("count".to_string(), json!(current + delta));
            Ok(())
        }),
    );

    let id = runtime
        .create_entity("Counter", "runtime_counter", json!({"count": 0}), 1)
        .await
        .unwrap();

    let invalid = runtime
        .apply_deterministic_command("Counter", &id, "increment", json!({"delta": "x"}))
        .await;
    assert!(invalid.is_err());

    let state = runtime.get_state("Counter", &id).unwrap();
    assert_eq!(count_from_state(&state), 0);

    runtime
        .apply_deterministic_command("Counter", &id, "increment", json!({"delta": 3}))
        .await
        .unwrap();
    let state = runtime.get_state("Counter", &id).unwrap();
    assert_eq!(count_from_state(&state), 3);
}

#[tokio::test]
async fn runtime_background_snapshot_worker_runs_outside_request_path() {
    let dir = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.snapshot = RuntimeSnapshotPolicy {
        snapshot_every_ops: 2,
        compact_if_journal_exceeds_bytes: 1024 * 1024,
        background_worker_interval_ms: Some(20),
    };

    let runtime = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir.path(), policy)
            .await
            .unwrap(),
    ));

    {
        let mut guard = runtime.lock().await;
        guard.register_deterministic_command(
            "Counter",
            "increment",
            Arc::new(|state, payload| {
                let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
                let fields = state.fields_object_mut()?;
                let current = fields
                    .get("count")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                fields.insert("count".to_string(), json!(current + delta));
                Ok(())
            }),
        );
    }

    let worker = spawn_runtime_snapshot_worker(runtime.clone())
        .await
        .unwrap();

    let id = {
        let mut guard = runtime.lock().await;
        guard
            .create_entity("Counter", "runtime_counter", json!({"count": 0}), 1)
            .await
            .unwrap()
    };

    {
        let mut guard = runtime.lock().await;
        for _ in 0..6 {
            guard
                .apply_deterministic_command("Counter", &id, "increment", json!({"delta": 1}))
                .await
                .unwrap();
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let stats_after_worker = {
        let guard = runtime.lock().await;
        guard.stats()
    };
    assert!(stats_after_worker.snapshot_worker_running);

    let paths = {
        let guard = runtime.lock().await;
        guard.paths()
    };
    assert!(paths.snapshot_file.exists());

    worker.stop().await.unwrap();
}

#[tokio::test]
async fn runtime_replication_journal_shipping_recovers_on_replica() {
    let primary = tempdir().unwrap();
    let replica = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.replication.mode = RuntimeReplicationMode::Sync;
    policy
        .replication
        .replica_roots
        .push(replica.path().to_path_buf());

    let id = {
        let mut runtime = PersistEntityRuntime::open(primary.path(), policy)
            .await
            .unwrap();
        runtime.register_deterministic_command(
            "Counter",
            "increment",
            Arc::new(|state, payload| {
                let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
                let fields = state.fields_object_mut()?;
                let current = fields
                    .get("count")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                fields.insert("count".to_string(), json!(current + delta));
                Ok(())
            }),
        );

        let id = runtime
            .create_entity("Counter", "runtime_counter", json!({"count": 0}), 1)
            .await
            .unwrap();
        runtime
            .apply_deterministic_command("Counter", &id, "increment", json!({"delta": 5}))
            .await
            .unwrap();
        runtime.force_snapshot_and_compact().await.unwrap();
        id
    };

    let mut follower =
        PersistEntityRuntime::open(replica.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap();
    follower.register_deterministic_command(
        "Counter",
        "increment",
        Arc::new(|state, payload| {
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("count".to_string(), json!(current + delta));
            Ok(())
        }),
    );

    let recovered = follower.get_state("Counter", &id).unwrap();
    assert_eq!(count_from_state(&recovered), 5);
}

#[tokio::test]
async fn runtime_lifecycle_passivate_resurrect_and_gc() {
    let dir = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.lifecycle = RuntimeLifecyclePolicy {
        passivate_after_ms: 5,
        gc_after_ms: 10,
        max_hot_objects: 100,
        gc_only_if_never_touched: false,
    };

    let mut runtime = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();

    let id = runtime
        .create_entity("Ephemeral", "ephemeral_table", json!({"count": 1}), 1)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(8)).await;
    let report1 = runtime.run_lifecycle_maintenance().await.unwrap();
    assert!(report1.passivated >= 1);

    let stats_after_passivation = runtime.stats();
    assert_eq!(stats_after_passivation.hot_entities, 0);
    assert_eq!(stats_after_passivation.cold_entities, 1);

    let resurrected = runtime.get_state("Ephemeral", &id).unwrap();
    assert_eq!(count_from_state(&resurrected), 1);

    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let report2 = runtime.run_lifecycle_maintenance().await.unwrap();
    assert!(report2.passivated >= 1);

    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let report3 = runtime.run_lifecycle_maintenance().await.unwrap();
    assert!(report3.gc_deleted >= 1);

    let missing = runtime.get_state("Ephemeral", &id);
    assert!(missing.is_err());
}

#[tokio::test]
async fn runtime_keeps_nonserializable_runtime_closures() {
    let dir = tempdir().unwrap();
    let mut runtime = PersistEntityRuntime::open(dir.path(), RuntimeOperationalPolicy::default())
        .await
        .unwrap();

    let id = runtime
        .create_entity(
            "RuntimeBox",
            "runtime_box",
            json!({"count": 0, "name": "box-a"}),
            1,
        )
        .await
        .unwrap();

    runtime.register_runtime_closure(
        "RuntimeBox",
        "set_name",
        std::sync::Arc::new(|state, args| {
            let name = args
                .first()
                .and_then(|v| match v {
                    Value::Text(s) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "default".to_string());
            let fields = state.fields_object_mut()?;
            fields.insert("name".to_string(), json!(name));
            Ok(Value::Boolean(true))
        }),
    );

    let result = runtime
        .invoke_runtime_closure(
            "RuntimeBox",
            &id,
            "set_name",
            vec![Value::Text("box-z".to_string())],
        )
        .await
        .unwrap();

    assert_eq!(result, Value::Boolean(true));
    let state = runtime.get_state("RuntimeBox", &id).unwrap();
    let name = state
        .fields
        .as_object()
        .and_then(|o| o.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert_eq!(name, "box-z");
}
