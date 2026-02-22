use rustmemodb::{
    PersistEntityRuntime, RuntimeCommandEnvelope, RuntimeCommandPayloadSchema,
    RuntimeConsistencyMode, RuntimeDeterminismPolicy, RuntimeDurabilityMode,
    RuntimeLifecyclePolicy, RuntimeOperationalPolicy, RuntimeOutboxStatus, RuntimePayloadType,
    RuntimeProjectionContract, RuntimeProjectionField, RuntimeReplicationMode,
    RuntimeSideEffectSpec, RuntimeSnapshotPolicy, RuntimeTombstonePolicy, Value,
    runtime_journal_compat_check, runtime_snapshot_compat_check, spawn_runtime_snapshot_worker,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Mutex;
use uuid::Uuid;

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
async fn runtime_snapshot_and_journal_compat_checks_flag_newer_schema_versions() {
    let dir = tempdir().unwrap();
    let mut runtime = PersistEntityRuntime::open(dir.path(), RuntimeOperationalPolicy::default())
        .await
        .unwrap();

    let _legacy = runtime
        .create_entity("Counter", "counter_state", json!({"count": 1}), 1)
        .await
        .unwrap();
    let newer = runtime
        .create_entity("Counter", "counter_state", json!({"count": 2}), 3)
        .await
        .unwrap();

    let paths = runtime.paths();
    let journal_report = runtime_journal_compat_check(&paths.journal_file, 2).unwrap();
    assert!(!journal_report.compatible);
    assert!(
        journal_report
            .issues
            .iter()
            .any(|issue| issue.persist_id == newer)
    );

    runtime.force_snapshot_and_compact().await.unwrap();

    let snapshot_report = runtime_snapshot_compat_check(&paths.snapshot_file, 2).unwrap();
    assert!(!snapshot_report.compatible);
    assert!(
        snapshot_report
            .issues
            .iter()
            .any(|issue| issue.persist_id == newer)
    );

    let snapshot_ok = runtime_snapshot_compat_check(&paths.snapshot_file, 3).unwrap();
    assert!(snapshot_ok.compatible);
    assert!(snapshot_ok.issues.is_empty());
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
async fn runtime_command_migration_rewrites_legacy_envelope_contract() {
    let dir = tempdir().unwrap();
    let mut runtime = PersistEntityRuntime::open(dir.path(), RuntimeOperationalPolicy::default())
        .await
        .unwrap();

    runtime.register_deterministic_command_with_schema(
        "Counter",
        "increment_v2",
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
    runtime
        .register_command_migration(
            "Counter",
            "increment",
            1,
            "increment_v2",
            2,
            Arc::new(|payload| {
                let amount = payload
                    .get("amount")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| {
                        rustmemodb::DbError::ExecutionError(
                            "legacy payload missing amount".to_string(),
                        )
                    })?;
                Ok(json!({ "delta": amount }))
            }),
        )
        .unwrap();

    let migrations = runtime.list_command_migrations("Counter");
    assert_eq!(migrations.len(), 1);
    assert_eq!(migrations[0].from_command, "increment");
    assert_eq!(migrations[0].to_command, "increment_v2");
    assert_eq!(migrations[0].from_payload_version, 1);
    assert_eq!(migrations[0].to_payload_version, 2);
    assert_eq!(runtime.stats().registered_command_migrations, 1);

    let id = runtime
        .create_entity("Counter", "counter_state", json!({"count": 0}), 1)
        .await
        .unwrap();

    let legacy_envelope =
        RuntimeCommandEnvelope::new("Counter", &id, "increment", json!({ "amount": 7 }))
            .with_expected_version(1)
            .with_idempotency_key("legacy-op-1");
    let applied = runtime
        .apply_command_envelope(legacy_envelope.clone())
        .await
        .unwrap();
    assert_eq!(count_from_state(&applied.state), 7);
    assert!(!applied.idempotent_replay);

    let replay = runtime
        .apply_command_envelope(legacy_envelope)
        .await
        .unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(count_from_state(&replay.state), 7);
}

#[tokio::test]
async fn runtime_legacy_envelope_without_migration_is_rejected() {
    let dir = tempdir().unwrap();
    let mut runtime = PersistEntityRuntime::open(dir.path(), RuntimeOperationalPolicy::default())
        .await
        .unwrap();

    runtime.register_deterministic_command_with_schema(
        "Counter",
        "increment_v2",
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
        .create_entity("Counter", "counter_state", json!({"count": 0}), 1)
        .await
        .unwrap();
    let err = runtime
        .apply_command_envelope(RuntimeCommandEnvelope::new(
            "Counter",
            &id,
            "increment",
            json!({"amount": 3}),
        ))
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Deterministic command 'increment' is not registered"),
        "unexpected error: {}",
        err
    );
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
async fn runtime_async_replication_eventually_recovers_on_replica() {
    let primary = tempdir().unwrap();
    let replica = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.replication.mode = RuntimeReplicationMode::AsyncBestEffort;
    policy
        .replication
        .replica_roots
        .push(replica.path().to_path_buf());

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
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(())
        }),
    );

    let id = runtime
        .create_entity("Counter", "runtime_counter", json!({"count": 0}), 1)
        .await
        .unwrap();
    runtime
        .apply_deterministic_command("Counter", &id, "increment", json!({"delta": 9}))
        .await
        .unwrap();
    assert_eq!(runtime.stats().replication_failures, 0);

    let mut recovered: Option<i64> = None;
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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
                state.metadata.version = state.metadata.version.saturating_add(1);
                Ok(())
            }),
        );

        if let Ok(state) = follower.get_state("Counter", &id) {
            let count = count_from_state(&state);
            if count == 9 {
                recovered = Some(count);
                break;
            }
        }
    }

    assert_eq!(recovered, Some(9));
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

#[tokio::test]
async fn runtime_envelope_path_supports_cas_idempotency_and_outbox_recovery() {
    let dir = tempdir().unwrap();
    let mut policy = RuntimeOperationalPolicy::default();
    policy.durability = RuntimeDurabilityMode::Strict;

    let persist_id = {
        let mut runtime = PersistEntityRuntime::open(dir.path(), policy.clone())
            .await
            .unwrap();

        runtime.register_deterministic_envelope_command_with_schema(
            "Counter",
            "increment",
            RuntimeCommandPayloadSchema::object()
                .require_field("delta", RuntimePayloadType::Integer)
                .allow_extra_fields(false),
            Arc::new(|state, envelope| {
                let delta = envelope
                    .payload_json
                    .get("delta")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| {
                        rustmemodb::DbError::ExecutionError("Missing delta".to_string())
                    })?;
                let fields = state.fields_object_mut()?;
                let current = fields
                    .get("count")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                fields.insert("count".to_string(), json!(current + delta));
                state.metadata.version = state.metadata.version.saturating_add(1);
                Ok(vec![RuntimeSideEffectSpec {
                    effect_type: "counter.incremented".to_string(),
                    payload_json: json!({
                        "delta": delta,
                        "entity_id": envelope.entity_id,
                    }),
                }])
            }),
        );

        let persist_id = runtime
            .create_entity("Counter", "runtime_counter", json!({"count": 0}), 1)
            .await
            .unwrap();
        let initial = runtime.get_state("Counter", &persist_id).unwrap();
        assert_eq!(initial.metadata.version, 1);

        let envelope =
            RuntimeCommandEnvelope::new("Counter", &persist_id, "increment", json!({"delta": 2}))
                .with_expected_version(1)
                .with_idempotency_key("counter-op-1");

        let applied = runtime
            .apply_command_envelope(envelope.clone())
            .await
            .unwrap();
        assert!(!applied.idempotent_replay);
        assert_eq!(count_from_state(&applied.state), 2);
        assert_eq!(applied.state.metadata.version, 2);
        assert_eq!(applied.outbox.len(), 1);
        assert_eq!(applied.outbox[0].status, RuntimeOutboxStatus::Pending);

        let replay = runtime
            .apply_command_envelope(envelope.clone())
            .await
            .unwrap();
        assert!(replay.idempotent_replay);
        assert_eq!(count_from_state(&replay.state), 2);

        let cas_conflict = runtime
            .apply_command_envelope(
                RuntimeCommandEnvelope::new(
                    "Counter",
                    &persist_id,
                    "increment",
                    json!({"delta": 1}),
                )
                .with_expected_version(1),
            )
            .await;
        assert!(cas_conflict.is_err());

        let pending_before_dispatch = runtime.list_pending_outbox_records();
        assert_eq!(pending_before_dispatch.len(), 1);

        runtime
            .mark_outbox_dispatched(&pending_before_dispatch[0].outbox_id)
            .await
            .unwrap();
        assert!(runtime.list_pending_outbox_records().is_empty());

        persist_id
    };

    let mut reopened = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();
    reopened.register_deterministic_envelope_command_with_schema(
        "Counter",
        "increment",
        RuntimeCommandPayloadSchema::object()
            .require_field("delta", RuntimePayloadType::Integer)
            .allow_extra_fields(false),
        Arc::new(|state, envelope| {
            let delta = envelope
                .payload_json
                .get("delta")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| rustmemodb::DbError::ExecutionError("Missing delta".to_string()))?;
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("count".to_string(), json!(current + delta));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(vec![RuntimeSideEffectSpec {
                effect_type: "counter.incremented".to_string(),
                payload_json: json!({
                    "delta": delta,
                    "entity_id": envelope.entity_id,
                }),
            }])
        }),
    );

    let replay_after_restart = reopened
        .apply_command_envelope(
            RuntimeCommandEnvelope::new("Counter", &persist_id, "increment", json!({"delta": 2}))
                .with_expected_version(1)
                .with_idempotency_key("counter-op-1"),
        )
        .await
        .unwrap();
    assert!(replay_after_restart.idempotent_replay);
    assert_eq!(count_from_state(&replay_after_restart.state), 2);
    assert!(reopened.list_pending_outbox_records().is_empty());

    let outbox = reopened.list_outbox_records();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].status, RuntimeOutboxStatus::Dispatched);
}

#[tokio::test]
async fn runtime_strict_context_policy_rejects_unsafe_handler_modes() {
    let dir = tempdir().unwrap();
    let mut policy = RuntimeOperationalPolicy::default();
    policy.determinism = RuntimeDeterminismPolicy::StrictContextOnly;

    let mut runtime = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();
    runtime.register_deterministic_command(
        "Counter",
        "increment",
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

    let err = runtime
        .apply_command_envelope(RuntimeCommandEnvelope::new(
            "Counter",
            &id,
            "increment",
            json!({"delta": 1}),
        ))
        .await
        .unwrap_err();
    let message = format!("{err}");
    assert!(message.contains("StrictContextOnly"));
}

#[tokio::test]
async fn runtime_context_handler_gets_deterministic_context_and_panic_rolls_back() {
    let dir = tempdir().unwrap();
    let mut policy = RuntimeOperationalPolicy::default();
    policy.determinism = RuntimeDeterminismPolicy::StrictContextOnly;

    let mut runtime = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();

    runtime.register_deterministic_context_command_with_schema(
        "Counter",
        "increment",
        RuntimeCommandPayloadSchema::object()
            .require_field("delta", RuntimePayloadType::Integer)
            .allow_extra_fields(false),
        Arc::new(|state, payload, ctx| {
            let delta = payload
                .get("delta")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("count".to_string(), json!(current + delta));
            Ok(vec![RuntimeSideEffectSpec {
                effect_type: "counter.incremented".to_string(),
                payload_json: json!({
                    "event_id": ctx.deterministic_uuid("counter.incremented"),
                    "delta": delta
                }),
            }])
        }),
    );

    runtime.register_deterministic_context_command(
        "Counter",
        "explode",
        Arc::new(|state, _payload, _ctx| {
            let fields = state.fields_object_mut()?;
            fields.insert("count".to_string(), json!(999));
            panic!("deterministic handler panic");
        }),
    );

    let id = runtime
        .create_entity("Counter", "runtime_counter", json!({"count": 0}), 1)
        .await
        .unwrap();

    let envelope = RuntimeCommandEnvelope::new("Counter", &id, "increment", json!({"delta": 3}))
        .with_expected_version(1);
    let expected_event_id = Uuid::new_v5(&envelope.envelope_id, b"counter.incremented");

    let applied = runtime.apply_command_envelope(envelope).await.unwrap();
    assert_eq!(count_from_state(&applied.state), 3);
    assert_eq!(
        applied.outbox[0]
            .payload_json
            .get("event_id")
            .and_then(|v| v.as_str())
            .unwrap(),
        expected_event_id.to_string()
    );

    let panic_result = runtime
        .apply_command_envelope(RuntimeCommandEnvelope::new(
            "Counter",
            &id,
            "explode",
            json!({}),
        ))
        .await;
    assert!(panic_result.is_err());

    let after_panic = runtime.get_state("Counter", &id).unwrap();
    assert_eq!(count_from_state(&after_panic), 3);
}

#[tokio::test]
async fn runtime_projection_sync_index_lookup_and_rebuild() {
    let dir = tempdir().unwrap();
    let policy = RuntimeOperationalPolicy::default();

    let user_id = {
        let mut runtime = PersistEntityRuntime::open(dir.path(), policy.clone())
            .await
            .unwrap();
        runtime.register_deterministic_command_with_schema(
            "User",
            "rename",
            RuntimeCommandPayloadSchema::object()
                .require_field("email", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            Arc::new(|state, payload| {
                let email = payload
                    .get("email")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        rustmemodb::DbError::ExecutionError("Missing email".to_string())
                    })?;
                let fields = state.fields_object_mut()?;
                fields.insert("email".to_string(), json!(email));
                Ok(())
            }),
        );

        runtime
            .register_projection_contract(
                RuntimeProjectionContract::new("User", "user_projection")
                    .with_field(
                        RuntimeProjectionField::new("email", "email", RuntimePayloadType::Text)
                            .indexed(true),
                    )
                    .with_field(RuntimeProjectionField::new(
                        "balance",
                        "balance",
                        RuntimePayloadType::Integer,
                    )),
            )
            .unwrap();

        let user_id = runtime
            .create_entity(
                "User",
                "user_state",
                json!({
                    "email": "alice@example.com",
                    "balance": 10
                }),
                1,
            )
            .await
            .unwrap();

        let created_rows = runtime.list_projection_rows("User").unwrap();
        assert_eq!(created_rows.len(), 1);
        assert_eq!(
            created_rows[0]
                .values
                .get("email")
                .and_then(|value| value.as_str()),
            Some("alice@example.com")
        );

        let ids_by_alice = runtime
            .find_projection_entity_ids_by_index("User", "email", &json!("alice@example.com"))
            .unwrap();
        assert_eq!(ids_by_alice, vec![user_id.clone()]);

        let renamed = RuntimeCommandEnvelope::new(
            "User",
            &user_id,
            "rename",
            json!({"email": "alice+v2@example.com"}),
        )
        .with_expected_version(1);
        runtime.apply_command_envelope(renamed).await.unwrap();

        let ids_by_old = runtime
            .find_projection_entity_ids_by_index("User", "email", &json!("alice@example.com"))
            .unwrap();
        assert!(ids_by_old.is_empty());

        let ids_by_new = runtime
            .find_projection_entity_ids_by_index("User", "email", &json!("alice+v2@example.com"))
            .unwrap();
        assert_eq!(ids_by_new, vec![user_id.clone()]);

        runtime.rebuild_registered_projections().unwrap();
        let rows_after_rebuild = runtime
            .find_projection_rows_by_index("User", "email", &json!("alice+v2@example.com"))
            .unwrap();
        assert_eq!(rows_after_rebuild.len(), 1);
        assert_eq!(rows_after_rebuild[0].entity_id, user_id);

        user_id
    };

    let mut reopened = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();
    reopened.register_deterministic_command_with_schema(
        "User",
        "rename",
        RuntimeCommandPayloadSchema::object()
            .require_field("email", RuntimePayloadType::Text)
            .allow_extra_fields(false),
        Arc::new(|state, payload| {
            let email = payload
                .get("email")
                .and_then(|value| value.as_str())
                .ok_or_else(|| rustmemodb::DbError::ExecutionError("Missing email".to_string()))?;
            let fields = state.fields_object_mut()?;
            fields.insert("email".to_string(), json!(email));
            Ok(())
        }),
    );
    reopened
        .register_projection_contract(
            RuntimeProjectionContract::new("User", "user_projection")
                .with_field(
                    RuntimeProjectionField::new("email", "email", RuntimePayloadType::Text)
                        .indexed(true),
                )
                .with_field(RuntimeProjectionField::new(
                    "balance",
                    "balance",
                    RuntimePayloadType::Integer,
                )),
        )
        .unwrap();

    let restored_rows = reopened
        .find_projection_rows_by_index("User", "email", &json!("alice+v2@example.com"))
        .unwrap();
    assert_eq!(restored_rows.len(), 1);
    assert_eq!(restored_rows[0].entity_id, user_id);
}

#[tokio::test]
async fn runtime_consistency_mode_normalizes_operational_policy() {
    let dir = tempdir().unwrap();

    let mut strong_policy = RuntimeOperationalPolicy::default();
    strong_policy.consistency = RuntimeConsistencyMode::Strong;
    strong_policy.durability = RuntimeDurabilityMode::Eventual {
        sync_interval_ms: 9_999,
    };
    strong_policy.replication.mode = RuntimeReplicationMode::AsyncBestEffort;
    let strong_runtime = PersistEntityRuntime::open(dir.path().join("strong"), strong_policy)
        .await
        .unwrap();
    assert!(matches!(
        strong_runtime.policy().durability,
        RuntimeDurabilityMode::Strict
    ));
    assert_eq!(
        strong_runtime.policy().replication.mode,
        RuntimeReplicationMode::Sync
    );

    let mut eventual_policy = RuntimeOperationalPolicy::default();
    eventual_policy.consistency = RuntimeConsistencyMode::Eventual;
    let eventual_runtime = PersistEntityRuntime::open(dir.path().join("eventual"), eventual_policy)
        .await
        .unwrap();
    match eventual_runtime.policy().durability {
        RuntimeDurabilityMode::Eventual { sync_interval_ms } => {
            assert_eq!(sync_interval_ms, 250);
        }
        RuntimeDurabilityMode::Strict => {
            panic!("expected eventual durability mode");
        }
    }
    assert_eq!(
        eventual_runtime.policy().replication.mode,
        RuntimeReplicationMode::AsyncBestEffort
    );
}

#[tokio::test]
async fn runtime_stats_expose_slo_metrics_and_lifecycle_churn() {
    let dir = tempdir().unwrap();
    let mut policy = RuntimeOperationalPolicy::default();
    policy.lifecycle = RuntimeLifecyclePolicy {
        passivate_after_ms: 0,
        gc_after_ms: 30_000,
        max_hot_objects: 100,
        gc_only_if_never_touched: false,
    };

    let mut runtime = PersistEntityRuntime::open(dir.path(), policy)
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
    runtime
        .register_projection_contract(
            RuntimeProjectionContract::new("Counter", "counter_projection").with_field(
                RuntimeProjectionField::new("count", "count", RuntimePayloadType::Integer),
            ),
        )
        .unwrap();

    let id = runtime
        .create_entity("Counter", "counter_state", json!({"count": 0}), 1)
        .await
        .unwrap();
    let applied = runtime
        .apply_command_envelope(
            RuntimeCommandEnvelope::new("Counter", &id, "increment", json!({"delta": 3}))
                .with_expected_version(1),
        )
        .await
        .unwrap();
    assert_eq!(count_from_state(&applied.state), 3);

    let stats = runtime.stats();
    let slo = runtime.slo_metrics();
    assert_eq!(stats.projection_lag_entities, 0);
    assert_eq!(stats.mailbox_entities, 1);
    assert_eq!(stats.mailbox_busy_entities, 0);
    assert!(stats.durability_lag_ms <= 5_000);
    assert_eq!(slo.projection_lag_entities, stats.projection_lag_entities);
    assert_eq!(slo.lifecycle_churn_total, stats.lifecycle_churn_total);
    assert_eq!(slo.mailbox_busy_entities, stats.mailbox_busy_entities);

    let report = runtime.run_lifecycle_maintenance().await.unwrap();
    assert_eq!(report.passivated, 1);
    let stats_after_lifecycle = runtime.stats();
    assert_eq!(stats_after_lifecycle.lifecycle_passivated_total, 1);
    assert_eq!(stats_after_lifecycle.lifecycle_gc_deleted_total, 0);
    assert_eq!(
        stats_after_lifecycle.lifecycle_churn_total,
        stats_after_lifecycle.lifecycle_passivated_total
            + stats_after_lifecycle.lifecycle_resurrected_total
            + stats_after_lifecycle.lifecycle_gc_deleted_total
    );
}

#[tokio::test]
async fn runtime_chaos_crash_recovery_with_lifecycle_preserves_state() {
    let dir = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.consistency = RuntimeConsistencyMode::Strong;
    policy.snapshot = RuntimeSnapshotPolicy {
        snapshot_every_ops: 4,
        compact_if_journal_exceeds_bytes: 1024 * 1024,
        background_worker_interval_ms: None,
    };
    policy.lifecycle = RuntimeLifecyclePolicy {
        passivate_after_ms: 0,
        gc_after_ms: 60_000,
        max_hot_objects: 100,
        gc_only_if_never_touched: false,
    };

    let mut entity_id: Option<String> = None;
    let mut expected = 0i64;

    for step in 0..24 {
        let mut runtime = PersistEntityRuntime::open(dir.path(), policy.clone())
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

        let current_id = if let Some(existing) = entity_id.clone() {
            existing
        } else {
            let id = runtime
                .create_entity("Counter", "counter_state", json!({"count": 0}), 1)
                .await
                .unwrap();
            entity_id = Some(id.clone());
            id
        };

        let delta = if step % 3 == 0 { 2 } else { 1 };
        let version = runtime
            .get_state("Counter", &current_id)
            .unwrap()
            .metadata
            .version as u64;
        runtime
            .apply_command_envelope(
                RuntimeCommandEnvelope::new(
                    "Counter",
                    &current_id,
                    "increment",
                    json!({ "delta": delta }),
                )
                .with_expected_version(version)
                .with_idempotency_key(format!("chaos-step-{step}")),
            )
            .await
            .unwrap();
        expected += delta;

        if step % 2 == 0 {
            let _ = runtime.run_lifecycle_maintenance().await.unwrap();
        }
        if step % 5 == 0 {
            runtime.force_snapshot_and_compact().await.unwrap();
        }
    }

    let mut recovered = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();
    recovered.register_deterministic_command_with_schema(
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

    let final_id = entity_id.expect("entity id must be created");
    let recovered_state = recovered.get_state("Counter", &final_id).unwrap();
    assert_eq!(count_from_state(&recovered_state), expected);

    let report = recovered.run_lifecycle_maintenance().await.unwrap();
    assert!(report.passivated >= 1);
    let stats = recovered.stats();
    assert_eq!(stats.lifecycle_gc_deleted_total, 0);
}

#[tokio::test]
async fn runtime_tombstones_survive_compaction_until_ttl_expires() {
    let dir = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.tombstone = RuntimeTombstonePolicy {
        ttl_ms: 1_000,
        retain_for_lifecycle_gc: true,
    };

    let mut runtime = PersistEntityRuntime::open(dir.path(), policy.clone())
        .await
        .unwrap();

    let id = runtime
        .create_entity(
            "Todo",
            "todo_state",
            json!({"title": "demo", "done": false}),
            1,
        )
        .await
        .unwrap();
    runtime
        .delete_entity("Todo", &id, "user_delete")
        .await
        .unwrap();

    assert_eq!(runtime.list_tombstones().len(), 1);
    runtime.force_snapshot_and_compact().await.unwrap();

    let journal = std::fs::read_to_string(runtime.paths().journal_file).unwrap_or_default();
    assert_eq!(
        journal
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count(),
        0
    );

    let reopened = PersistEntityRuntime::open(dir.path(), policy.clone())
        .await
        .unwrap();
    assert_eq!(reopened.list_tombstones().len(), 1);
    assert_eq!(reopened.stats().tombstones, 1);
    drop(reopened);

    tokio::time::sleep(std::time::Duration::from_millis(1_600)).await;

    let mut expired = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();
    let _ = expired.run_lifecycle_maintenance().await.unwrap();
    assert!(expired.list_tombstones().is_empty());
    expired.force_snapshot_and_compact().await.unwrap();

    let restored = PersistEntityRuntime::open(dir.path(), RuntimeOperationalPolicy::default())
        .await
        .unwrap();
    assert!(restored.list_tombstones().is_empty());
}

#[tokio::test]
async fn runtime_lifecycle_gc_can_skip_tombstones_by_policy() {
    let dir = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.lifecycle = RuntimeLifecyclePolicy {
        passivate_after_ms: 0,
        gc_after_ms: 10,
        max_hot_objects: 100,
        gc_only_if_never_touched: false,
    };
    policy.tombstone = RuntimeTombstonePolicy {
        ttl_ms: 5_000,
        retain_for_lifecycle_gc: false,
    };

    let mut runtime = PersistEntityRuntime::open(dir.path(), policy)
        .await
        .unwrap();
    let id = runtime
        .create_entity("Ephemeral", "ephemeral_state", json!({"count": 1}), 1)
        .await
        .unwrap();
    let _ = runtime.get_state("Ephemeral", &id).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let report1 = runtime.run_lifecycle_maintenance().await.unwrap();
    assert!(report1.passivated >= 1);

    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let report2 = runtime.run_lifecycle_maintenance().await.unwrap();
    assert!(report2.gc_deleted >= 1);
    assert!(runtime.list_tombstones().is_empty());
    assert_eq!(runtime.stats().tombstones, 0);
}
