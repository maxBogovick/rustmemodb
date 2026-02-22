use anyhow::{Context, Result};
use rustmemodb::{
    PersistEntityRuntime, RuntimeDurabilityMode, RuntimeLifecyclePolicy, RuntimeOperationalPolicy,
    Value, runtime_snapshot_compat_check,
};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let base_dir = PathBuf::from("examples/advanced/persist_runtime_showcase/.runtime_data");
    reset_demo_dir(&base_dir)?;

    println!("=== Persist Runtime Showcase ===");
    println!(
        "Goal: entities persist themselves through deterministic commands + durable runtime journal."
    );

    strict_runtime_story(&base_dir).await?;
    eventual_runtime_story(&base_dir).await?;

    println!("\nShowcase completed.");
    Ok(())
}

async fn strict_runtime_story(base_dir: &Path) -> Result<()> {
    println!("\n--- 1) Strict mode, crash recovery, lifecycle ---");
    let strict_dir = base_dir.join("strict");

    let mut runtime = open_incident_runtime(
        &strict_dir,
        RuntimeDurabilityMode::Strict,
        RuntimeLifecyclePolicy {
            passivate_after_ms: 30,
            gc_after_ms: 120,
            max_hot_objects: 1_000,
            gc_only_if_never_touched: false,
        },
    )
    .await?;

    let api_id = runtime
        .create_entity(
            "Incident",
            "incident_table",
            json!({
                "title": "API latency spike",
                "severity": 2,
                "status": "open",
                "notes": []
            }),
            1,
        )
        .await?;

    let db_id = runtime
        .create_entity(
            "Incident",
            "incident_table",
            json!({
                "title": "DB connection saturation",
                "severity": 3,
                "status": "open",
                "notes": []
            }),
            1,
        )
        .await?;

    runtime
        .apply_deterministic_command("Incident", &api_id, "bump_severity", json!({"delta": 2}))
        .await?;
    runtime
        .apply_deterministic_command(
            "Incident",
            &api_id,
            "append_note",
            json!({"note": "autoscaler triggered"}),
        )
        .await?;
    runtime
        .apply_deterministic_command(
            "Incident",
            &db_id,
            "set_status",
            json!({"status": "investigating"}),
        )
        .await?;

    let owner = runtime
        .invoke_runtime_closure(
            "Incident",
            &api_id,
            "suggest_owner",
            vec![Value::Integer(4)],
        )
        .await?;

    let api_state = runtime.get_state("Incident", &api_id)?;
    println!("API incident after commands: {}", incident_summary(&api_state));
    println!("Owner suggestion closure result: {:?}", owner);

    runtime.force_snapshot_and_compact().await?;
    let paths = runtime.paths();
    let compat = runtime_snapshot_compat_check(&paths.snapshot_file, 1)?;
    println!(
        "Snapshot compatibility (schema=1): {}",
        if compat.compatible { "OK" } else { "FAILED" }
    );
    println!(
        "Journal bytes after compaction: {}",
        file_size_or_zero(&paths.journal_file)
    );

    drop(runtime);

    let mut recovered = open_incident_runtime(
        &strict_dir,
        RuntimeDurabilityMode::Strict,
        RuntimeLifecyclePolicy {
            passivate_after_ms: 30,
            gc_after_ms: 120,
            max_hot_objects: 1_000,
            gc_only_if_never_touched: false,
        },
    )
    .await?;

    let recovered_api = recovered.get_state("Incident", &api_id)?;
    let recovered_db = recovered.get_state("Incident", &db_id)?;
    println!("Recovered API incident: {}", incident_summary(&recovered_api));
    println!("Recovered DB incident: {}", incident_summary(&recovered_db));

    sleep(Duration::from_millis(45)).await;
    let lifecycle_1 = recovered.run_lifecycle_maintenance().await?;
    println!(
        "Lifecycle cycle #1 -> passivated={}, resurrected={}, gc_deleted={}",
        lifecycle_1.passivated, lifecycle_1.resurrected, lifecycle_1.gc_deleted
    );

    let resurrected = recovered.get_state("Incident", &api_id)?;
    println!(
        "Resurrected incident after passivation: {}",
        incident_summary(&resurrected)
    );

    sleep(Duration::from_millis(130)).await;
    let lifecycle_2 = recovered.run_lifecycle_maintenance().await?;
    sleep(Duration::from_millis(130)).await;
    let lifecycle_3 = recovered.run_lifecycle_maintenance().await?;
    println!(
        "Lifecycle cycle #2 -> passivated={}, resurrected={}, gc_deleted={}",
        lifecycle_2.passivated, lifecycle_2.resurrected, lifecycle_2.gc_deleted
    );
    println!(
        "Lifecycle cycle #3 -> passivated={}, resurrected={}, gc_deleted={}",
        lifecycle_3.passivated, lifecycle_3.resurrected, lifecycle_3.gc_deleted
    );

    let stats = recovered.stats();
    println!(
        "Final strict runtime stats: hot={}, cold={}, next_seq={}",
        stats.hot_entities, stats.cold_entities, stats.next_seq
    );

    Ok(())
}

async fn eventual_runtime_story(base_dir: &Path) -> Result<()> {
    println!("\n--- 2) Eventual mode for throughput-oriented workloads ---");
    let eventual_dir = base_dir.join("eventual");

    let mut runtime = open_incident_runtime(
        &eventual_dir,
        RuntimeDurabilityMode::Eventual {
            sync_interval_ms: 250,
        },
        RuntimeLifecyclePolicy {
            passivate_after_ms: 1_000,
            gc_after_ms: 30_000,
            max_hot_objects: 10_000,
            gc_only_if_never_touched: false,
        },
    )
    .await?;

    let worker_id = runtime
        .create_entity(
            "Incident",
            "incident_table",
            json!({
                "title": "Worker queue lag",
                "severity": 1,
                "status": "open",
                "notes": []
            }),
            1,
        )
        .await?;

    for _ in 0..25 {
        runtime
            .apply_deterministic_command(
                "Incident",
                &worker_id,
                "bump_severity",
                json!({"delta": 1}),
            )
            .await?;
    }
    runtime
        .apply_deterministic_command(
            "Incident",
            &worker_id,
            "set_status",
            json!({"status": "mitigated"}),
        )
        .await?;

    let state = runtime.get_state("Incident", &worker_id)?;
    println!("Eventual-mode state: {}", incident_summary(&state));

    runtime.force_snapshot_and_compact().await?;
    let paths = runtime.paths();
    println!(
        "Eventual mode journal bytes after compaction: {}",
        file_size_or_zero(&paths.journal_file)
    );

    Ok(())
}

async fn open_incident_runtime(
    root_dir: &Path,
    durability: RuntimeDurabilityMode,
    lifecycle: RuntimeLifecyclePolicy,
) -> Result<PersistEntityRuntime> {
    let mut policy = RuntimeOperationalPolicy::default();
    policy.durability = durability;
    policy.lifecycle = lifecycle;
    policy.snapshot.snapshot_every_ops = 6;
    policy.snapshot.compact_if_journal_exceeds_bytes = 8 * 1024;
    policy.retry.max_attempts = 3;
    policy.retry.initial_backoff_ms = 5;
    policy.retry.max_backoff_ms = 100;
    policy.backpressure.max_inflight = 32;
    policy.backpressure.acquire_timeout_ms = 500;

    let mut runtime = PersistEntityRuntime::open(root_dir.to_path_buf(), policy).await?;
    register_incident_commands(&mut runtime);
    Ok(runtime)
}

fn register_incident_commands(runtime: &mut PersistEntityRuntime) {
    runtime.register_deterministic_command(
        "Incident",
        "bump_severity",
        Arc::new(|state, payload| {
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(1);
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("severity")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("severity".to_string(), json!(current + delta));
            Ok(())
        }),
    );

    runtime.register_deterministic_command(
        "Incident",
        "set_status",
        Arc::new(|state, payload| {
            let status = payload
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("open");
            let fields = state.fields_object_mut()?;
            fields.insert("status".to_string(), json!(status));
            Ok(())
        }),
    );

    runtime.register_deterministic_command(
        "Incident",
        "append_note",
        Arc::new(|state, payload| {
            let note = payload
                .get("note")
                .and_then(|v| v.as_str())
                .unwrap_or("note");

            let fields = state.fields_object_mut()?;
            let notes = fields
                .entry("notes".to_string())
                .or_insert_with(|| serde_json::Value::Array(Vec::new()));

            if let Some(items) = notes.as_array_mut() {
                items.push(json!(note));
            } else {
                *notes = json!([note]);
            }
            Ok(())
        }),
    );

    let oncall = vec![
        "alice".to_string(),
        "bob".to_string(),
        "carol".to_string(),
    ];

    runtime.register_runtime_closure(
        "Incident",
        "suggest_owner",
        Arc::new(move |state, args| {
            let threshold = args
                .first()
                .and_then(|value| match value {
                    Value::Integer(v) => Some(*v),
                    _ => None,
                })
                .unwrap_or(4);

            let severity = state
                .fields
                .as_object()
                .and_then(|obj| obj.get("severity"))
                .and_then(|v| v.as_i64())
                .unwrap_or_default();

            let owner = if severity >= threshold {
                oncall[0].clone()
            } else {
                let index = (severity.max(0) as usize) % oncall.len();
                oncall[index].clone()
            };

            let fields = state.fields_object_mut()?;
            fields.insert("suggested_owner".to_string(), json!(owner));
            Ok(Value::Text(owner))
        }),
    );
}

fn incident_summary(state: &rustmemodb::PersistState) -> String {
    let fields = state.fields.as_object();

    let title = fields
        .and_then(|obj| obj.get("title"))
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    let severity = fields
        .and_then(|obj| obj.get("severity"))
        .and_then(|v| v.as_i64())
        .unwrap_or_default();
    let status = fields
        .and_then(|obj| obj.get("status"))
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");

    format!(
        "id={} title='{}' severity={} status={} schema_version={}",
        state.persist_id, title, severity, status, state.metadata.schema_version
    )
}

fn reset_demo_dir(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove '{}'", path.display()))?;
    }
    std::fs::create_dir_all(path)
        .with_context(|| format!("failed to create '{}'", path.display()))?;
    Ok(())
}

fn file_size_or_zero(path: &Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}
