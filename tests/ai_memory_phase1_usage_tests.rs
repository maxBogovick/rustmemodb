use rustmemodb::core::Result;
use rustmemodb::prelude::dx::{
    AgentSessionRuntime, AgentSessionRuntimeConfig, AgentTimelineQuery, AgentWorkflowExecutor,
    AgentWorkflowStep,
};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

fn test_runtime_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rustmemodb_ai_memory_usage_{}_{}",
        name,
        Uuid::new_v4()
    ))
}

#[tokio::test]
async fn phase1_usage_example_session_workflow_and_replay() -> Result<()> {
    let root = test_runtime_root("phase1_usage");
    let mut runtime = AgentSessionRuntime::open(AgentSessionRuntimeConfig::new(&root)).await?;

    runtime.register_session_command(
        "set_status",
        Arc::new(|state, payload, _ctx| {
            let status = payload
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let fields = state.fields_object_mut()?;
            fields.insert("status".to_string(), json!(status));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(Vec::new())
        }),
    );

    let session_id = runtime
        .create_session(None, json!({ "status": "new" }))
        .await?;
    let executor = AgentWorkflowExecutor::new(runtime.memory_mut(), session_id.as_str());
    let (correlation_id, results) = executor
        .run_with_generated_correlation(vec![
            AgentWorkflowStep::new("set_status", json!({ "status": "planning" })),
            AgentWorkflowStep::new("set_status", json!({ "status": "running" })),
        ])
        .await?;

    assert_eq!(results.len(), 2);
    let final_state = runtime.get_session_state(session_id.as_str())?;
    assert_eq!(
        final_state
            .fields_object()?
            .get("status")
            .and_then(|value| value.as_str()),
        Some("running")
    );

    let replay = runtime
        .replay_envelopes_for_session(
            session_id.as_str(),
            &AgentTimelineQuery::new()
                .with_command_name("set_status")
                .with_correlation_id(correlation_id),
        )
        .await?;
    assert_eq!(replay.len(), 2);
    assert!(
        replay
            .iter()
            .all(|envelope| envelope.correlation_id == Some(correlation_id))
    );

    let _ = tokio::fs::remove_dir_all(&root).await;
    Ok(())
}
