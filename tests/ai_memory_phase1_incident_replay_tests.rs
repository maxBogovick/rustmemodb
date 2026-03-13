use rustmemodb::core::{DbError, Result};
use rustmemodb::prelude::dx::{
    AgentReplayRunOptions, AgentSessionRuntime, AgentSessionRuntimeConfig, AgentWorkflowExecutor,
    AgentWorkflowStep,
};
use rustmemodb::{DeterministicContextCommandHandler, RuntimeSideEffectSpec};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

fn test_runtime_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rustmemodb_ai_memory_incident_replay_{}_{}",
        name,
        Uuid::new_v4()
    ))
}

#[tokio::test]
async fn incident_forensics_and_replay_e2e_flow() -> Result<()> {
    // GOAL: demonstrate a realistic workflow + incident + replay path end-to-end.
    // DEBUG EXPECT: one correlation id groups all workflow commands.
    let root = test_runtime_root("e2e_flow");
    let mut runtime = AgentSessionRuntime::open(AgentSessionRuntimeConfig::new(&root)).await?;

    // GOAL: register "set_stage" command for state-machine transitions.
    // DEBUG EXPECT: each stage transition appends one stage.changed side-effect.
    let set_stage: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
        let stage = payload
            .get("stage")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        let fields = state.fields_object_mut()?;
        fields.insert("stage".to_string(), json!(stage));
        state.metadata.version = state.metadata.version.saturating_add(1);
        Ok(vec![RuntimeSideEffectSpec {
            effect_type: "stage.changed".to_string(),
            payload_json: json!({ "stage": stage }),
        }])
    });
    runtime.register_session_command("set_stage", set_stage);

    // GOAL: register "append_note" command for incident timeline enrichment.
    // DEBUG EXPECT: notes array grows deterministically and emits note.added side-effect.
    let append_note: DeterministicContextCommandHandler = Arc::new(|state, payload, _ctx| {
        let note = payload
            .get("note")
            .and_then(|value| value.as_str())
            .unwrap_or("empty");
        let fields = state.fields_object_mut()?;
        let notes_value = fields
            .entry("notes".to_string())
            .or_insert_with(|| json!([]));
        let notes = notes_value
            .as_array_mut()
            .ok_or_else(|| DbError::ExecutionError("Field `notes` must be an array".to_string()))?;
        notes.push(json!(note));
        state.metadata.version = state.metadata.version.saturating_add(1);
        Ok(vec![RuntimeSideEffectSpec {
            effect_type: "note.added".to_string(),
            payload_json: json!({ "note": note }),
        }])
    });
    runtime.register_session_command("append_note", append_note);

    // GOAL: create source session with known baseline for deterministic replay.
    // DEBUG EXPECT: source baseline = { stage: "new", notes: [] }.
    let source_session_id = runtime
        .create_session(None, json!({ "stage": "new", "notes": [] }))
        .await?;

    // GOAL: run a realistic 3-step workflow under one generated correlation id.
    // DEBUG EXPECT: stage should end at "running"; notes should include one ticket note.
    let executor = AgentWorkflowExecutor::new(runtime.memory_mut(), source_session_id.as_str());
    let (workflow_correlation_id, workflow_results) = executor
        .run_with_generated_correlation(vec![
            AgentWorkflowStep::new("set_stage", json!({ "stage": "planning" })),
            AgentWorkflowStep::new("append_note", json!({ "note": "incident-ticket-created" })),
            AgentWorkflowStep::new("set_stage", json!({ "stage": "running" })),
        ])
        .await?;
    assert_eq!(workflow_results.len(), 3);

    // GOAL: build incident report for that workflow correlation.
    // DEBUG EXPECT: report contains exactly 3 replayable commands from this workflow.
    let incident_report = runtime
        .incident_forensics_by_correlation(source_session_id.as_str(), workflow_correlation_id)
        .await?;
    assert_eq!(incident_report.timeline_records, 3);
    assert_eq!(incident_report.replayable_commands, 3);
    assert_eq!(incident_report.latest_command.as_deref(), Some("set_stage"));

    // GOAL: replay that incident slice into mirror session for deterministic verification.
    // DEBUG EXPECT: mirror state should exactly match source state after replay.
    let replay_report = runtime
        .replay_session_with_query(
            source_session_id.as_str(),
            &rustmemodb::AgentTimelineQuery::new().with_correlation_id(workflow_correlation_id),
            AgentReplayRunOptions::new()
                .with_target_session_id("mirror-session")
                .with_target_initial_fields(json!({ "stage": "new", "notes": [] })),
        )
        .await?;
    assert_eq!(replay_report.attempted_steps, 3);

    // GOAL: compare source and mirror state to confirm no drift.
    // DEBUG EXPECT: fields and version are equal across source vs mirror sessions.
    let source_state = runtime.get_session_state(source_session_id.as_str())?;
    let mirror_state = runtime.get_session_state("mirror-session")?;
    assert_eq!(mirror_state.fields, source_state.fields);
    assert_eq!(mirror_state.metadata.version, source_state.metadata.version);

    // GOAL: final sanity check for domain-visible output.
    // DEBUG EXPECT: stage = running and notes has one incident-ticket entry.
    let mirror_fields = mirror_state.fields_object()?;
    assert_eq!(
        mirror_fields.get("stage").and_then(|value| value.as_str()),
        Some("running")
    );
    assert_eq!(
        mirror_fields
            .get("notes")
            .and_then(|value| value.as_array())
            .map(|values| values.len()),
        Some(1)
    );

    let _ = tokio::fs::remove_dir_all(&root).await;
    Ok(())
}
