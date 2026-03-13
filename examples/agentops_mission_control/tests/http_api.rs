use agentops_mission_control::model::{AgentOpsWorkspace, IncidentSeverity};
use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use rustmemodb::PersistApp;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

#[tokio::test]
async fn generated_router_end_to_end_flow_with_views() {
    let app = TestApp::new().await;

    let (status, created_workspace) = request_json(
        &app.router,
        json_request(
            Method::POST,
            "/api/workspaces",
            json!({ "name": "AgentOps HQ" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let workspace_id = persist_id(&created_workspace);

    let (_, alpha_agent) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/register_agent"),
            json!({ "handle": "@alpha", "model": "gpt-5" }),
        ),
    )
    .await;
    let alpha_agent_id = string_field(&alpha_agent, "id");

    let (_, beta_agent) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/register_agent"),
            json!({ "handle": "@beta", "model": "gpt-5-mini" }),
        ),
    )
    .await;
    let beta_agent_id = string_field(&beta_agent, "id");

    let (_, mission) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/create_mission"),
            json!({
                "title": "Stabilize Incident Triage",
                "objective": "Classify and route all incidents under 2 minutes",
                "priority": 5
            }),
        ),
    )
    .await;
    let mission_id = string_field(&mission, "id");

    let (status, _) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/activate_mission"),
            json!({ "mission_id": mission_id, "owner_agent_id": alpha_agent_id }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, run) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/start_run"),
            json!({
                "mission_id": mission_id,
                "assigned_agent_id": alpha_agent_id
            }),
        ),
    )
    .await;
    let run_id = string_field(&run, "id");

    let (status, step) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/append_run_step"),
            json!({
                "run_id": run_id,
                "phase": "classify",
                "summary": "clustered root cause around queue depth",
                "latency_ms": 630,
                "token_cost": 220
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(string_field(&step, "phase"), "classify");

    let (status, _) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/handoff_run"),
            json!({
                "run_id": run_id,
                "to_agent_id": beta_agent_id,
                "note": "handoff to db specialist"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/accept_handoff"),
            json!({ "run_id": run_id }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, incident) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/raise_incident"),
            json!({
                "run_id": run_id,
                "severity": IncidentSeverity::High,
                "title": "Routing latency spike",
                "details": "Observed elevated queue fan-out latency"
            }),
        ),
    )
    .await;
    let incident_id = string_field(&incident, "id");

    let (status, _) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/resolve_incident"),
            json!({
                "incident_id": incident_id,
                "resolution_note": "load balancing policy adjusted"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/finish_run"),
            json!({ "run_id": run_id }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, mission_health) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/api/workspaces/{workspace_id}/mission_health?mission_id={mission_id}"
            ))
            .body(Body::empty())
            .expect("mission health request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        mission_health
            .get("successful_runs")
            .and_then(Value::as_u64),
        Some(1)
    );

    let (status, run_timeline) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/run_timeline?run_id={run_id}"))
            .body(Body::empty())
            .expect("run timeline request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        run_timeline
            .as_array()
            .expect("run timeline array")
            .len()
            >= 4
    );

    let (status, open_incidents) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/open_incidents"))
            .body(Body::empty())
            .expect("open incidents request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        open_incidents.as_array().expect("open incidents array").len(),
        0
    );

    let (status, ops_dashboard) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/views/ops_dashboard"))
            .body(Body::empty())
            .expect("ops dashboard request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ops_dashboard
            .get("workspace_name")
            .and_then(Value::as_str),
        Some("AgentOps HQ")
    );
    assert_eq!(
        ops_dashboard.get("agents_total").and_then(Value::as_u64),
        Some(2)
    );

    let (status, reliability) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/views/reliability"))
            .body(Body::empty())
            .expect("reliability request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        reliability.get("runs_total").and_then(Value::as_i64),
        Some(1)
    );
}

#[tokio::test]
async fn generated_router_replays_idempotent_append_step() {
    let app = TestApp::new().await;

    let (_, created_workspace) = request_json(
        &app.router,
        json_request(
            Method::POST,
            "/api/workspaces",
            json!({ "name": "Replay Workspace" }),
        ),
    )
    .await;
    let workspace_id = persist_id(&created_workspace);

    let (_, agent) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/register_agent"),
            json!({ "handle": "@runner", "model": "gpt-5" }),
        ),
    )
    .await;
    let agent_id = string_field(&agent, "id");

    let (_, mission) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/create_mission"),
            json!({
                "title": "Replay Guard",
                "objective": "ensure idempotent command replay",
                "priority": 4
            }),
        ),
    )
    .await;
    let mission_id = string_field(&mission, "id");

    let _ = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/activate_mission"),
            json!({ "mission_id": mission_id, "owner_agent_id": agent_id }),
        ),
    )
    .await;
    let (_, run) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/start_run"),
            json!({ "mission_id": mission_id, "assigned_agent_id": agent_id }),
        ),
    )
    .await;
    let run_id = string_field(&run, "id");

    let payload = json!({
        "run_id": run_id,
        "phase": "reason",
        "summary": "step should not duplicate on replay",
        "latency_ms": 500,
        "token_cost": 50
    });
    let path = format!("/api/workspaces/{workspace_id}/append_run_step");

    let (status, first) = request_json(
        &app.router,
        json_request_with_idempotency(Method::POST, &path, payload.clone(), "append-step-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, replayed) = request_json(
        &app.router,
        json_request_with_idempotency(Method::POST, &path, payload, "append-step-key"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first, replayed);

    let (status, timeline) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/run_timeline?run_id={run_id}"))
            .body(Body::empty())
            .expect("timeline request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(timeline.as_array().expect("timeline array").len(), 1);
}

#[tokio::test]
async fn generated_router_list_query_dsl_works() {
    let app = TestApp::new().await;

    for name in ["Gamma Ops", "Alpha Ops", "Beta Ops"] {
        let (status, _) = request_json(
            &app.router,
            json_request(Method::POST, "/api/workspaces", json!({ "name": name })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, sorted_page) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri("/api/workspaces?sort=name&page=1&per_page=2")
            .body(Body::empty())
            .expect("sorted query request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sorted = sorted_page.as_array().expect("sorted array");
    assert_eq!(sorted.len(), 2);
    assert_eq!(
        sorted[0]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Alpha Ops")
    );
    assert_eq!(
        sorted[1]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Beta Ops")
    );

    let (status, filtered) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri("/api/workspaces?name__contains=alpha")
            .body(Body::empty())
            .expect("filtered query request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let filtered = filtered.as_array().expect("filtered array");
    assert_eq!(filtered.len(), 1);
    assert_eq!(
        filtered[0]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Alpha Ops")
    );

    let (status, invalid_query) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri("/api/workspaces?score__between=10")
            .body(Body::empty())
            .expect("invalid query request"),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        invalid_query.get("code").and_then(Value::as_str),
        Some("input_error")
    );
}

#[tokio::test]
async fn generated_router_exposes_audits_openapi_and_restart_durability() {
    let temp_dir = TempDir::new().expect("temp dir");
    let data_dir = temp_dir.path().join("agentops-restart");

    let app1 = create_router(data_dir.clone()).await;
    let (_, created_workspace) = request_json(
        &app1,
        json_request(
            Method::POST,
            "/api/workspaces",
            json!({ "name": "Restart Ops" }),
        ),
    )
    .await;
    let workspace_id = persist_id(&created_workspace);

    let (_, agent) = request_json(
        &app1,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/register_agent"),
            json!({ "handle": "@persist", "model": "gpt-5" }),
        ),
    )
    .await;
    let agent_id = string_field(&agent, "id");

    let (_, mission) = request_json(
        &app1,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/create_mission"),
            json!({
                "title": "Durability Mission",
                "objective": "verify state survives restart",
                "priority": 3
            }),
        ),
    )
    .await;
    let mission_id = string_field(&mission, "id");

    let _ = request_json(
        &app1,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/activate_mission"),
            json!({ "mission_id": mission_id, "owner_agent_id": agent_id }),
        ),
    )
    .await;
    drop(app1);

    let app2 = create_router(data_dir).await;

    let (status, persisted_workspace) = request_json(
        &app2,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}"))
            .body(Body::empty())
            .expect("workspace request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        persisted_workspace
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Restart Ops")
    );

    let (status, audits) = request_json(
        &app2,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/_audits"))
            .body(Body::empty())
            .expect("audits request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!audits.as_array().expect("audits array").is_empty());

    let (status, openapi) = request_json(
        &app2,
        Request::builder()
            .method(Method::GET)
            .uri("/api/workspaces/_openapi.json")
            .body(Body::empty())
            .expect("openapi request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let paths = openapi
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths");
    assert!(paths.contains_key("/{id}/append_run_step"));
    assert!(paths.contains_key("/{id}/views/ops_dashboard"));
    assert!(paths.contains_key("/{id}/views/reliability"));

    let append_run_step_post = paths
        .get("/{id}/append_run_step")
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("post"))
        .and_then(Value::as_object)
        .expect("append_run_step post operation");
    let parameters = append_run_step_post
        .get("parameters")
        .and_then(Value::as_array)
        .expect("append_run_step parameters");
    let has_idempotency_header = parameters.iter().any(|param| {
        param.get("name").and_then(Value::as_str) == Some("Idempotency-Key")
            && param.get("in").and_then(Value::as_str) == Some("header")
    });
    assert!(
        has_idempotency_header,
        "generated command endpoint must document Idempotency-Key"
    );
}

struct TestApp {
    _tmp: TempDir,
    router: Router,
}

impl TestApp {
    async fn new() -> Self {
        let tmp = TempDir::new().expect("temp dir");
        let router = create_router(tmp.path().join("agentops_data")).await;
        Self { _tmp: tmp, router }
    }
}

async fn create_router(data_dir: std::path::PathBuf) -> Router {
    let app = PersistApp::open_auto(data_dir).await.expect("open persist app");
    let workspaces = app
        .serve_autonomous_model::<AgentOpsWorkspace>("workspaces")
        .await
        .expect("serve autonomous model");
    Router::new().nest("/api/workspaces", workspaces)
}

fn json_request(method: Method, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("json request")
}

fn json_request_with_idempotency(
    method: Method,
    uri: &str,
    body: Value,
    idempotency_key: &str,
) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("Idempotency-Key", idempotency_key)
        .body(Body::from(body.to_string()))
        .expect("json request with idempotency key")
}

async fn request_json(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("router response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();

    if bytes.is_empty() {
        return (status, Value::Null);
    }

    match serde_json::from_slice::<Value>(&bytes) {
        Ok(parsed) => (status, parsed),
        Err(_) => (
            status,
            Value::String(String::from_utf8_lossy(&bytes).to_string()),
        ),
    }
}

fn persist_id(record: &Value) -> String {
    string_field(record, "persist_id")
}

fn string_field(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("missing string field '{field}' in {value}"))
        .to_string()
}
