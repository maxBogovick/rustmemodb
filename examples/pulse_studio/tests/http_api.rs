use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use pulse_studio::model::PulseWorkspace;
use rustmemodb::PersistApp;
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

#[tokio::test]
async fn generated_router_demonstrates_modern_dx_end_to_end() {
    let test_app = TestApp::new().await;

    let (status, created_workspace) = request_json(
        &test_app.router,
        json_request(Method::POST, "/api/workspaces", json!({ "name": "Pulse Team" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let workspace_id = persist_id(&created_workspace);

    let (status, body) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/rename_workspace"),
            json!({ "name": "   " }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body.get("code").and_then(Value::as_str),
        Some("validation_error")
    );

    let long_name = "x".repeat(80);
    let (status, body) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/rename_workspace"),
            json!({ "name": long_name }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body.get("code").and_then(Value::as_str),
        Some("validation_error")
    );

    let (status, renamed) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/rename_workspace"),
            json!({ "name": "  Growth Team  " }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(renamed.as_str(), Some("Growth Team"));

    let add_channel_path = format!("/api/workspaces/{workspace_id}/add_channel");
    let add_channel_payload = json!({
        "platform": "YouTube",
        "handle": "PulseTeam",
        "active": true
    });

    let (status, first_channel) = request_json(
        &test_app.router,
        json_request_with_idempotency(
            Method::POST,
            &add_channel_path,
            add_channel_payload.clone(),
            "pulse-channel-key",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, replayed_channel) = request_json(
        &test_app.router,
        json_request_with_idempotency(
            Method::POST,
            &add_channel_path,
            add_channel_payload,
            "pulse-channel-key",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_channel, replayed_channel);
    let channel_id = first_channel
        .get("id")
        .and_then(Value::as_str)
        .expect("channel id")
        .to_string();

    let (status, campaign) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/launch_campaign"),
            json!({
                "channel_id": channel_id,
                "title": "Spring Drop",
                "budget_minor": 120000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let campaign_id = campaign
        .get("id")
        .and_then(Value::as_str)
        .expect("campaign id")
        .to_string();

    let (status, progress) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/record_spend"),
            json!({
                "campaign_id": campaign_id,
                "amount_minor": 15000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        progress.get("spent_minor").and_then(Value::as_i64),
        Some(15000)
    );

    let (status, _invalid_engagement_total) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/record_engagement"),
            json!({
                "campaign_id": campaign_id,
                "event_type": "click!",
                "points": 42
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, engagement_total) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/record_engagement"),
            json!({
                "campaign_id": campaign_id,
                "event_type": "click",
                "points": 42
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(engagement_total.as_i64(), Some(42));

    let (status, dashboard) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/workspace_dashboard"))
            .body(Body::empty())
            .expect("dashboard request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        dashboard.get("workspace_name").and_then(Value::as_str),
        Some("Growth Team")
    );
    assert_eq!(
        dashboard.get("channels_total").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        dashboard.get("campaigns_total").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        dashboard
            .get("engagement_points_total")
            .and_then(Value::as_i64),
        Some(42)
    );

    let (status, dashboard_view) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/views/dashboard"))
            .body(Body::empty())
            .expect("dashboard view request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        dashboard_view.get("workspace_name").and_then(Value::as_str),
        Some("Growth Team")
    );

    let (status, insights_view) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/views/insights"))
            .body(Body::empty())
            .expect("insights view request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        insights_view
            .get("campaigns_total")
            .and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        insights_view
            .get("spent_total_minor")
            .and_then(Value::as_i64),
        Some(15000)
    );
    let campaigns_by_status = insights_view
        .get("campaigns_by_status")
        .and_then(Value::as_object)
        .expect("campaigns_by_status");
    assert_eq!(
        campaigns_by_status.get("Running").and_then(Value::as_i64),
        Some(1)
    );

    let (status, progress_query) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/api/workspaces/{workspace_id}/campaign_progress?campaign_id={campaign_id}"
            ))
            .body(Body::empty())
            .expect("campaign progress request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        progress_query.get("campaign_id").and_then(Value::as_str),
        Some(campaign_id.as_str())
    );

    let (status, channel_overview) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!(
                "/api/workspaces/{workspace_id}/channel_overview?channel_id={channel_id}"
            ))
            .body(Body::empty())
            .expect("channel_overview request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        channel_overview
            .get("spent_total_minor")
            .and_then(Value::as_i64),
        Some(15000)
    );

    let (status, openapi) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri("/api/workspaces/_openapi.json")
            .body(Body::empty())
            .expect("openapi request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(openapi.get("openapi").and_then(Value::as_str), Some("3.1.0"));
    let paths = openapi
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths");
    assert!(paths.contains_key("/{id}/add_channel"));
    assert!(paths.contains_key("/{id}/workspace_dashboard"));
    assert!(paths.contains_key("/{id}/views/dashboard"));
    assert!(paths.contains_key("/{id}/views/insights"));

    let add_channel_post = paths
        .get("/{id}/add_channel")
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("post"))
        .and_then(Value::as_object)
        .expect("add_channel post operation");
    let parameters = add_channel_post
        .get("parameters")
        .and_then(Value::as_array)
        .expect("add_channel parameters");
    let has_idempotency_header = parameters.iter().any(|param| {
        param.get("name").and_then(Value::as_str) == Some("Idempotency-Key")
            && param.get("in").and_then(Value::as_str) == Some("header")
    });
    assert!(
        has_idempotency_header,
        "generated command endpoint must document Idempotency-Key"
    );
}

#[tokio::test]
async fn generated_router_maps_budget_conflict_to_409_without_partial_write() {
    let test_app = TestApp::new().await;

    let (status, created_workspace) = request_json(
        &test_app.router,
        json_request(Method::POST, "/api/workspaces", json!({ "name": "Budget Team" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let workspace_id = persist_id(&created_workspace);

    let (status, channel) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/add_channel"),
            json!({
                "platform": "TikTok",
                "handle": "@budget-team",
                "active": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let channel_id = channel
        .get("id")
        .and_then(Value::as_str)
        .expect("channel id")
        .to_string();

    let (status, campaign) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/launch_campaign"),
            json!({
                "channel_id": channel_id,
                "title": "Flash",
                "budget_minor": 1000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let campaign_id = campaign
        .get("id")
        .and_then(Value::as_str)
        .expect("campaign id")
        .to_string();

    let (status, error_body) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/record_spend"),
            json!({
                "campaign_id": campaign_id,
                "amount_minor": 5000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        error_body.get("code").and_then(Value::as_str),
        Some("budget_exceeded")
    );

    let (status, dashboard) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/workspace_dashboard"))
            .body(Body::empty())
            .expect("dashboard request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        dashboard.get("spent_total_minor").and_then(Value::as_i64),
        Some(0)
    );
}

#[tokio::test]
async fn generated_router_maps_duplicate_channel_handle_to_conflict() {
    let test_app = TestApp::new().await;

    let (status, created_workspace) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            "/api/workspaces",
            json!({ "name": "Duplicate Team" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let workspace_id = persist_id(&created_workspace);

    let add_channel_path = format!("/api/workspaces/{workspace_id}/add_channel");
    let payload = json!({
        "platform": "YouTube",
        "handle": "@same_handle",
        "active": true
    });

    let (status, _) = request_json(
        &test_app.router,
        json_request(Method::POST, &add_channel_path, payload.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &add_channel_path,
            json!({
                "platform": "TikTok",
                "handle": "@same_handle",
                "active": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = request_json(
        &test_app.router,
        json_request(Method::POST, &add_channel_path, payload),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body.get("code").and_then(Value::as_str),
        Some("duplicate_channel_handle")
    );
}

#[tokio::test]
async fn generated_router_rejects_spend_on_paused_campaign() {
    let test_app = TestApp::new().await;

    let (status, created_workspace) = request_json(
        &test_app.router,
        json_request(Method::POST, "/api/workspaces", json!({ "name": "Pause Team" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let workspace_id = persist_id(&created_workspace);

    let (status, channel) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/add_channel"),
            json!({
                "platform": "TikTok",
                "handle": "@pause-team",
                "active": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let channel_id = channel
        .get("id")
        .and_then(Value::as_str)
        .expect("channel id")
        .to_string();

    let (status, campaign) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/launch_campaign"),
            json!({
                "channel_id": channel_id,
                "title": "Paused Spend",
                "budget_minor": 10000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let campaign_id = campaign
        .get("id")
        .and_then(Value::as_str)
        .expect("campaign id")
        .to_string();

    let (status, _) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/set_channel_active"),
            json!({
                "channel_id": channel_id,
                "active": false
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/record_spend"),
            json!({
                "campaign_id": campaign_id,
                "amount_minor": 500
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body.get("code").and_then(Value::as_str),
        Some("campaign_not_running")
    );
}

#[tokio::test]
async fn generated_router_rejects_launch_on_inactive_channel() {
    let test_app = TestApp::new().await;

    let (status, created_workspace) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            "/api/workspaces",
            json!({ "name": "Inactive Team" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let workspace_id = persist_id(&created_workspace);

    let (status, channel) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/add_channel"),
            json!({
                "platform": "YouTube",
                "handle": "@inactive",
                "active": false
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let channel_id = channel
        .get("id")
        .and_then(Value::as_str)
        .expect("channel id")
        .to_string();

    let (status, body) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/launch_campaign"),
            json!({
                "channel_id": channel_id,
                "title": "Should Fail",
                "budget_minor": 10000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body.get("code").and_then(Value::as_str), Some("channel_inactive"));
}

#[tokio::test]
async fn generated_router_replays_idempotent_record_spend() {
    let test_app = TestApp::new().await;

    let (status, created_workspace) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            "/api/workspaces",
            json!({ "name": "Spend Idempotency Team" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let workspace_id = persist_id(&created_workspace);

    let (status, channel) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/add_channel"),
            json!({
                "platform": "YouTube",
                "handle": "@spend_idempotent",
                "active": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let channel_id = channel
        .get("id")
        .and_then(Value::as_str)
        .expect("channel id")
        .to_string();

    let (status, campaign) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/launch_campaign"),
            json!({
                "channel_id": channel_id,
                "title": "Spend Once",
                "budget_minor": 20000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let campaign_id = campaign
        .get("id")
        .and_then(Value::as_str)
        .expect("campaign id")
        .to_string();

    let spend_path = format!("/api/workspaces/{workspace_id}/record_spend");
    let spend_payload = json!({
        "campaign_id": campaign_id,
        "amount_minor": 7000
    });

    let (status, first_spend) = request_json(
        &test_app.router,
        json_request_with_idempotency(
            Method::POST,
            &spend_path,
            spend_payload.clone(),
            "spend-idempotency-key",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, replay_spend) = request_json(
        &test_app.router,
        json_request_with_idempotency(
            Method::POST,
            &spend_path,
            spend_payload,
            "spend-idempotency-key",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_spend, replay_spend);

    let (status, dashboard) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/workspace_dashboard"))
            .body(Body::empty())
            .expect("dashboard request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        dashboard.get("spent_total_minor").and_then(Value::as_i64),
        Some(7000)
    );
}

#[tokio::test]
async fn generated_router_persists_state_after_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let data_dir = temp_dir.path().join("pulse-restart");

    let app1 = create_router(data_dir.clone()).await;
    let (status, created_workspace) = request_json(
        &app1,
        json_request(Method::POST, "/api/workspaces", json!({ "name": "Restart Team" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let workspace_id = persist_id(&created_workspace);

    let (status, channel) = request_json(
        &app1,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/add_channel"),
            json!({
                "platform": "YouTube",
                "handle": "@restart-team",
                "active": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let channel_id = channel
        .get("id")
        .and_then(Value::as_str)
        .expect("channel id")
        .to_string();

    let (status, campaign) = request_json(
        &app1,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/launch_campaign"),
            json!({
                "channel_id": channel_id,
                "title": "Restart Campaign",
                "budget_minor": 20000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let campaign_id = campaign
        .get("id")
        .and_then(Value::as_str)
        .expect("campaign id")
        .to_string();

    let (status, _) = request_json(
        &app1,
        json_request(
            Method::POST,
            &format!("/api/workspaces/{workspace_id}/record_spend"),
            json!({
                "campaign_id": campaign_id,
                "amount_minor": 7000
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    drop(app1);

    let app2 = create_router(data_dir).await;
    let (status, dashboard) = request_json(
        &app2,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/workspaces/{workspace_id}/workspace_dashboard"))
            .body(Body::empty())
            .expect("dashboard request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        dashboard.get("spent_total_minor").and_then(Value::as_i64),
        Some(7000)
    );
    assert_eq!(
        dashboard.get("campaigns_total").and_then(Value::as_u64),
        Some(1)
    );
}

#[tokio::test]
async fn generated_router_supports_list_query_params() {
    let test_app = TestApp::new().await;

    for name in ["Gamma Studio", "Alpha Studio", "Beta Studio"] {
        let (status, _) = request_json(
            &test_app.router,
            json_request(Method::POST, "/api/workspaces", json!({ "name": name })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, sorted_page) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri("/api/workspaces?sort=name&page=1&per_page=2")
            .body(Body::empty())
            .expect("sorted workspaces request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sorted_items = sorted_page.as_array().expect("sorted workspaces array");
    assert_eq!(sorted_items.len(), 2);
    assert_eq!(
        sorted_items[0]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Alpha Studio")
    );
    assert_eq!(
        sorted_items[1]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Beta Studio")
    );

    let (status, filtered) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri("/api/workspaces?name__contains=alpha")
            .body(Body::empty())
            .expect("filtered workspaces request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let filtered_items = filtered.as_array().expect("filtered workspaces array");
    assert_eq!(filtered_items.len(), 1);
    assert_eq!(
        filtered_items[0]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Alpha Studio")
    );

    let (status, invalid_query_error) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri("/api/workspaces?score__between=10")
            .body(Body::empty())
            .expect("invalid query request"),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        invalid_query_error.get("code").and_then(Value::as_str),
        Some("input_error")
    );
}

struct TestApp {
    _tmp: TempDir,
    router: Router,
}

impl TestApp {
    async fn new() -> Self {
        let tmp = TempDir::new().expect("temp dir");
        let router = create_router(tmp.path().join("pulse_data")).await;
        Self { _tmp: tmp, router }
    }
}

async fn create_router(data_dir: std::path::PathBuf) -> Router {
    let app = PersistApp::open_auto(data_dir).await.expect("open persist app");
    let workspaces = app
        .serve_autonomous_model::<PulseWorkspace>("workspaces")
        .await
        .expect("serve autonomous model");
    Router::new().nest("/api/workspaces", workspaces)
}

async fn request_json(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("request must succeed");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();

    if bytes.is_empty() {
        (status, Value::Null)
    } else {
        (
            status,
            serde_json::from_slice(&bytes).expect("response must be json"),
        )
    }
}

fn json_request(method: Method, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("build request")
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
        .expect("build request")
}

fn persist_id(record: &Value) -> String {
    record
        .get("persist_id")
        .and_then(Value::as_str)
        .expect("persist_id")
        .to_string()
}
