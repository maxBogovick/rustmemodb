use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tower::ServiceExt;

#[domain(table = "view_boards", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ViewBoard {
    name: String,
    active: bool,
}

#[domain(table = "auto_view_boards", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AutoViewBoard {
    name: String,
    active: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, DomainError)]
enum ViewBoardError {
    #[api_error(status = 422, code = "validation_error")]
    Validation(String),
}

impl std::fmt::Display for ViewBoardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ViewBoardError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PersistView)]
#[persist_view(model = ViewBoard, name = "summary")]
struct ViewBoardSummary {
    name: String,
    active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PersistView)]
#[persist_view(model = ViewBoard, name = "status", compute = compute_status_view)]
struct ViewBoardStatus {
    status: String,
}

fn compute_status_view(model: &ViewBoard) -> ViewBoardStatus {
    ViewBoardStatus {
        status: if model.active {
            "active".to_string()
        } else {
            "inactive".to_string()
        },
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PersistView)]
#[persist_view(model = AutoViewBoard, name = "summary")]
struct AutoViewBoardSummary {
    name: String,
    active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PersistView)]
#[persist_view(model = AutoViewBoard, name = "status", compute = compute_auto_status_view)]
struct AutoViewBoardStatus {
    status: String,
}

fn compute_auto_status_view(model: &AutoViewBoard) -> AutoViewBoardStatus {
    AutoViewBoardStatus {
        status: if model.active {
            "active".to_string()
        } else {
            "inactive".to_string()
        },
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MetricsCampaign {
    status: String,
    spent_minor: i64,
    budget_minor: i64,
}

#[domain(table = "metrics_workspaces", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct MetricsWorkspace {
    name: String,
    channels: PersistJson<Vec<String>>,
    campaigns: PersistJson<Vec<MetricsCampaign>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PersistView)]
#[persist_view(model = MetricsWorkspace, name = "kpis")]
struct MetricsWorkspaceKpis {
    #[view_metric(kind = "copy", source = "name")]
    workspace_name: String,
    #[view_metric(kind = "count", source = "channels")]
    channels_total: i64,
    #[view_metric(kind = "count", source = "campaigns")]
    campaigns_total: i64,
    #[view_metric(kind = "sum", source = "campaigns", field = "spent_minor")]
    spent_total_minor: i64,
    #[view_metric(kind = "group_by", source = "campaigns", by = "status", op = "count")]
    campaigns_by_status: BTreeMap<String, i64>,
    #[view_metric(
        kind = "group_by",
        source = "campaigns",
        by = "status",
        field = "spent_minor"
    )]
    spent_by_status: BTreeMap<String, i64>,
}

#[api]
impl ViewBoard {
    pub fn new(name: String) -> Self {
        Self {
            name: name.trim().to_string(),
            active: false,
        }
    }

    pub fn activate(&mut self) -> Result<(), ViewBoardError> {
        self.active = true;
        Ok(())
    }
}

#[api(views(AutoViewBoardSummary, AutoViewBoardStatus))]
impl AutoViewBoard {
    pub fn new(name: String) -> Self {
        Self {
            name: name.trim().to_string(),
            active: false,
        }
    }

    pub fn activate(&mut self) -> Result<(), ViewBoardError> {
        self.active = true;
        Ok(())
    }
}

#[api(views(MetricsWorkspaceKpis))]
impl MetricsWorkspace {
    pub fn new(name: String) -> Self {
        Self {
            name: name.trim().to_string(),
            channels: PersistJson::default(),
            campaigns: PersistJson::default(),
        }
    }

    pub fn add_channel(&mut self, handle: String) -> Result<(), ViewBoardError> {
        self.channels.push(handle.trim().to_string());
        Ok(())
    }

    pub fn add_campaign(
        &mut self,
        status: String,
        spent_minor: i64,
        budget_minor: i64,
    ) -> Result<(), ViewBoardError> {
        self.campaigns.push(MetricsCampaign {
            status: status.trim().to_string(),
            spent_minor,
            budget_minor,
        });
        Ok(())
    }
}

#[tokio::test]
async fn register_view_handle_computes_declared_view() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("persist_view_handle"))
        .await
        .expect("open app");

    let boards = app
        .open_autonomous_model::<ViewBoard>("view_boards")
        .await
        .expect("open boards");
    let summary = app.register_view::<ViewBoard, ViewBoardSummary>(&boards);
    let status = boards.view::<ViewBoardStatus>();

    let created = boards
        .create_one(ViewBoard::new("Roadmap".to_string()))
        .await
        .expect("create board");

    let before_summary = summary.get(&created.persist_id).await.expect("summary");
    assert_eq!(
        before_summary,
        ViewBoardSummary {
            name: "Roadmap".to_string(),
            active: false,
        }
    );
    let before_status = status.get(&created.persist_id).await.expect("status");
    assert_eq!(before_status.status, "inactive");

    boards
        .activate(&created.persist_id)
        .await
        .expect("activate board");

    let after_summary = summary.get(&created.persist_id).await.expect("summary");
    assert!(after_summary.active);
    let after_status = status.get(&created.persist_id).await.expect("status");
    assert_eq!(after_status.status, "active");
}

#[tokio::test]
async fn serve_autonomous_model_with_view_exposes_generated_endpoint() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("persist_view_router"))
        .await
        .expect("open app");
    let router = app
        .serve_autonomous_model_with_view::<ViewBoard, ViewBoardSummary>("view_boards_route")
        .await
        .expect("serve router with view");

    let create = router
        .clone()
        .oneshot(json_request(Method::POST, "/", json!({ "name": "Alpha" })))
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::CREATED);
    let created_body = decode_json(create).await;
    let persist_id = created_body
        .get("persist_id")
        .and_then(Value::as_str)
        .expect("persist_id")
        .to_string();

    let view = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/{persist_id}/views/summary"))
                .body(Body::empty())
                .expect("view request"),
        )
        .await
        .expect("view response");
    assert_eq!(view.status(), StatusCode::OK);
    let view_body = decode_json(view).await;
    assert_eq!(view_body.get("name").and_then(Value::as_str), Some("Alpha"));
    assert_eq!(
        view_body.get("active").and_then(Value::as_bool),
        Some(false)
    );

    let missing = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/missing-id/views/summary")
                .body(Body::empty())
                .expect("missing view request"),
        )
        .await
        .expect("missing response");
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn api_attr_views_auto_mounts_typed_view_routes_and_openapi() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("persist_view_auto"))
        .await
        .expect("open app");
    let router = app
        .serve_autonomous_model::<AutoViewBoard>("auto_view_boards_route")
        .await
        .expect("serve autonomous router");

    let create = router
        .clone()
        .oneshot(json_request(Method::POST, "/", json!({ "name": "Beta" })))
        .await
        .expect("create response");
    assert_eq!(create.status(), StatusCode::CREATED);
    let created_body = decode_json(create).await;
    let persist_id = created_body
        .get("persist_id")
        .and_then(Value::as_str)
        .expect("persist_id")
        .to_string();

    let summary = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/{persist_id}/views/summary"))
                .body(Body::empty())
                .expect("summary request"),
        )
        .await
        .expect("summary response");
    assert_eq!(summary.status(), StatusCode::OK);
    let summary_body = decode_json(summary).await;
    assert_eq!(
        summary_body.get("name").and_then(Value::as_str),
        Some("Beta")
    );
    assert_eq!(
        summary_body.get("active").and_then(Value::as_bool),
        Some(false)
    );

    let status = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/{persist_id}/views/status"))
                .body(Body::empty())
                .expect("status request"),
        )
        .await
        .expect("status response");
    assert_eq!(status.status(), StatusCode::OK);
    let status_body = decode_json(status).await;
    assert_eq!(
        status_body.get("status").and_then(Value::as_str),
        Some("inactive")
    );

    let openapi = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/_openapi.json")
                .body(Body::empty())
                .expect("openapi request"),
        )
        .await
        .expect("openapi response");
    assert_eq!(openapi.status(), StatusCode::OK);
    let openapi_body = decode_json(openapi).await;
    let paths = openapi_body
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths");
    assert!(paths.contains_key("/{id}/views/summary"));
    assert!(paths.contains_key("/{id}/views/status"));
}

#[tokio::test]
async fn persist_view_metrics_compute_count_sum_group_by_without_manual_compute() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("persist_view_metrics"))
        .await
        .expect("open app");
    let workspaces = app
        .open_autonomous_model::<MetricsWorkspace>("metrics_workspaces")
        .await
        .expect("open metrics workspaces");
    let created = workspaces
        .create_one(MetricsWorkspace::new("Growth".to_string()))
        .await
        .expect("create workspace");

    workspaces
        .add_channel(&created.persist_id, "youtube".to_string())
        .await
        .expect("add channel");
    workspaces
        .add_channel(&created.persist_id, "tiktok".to_string())
        .await
        .expect("add channel");
    workspaces
        .add_campaign(&created.persist_id, "running".to_string(), 1500, 5000)
        .await
        .expect("add campaign");
    workspaces
        .add_campaign(&created.persist_id, "paused".to_string(), 200, 1000)
        .await
        .expect("add campaign");
    workspaces
        .add_campaign(&created.persist_id, "running".to_string(), 800, 2200)
        .await
        .expect("add campaign");

    let kpis = workspaces
        .view::<MetricsWorkspaceKpis>()
        .get(&created.persist_id)
        .await
        .expect("kpis view");
    assert_eq!(kpis.workspace_name, "Growth");
    assert_eq!(kpis.channels_total, 2);
    assert_eq!(kpis.campaigns_total, 3);
    assert_eq!(kpis.spent_total_minor, 2500);
    assert_eq!(kpis.campaigns_by_status.get("running"), Some(&2));
    assert_eq!(kpis.campaigns_by_status.get("paused"), Some(&1));
    assert_eq!(kpis.spent_by_status.get("running"), Some(&2300));
    assert_eq!(kpis.spent_by_status.get("paused"), Some(&200));
}

fn json_request(method: Method, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn decode_json(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("json body")
}
