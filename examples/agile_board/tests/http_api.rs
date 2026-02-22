use agile_board::model::Board;
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use rustmemodb::PersistApp;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
async fn generated_router_handles_board_task_flow() {
    let test_app = TestApp::new().await;

    let (status, created_board) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            "/api/boards",
            json!({ "name": "Platform Team" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let board_id = persist_id(&created_board);

    let (status, first_column_response) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/boards/{board_id}/add_column"),
            json!({ "title": "Backlog" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first_column_id: Uuid = serde_json::from_value(first_column_response)
        .expect("column id response must be a UUID string");

    let (status, second_column_response) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/boards/{board_id}/add_column"),
            json!({ "title": "In Progress" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let second_column_id: Uuid = serde_json::from_value(second_column_response)
        .expect("column id response must be a UUID string");
    assert_ne!(first_column_id, second_column_id);

    let (status, task_response) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/boards/{board_id}/add_task"),
            json!({
                "column_id": first_column_id,
                "title": "Design API",
                "description": "Describe clean domain surface"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let task_id: Uuid =
        serde_json::from_value(task_response).expect("task id response must be a UUID string");

    let (status, move_response) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/boards/{board_id}/move_task"),
            json!({
                "task_id": task_id,
                "to_column_id": second_column_id,
                "new_index": 0
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(move_response, Value::Null);

    let (status, board_record) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/boards/{board_id}"))
            .body(Body::empty())
            .expect("get board request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let board_model = board_record.get("model").expect("board record.model");
    let columns = board_model
        .get("columns")
        .and_then(Value::as_array)
        .expect("board columns must be array");
    assert_eq!(columns.len(), 2);

    let backlog_tasks = columns[0]
        .get("tasks")
        .and_then(Value::as_array)
        .expect("backlog tasks must be array");
    let in_progress_tasks = columns[1]
        .get("tasks")
        .and_then(Value::as_array)
        .expect("in progress tasks must be array");
    assert_eq!(backlog_tasks.len(), 0);
    assert_eq!(in_progress_tasks.len(), 1);

    let moved_task_id = in_progress_tasks[0]
        .get("id")
        .and_then(Value::as_str)
        .expect("task id must be string");
    assert_eq!(moved_task_id, task_id.to_string());
}

#[tokio::test]
async fn generated_router_maps_domain_errors_to_http_status_codes() {
    let test_app = TestApp::new().await;

    let (status, created_board) = request_json(
        &test_app.router,
        json_request(Method::POST, "/api/boards", json!({ "name": "Validation" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let board_id = persist_id(&created_board);

    let (status, response) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/boards/{board_id}/add_column"),
            json!({ "title": " " }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response.get("code").and_then(Value::as_str),
        Some("validation_error")
    );

    let (status, response) = request_json(
        &test_app.router,
        json_request(
            Method::POST,
            &format!("/api/boards/{board_id}/remove_task"),
            json!({ "task_id": Uuid::new_v4() }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        response.get("code").and_then(Value::as_str),
        Some("task_not_found")
    );
}

#[tokio::test]
async fn generated_router_replays_idempotent_command_by_default() {
    let test_app = TestApp::new().await;

    let (status, created_board) = request_json(
        &test_app.router,
        json_request(Method::POST, "/api/boards", json!({ "name": "Idempotent Board" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let board_id = persist_id(&created_board);

    let request_path = format!("/api/boards/{board_id}/add_column");
    let payload = json!({ "title": "Backlog" });

    let (status, first_column_id) = request_json(
        &test_app.router,
        json_request_with_idempotency(
            Method::POST,
            &request_path,
            payload.clone(),
            "board-add-column-key",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, replay_column_id) = request_json(
        &test_app.router,
        json_request_with_idempotency(
            Method::POST,
            &request_path,
            payload,
            "board-add-column-key",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_column_id, replay_column_id);

    let (status, board_record) = request_json(
        &test_app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/boards/{board_id}"))
            .body(Body::empty())
            .expect("get board request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let columns = board_record
        .get("model")
        .and_then(|model| model.get("columns"))
        .and_then(Value::as_array)
        .expect("columns must be array");
    assert_eq!(columns.len(), 1, "replay must not append duplicate column");
}

#[tokio::test]
async fn generated_router_persists_after_restart_and_exposes_openapi() {
    let temp_dir = TempDir::new().expect("temp dir");
    let data_dir = temp_dir.path().join("agile-data");

    let app1 = create_router(data_dir.clone()).await;
    let (status, created_board) = request_json(
        &app1,
        json_request(Method::POST, "/api/boards", json!({ "name": "Persisted" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let board_id = persist_id(&created_board);

    let (status, _) = request_json(
        &app1,
        json_request(
            Method::POST,
            &format!("/api/boards/{board_id}/add_column"),
            json!({ "title": "Backlog" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    drop(app1);

    let app2 = create_router(data_dir).await;
    let (status, board) = request_json(
        &app2,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/boards/{board_id}"))
            .body(Body::empty())
            .expect("get persisted board request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let columns = board
        .get("model")
        .and_then(|model| model.get("columns"))
        .and_then(Value::as_array)
        .expect("columns must be array");
    assert_eq!(columns.len(), 1);
    assert_eq!(
        columns[0].get("title").and_then(Value::as_str),
        Some("Backlog")
    );

    let (status, openapi) = request_json(
        &app2,
        Request::builder()
            .method(Method::GET)
            .uri("/api/boards/_openapi.json")
            .body(Body::empty())
            .expect("openapi request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        openapi.get("openapi").and_then(Value::as_str),
        Some("3.1.0")
    );
    let paths = openapi
        .get("paths")
        .and_then(Value::as_object)
        .expect("paths object");
    assert!(paths.contains_key("/{id}/add_column"));
    assert!(paths.contains_key("/{id}/move_task"));
    let move_task_post = paths
        .get("/{id}/move_task")
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("post"))
        .and_then(Value::as_object)
        .expect("move_task post operation");
    let responses = move_task_post
        .get("responses")
        .and_then(Value::as_object)
        .expect("move_task responses");
    assert!(responses.contains_key("204"));

    let add_column_post = paths
        .get("/{id}/add_column")
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("post"))
        .and_then(Value::as_object)
        .expect("add_column post operation");
    let parameters = add_column_post
        .get("parameters")
        .and_then(Value::as_array)
        .expect("add_column parameters");
    let has_idempotency_header = parameters.iter().any(|param| {
        param.get("name").and_then(Value::as_str) == Some("Idempotency-Key")
            && param.get("in").and_then(Value::as_str) == Some("header")
    });
    assert!(
        has_idempotency_header,
        "generated command endpoint must document Idempotency-Key header"
    );
}

struct TestApp {
    _tmp: TempDir,
    router: Router,
}

impl TestApp {
    async fn new() -> Self {
        let tmp = TempDir::new().expect("temp dir");
        let router = create_router(tmp.path().join("data")).await;
        Self { _tmp: tmp, router }
    }
}

async fn create_router(data_dir: std::path::PathBuf) -> Router {
    let app = PersistApp::open_auto(data_dir).await.expect("open app");
    let boards = app
        .serve_autonomous_model::<Board>("boards")
        .await
        .expect("serve model");
    Router::new().nest("/api/boards", boards)
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

fn persist_id(record: &Value) -> String {
    record
        .get("persist_id")
        .and_then(Value::as_str)
        .expect("persist_id must be present and string")
        .to_string()
}

async fn request_json(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("request must be served");

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

    let body = serde_json::from_slice::<Value>(&bytes).expect("valid json response");
    (status, body)
}
