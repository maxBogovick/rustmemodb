use axum::{Router, body::Body, http::Request, http::StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tempfile::tempdir;
use todo_persist_runtime::{AppConfig, bootstrap};
use tower::ServiceExt;

#[tokio::test]
async fn http_smoke_crud_snapshot_and_recovery_without_socket() {
    let data = tempdir().expect("temp dir");
    let config = AppConfig::for_testing(data.path().join("primary"));

    let boot1 = bootstrap(&config).await.expect("bootstrap #1");
    let app1 = boot1.router.clone();

    let (status, created) = request_json(
        &app1,
        Request::builder()
            .method("POST")
            .uri("/api/v1/todos")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"title":"Write smoke test", "priority": 2}).to_string(),
            ))
            .expect("valid request"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let todo_id = created
        .get("id")
        .and_then(|v| v.as_str())
        .expect("todo id present")
        .to_string();

    let (status, created_2) = request_json(
        &app1,
        Request::builder()
            .method("POST")
            .uri("/api/v1/todos")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"title":"Keep me after restart", "priority": 1}).to_string(),
            ))
            .expect("valid create request #2"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let second_todo_id = created_2
        .get("id")
        .and_then(|v| v.as_str())
        .expect("second todo id present")
        .to_string();

    let (status, patched) = request_json(
        &app1,
        Request::builder()
            .method("PATCH")
            .uri(format!("/api/v1/todos/{todo_id}"))
            .header("content-type", "application/json")
            .body(Body::from(json!({"completed": true}).to_string()))
            .expect("valid patch request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        patched.get("completed").and_then(|v| v.as_bool()),
        Some(true)
    );

    let (status, _deleted) = request_json(
        &app1,
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/todos/{todo_id}"))
            .body(Body::empty())
            .expect("valid delete request"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, stats) = request_json(
        &app1,
        Request::builder()
            .method("GET")
            .uri("/api/v1/admin/stats")
            .body(Body::empty())
            .expect("valid stats request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stats.get("todo_count").and_then(|v| v.as_u64()), Some(1));
    assert!(
        stats
            .get("snapshot_path")
            .and_then(|v| v.as_str())
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    );

    boot1.shutdown().await.expect("shutdown #1");

    let boot2 = bootstrap(&config).await.expect("bootstrap #2");
    let app2 = boot2.router.clone();

    let (status, todos_after_restart) = request_json(
        &app2,
        Request::builder()
            .method("GET")
            .uri("/api/v1/todos")
            .body(Body::empty())
            .expect("valid list request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let items = todos_after_restart
        .as_array()
        .expect("list response must be array");
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].get("id").and_then(|v| v.as_str()),
        Some(second_todo_id.as_str())
    );
    assert_eq!(
        items[0].get("title").and_then(|v| v.as_str()),
        Some("Keep me after restart")
    );

    boot2.shutdown().await.expect("shutdown #2");
}

async fn request_json(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("request must be served");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();

    if body.is_empty() {
        return (status, Value::Null);
    }

    let json = serde_json::from_slice::<Value>(&body).expect("json body");
    (status, json)
}
