use std::sync::Arc;

use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use serde_json::{Value, json};
use todo_backend::{app::build_router, repository::InMemoryTodoRepository, state::AppState};
use tower::ServiceExt;

fn app() -> axum::Router {
    let repo = Arc::new(InMemoryTodoRepository::new());
    build_router(AppState::new(repo))
}

async fn send_json(
    app: &axum::Router,
    method: Method,
    uri: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .expect("request should build");

    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("response expected");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");

    if body.is_empty() {
        return (status, Value::Null);
    }

    let json = serde_json::from_slice::<Value>(&body).expect("body should be valid JSON");
    (status, json)
}

async fn send_empty(app: &axum::Router, method: Method, uri: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request should build");

    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("response expected");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");

    if body.is_empty() {
        return (status, Value::Null);
    }

    let json = serde_json::from_slice::<Value>(&body).expect("body should be valid JSON");
    (status, json)
}

#[tokio::test]
async fn create_and_get_todo() {
    let app = app();

    let (status, body) = send_json(
        &app,
        Method::POST,
        "/api/v1/todos",
        json!({
            "title": "Write backend",
            "description": "Build complete CRUD",
            "priority": 4
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    let id = body["data"]["id"]
        .as_str()
        .expect("created response should have id");

    let (status, fetched) = send_empty(&app, Method::GET, &format!("/api/v1/todos/{id}")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["data"]["title"], "Write backend");
    assert_eq!(fetched["data"]["priority"], 4);
}

#[tokio::test]
async fn list_supports_pagination_and_filters() {
    let app = app();

    for title in ["buy milk", "learn rust", "call mom"] {
        let (_status, _) = send_json(
            &app,
            Method::POST,
            "/api/v1/todos",
            json!({ "title": title, "priority": 2 }),
        )
        .await;
    }

    let (status, page) = send_empty(
        &app,
        Method::GET,
        "/api/v1/todos?page=1&per_page=2&search=learn&sort_by=title&order=asc",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["data"]["total"], 1);
    assert_eq!(
        page["data"]["items"]
            .as_array()
            .expect("items should be array")
            .len(),
        1
    );
    assert_eq!(page["data"]["items"][0]["title"], "learn rust");
}

#[tokio::test]
async fn patch_updates_single_fields() {
    let app = app();

    let (_status, created) = send_json(
        &app,
        Method::POST,
        "/api/v1/todos",
        json!({ "title": "test patch", "priority": 1 }),
    )
    .await;

    let id = created["data"]["id"]
        .as_str()
        .expect("created response should have id");

    let (status, updated) = send_json(
        &app,
        Method::PATCH,
        &format!("/api/v1/todos/{id}"),
        json!({
            "status": "completed",
            "description": "done"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["data"]["status"], "completed");
    assert_eq!(updated["data"]["description"], "done");
    assert_ne!(updated["data"]["completed_at"], Value::Null);
}

#[tokio::test]
async fn put_replaces_entire_resource() {
    let app = app();

    let (_status, created) = send_json(
        &app,
        Method::POST,
        "/api/v1/todos",
        json!({ "title": "old title", "priority": 1 }),
    )
    .await;

    let id = created["data"]["id"]
        .as_str()
        .expect("created response should have id");

    let (status, replaced) = send_json(
        &app,
        Method::PUT,
        &format!("/api/v1/todos/{id}"),
        json!({
            "title": "new title",
            "description": "full replacement",
            "priority": 5,
            "status": "in_progress",
            "due_at": null
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(replaced["data"]["title"], "new title");
    assert_eq!(replaced["data"]["status"], "in_progress");
    assert_eq!(replaced["data"]["priority"], 5);
}

#[tokio::test]
async fn delete_soft_deletes_and_restore_brings_back() {
    let app = app();

    let (_status, created) = send_json(
        &app,
        Method::POST,
        "/api/v1/todos",
        json!({ "title": "to delete", "priority": 2 }),
    )
    .await;

    let id = created["data"]["id"]
        .as_str()
        .expect("created response should have id");

    let (status, _) = send_empty(&app, Method::DELETE, &format!("/api/v1/todos/{id}")).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send_empty(&app, Method::GET, &format!("/api/v1/todos/{id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, deleted_visible) = send_empty(
        &app,
        Method::GET,
        &format!("/api/v1/todos/{id}?include_deleted=true"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(deleted_visible["data"]["deleted_at"], Value::Null);

    let (status, restored) =
        send_empty(&app, Method::POST, &format!("/api/v1/todos/{id}/restore")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(restored["data"]["deleted_at"], Value::Null);

    let (status, _) = send_empty(&app, Method::GET, &format!("/api/v1/todos/{id}")).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn validation_errors_are_returned() {
    let app = app();

    let (status, bad_title) = send_json(
        &app,
        Method::POST,
        "/api/v1/todos",
        json!({ "title": "   " }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_title["error"], "title must not be blank");

    let (status, empty_patch) = send_json(
        &app,
        Method::PATCH,
        "/api/v1/todos/6f65e6b6-e201-4fc4-9d57-7dd9b33f8082",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        empty_patch["error"],
        "at least one field must be provided for PATCH"
    );

    let (status, bad_query) = send_empty(&app, Method::GET, "/api/v1/todos?per_page=1000").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_query["error"], "per_page must be between 1 and 100");
}

#[tokio::test]
async fn healthcheck_is_available() {
    let app = app();

    let (status, body) = send_empty(&app, Method::GET, "/health").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["message"], "ok");
}
