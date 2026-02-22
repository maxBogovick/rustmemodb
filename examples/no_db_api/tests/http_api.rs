use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use rustmemodb::PersistApp;
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

// Compile-time typed request models from example payloads (no handwritten DTO boilerplate).
rustmemodb::generate_struct_from_json! {
    name = "CreateUserPayload",
    json = r#"{"username":"alice","email":"alice@example.com","active":true}"#
}

rustmemodb::generate_struct_from_json! {
    name = "CreateUserWithNicknamePayload",
    json = r#"{"username":"alice","email":"alice@example.com","nickname":"ali"}"#
}

rustmemodb::generate_struct_from_json! {
    name = "PatchUserPayload",
    json = r#"{"active":false}"#
}

#[tokio::test]
async fn schema_first_crud_routes_work_without_manual_handlers() {
    let root = temp_root("crud");
    let schemas_dir = root.join("schemas");
    tokio::fs::create_dir_all(&schemas_dir)
        .await
        .expect("create schemas dir");
    tokio::fs::write(
        schemas_dir.join("users.json"),
        r#"{
            "type":"object",
            "properties":{
                "username":{"type":"string"},
                "email":{"type":"string"},
                "active":{"type":"boolean"}
            },
            "required":["username","email"]
        }"#,
    )
    .await
    .expect("write schema");

    let router = build_router(root.join("data"), &schemas_dir).await;

    let created = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/api/users",
            to_json_value(&CreateUserPayload {
                username: "alice".to_string(),
                email: "alice@example.com".to_string(),
                active: true,
            }),
        ))
        .await
        .expect("create response");
    assert_eq!(created.status(), StatusCode::CREATED);
    let created_json = decode_json(created).await;
    let id = created_json
        .get("id")
        .and_then(Value::as_str)
        .expect("id")
        .to_string();

    let listed = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(Body::empty())
                .expect("list request"),
        )
        .await
        .expect("list response");
    assert_eq!(listed.status(), StatusCode::OK);
    let listed_json = decode_json(listed).await;
    assert_eq!(listed_json.as_array().map(|v| v.len()), Some(1));

    let patched = router
        .clone()
        .oneshot(json_request(
            Method::PATCH,
            &format!("/api/users/{id}"),
            to_json_value(&PatchUserPayload { active: false }),
        ))
        .await
        .expect("patch response");
    assert_eq!(patched.status(), StatusCode::OK);
    let patched_json = decode_json(patched).await;
    assert_eq!(patched_json.get("active").and_then(Value::as_bool), Some(false));
    assert_eq!(patched_json.get("version").and_then(Value::as_i64), Some(2));

    let deleted = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("/api/users/{id}"))
                .body(Body::empty())
                .expect("delete request"),
        )
        .await
        .expect("delete response");
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn schema_hot_reload_applies_without_restart() {
    let root = temp_root("hot_reload");
    let schemas_dir = root.join("schemas");
    tokio::fs::create_dir_all(&schemas_dir)
        .await
        .expect("create schemas dir");
    let users_schema_path = schemas_dir.join("users.json");
    tokio::fs::write(
        &users_schema_path,
        r#"{
            "type":"object",
            "properties":{
                "username":{"type":"string"},
                "email":{"type":"string"}
            },
            "required":["username","email"]
        }"#,
    )
    .await
    .expect("write initial schema");

    let router = build_router(root.join("data"), &schemas_dir).await;

    let invalid_before_reload = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/api/users",
            to_json_value(&CreateUserWithNicknamePayload {
                username: "alice".to_string(),
                email: "alice@example.com".to_string(),
                nickname: "ali".to_string(),
            }),
        ))
        .await
        .expect("invalid-before response");
    assert_eq!(invalid_before_reload.status(), StatusCode::UNPROCESSABLE_ENTITY);

    tokio::fs::write(
        &users_schema_path,
        r#"{
            "type":"object",
            "properties":{
                "username":{"type":"string"},
                "email":{"type":"string"},
                "nickname":{"type":"string"}
            },
            "required":["username","email"]
        }"#,
    )
    .await
    .expect("write updated schema");

    tokio::time::sleep(Duration::from_millis(400)).await;

    let created_after_reload = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/api/users",
            to_json_value(&CreateUserWithNicknamePayload {
                username: "alice".to_string(),
                email: "alice@example.com".to_string(),
                nickname: "ali".to_string(),
            }),
        ))
        .await
        .expect("created-after response");
    let created_after_status = created_after_reload.status();
    let created_after_body = decode_body_text(created_after_reload).await;
    assert_eq!(
        created_after_status,
        StatusCode::CREATED,
        "unexpected create status/body after hot-reload: {} {}",
        created_after_status,
        created_after_body
    );
    let created_json: Value = serde_json::from_str(&created_after_body).expect("json body");
    assert_eq!(
        created_json.get("nickname").and_then(Value::as_str),
        Some("ali")
    );
}

#[tokio::test]
async fn openapi_document_is_generated_for_schema_collections() {
    let root = temp_root("openapi");
    let schemas_dir = root.join("schemas");
    tokio::fs::create_dir_all(&schemas_dir)
        .await
        .expect("create schemas dir");
    tokio::fs::write(
        schemas_dir.join("users.json"),
        r#"{
            "type":"object",
            "properties":{
                "username":{"type":"string"},
                "email":{"type":"string"},
                "active":{"type":"boolean"}
            },
            "required":["username","email"]
        }"#,
    )
    .await
    .expect("write schema");

    let router = build_router(root.join("data"), &schemas_dir).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/_openapi.json")
                .body(Body::empty())
                .expect("openapi request"),
        )
        .await
        .expect("openapi response");
    assert_eq!(response.status(), StatusCode::OK);

    let doc = decode_json(response).await;
    assert_eq!(doc.get("openapi").and_then(Value::as_str), Some("3.1.0"));

    let paths = doc
        .get("paths")
        .and_then(Value::as_object)
        .expect("paths object");
    assert!(paths.contains_key("/users"));
    assert!(paths.contains_key("/users/{id}"));
}

async fn build_router(data_dir: std::path::PathBuf, schemas_dir: &std::path::Path) -> axum::Router {
    let app = PersistApp::open_auto(data_dir).await.expect("open app");
    let dynamic = app
        .serve_json_schema_dir(schemas_dir)
        .await
        .expect("serve schema dir");
    axum::Router::new().nest("/api", dynamic)
}

fn temp_root(prefix: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("no_db_api_{prefix}_{nonce}"))
}

fn json_request(method: Method, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

fn to_json_value<T: serde::Serialize>(payload: &T) -> Value {
    serde_json::to_value(payload).expect("serialize payload")
}

async fn decode_json(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response bytes");
    serde_json::from_slice(&bytes).expect("json response")
}

async fn decode_body_text(response: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response bytes");
    String::from_utf8(bytes.to_vec()).expect("utf8 response")
}
