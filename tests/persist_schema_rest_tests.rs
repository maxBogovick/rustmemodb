use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use rustmemodb::PersistApp;
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

#[tokio::test]
async fn serve_json_schema_dir_provides_generic_crud_routes() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("persist_schema_rest_{nonce}"));
    let schemas_dir = root.join("schemas");
    tokio::fs::create_dir_all(&schemas_dir)
        .await
        .expect("create schemas dir");
    tokio::fs::write(
        schemas_dir.join("users.json"),
        r#"{
            "type": "object",
            "properties": {
                "username": { "type": "string" },
                "email": { "type": "string" },
                "age": { "type": "integer" },
                "active": { "type": "boolean" }
            },
            "required": ["username", "email"]
        }"#,
    )
    .await
    .expect("write users schema");

    let app = PersistApp::open_auto(root.join("data"))
        .await
        .expect("open app");
    let dynamic = app
        .serve_json_schema_dir(&schemas_dir)
        .await
        .expect("serve schema dir");
    let router = axum::Router::new().nest("/api", dynamic);

    let created = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/api/users",
            json!({
                "username": "alice",
                "email": "alice@example.com",
                "age": 30,
                "active": true
            }),
        ))
        .await
        .expect("create response");
    assert_eq!(created.status(), StatusCode::CREATED);
    let created_body = decode_json(created).await;
    let id = created_body
        .get("id")
        .and_then(Value::as_str)
        .expect("created id")
        .to_string();
    assert_eq!(
        created_body.get("username").and_then(Value::as_str),
        Some("alice")
    );
    assert_eq!(created_body.get("version").and_then(Value::as_i64), Some(1));

    let invalid = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/api/users",
            json!({
                "username": "bob",
                "email": "bob@example.com",
                "age": "old"
            }),
        ))
        .await
        .expect("invalid response");
    assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);

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
    let listed_body = decode_json(listed).await;
    let listed_items = listed_body.as_array().expect("list array");
    assert_eq!(listed_items.len(), 1);

    let fetched = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/users/{id}"))
                .body(Body::empty())
                .expect("get request"),
        )
        .await
        .expect("get response");
    assert_eq!(fetched.status(), StatusCode::OK);
    let fetched_body = decode_json(fetched).await;
    assert_eq!(
        fetched_body.get("id").and_then(Value::as_str),
        Some(id.as_str())
    );
    assert_eq!(
        fetched_body.get("email").and_then(Value::as_str),
        Some("alice@example.com")
    );

    let patched = router
        .clone()
        .oneshot(json_request(
            Method::PATCH,
            &format!("/api/users/{id}"),
            json!({
                "age": 31,
                "active": false
            }),
        ))
        .await
        .expect("patch response");
    assert_eq!(patched.status(), StatusCode::OK);
    let patched_body = decode_json(patched).await;
    assert_eq!(patched_body.get("age").and_then(Value::as_i64), Some(31));
    assert_eq!(
        patched_body.get("active").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(patched_body.get("version").and_then(Value::as_i64), Some(2));

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

    let missing = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/users/{id}"))
                .body(Body::empty())
                .expect("missing request"),
        )
        .await
        .expect("missing response");
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
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
