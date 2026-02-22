use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

pub async fn run_create_user_contract(app: Router) {
    let (status, created) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "alice@example.com",
                    "display_name": "Alice"
                })
                .to_string(),
            ))
            .expect("valid create request"),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        created.get("email").and_then(Value::as_str),
        Some("alice@example.com")
    );
    assert_eq!(
        created.get("display_name").and_then(Value::as_str),
        Some("Alice")
    );
    assert_eq!(created.get("active").and_then(Value::as_bool), Some(true));
    assert_eq!(created.get("version").and_then(Value::as_i64), Some(1));
    assert!(created.get("id").and_then(Value::as_str).is_some());

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "alice@example.com",
                    "display_name": "Alice Dup"
                })
                .to_string(),
            ))
            .expect("valid duplicate request"),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_problem(&problem, 409, "Conflict");

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "not-an-email",
                    "display_name": "Bad"
                })
                .to_string(),
            ))
            .expect("valid malformed email request"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let (status, problem) = request_json(
        app,
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "bob@example.com",
                    "display_name": "  "
                })
                .to_string(),
            ))
            .expect("valid blank display name request"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");
}

pub async fn run_health_contract(app: Router) {
    let (status, body) = request_json(
        app,
        Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .expect("valid health request"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("status").and_then(Value::as_str), Some("ok"));
}

fn assert_problem(problem: &Value, expected_status: u16, expected_title: &str) {
    assert_eq!(
        problem.get("status").and_then(Value::as_u64),
        Some(u64::from(expected_status))
    );
    assert_eq!(
        problem.get("title").and_then(Value::as_str),
        Some(expected_title)
    );
    assert!(problem.get("detail").and_then(Value::as_str).is_some());
    assert!(
        problem
            .get("correlation_id")
            .and_then(Value::as_str)
            .is_some()
    );
    assert!(problem.get("type").and_then(Value::as_str).is_some());
}

async fn request_json(app: Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app
        .oneshot(request)
        .await
        .expect("router should serve request");

    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body should collect")
        .to_bytes();

    if body.is_empty() {
        return (status, Value::Null);
    }

    let value = serde_json::from_slice(&body).expect("body should be valid json");
    (status, value)
}
