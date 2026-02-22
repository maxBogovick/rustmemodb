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

pub async fn run_read_users_contract(app: Router) {
    let (status, created_bob) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "bob@example.com",
                    "display_name": "Bob"
                })
                .to_string(),
            ))
            .expect("valid bob create request"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let bob_id = created_bob
        .get("id")
        .and_then(Value::as_str)
        .expect("created user must include id")
        .to_string();

    let (status, _created_zoe) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "zoe@example.com",
                    "display_name": "Zoe"
                })
                .to_string(),
            ))
            .expect("valid zoe create request"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, got_bob) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{bob_id}"))
            .body(Body::empty())
            .expect("valid get by id request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        got_bob.get("email").and_then(Value::as_str),
        Some("bob@example.com")
    );

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri("/api/v1/users/not-a-uuid")
            .body(Body::empty())
            .expect("valid malformed id request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri("/api/v1/users/00000000-0000-0000-0000-000000000000")
            .body(Body::empty())
            .expect("valid missing id request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_problem(&problem, 404, "Not found");

    let (status, paged) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri("/api/v1/users?page=1&per_page=2&sort_by=email&order=asc")
            .body(Body::empty())
            .expect("valid list request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(paged.get("page").and_then(Value::as_u64), Some(1));
    assert_eq!(paged.get("per_page").and_then(Value::as_u64), Some(2));
    assert_eq!(paged.get("total").and_then(Value::as_u64), Some(3));
    assert_eq!(paged.get("total_pages").and_then(Value::as_u64), Some(2));
    let items = paged
        .get("items")
        .and_then(Value::as_array)
        .expect("list must include items array");
    assert_eq!(items.len(), 2);
    assert_eq!(
        items[0].get("email").and_then(Value::as_str),
        Some("alice@example.com")
    );
    assert_eq!(
        items[1].get("email").and_then(Value::as_str),
        Some("bob@example.com")
    );

    let (status, filtered) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri("/api/v1/users?page=1&per_page=10&email_contains=zoe&active=true")
            .body(Body::empty())
            .expect("valid filtered list request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(filtered.get("total").and_then(Value::as_u64), Some(1));
    let filtered_items = filtered
        .get("items")
        .and_then(Value::as_array)
        .expect("filtered list must include items array");
    assert_eq!(filtered_items.len(), 1);
    assert_eq!(
        filtered_items[0].get("email").and_then(Value::as_str),
        Some("zoe@example.com")
    );

    let (status, problem) = request_json(
        app,
        Request::builder()
            .method("GET")
            .uri("/api/v1/users?page=0")
            .body(Body::empty())
            .expect("valid invalid-page request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");
}

pub async fn run_write_users_contract(app: Router) {
    let (status, created) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "write@example.com",
                    "display_name": "Writer"
                })
                .to_string(),
            ))
            .expect("valid write user create request"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let id = created
        .get("id")
        .and_then(Value::as_str)
        .expect("created user must include id")
        .to_string();
    let version = created
        .get("version")
        .and_then(Value::as_i64)
        .expect("created user must include version");
    assert_eq!(version, 1);

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("PATCH")
            .uri(format!("/api/v1/users/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "display_name": "Writer Updated",
                    "active": false
                })
                .to_string(),
            ))
            .expect("valid patch without if-match request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("PATCH")
            .uri(format!("/api/v1/users/{id}"))
            .header("if-match", "not-a-number")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "display_name": "Writer Updated",
                    "active": false
                })
                .to_string(),
            ))
            .expect("valid patch with invalid if-match request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("PATCH")
            .uri(format!("/api/v1/users/{id}"))
            .header("if-match", "999")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "display_name": "Writer Updated",
                    "active": false
                })
                .to_string(),
            ))
            .expect("valid patch with stale if-match request"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_problem(&problem, 409, "Conflict");

    let (status, patched) = request_json(
        app.clone(),
        Request::builder()
            .method("PATCH")
            .uri(format!("/api/v1/users/{id}"))
            .header("if-match", version.to_string())
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "display_name": "Writer Updated",
                    "active": false
                })
                .to_string(),
            ))
            .expect("valid patch with current if-match request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        patched.get("display_name").and_then(Value::as_str),
        Some("Writer Updated")
    );
    assert_eq!(patched.get("active").and_then(Value::as_bool), Some(false));
    assert_eq!(patched.get("version").and_then(Value::as_i64), Some(2));

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/users/{id}"))
            .header("if-match", "1")
            .body(Body::empty())
            .expect("valid delete with stale if-match request"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_problem(&problem, 409, "Conflict");

    let (status, body) = request_json(
        app.clone(),
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/users/{id}"))
            .header("if-match", "2")
            .body(Body::empty())
            .expect("valid delete with current if-match request"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(body, Value::Null);

    let (status, problem) = request_json(
        app,
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}"))
            .body(Body::empty())
            .expect("valid get after delete request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_problem(&problem, 404, "Not found");
}

pub async fn run_write_users_contract_dx(app: Router) {
    let (status, created) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "write@example.com",
                    "display_name": "Writer"
                })
                .to_string(),
            ))
            .expect("valid write user create request"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let id = created
        .get("id")
        .and_then(Value::as_str)
        .expect("created user must include id")
        .to_string();
    let version = created
        .get("version")
        .and_then(Value::as_i64)
        .expect("created user must include version");
    assert_eq!(version, 1);

    let (status, patched) = request_json(
        app.clone(),
        Request::builder()
            .method("PATCH")
            .uri(format!("/api/v1/users/{id}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "display_name": "Writer Updated",
                    "active": false
                })
                .to_string(),
            ))
            .expect("valid patch request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        patched.get("display_name").and_then(Value::as_str),
        Some("Writer Updated")
    );
    assert_eq!(patched.get("active").and_then(Value::as_bool), Some(false));
    assert_eq!(patched.get("version").and_then(Value::as_i64), Some(2));

    let (status, body) = request_json(
        app.clone(),
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/users/{id}"))
            .body(Body::empty())
            .expect("valid delete request"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(body, Value::Null);

    let (status, problem) = request_json(
        app,
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}"))
            .body(Body::empty())
            .expect("valid get after delete request"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_problem(&problem, 404, "Not found");
}

pub async fn run_commands_contract(app: Router) {
    let (status, created) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "commands@example.com",
                    "display_name": "Commander"
                })
                .to_string(),
            ))
            .expect("valid command user create request"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let id = created
        .get("id")
        .and_then(Value::as_str)
        .expect("created user must include id")
        .to_string();
    let version = created
        .get("version")
        .and_then(Value::as_i64)
        .expect("created user must include version");
    assert_eq!(version, 1);

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri(format!("/api/v1/users/{id}/commands"))
            .header("content-type", "application/json")
            .body(Body::from(json!({ "command": "deactivate" }).to_string()))
            .expect("valid command request without if-match"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri(format!("/api/v1/users/{id}/commands"))
            .header("if-match", "999")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "command": "deactivate" }).to_string()))
            .expect("valid stale command request"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_problem(&problem, 409, "Conflict");

    let (status, deactivated) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri(format!("/api/v1/users/{id}/commands"))
            .header("if-match", "1")
            .header("content-type", "application/json")
            .body(Body::from(json!({ "command": "deactivate" }).to_string()))
            .expect("valid command deactivate request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        deactivated.get("active").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(deactivated.get("version").and_then(Value::as_i64), Some(2));

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}/events?limit=0"))
            .body(Body::empty())
            .expect("valid invalid limit request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let (status, first_events) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}/events?limit=10"))
            .body(Body::empty())
            .expect("valid list events request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first_items = first_events
        .get("items")
        .and_then(Value::as_array)
        .expect("events response must include items array");
    assert_eq!(first_items.len(), 1);
    assert_eq!(
        first_items[0].get("event_type").and_then(Value::as_str),
        Some("set_active")
    );

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users/commands/bulk-lifecycle")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "ids": [], "command": "activate" }).to_string(),
            ))
            .expect("valid bulk request with empty ids"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let missing_id = "00000000-0000-0000-0000-000000000000";
    let (status, bulk) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users/commands/bulk-lifecycle")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "ids": [id, missing_id],
                    "command": "activate"
                })
                .to_string(),
            ))
            .expect("valid bulk lifecycle request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bulk.get("requested").and_then(Value::as_u64), Some(2));
    assert_eq!(bulk.get("processed").and_then(Value::as_u64), Some(1));
    assert_eq!(
        bulk.get("command").and_then(Value::as_str),
        Some("activate")
    );

    let (status, activated) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}"))
            .body(Body::empty())
            .expect("valid get after bulk command request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(activated.get("active").and_then(Value::as_bool), Some(true));

    let (status, events) = request_json(
        app,
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}/events?limit=10"))
            .body(Body::empty())
            .expect("valid events request after bulk"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let items = events
        .get("items")
        .and_then(Value::as_array)
        .expect("events response must include items array");
    assert!(items.len() >= 2, "expected at least 2 events");

    let event_types = items
        .iter()
        .filter_map(|item| item.get("event_type").and_then(Value::as_str))
        .collect::<Vec<_>>();

    assert!(event_types.contains(&"set_active"));
    assert!(event_types.contains(&"bulk_set_active"));
}

pub async fn run_commands_contract_dx(app: Router) {
    let (status, created) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "email": "commands@example.com",
                    "display_name": "Commander"
                })
                .to_string(),
            ))
            .expect("valid command user create request"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let id = created
        .get("id")
        .and_then(Value::as_str)
        .expect("created user must include id")
        .to_string();
    let version = created
        .get("version")
        .and_then(Value::as_i64)
        .expect("created user must include version");
    assert_eq!(version, 1);

    let (status, deactivated) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri(format!("/api/v1/users/{id}/commands"))
            .header("content-type", "application/json")
            .body(Body::from(json!({ "command": "deactivate" }).to_string()))
            .expect("valid command deactivate request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        deactivated.get("active").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(deactivated.get("version").and_then(Value::as_i64), Some(2));

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}/events?limit=0"))
            .body(Body::empty())
            .expect("valid invalid limit request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let (status, first_events) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}/events?limit=10"))
            .body(Body::empty())
            .expect("valid list events request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first_items = first_events
        .get("items")
        .and_then(Value::as_array)
        .expect("events response must include items array");
    assert_eq!(first_items.len(), 1);
    assert_eq!(
        first_items[0].get("event_type").and_then(Value::as_str),
        Some("set_active")
    );

    let (status, problem) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users/commands/bulk-lifecycle")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "ids": [], "command": "activate" }).to_string(),
            ))
            .expect("valid bulk request with empty ids"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_problem(&problem, 400, "Validation failed");

    let missing_id = "00000000-0000-0000-0000-000000000000";
    let (status, bulk) = request_json(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/api/v1/users/commands/bulk-lifecycle")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "ids": [id, missing_id],
                    "command": "activate"
                })
                .to_string(),
            ))
            .expect("valid bulk lifecycle request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bulk.get("requested").and_then(Value::as_u64), Some(2));
    assert_eq!(bulk.get("processed").and_then(Value::as_u64), Some(1));
    assert_eq!(
        bulk.get("command").and_then(Value::as_str),
        Some("activate")
    );

    let (status, activated) = request_json(
        app.clone(),
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}"))
            .body(Body::empty())
            .expect("valid get after bulk command request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(activated.get("active").and_then(Value::as_bool), Some(true));

    let (status, events) = request_json(
        app,
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/users/{id}/events?limit=10"))
            .body(Body::empty())
            .expect("valid events request after bulk"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let items = events
        .get("items")
        .and_then(Value::as_array)
        .expect("events response must include items array");
    assert!(items.len() >= 2, "expected at least 2 events");

    let event_types = items
        .iter()
        .filter_map(|item| item.get("event_type").and_then(Value::as_str))
        .collect::<Vec<_>>();

    assert!(event_types.contains(&"set_active"));
    assert!(event_types.contains(&"bulk_set_active"));
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
