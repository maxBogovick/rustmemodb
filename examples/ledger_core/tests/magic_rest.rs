use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use rustmemodb::PersistApp;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

#[tokio::test]
async fn generated_rest_endpoints_work_without_manual_api_store_layers() {
    let data_dir = std::env::temp_dir().join(format!(
        "ledger_core_magic_rest_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    let app = PersistApp::open_auto(data_dir).await.expect("open app");
    let ledgers = app
        .serve_autonomous_model::<ledger_core::model::LedgerBook>("ledgers")
        .await
        .expect("serve autonomous model");
    let router = axum::Router::new().nest("/api/ledgers", ledgers);

    let response = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/api/ledgers",
            json!({
                "name": "Personal"
            }),
        ))
        .await
        .expect("create ledger response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = decode_json(response).await;
    let ledger_id = body
        .get("persist_id")
        .and_then(Value::as_str)
        .expect("persist_id")
        .to_string();

    let response = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "Cash",
                "currency": "USD",
                "opening_balance_minor": 1000,
                "note": "seed"
            }),
        ))
        .await
        .expect("open account response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = decode_json(response).await;
    let account_id = body
        .get("id")
        .and_then(Value::as_str)
        .expect("account id")
        .to_string();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/ledgers/{ledger_id}/_audits"))
                .body(Body::empty())
                .expect("audits_before request"),
        )
        .await
        .expect("audits_before response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = decode_json(response).await;
    let audits_before_reads_len = body.as_array().expect("audits_before array").len();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/ledgers/{ledger_id}/balance_report"))
                .body(Body::empty())
                .expect("view request"),
        )
        .await
        .expect("view response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = decode_json(response).await;
    let tx_count = body
        .get("transaction_count")
        .and_then(Value::as_u64)
        .expect("transaction_count");
    assert!(tx_count >= 1);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/api/ledgers/{ledger_id}/account_balance?account_id={account_id}"
                ))
                .body(Body::empty())
                .expect("account_balance request"),
        )
        .await
        .expect("account_balance response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = decode_json(response).await;
    let balance = body.as_i64().expect("balance");
    assert_eq!(balance, 1000);

    let response = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/account_balance_body"),
            json!({
                "account_id": account_id
            }),
        ))
        .await
        .expect("account_balance_body response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = decode_json(response).await;
    let balance = body.as_i64().expect("balance from body view");
    assert_eq!(balance, 1000);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/ledgers/{ledger_id}/_audits"))
                .body(Body::empty())
                .expect("audits request"),
        )
        .await
        .expect("audits response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = decode_json(response).await;
    let audits = body.as_array().expect("audits array");
    assert!(!audits.is_empty());
    assert_eq!(audits.len(), audits_before_reads_len);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/ledgers/_openapi.json")
                .body(Body::empty())
                .expect("openapi request"),
        )
        .await
        .expect("openapi response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = decode_json(response).await;
    assert_eq!(body.get("openapi").and_then(Value::as_str), Some("3.1.0"));
    let paths = body
        .get("paths")
        .and_then(Value::as_object)
        .expect("paths object");
    assert!(paths.contains_key("/{id}/open_account"));
    assert!(paths.contains_key("/{id}/balance_report"));
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
