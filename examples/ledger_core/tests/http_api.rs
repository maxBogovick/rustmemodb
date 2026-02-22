use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use ledger_core::model::LedgerBook;
use rustmemodb::PersistApp;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

#[tokio::test]
async fn same_currency_transfer_updates_balances_and_reports() {
    let app = TestApp::new().await;

    let (status, created_ledger) = request_json(
        &app.router,
        json_request(Method::POST, "/api/ledgers", json!({ "name": "Household" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let ledger_id = record_id(&created_ledger);

    let (_, account_a) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "Main Wallet",
                "currency": "USD",
                "opening_balance_minor": 10_000,
                "note": null
            }),
        ),
    )
    .await;
    let account_a_id = json_string_field(&account_a, "id");

    let (_, account_b) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "Savings",
                "currency": "USD",
                "opening_balance_minor": 2_500,
                "note": null
            }),
        ),
    )
    .await;
    let account_b_id = json_string_field(&account_b, "id");

    let (status, transfer) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/create_transfer"),
            json!({
                "from_account_id": account_a_id,
                "to_account_id": account_b_id,
                "amount_minor": 1_500,
                "to_amount_minor": null,
                "note": "Pay monthly savings"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_string_field(&transfer, "kind"), "transfer");

    let (status, ledger) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/ledgers/{ledger_id}"))
            .body(Body::empty())
            .expect("get ledger request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let accounts = record_model(&ledger)
        .get("accounts")
        .and_then(Value::as_array)
        .expect("accounts must be array");
    let main_wallet = accounts
        .iter()
        .find(|account| json_string_field(account, "owner_name") == "Main Wallet")
        .expect("main wallet account");
    let savings = accounts
        .iter()
        .find(|account| json_string_field(account, "owner_name") == "Savings")
        .expect("savings account");

    assert_eq!(json_i64_field(main_wallet, "balance_minor"), 8_500);
    assert_eq!(json_i64_field(savings, "balance_minor"), 4_000);

    let (status, balances) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/ledgers/{ledger_id}/balance_report"))
            .body(Body::empty())
            .expect("get balances request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(balances["all_balances_consistent"], json!(true));

    let currency_lines = balances
        .get("currency_lines")
        .and_then(Value::as_array)
        .expect("currency lines must be array");
    assert_eq!(currency_lines.len(), 1);
    assert_eq!(json_string_field(&currency_lines[0], "currency"), "USD");
    assert_eq!(json_i64_field(&currency_lines[0], "total_minor"), 12_500);

    let transactions = record_model(&ledger)
        .get("transactions")
        .and_then(Value::as_array)
        .expect("transactions array");
    assert_eq!(transactions.len(), 3);
}

#[tokio::test]
async fn cross_currency_transfer_requires_to_amount_and_remains_atomic_on_failure() {
    let app = TestApp::new().await;

    let (_, ledger) = request_json(
        &app.router,
        json_request(Method::POST, "/api/ledgers", json!({ "name": "Travel" })),
    )
    .await;
    let ledger_id = record_id(&ledger);

    let (_, usd_account) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "USD Wallet",
                "currency": "USD",
                "opening_balance_minor": 10_000,
                "note": null
            }),
        ),
    )
    .await;
    let usd_account_id = json_string_field(&usd_account, "id");

    let (_, eur_account) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "EUR Wallet",
                "currency": "EUR",
                "opening_balance_minor": 0,
                "note": null
            }),
        ),
    )
    .await;
    let eur_account_id = json_string_field(&eur_account, "id");

    let (status, _) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/create_transfer"),
            json!({
                "from_account_id": usd_account_id,
                "to_account_id": eur_account_id,
                "amount_minor": 1_000,
                "to_amount_minor": null,
                "note": null
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, ledger_after_failure) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/ledgers/{ledger_id}"))
            .body(Body::empty())
            .expect("get ledger request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let accounts_after_failure = record_model(&ledger_after_failure)
        .get("accounts")
        .and_then(Value::as_array)
        .expect("accounts array");
    let usd_after_failure = accounts_after_failure
        .iter()
        .find(|account| json_string_field(account, "owner_name") == "USD Wallet")
        .expect("USD account");
    let eur_after_failure = accounts_after_failure
        .iter()
        .find(|account| json_string_field(account, "owner_name") == "EUR Wallet")
        .expect("EUR account");

    assert_eq!(json_i64_field(usd_after_failure, "balance_minor"), 10_000);
    assert_eq!(json_i64_field(eur_after_failure, "balance_minor"), 0);

    let (status, _) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/create_transfer"),
            json!({
                "from_account_id": json_string_field(&usd_account, "id"),
                "to_account_id": json_string_field(&eur_account, "id"),
                "amount_minor": 1_000,
                "to_amount_minor": 920,
                "note": "FX conversion"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, balances) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/ledgers/{ledger_id}/balance_report"))
            .body(Body::empty())
            .expect("balances request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let account_lines = balances
        .get("account_lines")
        .and_then(Value::as_array)
        .expect("account lines array");
    let usd_line = account_lines
        .iter()
        .find(|line| json_string_field(line, "currency") == "USD")
        .expect("USD line");
    let eur_line = account_lines
        .iter()
        .find(|line| json_string_field(line, "currency") == "EUR")
        .expect("EUR line");

    assert_eq!(json_i64_field(usd_line, "balance_minor"), 9_000);
    assert_eq!(json_i64_field(eur_line, "balance_minor"), 920);

    let (status, audits) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/ledgers/{ledger_id}/_audits"))
            .body(Body::empty())
            .expect("audits request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let audits = audits.as_array().expect("audits array");
    assert!(!audits.is_empty());
    let has_transfer_event = audits
        .iter()
        .any(|event| event.to_string().contains("transfer"));
    assert!(has_transfer_event);
}

#[tokio::test]
async fn insufficient_funds_returns_conflict_without_partial_write() {
    let app = TestApp::new().await;

    let (_, ledger) = request_json(
        &app.router,
        json_request(Method::POST, "/api/ledgers", json!({ "name": "Budget" })),
    )
    .await;
    let ledger_id = record_id(&ledger);

    let (_, source) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "Daily",
                "currency": "USD",
                "opening_balance_minor": 100,
                "note": null
            }),
        ),
    )
    .await;

    let (_, target) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "Reserve",
                "currency": "USD",
                "opening_balance_minor": 0,
                "note": null
            }),
        ),
    )
    .await;

    let (status, error) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/create_transfer"),
            json!({
                "from_account_id": json_string_field(&source, "id"),
                "to_account_id": json_string_field(&target, "id"),
                "amount_minor": 999,
                "to_amount_minor": null,
                "note": null
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        error.get("code").and_then(Value::as_str),
        Some("insufficient_funds")
    );

    let (status, ledger_after) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/ledgers/{ledger_id}"))
            .body(Body::empty())
            .expect("get ledger request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let accounts = record_model(&ledger_after)
        .get("accounts")
        .and_then(Value::as_array)
        .expect("accounts array");
    let daily = accounts
        .iter()
        .find(|account| json_string_field(account, "owner_name") == "Daily")
        .expect("daily account");
    let reserve = accounts
        .iter()
        .find(|account| json_string_field(account, "owner_name") == "Reserve")
        .expect("reserve account");

    assert_eq!(json_i64_field(daily, "balance_minor"), 100);
    assert_eq!(json_i64_field(reserve, "balance_minor"), 0);

    let transactions = record_model(&ledger_after)
        .get("transactions")
        .and_then(Value::as_array)
        .expect("transactions array");
    assert_eq!(transactions.len(), 1);
}

#[tokio::test]
async fn idempotency_key_replays_transfer_without_double_spend() {
    let app = TestApp::new().await;

    let (_, created_ledger) = request_json(
        &app.router,
        json_request(
            Method::POST,
            "/api/ledgers",
            json!({ "name": "Idempotent" }),
        ),
    )
    .await;
    let ledger_id = record_id(&created_ledger);

    let (_, source_account) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "Source",
                "currency": "USD",
                "opening_balance_minor": 10_000,
                "note": null
            }),
        ),
    )
    .await;
    let source_id = json_string_field(&source_account, "id");

    let (_, target_account) = request_json(
        &app.router,
        json_request(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/open_account"),
            json!({
                "owner_name": "Target",
                "currency": "USD",
                "opening_balance_minor": 500,
                "note": null
            }),
        ),
    )
    .await;
    let target_id = json_string_field(&target_account, "id");

    let transfer_payload = json!({
        "from_account_id": source_id,
        "to_account_id": target_id,
        "amount_minor": 1_500,
        "to_amount_minor": null,
        "note": "idempotent transfer"
    });

    let (status, first_transfer) = request_json(
        &app.router,
        json_request_with_idempotency(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/create_transfer"),
            transfer_payload.clone(),
            "transfer-key-1",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, replay_transfer) = request_json(
        &app.router,
        json_request_with_idempotency(
            Method::POST,
            &format!("/api/ledgers/{ledger_id}/create_transfer"),
            transfer_payload,
            "transfer-key-1",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_transfer, replay_transfer);

    let (status, ledger_after) = request_json(
        &app.router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/api/ledgers/{ledger_id}"))
            .body(Body::empty())
            .expect("get ledger request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let accounts = record_model(&ledger_after)
        .get("accounts")
        .and_then(Value::as_array)
        .expect("accounts array");
    let source_after = accounts
        .iter()
        .find(|account| json_string_field(account, "owner_name") == "Source")
        .expect("source account");
    let target_after = accounts
        .iter()
        .find(|account| json_string_field(account, "owner_name") == "Target")
        .expect("target account");
    assert_eq!(json_i64_field(source_after, "balance_minor"), 8_500);
    assert_eq!(json_i64_field(target_after, "balance_minor"), 2_000);

    let transactions = record_model(&ledger_after)
        .get("transactions")
        .and_then(Value::as_array)
        .expect("transactions array");
    assert_eq!(transactions.len(), 3);
}

struct TestApp {
    _temp_dir: TempDir,
    _app: PersistApp,
    router: Router,
}

impl TestApp {
    async fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let app = PersistApp::open_auto(temp_dir.path().to_path_buf())
            .await
            .expect("open persist app");
        let ledgers = app
            .serve_autonomous_model::<LedgerBook>("ledgers")
            .await
            .expect("serve autonomous model");
        let router = Router::new().nest("/api/ledgers", ledgers);

        Self {
            _temp_dir: temp_dir,
            _app: app,
            router,
        }
    }
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

async fn request_json(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("router response");
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

    let parsed = serde_json::from_slice::<Value>(&bytes).expect("json body");
    (status, parsed)
}

fn record_id(record: &Value) -> String {
    json_string_field(record, "persist_id")
}

fn record_model(record: &Value) -> &Value {
    record
        .get("model")
        .unwrap_or_else(|| panic!("expected model field in record: {record}"))
}

fn json_string_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("expected string field '{key}' in {value}"))
        .to_string()
}

fn json_i64_field(value: &Value, key: &str) -> i64 {
    value
        .get(key)
        .and_then(Value::as_i64)
        .unwrap_or_else(|| panic!("expected i64 field '{key}' in {value}"))
}
