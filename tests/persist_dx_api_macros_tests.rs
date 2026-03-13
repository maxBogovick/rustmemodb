use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower::ServiceExt;

#[domain(table = "macro_boards", schema_version = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct MacroBoard {
    name: String,
    active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, DomainError)]
enum MacroBoardError {
    #[api_error(status = 422, code = "validation_error")]
    Validation(String),
}

impl std::fmt::Display for MacroBoardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for MacroBoardError {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Validate)]
struct RenamePayload {
    #[validate(trim, non_empty, len_max = 32)]
    name: String,
}

#[api]
impl MacroBoard {
    pub fn new(name: String) -> Self {
        Self {
            name,
            active: false,
        }
    }

    #[command(validate = true)]
    pub fn rename(&mut self, payload: RenamePayload) -> Result<String, MacroBoardError> {
        self.name = payload.name;
        Ok(self.name.clone())
    }

    pub fn activate(&mut self) -> bool {
        self.active = true;
        self.active
    }

    pub fn current_name(&self) -> String {
        self.name.clone()
    }

    #[query]
    pub fn is_active(&self) -> bool {
        self.active
    }
}

#[test]
fn validate_derive_normalizes_and_rejects_invalid_payload() {
    let _ = MacroBoardError::Validation("sample".to_string());

    let mut valid = RenamePayload {
        name: "  Product Team  ".to_string(),
    };
    PersistInputValidate::normalize_and_validate(&mut valid).expect("valid payload");
    assert_eq!(valid.name, "Product Team");

    let mut invalid = RenamePayload {
        name: "   ".to_string(),
    };
    let error = PersistInputValidate::normalize_and_validate(&mut invalid)
        .expect_err("empty payload should be rejected");
    assert_eq!(error, "field 'name' must not be empty");
}

#[tokio::test]
async fn api_macro_generates_handle_methods_for_auto_command_and_query() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("dx_macro_handle"))
        .await
        .expect("open app");
    let boards = app
        .open_autonomous_model::<MacroBoard>("macro_boards_handle")
        .await
        .expect("open autonomous model");

    let created = boards
        .create_one(MacroBoard::new("Initial".to_string()))
        .await
        .expect("create board");

    let before = boards
        .get_one(&created.persist_id)
        .await
        .expect("get before activate");
    assert!(!before.model.active);

    let activated = boards
        .activate(&created.persist_id)
        .await
        .expect("activate command");
    assert!(activated);

    let renamed = boards
        .rename(
            &created.persist_id,
            RenamePayload {
                name: "Renamed".to_string(),
            },
        )
        .await
        .expect("rename command");
    assert_eq!(renamed, "Renamed");

    let after = boards
        .get_one(&created.persist_id)
        .await
        .expect("get after activate/rename");
    assert!(after.model.active);
    assert_eq!(after.model.name, "Renamed");
}

#[tokio::test]
async fn api_macro_rest_validate_returns_422_and_normalizes_payload() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("dx_macro_rest"))
        .await
        .expect("open app");
    let boards = app
        .open_autonomous_model::<MacroBoard>("macro_boards_rest")
        .await
        .expect("open autonomous model");

    let created = boards
        .create_one(MacroBoard::new("Initial".to_string()))
        .await
        .expect("create board");

    let router = app
        .serve_autonomous_model::<MacroBoard>("macro_boards_rest")
        .await
        .expect("serve router");

    let invalid = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            &format!("/{}/rename", created.persist_id),
            json!({ "name": "   " }),
        ))
        .await
        .expect("invalid response");
    assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let renamed = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            &format!("/{}/rename", created.persist_id),
            json!({ "name": "  Renamed  " }),
        ))
        .await
        .expect("rename response");
    assert_eq!(renamed.status(), StatusCode::OK);
    let renamed_body = decode_json(renamed).await;
    assert_eq!(renamed_body.as_str(), Some("Renamed"));

    let current_name = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/{}/current_name", created.persist_id))
                .body(Body::empty())
                .expect("current_name request"),
        )
        .await
        .expect("current_name response");
    assert_eq!(current_name.status(), StatusCode::OK);
    let current_name_body = decode_json(current_name).await;
    assert_eq!(current_name_body.as_str(), Some("Renamed"));

    let activate = router
        .clone()
        .oneshot(json_request(
            Method::POST,
            &format!("/{}/activate", created.persist_id),
            json!({}),
        ))
        .await
        .expect("activate response");
    assert_eq!(activate.status(), StatusCode::OK);
    let activate_body = decode_json(activate).await;
    assert_eq!(activate_body.as_bool(), Some(true));
}

#[tokio::test]
async fn api_macro_generated_list_endpoint_accepts_query_dsl_params() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("dx_macro_list_query"))
        .await
        .expect("open app");
    let boards = app
        .open_autonomous_model::<MacroBoard>("macro_boards_list_query")
        .await
        .expect("open autonomous model");

    let alpha = boards
        .create_one(MacroBoard::new("Alpha".to_string()))
        .await
        .expect("create alpha");
    let _ = boards
        .create_one(MacroBoard::new("Gamma".to_string()))
        .await
        .expect("create gamma");
    let _ = boards
        .create_one(MacroBoard::new("Beta".to_string()))
        .await
        .expect("create beta");
    boards
        .activate(&alpha.persist_id)
        .await
        .expect("activate alpha");

    let router = app
        .serve_autonomous_model::<MacroBoard>("macro_boards_list_query")
        .await
        .expect("serve router");

    let sorted_page = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/?sort=name&page=1&per_page=2")
                .body(Body::empty())
                .expect("list request"),
        )
        .await
        .expect("list response");
    assert_eq!(sorted_page.status(), StatusCode::OK);
    let sorted_body = decode_json(sorted_page).await;
    let sorted_items = sorted_body.as_array().expect("sorted array");
    assert_eq!(sorted_items.len(), 2);
    assert_eq!(
        sorted_items[0]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Alpha")
    );
    assert_eq!(
        sorted_items[1]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Beta")
    );

    let filtered = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/?active=true")
                .body(Body::empty())
                .expect("filtered list request"),
        )
        .await
        .expect("filtered list response");
    assert_eq!(filtered.status(), StatusCode::OK);
    let filtered_body = decode_json(filtered).await;
    let filtered_items = filtered_body.as_array().expect("filtered array");
    assert_eq!(filtered_items.len(), 1);
    assert_eq!(
        filtered_items[0]
            .get("model")
            .and_then(|model| model.get("name"))
            .and_then(Value::as_str),
        Some("Alpha")
    );
}

#[tokio::test]
async fn api_macro_generated_list_endpoint_rejects_invalid_query_dsl_params() {
    let temp = tempfile::tempdir().expect("temp dir");
    let app = PersistApp::open_auto(temp.path().join("dx_macro_list_query_invalid"))
        .await
        .expect("open app");
    let boards = app
        .open_autonomous_model::<MacroBoard>("macro_boards_list_query_invalid")
        .await
        .expect("open autonomous model");
    boards
        .create_one(MacroBoard::new("Alpha".to_string()))
        .await
        .expect("create alpha");

    let router = app
        .serve_autonomous_model::<MacroBoard>("macro_boards_list_query_invalid")
        .await
        .expect("serve router");

    let invalid_sort = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/?sort=:desc")
                .body(Body::empty())
                .expect("invalid sort request"),
        )
        .await
        .expect("invalid sort response");
    assert_eq!(invalid_sort.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let invalid_op = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/?score__between=10")
                .body(Body::empty())
                .expect("invalid op request"),
        )
        .await
        .expect("invalid op response");
    assert_eq!(invalid_op.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let invalid_page = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/?page=0")
                .body(Body::empty())
                .expect("invalid page request"),
        )
        .await
        .expect("invalid page response");
    assert_eq!(invalid_page.status(), StatusCode::UNPROCESSABLE_ENTITY);
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
