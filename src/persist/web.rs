//! High-level web adapter primitives for persistence-facing HTTP handlers.
//!
//! The goal of this module is to remove repeated parsing/mapping boilerplate
//! in application handlers while keeping the underlying web framework optional.

use crate::core::DbError;
use crate::persist::app::{
    ManagedConflictKind, PersistDomainError, PersistDomainMutationError, classify_managed_conflict,
};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use std::fmt;

/// Validation message returned when `If-Match` header is missing.
pub const IF_MATCH_REQUIRED_MESSAGE: &str = "If-Match header is required";
/// Validation message returned when `If-Match` header is not valid ASCII.
pub const IF_MATCH_INVALID_ASCII_MESSAGE: &str = "If-Match header must be valid ASCII";
/// Validation message returned when `If-Match` does not contain a positive integer.
pub const IF_MATCH_INVALID_VERSION_MESSAGE: &str =
    "If-Match header must contain a positive integer version";
/// Validation message returned when idempotency key contains invalid characters.
pub const IDEMPOTENCY_KEY_INVALID_MESSAGE: &str = "Idempotency-Key must be valid ASCII";
/// Validation message returned when idempotency key is too long.
pub const IDEMPOTENCY_KEY_TOO_LONG_MESSAGE: &str = "Idempotency-Key must not exceed 128 characters";

/// Upper bound for normalized idempotency keys.
pub const IDEMPOTENCY_KEY_MAX_LEN: usize = 128;

/// Input-validation error for web adapter helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistWebInputError {
    message: &'static str,
}

impl PersistWebInputError {
    fn new(message: &'static str) -> Self {
        Self { message }
    }

    /// Returns a stable message suitable for HTTP validation responses.
    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for PersistWebInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for PersistWebInputError {}

/// Framework-agnostic problem mapping payload for persistence conflicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistWebProblem {
    /// HTTP status code.
    pub status: u16,
    /// Short title intended for problem-details responses.
    pub title: &'static str,
    /// Stable machine-readable code.
    pub code: &'static str,
}

/// High-level service error intended for application layers.
///
/// This keeps app services and handlers independent from low-level persistence
/// errors while preserving stable HTTP mapping semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistServiceError {
    /// Requested domain object is absent.
    NotFound(String),
    /// Concurrent/uniqueness conflict.
    Conflict(String),
    /// Business validation/input error.
    Validation(String),
    /// Infrastructure/runtime failure.
    Internal(String),
    /// Explicit status/code mapping supplied by domain-level API error derive.
    Custom {
        /// HTTP status code returned to the client.
        status: u16,
        /// Stable machine-readable problem code.
        code: String,
        /// Human-readable error message.
        message: String,
    },
}

impl PersistServiceError {
    /// Creates a standardized not-found error with entity context.
    pub fn not_found(entity: &str, id: impl fmt::Display) -> Self {
        Self::NotFound(format!("{entity} not found: {id}"))
    }

    /// Creates a validation error.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    /// Creates a fully explicit service error with custom HTTP status and code.
    pub fn custom(status: u16, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Custom {
            status,
            code: code.into(),
            message: message.into(),
        }
    }

    /// Maps a domain error and rewrites `NotFound` with entity context.
    pub fn from_domain_for(entity: &str, id: impl fmt::Display, err: PersistDomainError) -> Self {
        match err {
            PersistDomainError::NotFound => Self::not_found(entity, id),
            other => other.into(),
        }
    }

    /// Maps a mutation error and rewrites domain `NotFound` with entity context.
    ///
    /// User errors (`E`) are converted through `Into<PersistServiceError>`, so
    /// business modules can provide explicit status semantics once and reuse them.
    pub fn from_mutation_for<E>(
        entity: &str,
        id: impl fmt::Display,
        err: PersistDomainMutationError<E>,
    ) -> Self
    where
        E: Into<PersistServiceError>,
    {
        match err {
            PersistDomainMutationError::Domain(domain) => Self::from_domain_for(entity, id, domain),
            PersistDomainMutationError::User(user) => user.into(),
        }
    }
}

impl fmt::Display for PersistServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(message)
            | Self::Conflict(message)
            | Self::Validation(message)
            | Self::Internal(message) => write!(f, "{message}"),
            Self::Custom { message, .. } => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for PersistServiceError {}

impl From<std::convert::Infallible> for PersistServiceError {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

impl From<PersistDomainError> for PersistServiceError {
    fn from(value: PersistDomainError) -> Self {
        match value {
            PersistDomainError::NotFound => Self::NotFound("entity not found".to_string()),
            PersistDomainError::ConflictConcurrent(message)
            | PersistDomainError::ConflictUnique(message) => Self::Conflict(message),
            PersistDomainError::Validation(message) => Self::Validation(message),
            PersistDomainError::Internal(message) => Self::Internal(message),
        }
    }
}

impl<E> From<PersistDomainMutationError<E>> for PersistServiceError
where
    E: Into<PersistServiceError>,
{
    fn from(value: PersistDomainMutationError<E>) -> Self {
        match value {
            PersistDomainMutationError::Domain(domain) => Self::from(domain),
            PersistDomainMutationError::User(user) => user.into(),
        }
    }
}

/// Request input location used in generated OpenAPI descriptors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistOpenApiInputLocation {
    /// Request payload is encoded in the URL query-string.
    Query,
    /// Request payload is encoded in the request body.
    Body,
}

/// One generated REST operation descriptor for OpenAPI emission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistOpenApiOperation {
    /// Lowercase HTTP method (`get`, `post`, ...).
    pub method: &'static str,
    /// Route path mounted by generated router (for example `/{id}/transfer`).
    pub path: &'static str,
    /// Stable operation identifier.
    pub operation_id: &'static str,
    /// Human-readable summary.
    pub summary: &'static str,
    /// Optional request rust type name.
    pub request_rust_type: Option<&'static str>,
    /// Optional request payload location.
    pub request_location: Option<PersistOpenApiInputLocation>,
    /// Optional successful response rust type name.
    pub response_rust_type: Option<&'static str>,
    /// Successful status code.
    pub success_status: u16,
    /// Whether operation supports `Idempotency-Key` header.
    pub idempotent: bool,
}

/// Builds an OpenAPI 3.1 document from generated autonomous operation metadata.
///
/// This keeps router generation macro output compact while still exposing a
/// self-describing API surface for commands and queries.
pub fn build_autonomous_openapi_document(
    title: &str,
    operations: &[PersistOpenApiOperation],
) -> JsonValue {
    let mut paths = JsonMap::<String, JsonValue>::new();
    let mut schemas = JsonMap::<String, JsonValue>::new();
    schemas.insert(
        "PersistWebError".to_string(),
        json!({
            "type": "object",
            "properties": {
                "error": {"type": "string"},
                "code": {"type": "string"}
            },
            "required": ["error", "code"]
        }),
    );

    for operation in operations {
        let mut op_obj = JsonMap::<String, JsonValue>::new();
        op_obj.insert("operationId".to_string(), json!(operation.operation_id));
        op_obj.insert("summary".to_string(), json!(operation.summary));

        let mut parameters = Vec::<JsonValue>::new();
        if operation.idempotent {
            parameters.push(json!({
                "name": "Idempotency-Key",
                "in": "header",
                "required": false,
                "schema": {
                    "type": "string",
                    "maxLength": IDEMPOTENCY_KEY_MAX_LEN
                }
            }));
        }

        if let (Some(request_rust_type), Some(location)) =
            (operation.request_rust_type, operation.request_location)
        {
            let component_name = register_openapi_component(&mut schemas, request_rust_type);
            let schema_ref = json!({
                "$ref": format!("#/components/schemas/{component_name}")
            });
            match location {
                PersistOpenApiInputLocation::Body => {
                    op_obj.insert(
                        "requestBody".to_string(),
                        json!({
                            "required": true,
                            "content": {
                                "application/json": {
                                    "schema": schema_ref
                                }
                            }
                        }),
                    );
                }
                PersistOpenApiInputLocation::Query => {
                    parameters.push(json!({
                        "name": "params",
                        "in": "query",
                        "required": true,
                        "schema": schema_ref
                    }));
                }
            }
        }

        if !parameters.is_empty() {
            op_obj.insert("parameters".to_string(), JsonValue::Array(parameters));
        }

        let mut responses = JsonMap::<String, JsonValue>::new();
        if let Some(response_rust_type) =
            operation.response_rust_type.filter(|ty| !is_unit_type(ty))
        {
            let component_name = register_openapi_component(&mut schemas, response_rust_type);
            responses.insert(
                operation.success_status.to_string(),
                json!({
                    "description": "Success",
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": format!("#/components/schemas/{component_name}")
                            }
                        }
                    }
                }),
            );
        } else {
            responses.insert(
                operation.success_status.to_string(),
                json!({"description": "Success"}),
            );
        }

        for (status, description) in [
            ("404", "Not Found"),
            ("409", "Conflict"),
            ("422", "Validation Error"),
            ("500", "Internal Error"),
        ] {
            responses.insert(
                status.to_string(),
                json!({
                    "description": description,
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": "#/components/schemas/PersistWebError"
                            }
                        }
                    }
                }),
            );
        }
        op_obj.insert("responses".to_string(), JsonValue::Object(responses));

        let path_item = paths
            .entry(operation.path.to_string())
            .or_insert_with(|| JsonValue::Object(JsonMap::new()));
        if let JsonValue::Object(path_methods) = path_item {
            path_methods.insert(
                operation.method.to_ascii_lowercase(),
                JsonValue::Object(op_obj),
            );
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": title,
            "version": "1.0.0"
        },
        "paths": JsonValue::Object(paths),
        "components": {
            "schemas": JsonValue::Object(schemas)
        }
    })
}

/// Returns an OpenAPI schema object inferred from a Rust type-name string.
pub fn openapi_schema_for_rust_type_name(rust_type: &str) -> JsonValue {
    let normalized = rust_type.replace(' ', "");
    if normalized == "()" {
        return json!({"type": "null"});
    }
    if matches!(
        normalized.as_str(),
        "String" | "&str" | "str" | "std::string::String"
    ) {
        return json!({"type": "string"});
    }
    if matches!(normalized.as_str(), "bool" | "std::primitive::bool") {
        return json!({"type": "boolean"});
    }
    if is_integer_type(&normalized) {
        return json!({"type": "integer"});
    }
    if is_float_type(&normalized) {
        return json!({"type": "number"});
    }
    if let Some(inner) = generic_inner(&normalized, "Option") {
        let mut schema = openapi_schema_for_rust_type_name(inner);
        if let JsonValue::Object(ref mut obj) = schema {
            obj.insert("nullable".to_string(), json!(true));
        }
        return schema;
    }
    if let Some(inner) = generic_inner(&normalized, "Vec") {
        return json!({
            "type": "array",
            "items": openapi_schema_for_rust_type_name(inner)
        });
    }

    json!({
        "type": "object",
        "additionalProperties": true,
        "x-rust-type": rust_type
    })
}

fn register_openapi_component(schemas: &mut JsonMap<String, JsonValue>, rust_type: &str) -> String {
    let component_name = openapi_component_name_for_rust_type(rust_type);
    schemas
        .entry(component_name.clone())
        .or_insert_with(|| openapi_schema_for_rust_type_name(rust_type));
    component_name
}

fn openapi_component_name_for_rust_type(rust_type: &str) -> String {
    let mut result = String::new();
    for ch in rust_type.chars() {
        if ch.is_ascii_alphanumeric() {
            result.push(ch);
        } else if !result.ends_with('_') {
            result.push('_');
        }
    }
    let trimmed = result.trim_matches('_');
    if trimmed.is_empty() {
        "Type".to_string()
    } else {
        trimmed.to_string()
    }
}

fn is_unit_type(rust_type: &str) -> bool {
    rust_type.replace(' ', "") == "()"
}

fn generic_inner<'a>(normalized: &'a str, generic: &str) -> Option<&'a str> {
    let prefix = format!("{generic}<");
    if !normalized.starts_with(&prefix) || !normalized.ends_with('>') {
        return None;
    }
    Some(&normalized[prefix.len()..normalized.len() - 1])
}

fn is_integer_type(normalized: &str) -> bool {
    matches!(
        normalized,
        "i8" | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "std::primitive::i8"
            | "std::primitive::i16"
            | "std::primitive::i32"
            | "std::primitive::i64"
            | "std::primitive::i128"
            | "std::primitive::isize"
            | "std::primitive::u8"
            | "std::primitive::u16"
            | "std::primitive::u32"
            | "std::primitive::u64"
            | "std::primitive::u128"
            | "std::primitive::usize"
    )
}

fn is_float_type(normalized: &str) -> bool {
    matches!(
        normalized,
        "f32" | "f64" | "std::primitive::f32" | "std::primitive::f64"
    )
}

/// Parses `If-Match` header value into optimistic-lock version.
///
/// Accepts optional quotes and surrounding whitespace, e.g. `"5"` or `5`.
pub fn parse_if_match_header(raw_if_match: Option<&str>) -> Result<i64, PersistWebInputError> {
    let Some(raw_if_match) = raw_if_match else {
        return Err(PersistWebInputError::new(IF_MATCH_REQUIRED_MESSAGE));
    };

    let normalized = raw_if_match.trim().trim_matches('"');
    let expected_version = normalized
        .parse::<i64>()
        .map_err(|_| PersistWebInputError::new(IF_MATCH_INVALID_VERSION_MESSAGE))?;

    if expected_version <= 0 {
        return Err(PersistWebInputError::new(IF_MATCH_INVALID_VERSION_MESSAGE));
    }

    Ok(expected_version)
}

/// Normalizes optional idempotency key.
///
/// Empty/whitespace values become `None`.
pub fn normalize_idempotency_key(
    raw_key: Option<&str>,
) -> Result<Option<String>, PersistWebInputError> {
    let Some(raw_key) = raw_key else {
        return Ok(None);
    };

    let trimmed = raw_key.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if !trimmed.is_ascii() {
        return Err(PersistWebInputError::new(IDEMPOTENCY_KEY_INVALID_MESSAGE));
    }

    if trimmed.len() > IDEMPOTENCY_KEY_MAX_LEN {
        return Err(PersistWebInputError::new(IDEMPOTENCY_KEY_TOO_LONG_MESSAGE));
    }

    Ok(Some(trimmed.to_string()))
}

/// Normalizes optional request-correlation identifier.
///
/// Empty/whitespace values become `None`.
pub fn normalize_request_id(raw_request_id: Option<&str>) -> Option<String> {
    raw_request_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

/// Maps persistence conflict errors to framework-agnostic problem metadata.
pub fn map_conflict_problem(err: &DbError) -> Option<PersistWebProblem> {
    let kind = classify_managed_conflict(err)?;
    let problem = match kind {
        ManagedConflictKind::OptimisticLock => PersistWebProblem {
            status: 409,
            title: "Optimistic lock conflict",
            code: "optimistic_lock_conflict",
        },
        ManagedConflictKind::UniqueConstraint => PersistWebProblem {
            status: 409,
            title: "Unique constraint conflict",
            code: "unique_key_conflict",
        },
        ManagedConflictKind::WriteWrite => PersistWebProblem {
            status: 409,
            title: "Concurrent write conflict",
            code: "write_write_conflict",
        },
    };
    Some(problem)
}

#[cfg(test)]
mod tests {
    use super::{
        PersistOpenApiInputLocation, PersistOpenApiOperation, PersistServiceError,
        build_autonomous_openapi_document, parse_if_match_header,
    };
    use crate::persist::app::{PersistDomainError, PersistDomainMutationError};

    #[test]
    fn parse_if_match_accepts_positive_version() {
        let version = parse_if_match_header(Some("\"7\"")).expect("must parse");
        assert_eq!(version, 7);
    }

    #[test]
    fn service_error_maps_domain_not_found_with_context() {
        let mapped =
            PersistServiceError::from_domain_for("board", "b-1", PersistDomainError::NotFound);
        assert_eq!(
            mapped,
            PersistServiceError::NotFound("board not found: b-1".to_string())
        );
    }

    #[test]
    fn service_error_maps_user_mutation_error_to_validation_by_default() {
        let mapped = PersistServiceError::from(PersistDomainMutationError::User(
            PersistServiceError::validation("bad input"),
        ));
        assert_eq!(
            mapped,
            PersistServiceError::Validation("bad input".to_string())
        );
    }

    #[test]
    fn openapi_builder_includes_generated_command_path() {
        let doc = build_autonomous_openapi_document(
            "Test",
            &[PersistOpenApiOperation {
                method: "post",
                path: "/{id}/transfer",
                operation_id: "transfer",
                summary: "transfer",
                request_rust_type: Some("CreateTransferInput"),
                request_location: Some(PersistOpenApiInputLocation::Body),
                response_rust_type: Some("TransferOutput"),
                success_status: 200,
                idempotent: true,
            }],
        );

        let paths = doc
            .get("paths")
            .and_then(serde_json::Value::as_object)
            .expect("paths");
        assert!(paths.contains_key("/{id}/transfer"));
    }
}
