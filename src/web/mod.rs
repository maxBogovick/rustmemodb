//! Invisible Web Module
//!
//! This module provides the `#[api_service]` macro and runtime support for
//! generating "Invisible" HTTP APIs from pure Rust traits.
//!
//! Usage note:
//! - Annotate both the service trait and its impl with `#[async_trait::async_trait]`.
//! - This guarantees `Send` futures, which axum requires for generated handlers.

use crate::core::DbError;
use crate::persist::app::{PersistDomainError, PersistDomainMutationError};
use crate::persist::web::PersistServiceError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

// Re-export the macro
pub use rustmemodb_derive::api_service;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

#[derive(Debug)]
pub enum WebError {
    Database(DbError),
    Input(String),
    NotFound(String),
    Conflict(String),
    Internal(String),
    Custom {
        status: StatusCode,
        code: String,
        message: String,
    },
}

impl From<DbError> for WebError {
    fn from(err: DbError) -> Self {
        WebError::Database(err)
    }
}

impl From<PersistDomainError> for WebError {
    fn from(err: PersistDomainError) -> Self {
        match err {
            PersistDomainError::NotFound => Self::NotFound("entity not found".to_string()),
            PersistDomainError::ConflictConcurrent(message)
            | PersistDomainError::ConflictUnique(message) => Self::Conflict(message),
            PersistDomainError::Validation(message) => Self::Input(message),
            PersistDomainError::Internal(message) => Self::Internal(message),
        }
    }
}

impl<E> From<PersistDomainMutationError<E>> for WebError
where
    E: Into<PersistServiceError>,
{
    fn from(err: PersistDomainMutationError<E>) -> Self {
        Self::from(PersistServiceError::from(err))
    }
}

impl From<PersistServiceError> for WebError {
    fn from(err: PersistServiceError) -> Self {
        match err {
            PersistServiceError::NotFound(message) => Self::NotFound(message),
            PersistServiceError::Conflict(message) => Self::Conflict(message),
            PersistServiceError::Validation(message) => Self::Input(message),
            PersistServiceError::Internal(message) => Self::Internal(message),
            PersistServiceError::Custom {
                status,
                code,
                message,
            } => Self::Custom {
                status: StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                code,
                message,
            },
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let (status, message, code) = match self {
            WebError::Database(DbError::TableExists(msg)) => {
                (StatusCode::CONFLICT, msg, "conflict".to_string())
            }
            WebError::Database(DbError::ConstraintViolation(msg)) => {
                (StatusCode::CONFLICT, msg, "conflict".to_string())
            }
            WebError::Database(DbError::TableNotFound(msg)) => {
                (StatusCode::NOT_FOUND, msg, "not_found".to_string())
            }
            WebError::Database(DbError::ColumnNotFound(col, table)) => (
                StatusCode::NOT_FOUND,
                format!("Column '{}' not found in table '{}'", col, table),
                "not_found".to_string(),
            ),
            WebError::Database(DbError::ParseError(msg)) => {
                (StatusCode::BAD_REQUEST, msg, "parse_error".to_string())
            }
            WebError::Database(DbError::TypeMismatch(msg)) => {
                (StatusCode::BAD_REQUEST, msg, "type_mismatch".to_string())
            }
            WebError::Database(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
                "database_error".to_string(),
            ),

            WebError::Input(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                msg,
                "input_error".to_string(),
            ),
            WebError::NotFound(msg) => (StatusCode::NOT_FOUND, msg, "not_found".to_string()),
            WebError::Conflict(msg) => (StatusCode::CONFLICT, msg, "conflict".to_string()),
            WebError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                msg,
                "internal_error".to_string(),
            ),
            WebError::Custom {
                status,
                code,
                message,
            } => (status, message, code),
        };

        let body = Json(ErrorResponse {
            error: message,
            code,
        });

        (status, body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, WebError>;

#[cfg(test)]
mod tests {
    use super::WebError;
    use crate::persist::web::PersistServiceError;
    use axum::http::StatusCode;

    #[test]
    fn custom_service_error_maps_to_custom_web_error() {
        let mapped = WebError::from(PersistServiceError::custom(409, "domain_conflict", "boom"));
        match mapped {
            WebError::Custom {
                status,
                code,
                message,
            } => {
                assert_eq!(status, StatusCode::CONFLICT);
                assert_eq!(code, "domain_conflict");
                assert_eq!(message, "boom");
            }
            other => panic!("expected custom web error, got {other:?}"),
        }
    }
}
