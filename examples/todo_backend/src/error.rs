use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("database error")]
    Database(#[from] sqlx::Error),
    #[error("migration error")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("internal server error")]
    Internal,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

impl AppError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub fn internal() -> Self {
        Self::Internal
    }

    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage(message.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Validation(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            Self::Conflict(msg) => (StatusCode::CONFLICT, msg),
            Self::Database(err) => match &err {
                sqlx::Error::Database(db_err) => {
                    if db_err.code().as_deref() == Some("23505") {
                        (StatusCode::CONFLICT, "resource already exists".to_string())
                    } else {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "database operation failed".to_string(),
                        )
                    }
                }
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database operation failed".to_string(),
                ),
            },
            Self::Migration(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "database migration failed".to_string(),
            ),
            Self::Storage(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            ),
        };

        (status, Json(ErrorBody { error: message })).into_response()
    }
}
