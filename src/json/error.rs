//! JSON-specific error types
//!
//! Provides domain-specific errors for JSON operations with clear
//! error messages and context.

use crate::core::DbError;
use thiserror::Error;

pub type JsonResult<T> = Result<T, JsonError>;

#[derive(Debug, Error)]
pub enum JsonError {
    #[error("JSON parse error: {0}")]
    ParseError(String),

    #[error("Schema inference error: {0}")]
    SchemaInferenceError(String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Invalid collection name: {0}")]
    InvalidCollectionName(String),

    #[error("Empty document array")]
    EmptyDocument,

    #[error("SQL validation error: {0}")]
    ValidationError(String),

    #[error("SQL injection attempt detected: {0}")]
    SqlInjectionAttempt(String),

    #[error("Database error: {0}")]
    DatabaseError(#[from] DbError),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid document structure: {0}")]
    InvalidStructure(String),
}

impl From<serde_json::Error> for JsonError {
    fn from(err: serde_json::Error) -> Self {
        JsonError::ParseError(err.to_string())
    }
}
