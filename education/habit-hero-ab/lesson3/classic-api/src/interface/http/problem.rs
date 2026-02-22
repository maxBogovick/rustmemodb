use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use uuid::Uuid;

use crate::domain::errors::DomainError;

pub type ApiResult<T> = Result<T, ApiProblem>;

#[derive(Debug)]
pub struct ApiProblem {
    status: StatusCode,
    title: &'static str,
    detail: String,
    kind: &'static str,
    correlation_id: String,
}

impl ApiProblem {
    pub fn from_domain(error: DomainError) -> Self {
        match error {
            DomainError::Validation(detail) => Self::new(
                StatusCode::BAD_REQUEST,
                "Validation failed",
                "https://habithero.dev/problems/validation",
                detail,
            ),
            DomainError::NotFound(detail) => Self::new(
                StatusCode::NOT_FOUND,
                "Not found",
                "https://habithero.dev/problems/not-found",
                detail,
            ),
            DomainError::Conflict(detail) => Self::new(
                StatusCode::CONFLICT,
                "Conflict",
                "https://habithero.dev/problems/conflict",
                detail,
            ),
            DomainError::Storage(detail) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Storage error",
                "https://habithero.dev/problems/storage",
                detail,
            ),
            DomainError::Internal(detail) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error",
                "https://habithero.dev/problems/internal",
                detail,
            ),
        }
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "https://habithero.dev/problems/internal",
            detail,
        )
    }

    fn new(
        status: StatusCode,
        title: &'static str,
        kind: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            status,
            title,
            detail: detail.into(),
            kind,
            correlation_id: Uuid::new_v4().to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ProblemDetails {
    #[serde(rename = "type")]
    kind: String,
    title: String,
    status: u16,
    detail: String,
    correlation_id: String,
}

impl IntoResponse for ApiProblem {
    fn into_response(self) -> Response {
        let payload = ProblemDetails {
            kind: self.kind.to_string(),
            title: self.title.to_string(),
            status: self.status.as_u16(),
            detail: self.detail,
            correlation_id: self.correlation_id,
        };

        let mut response = (self.status, Json(payload)).into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );

        response
    }
}
