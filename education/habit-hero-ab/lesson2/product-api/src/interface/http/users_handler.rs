use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use uuid::Uuid;

use crate::{
    application::dto::{
        CreateUserRequest, HealthResponse, ListUsersQueryRequest, PaginatedUsersResponse,
        UserResponse,
    },
    domain::errors::DomainError,
    interface::http::problem::{ApiProblem, ApiResult},
    state::AppState,
};

pub async fn healthcheck() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub async fn create_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateUserRequest>,
) -> ApiResult<(StatusCode, Json<UserResponse>)> {
    let correlation_id = request_correlation_id(&headers);
    let created = state
        .user_service
        .create_user(request)
        .await
        .map_err(|error| ApiProblem::from_domain_with_correlation(error, correlation_id))?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<UserResponse>> {
    let correlation_id = request_correlation_id(&headers);
    let user_id = parse_uuid(&id, correlation_id.clone())?;
    let user = state
        .user_service
        .get_user(user_id)
        .await
        .map_err(|error| ApiProblem::from_domain_with_correlation(error, correlation_id))?;
    Ok(Json(user))
}

pub async fn list_users(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListUsersQueryRequest>,
) -> ApiResult<Json<PaginatedUsersResponse>> {
    let correlation_id = request_correlation_id(&headers);
    let users = state
        .user_service
        .list_users(query)
        .await
        .map_err(|error| ApiProblem::from_domain_with_correlation(error, correlation_id))?;
    Ok(Json(users))
}

fn request_correlation_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn parse_uuid(raw: &str, correlation_id: Option<String>) -> ApiResult<Uuid> {
    Uuid::parse_str(raw).map_err(|_| {
        ApiProblem::from_domain_with_correlation(
            DomainError::validation("id must be a valid UUID string"),
            correlation_id,
        )
    })
}
