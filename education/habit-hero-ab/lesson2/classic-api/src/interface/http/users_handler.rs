use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
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
    Json(request): Json<CreateUserRequest>,
) -> ApiResult<(StatusCode, Json<UserResponse>)> {
    let created = state
        .user_service
        .create_user(request)
        .await
        .map_err(ApiProblem::from_domain)?;

    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_user(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<UserResponse>> {
    let user_id = parse_uuid(&id)?;
    let user = state
        .user_service
        .get_user(user_id)
        .await
        .map_err(ApiProblem::from_domain)?;
    Ok(Json(user))
}

pub async fn list_users(
    State(state): State<AppState>,
    Query(query): Query<ListUsersQueryRequest>,
) -> ApiResult<Json<PaginatedUsersResponse>> {
    let users = state
        .user_service
        .list_users(query)
        .await
        .map_err(ApiProblem::from_domain)?;
    Ok(Json(users))
}

fn parse_uuid(raw: &str) -> ApiResult<Uuid> {
    Uuid::parse_str(raw).map_err(|_| {
        ApiProblem::from_domain(DomainError::validation("id must be a valid UUID string"))
    })
}
