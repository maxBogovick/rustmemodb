use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
};
use uuid::Uuid;

use crate::{
    application::dto::{
        CreateUserRequest, HealthResponse, ListUsersQueryRequest, PaginatedUsersResponse,
        UpdateUserRequest, UserResponse,
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

pub async fn update_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdateUserRequest>,
) -> ApiResult<Json<UserResponse>> {
    let user_id = parse_uuid(&id)?;
    let expected_version = parse_expected_version(&headers)?;
    let updated = state
        .user_service
        .update_user(user_id, request, expected_version)
        .await
        .map_err(ApiProblem::from_domain)?;

    Ok(Json(updated))
}

pub async fn delete_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    let user_id = parse_uuid(&id)?;
    let expected_version = parse_expected_version(&headers)?;
    state
        .user_service
        .delete_user(user_id, expected_version)
        .await
        .map_err(ApiProblem::from_domain)?;

    Ok(StatusCode::NO_CONTENT)
}

fn parse_uuid(raw: &str) -> ApiResult<Uuid> {
    Uuid::parse_str(raw).map_err(|_| {
        ApiProblem::from_domain(DomainError::validation("id must be a valid UUID string"))
    })
}

fn parse_expected_version(headers: &HeaderMap) -> ApiResult<i64> {
    let Some(raw_if_match) = headers.get(header::IF_MATCH) else {
        return Err(ApiProblem::from_domain(DomainError::validation(
            "If-Match header is required",
        )));
    };

    let raw_if_match = raw_if_match.to_str().map_err(|_| {
        ApiProblem::from_domain(DomainError::validation(
            "If-Match header must be valid ASCII",
        ))
    })?;

    let normalized = raw_if_match.trim().trim_matches('"');
    let expected_version = normalized.parse::<i64>().map_err(|_| {
        ApiProblem::from_domain(DomainError::validation(
            "If-Match header must contain a positive integer version",
        ))
    })?;

    if expected_version <= 0 {
        return Err(ApiProblem::from_domain(DomainError::validation(
            "If-Match header must contain a positive integer version",
        )));
    }

    Ok(expected_version)
}
