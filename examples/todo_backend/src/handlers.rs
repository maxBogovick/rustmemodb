use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use uuid::Uuid;

use crate::{
    error::{AppError, AppResult},
    models::{
        ApiMessage, ApiResponse, CreateTodoRequest, GetTodoQuery, ListTodosQuery,
        ReplaceTodoRequest, UpdateTodoPatchRequest,
    },
    state::AppState,
};

pub async fn healthcheck() -> Json<ApiResponse<ApiMessage>> {
    Json(ApiResponse {
        data: ApiMessage {
            message: "ok".to_string(),
        },
    })
}

pub async fn create_todo(
    State(state): State<AppState>,
    Json(payload): Json<CreateTodoRequest>,
) -> AppResult<(StatusCode, Json<ApiResponse<crate::models::Todo>>)> {
    validate_create_request(&payload)?;

    let todo = state.repo.create(payload).await?;

    Ok((StatusCode::CREATED, Json(ApiResponse { data: todo })))
}

pub async fn list_todos(
    State(state): State<AppState>,
    Query(query): Query<ListTodosQuery>,
) -> AppResult<Json<ApiResponse<crate::models::PaginatedTodos>>> {
    query.validate().map_err(AppError::validation)?;

    let todos = state.repo.list(query).await?;
    Ok(Json(ApiResponse { data: todos }))
}

pub async fn get_todo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<GetTodoQuery>,
) -> AppResult<Json<ApiResponse<crate::models::Todo>>> {
    let todo = state
        .repo
        .get_by_id(id, query.include_deleted)
        .await?
        .ok_or_else(|| AppError::not_found("todo not found"))?;

    Ok(Json(ApiResponse { data: todo }))
}

pub async fn patch_todo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateTodoPatchRequest>,
) -> AppResult<Json<ApiResponse<crate::models::Todo>>> {
    validate_patch_request(&payload)?;

    if !payload.has_changes() {
        return Err(AppError::validation(
            "at least one field must be provided for PATCH",
        ));
    }

    if let Some(title) = payload.title.as_deref() {
        ensure_title_not_blank(title)?;
    }

    let todo = state
        .repo
        .patch(id, payload)
        .await?
        .ok_or_else(|| AppError::not_found("todo not found"))?;

    Ok(Json(ApiResponse { data: todo }))
}

pub async fn replace_todo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<ReplaceTodoRequest>,
) -> AppResult<Json<ApiResponse<crate::models::Todo>>> {
    validate_replace_request(&payload)?;

    let todo = state
        .repo
        .replace(id, payload)
        .await?
        .ok_or_else(|| AppError::not_found("todo not found"))?;

    Ok(Json(ApiResponse { data: todo }))
}

pub async fn delete_todo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<StatusCode> {
    let deleted = state.repo.delete(id).await?;
    if !deleted {
        return Err(AppError::not_found("todo not found"));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn restore_todo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<ApiResponse<crate::models::Todo>>> {
    let restored = state
        .repo
        .restore(id)
        .await?
        .ok_or_else(|| AppError::not_found("todo not found or not deleted"))?;

    Ok(Json(ApiResponse { data: restored }))
}

fn ensure_title_not_blank(title: &str) -> AppResult<()> {
    if title.trim().is_empty() {
        return Err(AppError::validation("title must not be blank"));
    }
    if title.len() > 200 {
        return Err(AppError::validation("title must be at most 200 characters"));
    }
    Ok(())
}

fn validate_description(description: Option<&str>) -> AppResult<()> {
    if let Some(description) = description
        && description.len() > 5000
    {
        return Err(AppError::validation(
            "description must be at most 5000 characters",
        ));
    }
    Ok(())
}

fn validate_priority(priority: Option<i16>) -> AppResult<()> {
    if let Some(priority) = priority
        && !(1..=5).contains(&priority)
    {
        return Err(AppError::validation("priority must be between 1 and 5"));
    }
    Ok(())
}

fn validate_create_request(payload: &CreateTodoRequest) -> AppResult<()> {
    ensure_title_not_blank(&payload.title)?;
    validate_description(payload.description.as_deref())?;
    validate_priority(payload.priority)?;
    Ok(())
}

fn validate_patch_request(payload: &UpdateTodoPatchRequest) -> AppResult<()> {
    if let Some(title) = payload.title.as_deref() {
        ensure_title_not_blank(title)?;
    }
    validate_description(
        payload
            .description
            .as_ref()
            .and_then(|description| description.as_deref()),
    )?;
    validate_priority(payload.priority)?;
    Ok(())
}

fn validate_replace_request(payload: &ReplaceTodoRequest) -> AppResult<()> {
    ensure_title_not_blank(&payload.title)?;
    validate_description(payload.description.as_deref())?;
    validate_priority(Some(payload.priority))?;
    Ok(())
}
