use crate::model::{CreateTaskRequest, Task, TaskResponse, TaskStatus};
use axum::{
    Router,
    extract::{Json, State},
    response::Html,
    routing::{get, post},
};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

pub fn router(pool: PgPool) -> Router {
    let state = AppState { pool };
    Router::new()
        .route("/", get(index))
        .route("/api/tasks", post(create_task).get(list_tasks))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn create_task(
    State(state): State<AppState>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<Json<TaskResponse>, String> {
    let id = Uuid::new_v4();
    let status = TaskStatus::Pending;

    let task = sqlx::query_as::<_, Task>(
        r#"
        INSERT INTO tasks (id, name, schedule_time, command_type, command_payload, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(&req.name)
    .bind(req.schedule_time)
    .bind(&req.command_type)
    .bind(&req.payload)
    .bind(&status)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| format!("Failed to insert task: {}", e))?;

    Ok(Json(TaskResponse::from(task)))
}

async fn list_tasks(State(state): State<AppState>) -> Result<Json<Vec<TaskResponse>>, String> {
    let tasks = sqlx::query_as::<_, Task>("SELECT * FROM tasks ORDER BY schedule_time ASC")
        .fetch_all(&state.pool)
        .await
        .map_err(|e| format!("Failed to fetch tasks: {}", e))?;

    let response: Vec<TaskResponse> = tasks.into_iter().map(TaskResponse::from).collect();
    Ok(Json(response))
}
