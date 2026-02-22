use crate::model::{Command, Task, TaskDraft, TaskPatch, TaskStatus, TaskVec};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::Html,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use rustmemodb::{classify_managed_conflict, DbError, ManagedConflictKind, ManagedPersistVec};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppState {
    pub tasks: Arc<Mutex<ManagedPersistVec<TaskVec>>>,
}

#[derive(Deserialize)]
pub struct CreateTaskRequest {
    pub name: String,
    pub schedule_time: u64,
    pub command_type: String, // "log" or "play"
    pub payload: String,
}

#[derive(Deserialize, Default)]
pub struct PatchTaskRequest {
    pub name: Option<String>,
    pub schedule_time: Option<u64>,
    pub command_type: Option<String>,
    pub payload: Option<String>,
}

#[derive(Serialize)]
pub struct TaskResponse {
    pub id: String,
    pub name: String,
    pub schedule_time: u64,
    pub command: Command,
    pub status: TaskStatus,
    pub last_error: Option<String>,
    pub version: i64,
    pub updated_at: String,
}

pub fn router(state: AppState) -> Router {
    // OLD IMPLEMENTATION (kept for comparison):
    //
    // Router::new()
    //     .route("/", get(index))
    //     .route("/api/tasks", post(create_task).get(list_tasks))
    //     .with_state(state)
    //
    // Why new approach is better:
    // - complete read/update API for demo quality
    // - typed HTTP errors instead of panic via expect(...)
    // - demonstrates persist Draft/Patch flow in handlers
    Router::new()
        .route("/", get(index))
        .route("/api/tasks", post(create_task).get(list_tasks))
        .route("/api/tasks/{id}", get(get_task).patch(patch_task))
        .route("/api/stats", get(runtime_stats))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn create_task(
    State(state): State<AppState>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<(StatusCode, Json<TaskResponse>), ApiError> {
    // OLD IMPLEMENTATION (kept for comparison):
    //
    // let mut tasks = state.persist.open_vec::<TaskVec>("tasks").await.expect("...");
    // tasks.create(task.clone()).await.expect("...");
    //
    // New approach: validation + create_from_draft + typed error mapping.
    let normalized_name = req.name.trim().to_string();
    if normalized_name.is_empty() {
        return Err(ApiError::bad_request("name must not be empty"));
    }

    let command = parse_command(&req.command_type, req.payload)?;
    let draft = TaskDraft::new(
        normalized_name,
        req.schedule_time,
        command,
        TaskStatus::Pending,
        None,
    );

    let mut tasks = state.tasks.lock().await;
    let id = tasks
        .create_from_draft(draft)
        .await
        .map_err(ApiError::from)?;
    let created = tasks
        .get(&id)
        .map(task_to_response)
        .ok_or_else(|| ApiError::internal("task disappeared after create_from_draft"))?;

    Ok((StatusCode::CREATED, Json(created)))
}

async fn list_tasks(State(state): State<AppState>) -> Result<Json<Vec<TaskResponse>>, ApiError> {
    let tasks = state.tasks.lock().await;
    let sorted = tasks.list_sorted_by(|left, right| {
        left.schedule_time()
            .cmp(right.schedule_time())
            .then_with(|| left.persist_id().cmp(right.persist_id()))
    });

    let response = sorted.into_iter().map(task_to_response).collect::<Vec<_>>();

    Ok(Json(response))
}

async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TaskResponse>, ApiError> {
    let tasks = state.tasks.lock().await;
    let task = tasks
        .get(&id)
        .map(task_to_response)
        .ok_or_else(|| ApiError::not_found(format!("task not found: {id}")))?;
    Ok(Json(task))
}

async fn patch_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<PatchTaskRequest>,
) -> Result<Json<TaskResponse>, ApiError> {
    let has_command_update = req.command_type.is_some() || req.payload.is_some();
    if req.name.is_none() && req.schedule_time.is_none() && !has_command_update {
        return Err(ApiError::bad_request(
            "at least one field must be provided: name, schedule_time, command_type+payload",
        ));
    }

    let mut patch = TaskPatch::default();

    if let Some(name) = req.name {
        let normalized = name.trim().to_string();
        if normalized.is_empty() {
            return Err(ApiError::bad_request("name must not be empty"));
        }
        patch.name = Some(normalized);
    }

    if let Some(schedule_time) = req.schedule_time {
        patch.schedule_time = Some(schedule_time);
    }

    if has_command_update {
        let command_type = req.command_type.ok_or_else(|| {
            ApiError::bad_request("command_type is required when payload is provided")
        })?;
        let payload = req.payload.ok_or_else(|| {
            ApiError::bad_request("payload is required when command_type is provided")
        })?;
        patch.command = Some(parse_command(&command_type, payload)?);
    }

    let mut tasks = state.tasks.lock().await;
    let found = tasks.patch(&id, patch).await.map_err(ApiError::from)?;
    if !found {
        return Err(ApiError::not_found(format!("task not found: {id}")));
    }

    let updated = tasks
        .get(&id)
        .map(task_to_response)
        .ok_or_else(|| ApiError::internal("task disappeared after successful patch"))?;
    Ok(Json(updated))
}

#[derive(Serialize)]
struct RuntimeStatsResponse {
    total: usize,
    pending: usize,
    in_progress: usize,
    failed: usize,
    completed: usize,
    snapshot_every_ops: usize,
    ops_since_snapshot: usize,
    snapshot_path: String,
    replication_mode: String,
    replication_targets: usize,
    replication_failures: u64,
    last_snapshot_at: Option<String>,
}

async fn runtime_stats(
    State(state): State<AppState>,
) -> Result<Json<RuntimeStatsResponse>, ApiError> {
    let tasks = state.tasks.lock().await;
    let stats = tasks.stats();

    let total = tasks.list().len();
    let pending = tasks
        .list_filtered(|t| *t.status() == TaskStatus::Pending)
        .len();
    let in_progress = tasks
        .list_filtered(|t| *t.status() == TaskStatus::InProgress)
        .len();
    let failed = tasks
        .list_filtered(|t| *t.status() == TaskStatus::Failed)
        .len();
    let completed = tasks
        .list_filtered(|t| *t.status() == TaskStatus::Completed)
        .len();

    Ok(Json(RuntimeStatsResponse {
        total,
        pending,
        in_progress,
        failed,
        completed,
        snapshot_every_ops: stats.snapshot_every_ops,
        ops_since_snapshot: stats.ops_since_snapshot,
        snapshot_path: stats.snapshot_path,
        replication_mode: stats.replication_mode,
        replication_targets: stats.replication_targets,
        replication_failures: stats.replication_failures,
        last_snapshot_at: stats.last_snapshot_at,
    }))
}

fn parse_command(command_type: &str, payload: String) -> Result<Command, ApiError> {
    let normalized_payload = payload.trim().to_string();
    if normalized_payload.is_empty() {
        return Err(ApiError::bad_request("payload must not be empty"));
    }

    match command_type {
        "log" => Ok(Command::Log(normalized_payload)),
        "play" => Ok(Command::PlaySound(normalized_payload)),
        other => Err(ApiError::bad_request(format!(
            "unsupported command_type: {other}; expected one of: log, play"
        ))),
    }
}

fn task_to_response(task: &Task) -> TaskResponse {
    TaskResponse {
        id: task.persist_id().to_string(),
        name: task.name().clone(),
        schedule_time: *task.schedule_time(),
        command: task.command().clone(),
        status: task.status().clone(),
        last_error: task.last_error().clone(),
        version: task.metadata().version,
        updated_at: task.metadata().updated_at.to_rfc3339(),
    }
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl From<DbError> for ApiError {
    fn from(value: DbError) -> Self {
        if let Some(kind) = classify_managed_conflict(&value) {
            let status = match kind {
                ManagedConflictKind::OptimisticLock
                | ManagedConflictKind::WriteWrite
                | ManagedConflictKind::UniqueConstraint => StatusCode::CONFLICT,
            };
            return Self {
                status,
                message: value.to_string(),
            };
        }

        Self::internal(value.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({
                "error": self.message,
                "status": self.status.as_u16(),
            })),
        )
            .into_response()
    }
}
