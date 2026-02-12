use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use rustmemodb::{
    DbError, ManagedPersistVec, PersistApp, PersistAppAutoPolicy, PersistReplicationMode,
    PersistReplicationPolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::macro_showcase::{PersistedTodo, PersistedTodoDraft, PersistedTodoPatch, TodoVec};

pub mod macro_showcase;

type SharedTodos = Arc<Mutex<ManagedPersistVec<TodoVec>>>;

#[derive(Clone)]
struct AppState {
    todos: SharedTodos,
    durability_mode: RuntimeDurabilityMode,
}

#[derive(Debug)]
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

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
                "status": self.status.as_u16()
            })),
        )
            .into_response()
    }
}

impl From<DbError> for ApiError {
    fn from(value: DbError) -> Self {
        let message = value.to_string();
        if message.to_ascii_lowercase().contains("todo not found") {
            return ApiError::not_found(message);
        }
        ApiError::internal(message)
    }
}

#[derive(Debug, Deserialize)]
struct CreateTodoRequest {
    title: String,
    #[serde(default)]
    priority: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct PatchTodoRequest {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    completed: Option<bool>,
    #[serde(default)]
    priority: Option<i64>,
}

#[derive(Debug, Serialize)]
struct TodoResponse {
    id: String,
    title: String,
    completed: bool,
    priority: i64,
    created_at: String,
    updated_at: String,
    schema_version: u32,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct RuntimeStatsResponse {
    todo_count: usize,
    snapshot_every_ops: usize,
    ops_since_snapshot: usize,
    snapshot_path: String,
    replication_targets: usize,
    replication_mode: String,
    replication_failures: u64,
    durability_mode: String,
    last_snapshot_at: Option<String>,
}

#[derive(Debug, Clone)]
pub enum RuntimeDurabilityMode {
    Strict,
    Eventual { sync_interval_ms: u64 },
}

impl RuntimeDurabilityMode {
    fn as_string(&self) -> String {
        match self {
            RuntimeDurabilityMode::Strict => "strict".to_string(),
            RuntimeDurabilityMode::Eventual { sync_interval_ms } => {
                format!("eventual(sync_interval_ms={sync_interval_ms})")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum RuntimeReplicationMode {
    Sync,
    AsyncBestEffort,
}

impl RuntimeReplicationMode {
    fn to_policy_mode(&self) -> PersistReplicationMode {
        match self {
            RuntimeReplicationMode::Sync => PersistReplicationMode::Sync,
            RuntimeReplicationMode::AsyncBestEffort => PersistReplicationMode::AsyncBestEffort,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    bind_addr: SocketAddr,
    data_dir: PathBuf,
    durability_mode: RuntimeDurabilityMode,
    snapshot_every_ops: usize,
    replication_mode: RuntimeReplicationMode,
    replica_dirs: Vec<PathBuf>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env_string("TODO_BIND_ADDR", "127.0.0.1:8090")
            .parse::<SocketAddr>()
            .context("TODO_BIND_ADDR must be valid host:port")?;

        let data_dir = PathBuf::from(env_string(
            "TODO_DATA_DIR",
            "examples/todo_persist_runtime/.data/primary",
        ));

        let durability_mode = match env_string("TODO_DURABILITY_MODE", "strict")
            .to_ascii_lowercase()
            .as_str()
        {
            "strict" => RuntimeDurabilityMode::Strict,
            "eventual" => RuntimeDurabilityMode::Eventual {
                sync_interval_ms: env_u64("TODO_EVENTUAL_SYNC_MS", 250)?,
            },
            other => {
                return Err(anyhow::anyhow!(
                    "TODO_DURABILITY_MODE must be strict|eventual, got {}",
                    other
                ));
            }
        };

        let replication_mode = match env_string("TODO_REPLICATION_MODE", "sync")
            .to_ascii_lowercase()
            .as_str()
        {
            "sync" => RuntimeReplicationMode::Sync,
            "async" => RuntimeReplicationMode::AsyncBestEffort,
            other => {
                return Err(anyhow::anyhow!(
                    "TODO_REPLICATION_MODE must be sync|async, got {}",
                    other
                ));
            }
        };

        Ok(Self {
            bind_addr,
            data_dir,
            durability_mode,
            snapshot_every_ops: env_usize("TODO_SNAPSHOT_EVERY_OPS", 1)?,
            replication_mode,
            replica_dirs: parse_replica_dirs("TODO_REPLICA_DIRS"),
        })
    }

    pub fn for_testing(data_dir: PathBuf) -> Self {
        Self {
            bind_addr: "127.0.0.1:0".parse().expect("valid loopback address"),
            data_dir,
            durability_mode: RuntimeDurabilityMode::Strict,
            snapshot_every_ops: 1,
            replication_mode: RuntimeReplicationMode::Sync,
            replica_dirs: Vec::new(),
        }
    }

    pub fn with_replication(
        mut self,
        mode: RuntimeReplicationMode,
        replica_dirs: Vec<PathBuf>,
    ) -> Self {
        self.replication_mode = mode;
        self.replica_dirs = replica_dirs;
        self
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}

pub struct TodoBootstrap {
    pub router: Router,
    todos: SharedTodos,
}

impl TodoBootstrap {
    pub async fn shutdown(self) -> Result<()> {
        let mut todos = self.todos.lock().await;
        if todos.stats().ops_since_snapshot > 0 {
            todos.force_snapshot().await?;
        }
        Ok(())
    }
}

pub async fn bootstrap(config: &AppConfig) -> Result<TodoBootstrap> {
    info!(
        bind = %config.bind_addr,
        data_dir = %config.data_dir.display(),
        "bootstrapping todo app via PersistApp"
    );

    let policy = PersistAppAutoPolicy {
        snapshot_every_ops: config.snapshot_every_ops.max(1),
        replication: PersistReplicationPolicy {
            mode: config.replication_mode.to_policy_mode(),
            replica_roots: config.replica_dirs.clone(),
        },
    };

    let persist_app = PersistApp::open_auto_with(config.data_dir.clone(), policy).await?;
    let todos = Arc::new(Mutex::new(persist_app.open_vec::<TodoVec>("todo_api").await?));

    let router = build_router(todos.clone(), config.durability_mode.clone());

    Ok(TodoBootstrap {
        router,
        todos,
    })
}
fn build_router(todos: SharedTodos, durability_mode: RuntimeDurabilityMode) -> Router {
    let state = AppState {
        todos,
        durability_mode,
    };

    Router::new()
        .route("/health", get(health))
        .route("/api/v1/todos", post(create_todo).get(list_todos))
        .route(
            "/api/v1/todos/{id}",
            get(get_todo).patch(patch_todo).delete(delete_todo),
        )
        .route("/api/v1/admin/stats", get(runtime_stats))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_headers(Any)
                .allow_methods(Any),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn create_todo(
    State(state): State<AppState>,
    Json(payload): Json<CreateTodoRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let title = payload.title.trim();
    if title.is_empty() {
        return Err(ApiError::bad_request("title must not be empty"));
    }

    let now = Utc::now().to_rfc3339();
    let draft = PersistedTodoDraft::new(
        title.to_string(),
        false,
        payload.priority.unwrap_or(0),
        now.clone(),
        now,
    );

    let mut todos = state.todos.lock().await;
    let todo_id = todos.create_from_draft(draft).await.map_err(ApiError::from)?;
    let created = todos
        .get(&todo_id)
        .map(todo_to_response)
        .ok_or_else(|| ApiError::internal("todo disappeared after create_from_draft"))?;

    Ok((StatusCode::CREATED, Json(created)))
}

async fn list_todos(State(state): State<AppState>) -> Result<Json<Vec<TodoResponse>>, ApiError> {
    let todos = state.todos.lock().await;

    let mut rows = todos
        .list()
        .iter()
        .filter(|item| item.metadata().persisted)
        .map(todo_to_response)
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| a.created_at.cmp(&b.created_at));

    Ok(Json(rows))
}

async fn get_todo(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<TodoResponse>, ApiError> {
    let todos = state.todos.lock().await;

    let todo = todos
        .get(&id)
        .map(todo_to_response)
        .ok_or_else(|| ApiError::not_found(format!("todo not found: {id}")))?;

    Ok(Json(todo))
}

async fn patch_todo(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(payload): Json<PatchTodoRequest>,
) -> Result<Json<TodoResponse>, ApiError> {
    if payload.title.is_none() && payload.completed.is_none() && payload.priority.is_none() {
        return Err(ApiError::bad_request(
            "at least one field must be provided: title, completed, priority",
        ));
    }

    let normalized_title = match payload.title {
        Some(title) => {
            let title = title.trim();
            if title.is_empty() {
                return Err(ApiError::bad_request("title must not be empty"));
            }
            Some(title.to_string())
        }
        None => None,
    };

    let patch = PersistedTodoPatch {
        title: normalized_title,
        completed: payload.completed,
        priority: payload.priority,
        updated_at: Some(Utc::now().to_rfc3339()),
        ..Default::default()
    };

    let mut todos = state.todos.lock().await;
    let updated = todos.patch(&id, patch).await.map_err(ApiError::from)?;

    if !updated {
        return Err(ApiError::not_found(format!("todo not found: {id}")));
    }

    let todo = todos
        .get(&id)
        .map(todo_to_response)
        .ok_or_else(|| ApiError::internal("todo disappeared after successful update"))?;

    Ok(Json(todo))
}

async fn delete_todo(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, ApiError> {
    let mut todos = state.todos.lock().await;

    let deleted = todos.delete(&id).await.map_err(ApiError::from)?;
    if !deleted {
        return Err(ApiError::not_found(format!("todo not found: {id}")));
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn runtime_stats(State(state): State<AppState>) -> Result<Json<RuntimeStatsResponse>, ApiError> {
    let todos = state.todos.lock().await;
    let stats = todos.stats();
    let todo_count = todos
        .list()
        .iter()
        .filter(|item| item.metadata().persisted)
        .count();

    Ok(Json(RuntimeStatsResponse {
        todo_count,
        snapshot_every_ops: stats.snapshot_every_ops,
        ops_since_snapshot: stats.ops_since_snapshot,
        snapshot_path: stats.snapshot_path,
        replication_targets: stats.replication_targets,
        replication_mode: stats.replication_mode,
        replication_failures: stats.replication_failures,
        durability_mode: state.durability_mode.as_string(),
        last_snapshot_at: stats.last_snapshot_at,
    }))
}

fn todo_to_response(todo: &PersistedTodo) -> TodoResponse {
    TodoResponse {
        id: todo.persist_id().to_string(),
        title: todo.title().to_string(),
        completed: *todo.completed(),
        priority: *todo.priority(),
        created_at: todo.created_at().to_string(),
        updated_at: todo.updated_at().to_string(),
        schema_version: todo.metadata().schema_version,
    }
}

pub fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("todo_persist_runtime=debug,tower_http=info")),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!(error = %err, "unable to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                error!(error = %err, "unable to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> Result<u64> {
    let raw = std::env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse::<u64>()
        .with_context(|| format!("{key} must be u64"))
}

fn env_usize(key: &str, default: usize) -> Result<usize> {
    let raw = std::env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse::<usize>()
        .with_context(|| format!("{key} must be usize"))
}

fn parse_replica_dirs(key: &str) -> Vec<PathBuf> {
    let Ok(raw) = std::env::var(key) else {
        return Vec::new();
    };

    raw.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(PathBuf::from)
        .collect()
}
