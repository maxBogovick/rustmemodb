use crate::service::GameService;
use axum::{
    Json, Router,
    extract::{Path, State},
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex; // Using a simple Mutex for the service wrapper for this example

// App State
#[derive(Clone)]
pub struct AppState {
    pub service: Arc<Mutex<GameService>>,
}

// Handlers

#[derive(Deserialize)]
pub struct RegisterReq {
    username: String,
}

pub async fn register_player(
    State(state): State<AppState>,
    Json(payload): Json<RegisterReq>,
) -> impl IntoResponse {
    let mut svc = state.service.lock().await;
    match svc.register(payload.username).await {
        Ok(id) => Json(serde_json::json!({ "id": id, "status": "created" })).into_response(),
        Err(e) => {
            // RustMemDB errors propagate clean descriptions
            (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
    }
}

pub async fn queue_match(
    State(state): State<AppState>,
    Path(player_id): Path<String>,
) -> impl IntoResponse {
    let mut svc = state.service.lock().await;
    match svc.queue_match(&player_id).await {
        Ok(Some(lobby_id)) => {
            Json(serde_json::json!({ "status": "match_found", "lobby_id": lobby_id }))
                .into_response()
        }
        Ok(None) => Json(serde_json::json!({ "status": "waiting" })).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
pub struct FinishReq {
    winner_id: String,
}

pub async fn finish_match(
    State(state): State<AppState>,
    Path(lobby_id): Path<String>,
    Json(payload): Json<FinishReq>,
) -> impl IntoResponse {
    let mut svc = state.service.lock().await;
    match svc.finish_match(&lobby_id, &payload.winner_id).await {
        Ok(()) => Json(serde_json::json!({ "status": "recorded" })).into_response(),
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

pub async fn get_leaderboard(State(state): State<AppState>) -> impl IntoResponse {
    let svc = state.service.lock().await;
    Json(svc.leaderboard()).into_response()
}

pub fn router(service: GameService) -> Router {
    let state = AppState {
        service: Arc::new(Mutex::new(service)),
    };

    Router::new()
        .route("/players", post(register_player))
        .route("/matchmaking/:player_id", post(queue_match))
        .route("/lobbies/:lobby_id/finish", post(finish_match))
        .route("/leaderboard", get(get_leaderboard))
        .with_state(state)
}
