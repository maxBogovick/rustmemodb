use axum::{
    Router,
    http::Method,
    routing::{get, post},
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use crate::{
    handlers::{
        create_todo, delete_todo, get_todo, healthcheck, list_todos, patch_todo, replace_todo,
        restore_todo,
    },
    state::AppState,
};

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(healthcheck))
        .route("/api/v1/todos", post(create_todo).get(list_todos))
        .route(
            "/api/v1/todos/{id}",
            get(get_todo)
                .patch(patch_todo)
                .put(replace_todo)
                .delete(delete_todo),
        )
        .route("/api/v1/todos/{id}/restore", post(restore_todo))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_headers(Any)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::PATCH,
                    Method::DELETE,
                    Method::OPTIONS,
                ]),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
