use axum::{
    Router,
    http::{HeaderName, Method},
    routing::{get, post},
};
use tower_http::{
    cors::{Any, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

use crate::{
    interface::http::users_handler::{
        apply_user_lifecycle_command, bulk_apply_lifecycle_command, create_user, delete_user,
        get_user, healthcheck, list_user_events, list_users, update_user,
    },
    state::AppState,
};

pub fn build_router(state: AppState) -> Router {
    let request_id_header = HeaderName::from_static("x-request-id");

    Router::new()
        .route("/health", get(healthcheck))
        .route("/api/v1/users", post(create_user).get(list_users))
        .route(
            "/api/v1/users/{id}",
            get(get_user).patch(update_user).delete(delete_user),
        )
        .route(
            "/api/v1/users/{id}/commands",
            post(apply_user_lifecycle_command),
        )
        .route("/api/v1/users/{id}/events", get(list_user_events))
        .route(
            "/api/v1/users/commands/bulk-lifecycle",
            post(bulk_apply_lifecycle_command),
        )
        .layer(TraceLayer::new_for_http())
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(SetRequestIdLayer::new(request_id_header, MakeRequestUuid))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_headers(Any)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PATCH,
                    Method::PUT,
                    Method::DELETE,
                    Method::OPTIONS,
                ]),
        )
        .with_state(state)
}
