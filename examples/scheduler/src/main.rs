mod model;
mod scheduler;
mod web;

use crate::model::TaskVec;
use anyhow::Context;
use rustmemodb::{ManagedPersistVec, PersistApp};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting Scheduler Example");

    // OLD IMPLEMENTATION (kept for comparison):
    //
    // let app: PersistApp = PersistApp::open_auto("scheduler_db").await?;
    // let app = Arc::new(app);
    // tokio::spawn(async move {
    //     scheduler::start_scheduler_loop(app.clone()).await;
    // });
    // let state = web::AppState { persist: app };
    //
    // Why new approach is better:
    // - open managed vec once during bootstrap
    // - share one synchronized handle between HTTP and scheduler loop
    // - avoid reopen/restore overhead on every request/tick
    let persist = PersistApp::open_auto("scheduler_db")
        .await
        .context("failed to open persist app at scheduler_db")?;
    let tasks: Arc<Mutex<ManagedPersistVec<TaskVec>>> = Arc::new(Mutex::new(
        persist
            .open_vec::<TaskVec>("tasks")
            .await
            .context("failed to open managed tasks vec")?,
    ));

    // Start Scheduler Loop with shared managed collection.
    let scheduler_tasks = tasks.clone();
    tokio::spawn(async move {
        scheduler::start_scheduler_loop(scheduler_tasks).await;
    });

    // Start Web Server
    let state = web::AppState { tasks };
    let api_routes = web::router(state);

    let app_router = axum::Router::new().merge(api_routes).fallback_service(
        tower_http::services::ServeDir::new("examples/scheduler/static"),
    );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!("Server listening on http://localhost:3000");
    axum::serve(listener, app_router).await?;

    Ok(())
}
