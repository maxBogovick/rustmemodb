use std::sync::Arc;

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use todo_backend::{
    build_router,
    config::{AppConfig, DatabaseBackend},
    repository::{PgTodoRepository, RustMemDbTodoRepository, TodoRepository},
    state::AppState,
};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = AppConfig::from_env().context("failed to load application configuration")?;

    let repository: Arc<dyn TodoRepository> = match config.database_backend {
        DatabaseBackend::Postgres => {
            info!("database backend: postgres");
            let pool = PgPoolOptions::new()
                .max_connections(config.db_max_connections)
                .connect(&config.database_url)
                .await
                .context("failed to connect to PostgreSQL")?;
            Arc::new(PgTodoRepository::new(pool))
        }
        DatabaseBackend::RustMemDb => {
            info!("database backend: rustmemodb");
            let rust_repo = match RustMemDbTodoRepository::connect_url(&config.database_url).await {
                Ok(repo) => repo,
                Err(_) => RustMemDbTodoRepository::connect(
                    &config.rustmemodb_username,
                    &config.rustmemodb_password,
                )
                .await
                .context("failed to connect to RustMemDB with fallback credentials")?,
            };
            Arc::new(rust_repo)
        }
    };

    repository
        .init()
        .await
        .context("failed to initialize todo schema")?;

    let app = build_router(AppState::new(repository));

    let addr = config.address();
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind to {addr}"))?;

    info!(address = %addr, "todo backend started");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("todo_backend=debug,tower_http=info")),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!(error = %err, "unable to install Ctrl+C signal handler");
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
