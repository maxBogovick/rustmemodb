use anyhow::{Context, Result};
use todo_persist_runtime::{AppConfig, bootstrap, init_tracing, shutdown_signal};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let config = AppConfig::from_env().context("failed to read config")?;

    let boot = bootstrap(&config).await?;

    let listener = tokio::net::TcpListener::bind(config.bind_addr())
        .await
        .context("failed to bind listener")?;

    axum::serve(listener, boot.router.clone())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve error")?;

    boot.shutdown().await
}
