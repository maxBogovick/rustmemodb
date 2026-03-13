use anyhow::Context;
use axum::Router;
use pulse_studio::model::PulseWorkspace;
use rustmemodb::prelude::dx::PersistApp;
use std::{env, net::SocketAddr, path::PathBuf};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let data_dir = data_dir_from_env()?;
    let port = port_from_env()?;

    let app = PersistApp::open_auto(data_dir.clone()).await?;
    let workspaces_router = rustmemodb::serve_domain!(app, PulseWorkspace, "workspaces")?;
    let app = Router::new().nest("/api/workspaces", workspaces_router);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    info!(
        address = %addr,
        data_dir = %data_dir.display(),
        "PulseStudio listening"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn data_dir_from_env() -> anyhow::Result<PathBuf> {
    match env::var("PULSE_STUDIO_DATA_DIR") {
        Ok(path) => Ok(PathBuf::from(path)),
        Err(_) => Ok(env::current_dir()
            .context("resolve current directory for default data dir")?
            .join("pulse_studio_data")),
    }
}

fn port_from_env() -> anyhow::Result<u16> {
    let raw = env::var("PULSE_STUDIO_PORT").unwrap_or_else(|_| "3022".to_string());
    raw.parse::<u16>()
        .with_context(|| format!("invalid PULSE_STUDIO_PORT='{raw}'"))
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        info!(%error, "failed to wait for shutdown signal");
        return;
    }
    info!("shutdown signal received");
}
