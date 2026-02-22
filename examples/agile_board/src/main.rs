use agile_board::model::Board;
use anyhow::Context;
use axum::Router;
use rustmemodb::prelude::dx::PersistApp;
use std::{env, net::SocketAddr, path::PathBuf};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let data_dir = data_dir_from_env()?;
    let port = port_from_env()?;

    let app = PersistApp::open_auto(data_dir).await?;
    let boards_router = app.serve_autonomous_model::<Board>("boards").await?;
    let app = Router::new().nest("/api/boards", boards_router);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("AgileBoard listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn data_dir_from_env() -> anyhow::Result<PathBuf> {
    match env::var("AGILE_BOARD_DATA_DIR") {
        Ok(path) => Ok(PathBuf::from(path)),
        Err(_) => Ok(env::current_dir()
            .context("resolve current directory for default data dir")?
            .join("agile_board_data")),
    }
}

fn port_from_env() -> anyhow::Result<u16> {
    let raw = env::var("AGILE_BOARD_PORT").unwrap_or_else(|_| "3002".to_string());
    raw.parse::<u16>()
        .with_context(|| format!("invalid AGILE_BOARD_PORT='{raw}'"))
}
