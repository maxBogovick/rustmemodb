use anyhow::Context;
use axum::Router;
use ledger_core::model::LedgerBook;
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
    let ledgers_router = app.serve_autonomous_model::<LedgerBook>("ledgers").await?;
    let app = Router::new().nest("/api/ledgers", ledgers_router);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("LedgerCore listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn data_dir_from_env() -> anyhow::Result<PathBuf> {
    match env::var("LEDGER_CORE_DATA_DIR") {
        Ok(path) => Ok(PathBuf::from(path)),
        Err(_) => Ok(env::current_dir()
            .context("resolve current directory for default data dir")?
            .join("ledger_core_data")),
    }
}

fn port_from_env() -> anyhow::Result<u16> {
    let raw = env::var("LEDGER_CORE_PORT").unwrap_or_else(|_| "3012".to_string());
    raw.parse::<u16>()
        .with_context(|| format!("invalid LEDGER_CORE_PORT='{raw}'"))
}
