mod domain;
mod service;
mod web;

use crate::service::GameService;
use rustmemodb::{PersistApp, persist_vec};
use std::net::SocketAddr;

// Define Collections
// managed via macros -> creates type alias `PlayerVec`, `LobbyVec` etc.
persist_vec!(pub PlayerVec, domain::Player);
persist_vec!(pub LobbyVec, domain::Lobby);
persist_vec!(pub HistoryVec, domain::MatchResult);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing for logs
    tracing_subscriber::fmt::init();

    println!("🎮 GameMaster Server Starting...");

    // 1. Initialize DB (Auto-Persistence enabled)
    // Data stores in ./gamemaster_data
    // On restart, it replays WAL to restore consistency.
    let app = PersistApp::open_auto("gamemaster_data").await?;

    // 2. Initialize Service
    let service = GameService::new(&app).await?;

    // 3. Build Router
    let app_router = web::router(service);

    // 4. Run Server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("🚀 Listening on http://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app_router).await?;

    Ok(())
}
