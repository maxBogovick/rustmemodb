use rustmemodb::PersistApp;
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    println!("Starting No-DB API Example...");

    // 1. Determine data directory
    let data_dir = std::env::current_dir()?.join("no_db_data");
    println!("Storage location: {:?}", data_dir);

    // 2. Load JSON schemas and let persist build generic CRUD REST automatically
    let schemas_dir = std::env::current_dir()?.join("schemas");
    let persist = PersistApp::open_auto(data_dir).await?;
    let dynamic_router = persist.serve_json_schema_dir(&schemas_dir).await?;

    // 3. Mount router
    let app = axum::Router::new().nest("/api", dynamic_router);

    // 4. Run Server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3001));
    println!("Listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
