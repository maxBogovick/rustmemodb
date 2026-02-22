use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use std::path::Path;
use tracing::info;

mod model;
mod scheduler;
mod web;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load .env
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    info!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Run migrations
    info!("Running migrations...");
    MIGRATOR.run(&pool).await?;
    info!("Migrations applied successfully.");

    // Start Scheduler
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        scheduler::start_scheduler_loop(pool_clone).await;
    });

    // Start Web Server
    let app = web::router(pool);
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
