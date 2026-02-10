use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use todo_backend::config::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::from_env().context("failed to read configuration")?;

    let pool = PgPoolOptions::new()
        .max_connections(config.db_max_connections)
        .connect(&config.database_url)
        .await
        .context("failed to connect to PostgreSQL")?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("failed to run migrations")?;

    println!("Migrations applied successfully");
    Ok(())
}
