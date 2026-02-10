use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use todo_backend::{
    config::{AppConfig, DatabaseBackend},
    repository::{RustMemDbTodoRepository, TodoRepository},
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::from_env().context("failed to read configuration")?;

    match config.database_backend {
        DatabaseBackend::Postgres => {
            let pool = PgPoolOptions::new()
                .max_connections(config.db_max_connections)
                .connect(&config.database_url)
                .await
                .context("failed to connect to PostgreSQL")?;

            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .context("failed to run migrations")?;
        }
        DatabaseBackend::RustMemDb => {
            let repo = match RustMemDbTodoRepository::connect_url(&config.database_url).await {
                Ok(repo) => repo,
                Err(_) => RustMemDbTodoRepository::connect(
                    &config.rustmemodb_username,
                    &config.rustmemodb_password,
                )
                .await
                .context("failed to connect to RustMemDB")?,
            };
            repo.init().await.context("failed to initialize schema")?;
        }
    }

    println!("Migrations applied successfully");
    Ok(())
}
