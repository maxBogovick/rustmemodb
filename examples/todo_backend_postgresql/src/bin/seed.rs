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

    sqlx::query(
        r#"
        INSERT INTO todos (title, description, priority, status)
        VALUES
            ('Ship v1 API', 'Finalize docs and endpoint tests', 5, 'in_progress'),
            ('Prepare release notes', 'Collect changes from main branch', 3, 'pending'),
            ('Clean obsolete tasks', 'Archive old completed todos', 2, 'completed')
        "#,
    )
    .execute(&pool)
    .await
    .context("failed to insert seed records")?;

    println!("Seed data inserted successfully");
    Ok(())
}
