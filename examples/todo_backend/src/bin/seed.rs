use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use todo_backend::{
    config::{AppConfig, DatabaseBackend},
    models::{CreateTodoRequest, TodoStatus},
    repository::{PgTodoRepository, RustMemDbTodoRepository, TodoRepository},
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::from_env().context("failed to read configuration")?;

    let repo: Box<dyn TodoRepository> = match config.database_backend {
        DatabaseBackend::Postgres => {
            let pool = PgPoolOptions::new()
                .max_connections(config.db_max_connections)
                .connect(&config.database_url)
                .await
                .context("failed to connect to PostgreSQL")?;
            Box::new(PgTodoRepository::new(pool))
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
            Box::new(repo)
        }
    };

    repo.init().await.context("failed to initialize schema")?;

    let payloads = vec![
        CreateTodoRequest {
            title: "Ship v1 API".to_string(),
            description: Some("Finalize docs and endpoint tests".to_string()),
            priority: Some(5),
            due_at: None,
            status: Some(TodoStatus::InProgress),
        },
        CreateTodoRequest {
            title: "Prepare release notes".to_string(),
            description: Some("Collect changes from main branch".to_string()),
            priority: Some(3),
            due_at: None,
            status: Some(TodoStatus::Pending),
        },
        CreateTodoRequest {
            title: "Clean obsolete tasks".to_string(),
            description: Some("Archive old completed todos".to_string()),
            priority: Some(2),
            due_at: None,
            status: Some(TodoStatus::Completed),
        },
    ];

    for payload in payloads {
        let _ = repo
            .create(payload)
            .await
            .context("failed to insert seed todo")?;
    }

    println!("Seed data inserted successfully");
    Ok(())
}
