use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::env;
use uuid::Uuid;

// ============================================================================
// Domain Models (Pure Rust, no DB dependencies)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Todo {
    pub id: String,
    pub title: String,
    pub completed: bool,
}

impl Todo {
    pub fn new(title: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            title,
            completed: false,
        }
    }
}

// ============================================================================
// Repository Interface (Port)
// ============================================================================

/// The abstract interface for storing Todos.
/// The application logic depends on this, not on specific databases.
#[async_trait]
pub trait TodoRepository: Send + Sync {
    async fn init(&self) -> anyhow::Result<()>;
    async fn create(&self, todo: &Todo) -> anyhow::Result<()>;
    async fn list(&self) -> anyhow::Result<Vec<Todo>>;
    async fn complete(&self, id: &str) -> anyhow::Result<()>;
}

// ============================================================================
// Application Service
// ============================================================================

pub struct TodoService {
    repository: Box<dyn TodoRepository>,
}

impl TodoService {
    pub fn new(repository: Box<dyn TodoRepository>) -> Self {
        Self { repository }
    }

    pub async fn init(&self) -> anyhow::Result<()> {
        self.repository.init().await
    }

    pub async fn add_todo(&self, title: String) -> anyhow::Result<Todo> {
        let todo = Todo::new(title);
        self.repository.create(&todo).await?;
        println!("Created todo: {}", todo.title);
        Ok(todo)
    }

    pub async fn get_all_todos(&self) -> anyhow::Result<Vec<Todo>> {
        self.repository.list().await
    }

    pub async fn mark_completed(&self, id: &str) -> anyhow::Result<()> {
        self.repository.complete(id).await
    }
}

// ============================================================================
// Real PostgreSQL Implementation (Adapter)
// ============================================================================

pub struct PostgresRepository {
    client: tokio_postgres::Client,
}

impl PostgresRepository {
    pub async fn connect(config: &str) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(config, tokio_postgres::NoTls).await?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {}", e);
            }
        });

        Ok(Self { client })
    }
}

#[async_trait]
impl TodoRepository for PostgresRepository {
    async fn init(&self) -> anyhow::Result<()> {
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS todos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed BOOLEAN NOT NULL
            )",
                &[],
            )
            .await?;
        Ok(())
    }

    async fn create(&self, todo: &Todo) -> anyhow::Result<()> {
        self.client
            .execute(
                "INSERT INTO todos (id, title, completed) VALUES ($1, $2, $3)",
                &[&todo.id, &todo.title, &todo.completed],
            )
            .await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<Todo>> {
        let rows = self
            .client
            .query("SELECT id, title, completed FROM todos", &[])
            .await?;

        let mut todos = Vec::new();
        for row in rows {
            todos.push(Todo {
                id: row.get(0),
                title: row.get(1),
                completed: row.get(2),
            });
        }
        Ok(todos)
    }

    async fn complete(&self, id: &str) -> anyhow::Result<()> {
        self.client
            .execute("UPDATE todos SET completed = true WHERE id = $1", &[&id])
            .await?;
        Ok(())
    }
}

// ============================================================================
// RustMemDB Implementation (Adapter for Testing)
// ============================================================================

// This is the part that connects "My Database" to the application interface.
// It allows the exact same application logic to run in tests.

pub struct RustMemDbRepository {
    client: rustmemodb::Client,
}

impl RustMemDbRepository {
    pub async fn new_isolated() -> Self {
        // Connect to an isolated in-memory instance
        let client = rustmemodb::Client::connect_local("admin", "adminpass")
            .await
            .unwrap();
        Self { client }
    }
}

#[async_trait]
impl TodoRepository for RustMemDbRepository {
    async fn init(&self) -> anyhow::Result<()> {
        // RustMemDB SQL syntax is compatible for this simple case
        self.client
            .execute(
                "CREATE TABLE todos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed BOOLEAN NOT NULL
            )",
            )
            .await?;
        Ok(())
    }

    async fn create(&self, todo: &Todo) -> anyhow::Result<()> {
        // RustMemDB currently supports basic SQL. Parameterized queries
        // via `prepared` are not yet fully exposed in the simplified client API,
        // so we format the string (safe in this controlled env, but generic client supports it).
        // Note: In a full impl, we would use the parameterized API if available.
        let sql = format!(
            "INSERT INTO todos (id, title, completed) VALUES ('{}', '{}', {})",
            todo.id, todo.title, todo.completed
        );
        self.client.execute(&sql).await?;
        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Vec<Todo>> {
        let result = self
            .client
            .query("SELECT id, title, completed FROM todos")
            .await?;

        let mut todos = Vec::new();
        for row in result.rows() {
            // Row is Vec<Value>
            todos.push(Todo {
                id: row[0].as_str().unwrap().to_string(),
                title: row[1].as_str().unwrap().to_string(),
                completed: row[2].as_bool(),
            });
        }
        Ok(todos)
    }

    async fn complete(&self, id: &str) -> anyhow::Result<()> {
        let sql = format!("UPDATE todos SET completed = true WHERE id = '{}'", id);
        self.client.execute(&sql).await?;
        Ok(())
    }
}

// ============================================================================
// Main Application Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Try to connect to real Postgres
    let conn_str = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "host=localhost user=postgres password=postgres".to_string());

    println!("Connecting to PostgreSQL at: {}", conn_str);

    match PostgresRepository::connect(&conn_str).await {
        Ok(repo) => {
            println!("Connected to PostgreSQL successfully!");
            let service = TodoService::new(Box::new(repo));

            // Run the app logic
            service.init().await?;
            let t = service.add_todo("Learn Rust".to_string()).await?;
            println!("Created: {:?}", t);

            let list = service.get_all_todos().await?;
            println!("Current todos: {} items", list.len());
        }
        Err(e) => {
            eprintln!("Failed to connect to PostgreSQL: {}", e);
            eprintln!(
                "(This is expected if no Postgres is running. Run unit tests to see RustMemDB in action.)"
            );
        }
    }

    Ok(())
}

// ============================================================================
// Integration Tests (Using RustMemDB)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_todo_application_logic() -> anyhow::Result<()> {
        println!("Running integration test with RustMemDB...");

        // 1. Dependency Injection: Use RustMemDB implementation
        let repository = RustMemDbRepository::new_isolated().await;

        // 2. Initialize Service with the mock DB
        let service = TodoService::new(Box::new(repository));

        // 3. Perform Business Logic Tests
        service.init().await?;

        // Create
        let todo = service.add_todo("Test generic app".to_string()).await?;
        assert_eq!(todo.title, "Test generic app");
        assert!(!todo.completed);

        // List
        let list = service.get_all_todos().await?;
        assert_eq!(list.len(), 1);

        // Update
        service.mark_completed(&todo.id).await?;

        // Verify
        let list_after = service.get_all_todos().await?;
        let updated = list_after.first().unwrap();
        assert!(updated.completed);

        println!("Test Passed: Application logic verified using In-Memory Database!");
        Ok(())
    }
}
