use async_trait::async_trait;
use crate::core::Result;
use crate::result::QueryResult;

/// A generic trait for database clients.
///
/// This trait allows writing code that is agnostic to the underlying database implementation.
/// You can use `Client` (the in-memory DB) for tests and simple apps, or wrap a real database client
/// (like Postgres or MySQL) to implement this trait for production use.
#[async_trait]
pub trait DatabaseClient: Send + Sync {
    /// Execute a query that is expected to return rows (SELECT).
    async fn query(&self, sql: &str) -> Result<QueryResult>;

    /// Execute a query that modifies data (INSERT, UPDATE, DELETE, DDL).
    async fn execute(&self, sql: &str) -> Result<QueryResult>;

    /// Check if the connection is active
    async fn ping(&self) -> Result<()>;
}

/// A factory trait for creating database clients.
#[async_trait]
pub trait DatabaseFactory: Send + Sync {
    type Client: DatabaseClient;

    /// Connect to the database using a connection string.
    async fn connect(&self, url: &str) -> Result<Self::Client>;
}
