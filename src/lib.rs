// ============================================================================
// RustMemDB Library
// ============================================================================

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod connection;
pub mod core;
mod evaluator;
mod executor;
mod expression;
pub mod facade;
pub mod interface;
pub mod json;
pub mod model_lang;
mod parser;
pub mod planner;
mod plugins;
pub mod result;
pub mod server;
pub mod storage;
pub mod transaction;

// Re-export main types for convenience
pub use core::{DataType, DbError, Result, Row, Value};
pub use facade::InMemoryDB;
pub use interface::{DatabaseClient, DatabaseFactory};
pub use model_lang::{
    FieldDecl, FieldType, ModelProgram, StructDecl, parse_and_materialize_models,
};
pub use result::QueryResult;

// Re-export persistence types
pub use storage::{DurabilityMode, PersistenceManager, WalEntry};

// Re-export JSON API
pub use json::{JsonError, JsonResult, JsonStorageAdapter};

// Re-export connection API
pub use connection::{
    Connection,
    auth::{AuthManager, Permission, User},
    config::ConnectionConfig,
    pool::{ConnectionPool, PoolGuard, PoolStats},
};

// ============================================================================
// High-level Client API (PostgreSQL/MySQL-like)
// ============================================================================

/// Database client with connection pooling
///
/// This is the recommended way to use RustMemDB in applications.
/// Similar to:
/// - PostgreSQL: `postgres::Client`
/// - MySQL: `mysql::Pool`
///
/// # Examples
///
/// ```
/// use rustmemodb::Client;
///
/// # tokio_test::block_on(async {
/// // Connect to database
/// let client = Client::connect("admin", "adminpass").await?;
///
/// // Execute queries
/// client.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").await?;
/// client.execute("INSERT INTO users VALUES (1, 'Alice', 30)").await?;
///
/// let result = client.query("SELECT * FROM users WHERE age > 25").await?;
/// println!("Found {} users", result.row_count());
/// # Ok::<(), rustmemodb::core::DbError>(())
/// # }).unwrap();
/// ```
pub struct Client {
    pool: ConnectionPool,
}

impl Client {
    /// Connect to database with username and password
    ///
    /// Uses default configuration with connection pooling.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # tokio_test::block_on(async {
    /// let client = Client::connect("admin", "adminpass").await?;
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn connect(username: &str, password: &str) -> Result<Self> {
        let config = ConnectionConfig::new(username, password);
        let pool = ConnectionPool::new(config).await?;
        Ok(Self { pool })
    }

    /// Connect with custom configuration
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::{Client, ConnectionConfig};
    /// # tokio_test::block_on(async {
    /// let config = ConnectionConfig::new("admin", "admin")
    ///     .max_connections(20)
    ///     .database("mydb");
    ///
    /// let client = Client::connect_with_config(config).await?;
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn connect_with_config(config: ConnectionConfig) -> Result<Self> {
        let pool = ConnectionPool::new(config).await?;
        Ok(Self { pool })
    }

    /// Connect using a connection string
    ///
    /// Supports the following formats:
    /// - `rustmemodb://username:password@host:port/database`
    /// - `postgres://username:password@host:port/database`
    /// - `postgresql://username:password@host:port/database`
    /// - `mysql://username:password@host:port/database`
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_url(
    ///     "postgres://admin:adminpass@localhost:5432/mydb"
    /// ).await?;
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn connect_url(url: &str) -> Result<Self> {
        let config = ConnectionConfig::from_url(url).map_err(DbError::ParseError)?;
        let pool = ConnectionPool::new(config).await?;
        Ok(Self { pool })
    }

    /// Create an isolated client with its own in-memory database instance.
    ///
    /// This is ideal for unit tests, ensuring that parallel tests do not interfere with each other.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # tokio_test::block_on(async {
    /// let client = Client::connect_local("admin", "adminpass").await?;
    /// // This client is completely isolated from other clients
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn connect_local(username: &str, password: &str) -> Result<Self> {
        let config = ConnectionConfig::new(username, password);
        let pool = ConnectionPool::new_isolated(config).await?;
        Ok(Self { pool })
    }

    /// Execute a SQL query
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # tokio_test::block_on(async {
    /// # let client = Client::connect("admin", "adminpass").await?;
    /// let result = client.query("SELECT * FROM users").await?;
    /// for row in result.rows() {
    ///     println!("{:?}", row);
    /// }
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        let mut conn = self.pool.get_connection().await?;
        conn.execute(sql).await
    }

    /// Execute a statement (alias for query)
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # tokio_test::block_on(async {
    /// # let client = Client::connect("admin", "adminpass").await?;
    /// client.execute("CREATE TABLE users (id INTEGER, name TEXT)").await?;
    /// client.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.query(sql).await
    }

    /// Get a connection from the pool for advanced usage
    ///
    /// Use this when you need transaction support or multiple operations
    /// on the same connection.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # tokio_test::block_on(async {
    /// # let client = Client::connect("admin", "adminpass").await?;
    /// let mut conn = client.get_connection().await?;
    ///
    /// conn.begin().await?;
    /// conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)").await?;
    /// conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)").await?;
    /// conn.commit().await?;
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn get_connection(&self) -> Result<PoolGuard> {
        self.pool.get_connection().await
    }

    /// Get pool statistics
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # tokio_test::block_on(async {
    /// # let client = Client::connect("admin", "adminpass").await?;
    /// let stats = client.stats().await;
    /// println!("Active connections: {}", stats.active_connections);
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn stats(&self) -> PoolStats {
        self.pool.stats().await
    }

    /// Get the authentication manager for user management
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::{Client, Permission};
    /// # tokio_test::block_on(async {
    /// # let client = Client::connect("admin", "adminpass").await?;
    /// let auth = client.auth_manager();
    /// auth.create_user("alice", "password123", vec![Permission::Select]).await?;
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub fn auth_manager(&self) -> &std::sync::Arc<AuthManager> {
        self.pool.auth_manager()
    }

    /// Create a lightweight, isolated fork of the current database.
    ///
    /// This uses Copy-On-Write (COW) mechanics, so it is an O(1) operation
    /// regardless of database size. It is perfect for test isolation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # tokio_test::block_on(async {
    /// let master = Client::connect_local("admin", "pass").await?;
    /// master.execute("CREATE TABLE test (id INT)").await?;
    ///
    /// // Create two independent forks
    /// let test1 = master.fork().await?;
    /// let test2 = master.fork().await?;
    ///
    /// test1.execute("INSERT INTO test VALUES (1)").await?;
    /// test2.execute("INSERT INTO test VALUES (2)").await?;
    ///
    /// // Master is untouched
    /// let count = master.query("SELECT * FROM test").await?.row_count();
    /// assert_eq!(count, 0);
    /// # Ok::<(), rustmemodb::core::DbError>(())
    /// # });
    /// ```
    pub async fn fork(&self) -> Result<Self> {
        let new_pool = self.pool.fork().await?;
        Ok(Self { pool: new_pool })
    }
}

#[async_trait::async_trait]
impl interface::DatabaseClient for Client {
    async fn query(&self, sql: &str) -> Result<QueryResult> {
        self.query(sql).await
    }

    async fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.execute(sql).await
    }

    async fn ping(&self) -> Result<()> {
        // Simple ping implementation: execute a lightweight query
        self.execute("SELECT 1").await.map(|_| ())
    }
}

/// Default factory for creating RustMemDB clients
pub struct RustMemDbFactory;

#[async_trait::async_trait]
impl interface::DatabaseFactory for RustMemDbFactory {
    type Client = Client;

    async fn connect(&self, url: &str) -> Result<Self::Client> {
        Client::connect_url(url).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_connect() {
        let client = Client::connect("admin", "adminpass").await.unwrap();
        let stats = client.stats().await;
        assert!(stats.total_connections > 0);
    }

    #[tokio::test]
    async fn test_client_execute() {
        let client = Client::connect("admin", "adminpass").await.unwrap();

        client
            .execute("CREATE TABLE test (id INTEGER)")
            .await
            .unwrap();
        client.execute("INSERT INTO test VALUES (1)").await.unwrap();

        let result = client.query("SELECT * FROM test").await.unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[tokio::test]
    async fn test_client_transaction() {
        let client = Client::connect("admin", "adminpass").await.unwrap();

        client
            .execute("CREATE TABLE test1 (id INTEGER)")
            .await
            .unwrap();

        let mut conn = client.get_connection().await.unwrap();

        conn.begin().await.unwrap();
        conn.execute("INSERT INTO test1 VALUES (1)").await.unwrap();
        conn.execute("INSERT INTO test1 VALUES (2)").await.unwrap();
        conn.commit().await.unwrap();

        let result = client.query("SELECT * FROM test1").await.unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[tokio::test]
    async fn test_client_from_url() {
        let client = Client::connect_url("postgres://admin:adminpass@localhost:5432/testdb")
            .await
            .unwrap();

        assert!(client.stats().await.total_connections > 0);
    }
}
