// ============================================================================
// RustMemDB Library
// ============================================================================

pub mod core;
pub mod storage;
pub mod result;
pub mod facade;
pub mod connection;
mod parser;
mod planner;
mod executor;
mod expression;
mod plugins;
mod evaluator;

// Re-export main types for convenience
pub use facade::InMemoryDB;
pub use core::{Result, DbError, Value, DataType};
pub use result::QueryResult;

// Re-export connection API
pub use connection::{
    Connection,
    auth::{AuthManager, User, Permission},
    pool::{ConnectionPool, PoolGuard, PoolStats},
    config::ConnectionConfig,
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
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Connect to database
/// let client = Client::connect("admin", "admin")?;
///
/// // Execute queries
/// client.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")?;
/// client.execute("INSERT INTO users VALUES (1, 'Alice', 30)")?;
///
/// let result = client.query("SELECT * FROM users WHERE age > 25")?;
/// println!("Found {} users", result.row_count());
/// # Ok(())
/// # }
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::connect("admin", "admin")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect(username: &str, password: &str) -> Result<Self> {
        let config = ConnectionConfig::new(username, password);
        let pool = ConnectionPool::new(config)?;
        Ok(Self { pool })
    }

    /// Connect with custom configuration
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::{Client, ConnectionConfig};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ConnectionConfig::new("admin", "admin")
    ///     .max_connections(20)
    ///     .database("mydb");
    ///
    /// let client = Client::connect_with_config(config)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_with_config(config: ConnectionConfig) -> Result<Self> {
        let pool = ConnectionPool::new(config)?;
        Ok(Self { pool })
    }

    /// Connect using a connection string
    ///
    /// Format: `rustmemodb://username:password@host:port/database`
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::connect_url(
    ///     "rustmemodb://admin:admin@localhost:5432/mydb"
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn connect_url(url: &str) -> Result<Self> {
        let config = ConnectionConfig::from_url(url)
            .map_err(|e| DbError::ParseError(e))?;
        let pool = ConnectionPool::new(config)?;
        Ok(Self { pool })
    }

    /// Execute a SQL query
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("admin", "admin")?;
    /// let result = client.query("SELECT * FROM users")?;
    /// for row in result.rows() {
    ///     println!("{:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        let mut conn = self.pool.get_connection()?;
        conn.execute(sql)
    }

    /// Execute a statement (alias for query)
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("admin", "admin")?;
    /// client.execute("CREATE TABLE users (id INTEGER, name TEXT)")?;
    /// client.execute("INSERT INTO users VALUES (1, 'Alice')")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute(&self, sql: &str) -> Result<QueryResult> {
        self.query(sql)
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
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("admin", "admin")?;
    /// let mut conn = client.get_connection()?;
    ///
    /// conn.begin()?;
    /// conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    /// conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)")?;
    /// conn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_connection(&self) -> Result<PoolGuard> {
        self.pool.get_connection()
    }

    /// Get pool statistics
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::Client;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("admin", "admin")?;
    /// let stats = client.stats();
    /// println!("Active connections: {}", stats.active_connections);
    /// # Ok(())
    /// # }
    /// ```
    pub fn stats(&self) -> PoolStats {
        self.pool.stats()
    }

    /// Get the authentication manager for user management
    ///
    /// # Examples
    ///
    /// ```
    /// # use rustmemodb::{Client, Permission};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect("admin", "admin")?;
    /// let auth = client.auth_manager();
    /// auth.create_user("alice", "password123", vec![Permission::Select])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn auth_manager(&self) -> &std::sync::Arc<AuthManager> {
        self.pool.auth_manager()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_connect() {
        let client = Client::connect("admin", "admin").unwrap();
        let stats = client.stats();
        assert!(stats.total_connections > 0);
    }

    #[test]
    fn test_client_execute() {
        let client = Client::connect("admin", "admin").unwrap();

        client.execute("CREATE TABLE test (id INTEGER)").unwrap();
        client.execute("INSERT INTO test VALUES (1)").unwrap();

        let result = client.query("SELECT * FROM test").unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn test_client_transaction() {
        let client = Client::connect("admin", "admin").unwrap();

        client.execute("CREATE TABLE test (id INTEGER)").unwrap();

        let mut conn = client.get_connection().unwrap();

        conn.begin().unwrap();
        conn.execute("INSERT INTO test VALUES (1)").unwrap();
        conn.execute("INSERT INTO test VALUES (2)").unwrap();
        conn.commit().unwrap();

        let result = client.query("SELECT * FROM test").unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn test_client_from_url() {
        let client = Client::connect_url(
            "rustmemodb://admin:admin@localhost:5432/testdb"
        ).unwrap();

        assert!(client.stats().total_connections > 0);
    }
}
