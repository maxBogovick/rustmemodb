use super::{Connection, config::ConnectionConfig, auth::AuthManager};
use crate::core::{DbError, Result};
use crate::facade::InMemoryDB;
use std::sync::{Arc};
use tokio::sync::{RwLock, Mutex};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Connection pool
///
/// Manages a pool of database connections for efficient resource utilization.
/// Similar to connection pools in PostgreSQL (pgpool) or MySQL (connection pooling).
pub struct ConnectionPool {
    /// Pool configuration
    config: ConnectionConfig,
    /// Available connections
    available: Arc<Mutex<VecDeque<PooledConnection>>>,
    /// Total number of connections created
    total_connections: Arc<Mutex<usize>>,
    /// Shared database instance
    db: Arc<RwLock<InMemoryDB>>,
    /// Next connection ID
    next_id: Arc<Mutex<u64>>,
}

/// A connection from the pool
struct PooledConnection {
    connection: Connection,
    created_at: Instant,
    last_used: Instant,
}

impl PooledConnection {
    fn new(connection: Connection) -> Self {
        let now = Instant::now();
        Self {
            connection,
            created_at: now,
            last_used: now,
        }
    }

    fn is_expired(&self, max_lifetime: Option<Duration>) -> bool {
        if let Some(lifetime) = max_lifetime {
            self.created_at.elapsed() > lifetime
        } else {
            false
        }
    }

    fn is_idle_too_long(&self, idle_timeout: Option<Duration>) -> bool {
        if let Some(timeout) = idle_timeout {
            self.last_used.elapsed() > timeout
        } else {
            false
        }
    }

    fn refresh_last_used(&mut self) {
        self.last_used = Instant::now();
    }
}

impl ConnectionPool {
    /// Create a new connection pool
    pub async fn new(config: ConnectionConfig) -> Result<Self> {
        config.validate()
            .map_err(|e| DbError::ExecutionError(e))?;

        let db = Arc::clone(InMemoryDB::global());
        let available = Arc::new(Mutex::new(VecDeque::new()));
        let total_connections = Arc::new(Mutex::new(0));
        let next_id = Arc::new(Mutex::new(1));

        let pool = Self {
            config,
            available,
            total_connections,
            db,
            next_id,
        };

        // Pre-create minimum connections
        pool.ensure_min_connections().await?;

        Ok(pool)
    }

    /// Create a connection pool with custom authentication manager
    #[deprecated(since = "0.1.0", note = "AuthManager is now a global singleton. Use ConnectionPool::new() instead.")]
    pub async fn with_auth_manager(
        config: ConnectionConfig,
        _auth_manager: AuthManager,
    ) -> Result<Self> {
        // Ignore the provided auth_manager and use the global singleton
        Self::new(config).await
    }

    /// Get a connection from the pool
    pub async fn get_connection(&self) -> Result<PoolGuard> {
        let start = Instant::now();

        loop {
            // Try to get an available connection
            if let Some(mut pooled) = self.try_get_available().await? {
                pooled.refresh_last_used();
                return Ok(PoolGuard {
                    connection: Some(pooled.connection),
                    pool: self.available.clone(),
                });
            }

            // Try to create a new connection if under limit
            if let Some(conn) = self.try_create_connection().await? {
                return Ok(PoolGuard {
                    connection: Some(conn),
                    pool: self.available.clone(),
                });
            }

            // Check timeout
            if start.elapsed() > self.config.connect_timeout {
                return Err(DbError::ExecutionError(
                    "Connection pool timeout: no connections available".into()
                ));
            }

            // Wait a bit before retrying
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Try to get an available connection from the pool
    async fn try_get_available(&self) -> Result<Option<PooledConnection>> {
        let mut available = self.available.lock().await;

        // Remove expired/idle connections
        available.retain(|pooled| {
            !pooled.is_expired(self.config.max_lifetime) &&
            !pooled.is_idle_too_long(self.config.idle_timeout)
        });

        Ok(available.pop_front())
    }

    /// Try to create a new connection if under limit
    async fn try_create_connection(&self) -> Result<Option<Connection>> {
        let mut total = self.total_connections.lock().await;

        if *total >= self.config.max_connections {
            return Ok(None);
        }

        // Authenticate user
        let user = AuthManager::global().authenticate(
            &self.config.username,
            &self.config.password,
        ).await?;

        // Get next connection ID
        let mut next_id = self.next_id.lock().await;
        let id = *next_id;
        *next_id += 1;

        // Create connection
        let connection = Connection::new(id, user, Arc::clone(&self.db));

        *total += 1;

        Ok(Some(connection))
    }

    /// Ensure minimum number of connections
    async fn ensure_min_connections(&self) -> Result<()> {
        let mut total = self.total_connections.lock().await;
        let mut available = self.available.lock().await;

        while *total < self.config.min_connections {
            // Authenticate user
            let user = AuthManager::global().authenticate(
                &self.config.username,
                &self.config.password,
            ).await?;

            let mut next_id = self.next_id.lock().await;
            let id = *next_id;
            *next_id += 1;

            let connection = Connection::new(id, user, Arc::clone(&self.db));
            available.push_back(PooledConnection::new(connection));

            *total += 1;
        }

        Ok(())
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        let total = self.total_connections.lock().await;
        let available = self.available.lock().await;

        PoolStats {
            total_connections: *total,
            available_connections: available.len(),
            active_connections: *total - available.len(),
            max_connections: self.config.max_connections,
        }
    }

    /// Get the authentication manager
    pub fn auth_manager(&self) -> &Arc<AuthManager> {
        AuthManager::global()
    }
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_connections: usize,
    pub available_connections: usize,
    pub active_connections: usize,
    pub max_connections: usize,
}

impl std::fmt::Display for PoolStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Pool Stats: {}/{} active, {} available, max {}",
            self.active_connections,
            self.total_connections,
            self.available_connections,
            self.max_connections
        )
    }
}

/// RAII guard for pooled connections
///
/// Returns the connection to the pool when dropped
pub struct PoolGuard {
    connection: Option<Connection>,
    pool: Arc<Mutex<VecDeque<PooledConnection>>>,
}

impl PoolGuard {
    /// Get a reference to the connection
    pub fn connection(&mut self) -> &mut Connection {
        self.connection.as_mut().expect("Connection already returned to pool")
    }

    /// Execute a query (convenience method)
    pub async fn execute(&mut self, sql: &str) -> Result<crate::result::QueryResult> {
        self.connection().execute(sql).await
    }

    /// Begin a transaction (convenience method)
    pub async fn begin(&mut self) -> Result<()> {
        self.connection().begin().await
    }

    /// Commit a transaction (convenience method)
    pub async fn commit(&mut self) -> Result<()> {
        self.connection().commit().await
    }

    /// Rollback a transaction (convenience method)
    pub async fn rollback(&mut self) -> Result<()> {
        self.connection().rollback().await
    }

    /// Explicitly close the guard and return the connection to the pool.
    ///
    /// This method allows for async cleanup (rollback) which is not possible in Drop.
    pub async fn close(mut self) -> Result<()> {
        if let Some(mut connection) = self.connection.take() {
            // Rollback if needed
            if connection.is_in_transaction() {
                connection.rollback().await?;
            }

            // Return to pool
            let mut pool = self.pool.lock().await;
            pool.push_back(PooledConnection::new(connection));
        }
        Ok(())
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            // If we are here, close() was not called.
            // Check if we can return it to the pool immediately (only if no transaction).
            
            if connection.is_in_transaction() {
                 eprintln!("Warning: PoolGuard dropped with active transaction. Connection dropped/leaked because async rollback is not possible in Drop. Use pool_guard.close().await.");
                 // Connection is dropped here (leaked from pool perspective, but memory freed)
                 // NOTE: We can't easily decrement total_connections here because we can't lock async mutex.
                 return;
            }

            // Try to return to pool if we can acquire the lock immediately
            if let Ok(mut pool) = self.pool.try_lock() {
                pool.push_back(PooledConnection::new(connection));
            } else {
                 eprintln!("Warning: PoolGuard dropped and pool lock busy. Connection dropped/leaked. Use pool_guard.close().await.");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pool_creation() {
        let config = ConnectionConfig::new("admin", "adminpass")
            .min_connections(2)
            .max_connections(5);

        let pool = ConnectionPool::new(config).await.unwrap();
        let stats = pool.stats().await;

        assert_eq!(stats.total_connections, 2); // min_connections
        assert_eq!(stats.available_connections, 2);
    }

    #[tokio::test]
    async fn test_get_connection() {
        let config = ConnectionConfig::new("admin", "adminpass")
            .max_connections(5);

        let pool = ConnectionPool::new(config).await.unwrap();
        let mut conn = pool.get_connection().await.unwrap();

        assert!(conn.connection().is_active());
    }

    #[tokio::test]
    async fn test_connection_return_to_pool() {
        let config = ConnectionConfig::new("admin", "adminpass")
            .min_connections(1)
            .max_connections(5);

        let pool = ConnectionPool::new(config).await.unwrap();

        {
            let _conn = pool.get_connection().await.unwrap();
            let stats = pool.stats().await;
            assert_eq!(stats.active_connections, 1);
            assert_eq!(stats.available_connections, 0);
        } // Connection returned here

        // Wait a bit for the connection to be returned
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = pool.stats().await;
        assert_eq!(stats.available_connections, 1);
    }

    #[tokio::test]
    async fn test_max_connections_limit() {
        let config = ConnectionConfig::new("admin", "adminpass")
            .max_connections(2)
            .connect_timeout(Duration::from_millis(100));

        let pool = ConnectionPool::new(config).await.unwrap();

        let _conn1 = pool.get_connection().await.unwrap();
        let _conn2 = pool.get_connection().await.unwrap();

        // Third connection should timeout
        let result = pool.get_connection().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pool_stats() {
        let config = ConnectionConfig::new("admin", "adminpass")
            .min_connections(2)
            .max_connections(10);

        let pool = ConnectionPool::new(config).await.unwrap();
        let stats = pool.stats().await;

        assert_eq!(stats.max_connections, 10);
        assert_eq!(stats.total_connections, 2);
    }
}
