use super::{Connection, config::ConnectionConfig, auth::AuthManager};
use crate::core::{DbError, Result};
use crate::facade::InMemoryDB;
use std::sync::{Arc, RwLock, Mutex};
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
    /// Authentication manager
    auth_manager: Arc<AuthManager>,
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
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let config = ConnectionConfig::new("admin", "password")
    ///     .max_connections(10);
    ///
    /// let pool = ConnectionPool::new(config)?;
    /// ```
    pub fn new(config: ConnectionConfig) -> Result<Self> {
        config.validate()
            .map_err(|e| DbError::ExecutionError(e))?;

        let auth_manager = Arc::new(AuthManager::new());
        let db = Arc::new(RwLock::new(InMemoryDB::new()));
        let available = Arc::new(Mutex::new(VecDeque::new()));
        let total_connections = Arc::new(Mutex::new(0));
        let next_id = Arc::new(Mutex::new(1));

        let pool = Self {
            config,
            available,
            total_connections,
            auth_manager,
            db,
            next_id,
        };

        // Pre-create minimum connections
        pool.ensure_min_connections()?;

        Ok(pool)
    }

    /// Create a connection pool with custom authentication manager
    pub fn with_auth_manager(
        config: ConnectionConfig,
        auth_manager: AuthManager,
    ) -> Result<Self> {
        config.validate()
            .map_err(|e| DbError::ExecutionError(e))?;

        let auth_manager = Arc::new(auth_manager);
        let db = Arc::new(RwLock::new(InMemoryDB::new()));
        let available = Arc::new(Mutex::new(VecDeque::new()));
        let total_connections = Arc::new(Mutex::new(0));
        let next_id = Arc::new(Mutex::new(1));

        let pool = Self {
            config,
            available,
            total_connections,
            auth_manager,
            db,
            next_id,
        };

        pool.ensure_min_connections()?;

        Ok(pool)
    }

    /// Get a connection from the pool
    ///
    /// Blocks until a connection is available or timeout is reached.
    pub fn get_connection(&self) -> Result<PoolGuard> {
        let start = Instant::now();

        loop {
            // Try to get an available connection
            if let Some(mut pooled) = self.try_get_available()? {
                pooled.refresh_last_used();
                return Ok(PoolGuard {
                    connection: Some(pooled.connection),
                    pool: self.available.clone(),
                });
            }

            // Try to create a new connection if under limit
            if let Some(conn) = self.try_create_connection()? {
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
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Try to get an available connection from the pool
    fn try_get_available(&self) -> Result<Option<PooledConnection>> {
        let mut available = self.available.lock()
            .map_err(|_| DbError::LockError("Failed to acquire pool lock".into()))?;

        // Remove expired/idle connections
        available.retain(|pooled| {
            !pooled.is_expired(self.config.max_lifetime) &&
            !pooled.is_idle_too_long(self.config.idle_timeout)
        });

        Ok(available.pop_front())
    }

    /// Try to create a new connection if under limit
    fn try_create_connection(&self) -> Result<Option<Connection>> {
        let mut total = self.total_connections.lock()
            .map_err(|_| DbError::LockError("Failed to acquire connection counter lock".into()))?;

        if *total >= self.config.max_connections {
            return Ok(None);
        }

        // Authenticate user
        let user = self.auth_manager.authenticate(
            &self.config.username,
            &self.config.password,
        )?;

        // Get next connection ID
        let mut next_id = self.next_id.lock()
            .map_err(|_| DbError::LockError("Failed to acquire ID lock".into()))?;
        let id = *next_id;
        *next_id += 1;

        // Create connection
        let connection = Connection::new(id, user, Arc::clone(&self.db));

        *total += 1;

        Ok(Some(connection))
    }

    /// Ensure minimum number of connections
    fn ensure_min_connections(&self) -> Result<()> {
        let mut total = self.total_connections.lock()
            .map_err(|_| DbError::LockError("Failed to acquire connection counter lock".into()))?;

        let mut available = self.available.lock()
            .map_err(|_| DbError::LockError("Failed to acquire pool lock".into()))?;

        while *total < self.config.min_connections {
            // Authenticate user
            let user = self.auth_manager.authenticate(
                &self.config.username,
                &self.config.password,
            )?;

            let mut next_id = self.next_id.lock()
                .map_err(|_| DbError::LockError("Failed to acquire ID lock".into()))?;
            let id = *next_id;
            *next_id += 1;

            let connection = Connection::new(id, user, Arc::clone(&self.db));
            available.push_back(PooledConnection::new(connection));

            *total += 1;
        }

        Ok(())
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let total = self.total_connections.lock().unwrap();
        let available = self.available.lock().unwrap();

        PoolStats {
            total_connections: *total,
            available_connections: available.len(),
            active_connections: *total - available.len(),
            max_connections: self.config.max_connections,
        }
    }

    /// Get the authentication manager
    pub fn auth_manager(&self) -> &Arc<AuthManager> {
        &self.auth_manager
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
    pub fn execute(&mut self, sql: &str) -> Result<crate::result::QueryResult> {
        self.connection().execute(sql)
    }

    /// Begin a transaction (convenience method)
    pub fn begin(&mut self) -> Result<()> {
        self.connection().begin()
    }

    /// Commit a transaction (convenience method)
    pub fn commit(&mut self) -> Result<()> {
        self.connection().commit()
    }

    /// Rollback a transaction (convenience method)
    pub fn rollback(&mut self) -> Result<()> {
        self.connection().rollback()
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            // Return connection to pool
            if let Ok(mut pool) = self.pool.lock() {
                pool.push_back(PooledConnection::new(connection));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let config = ConnectionConfig::new("admin", "admin")
            .min_connections(2)
            .max_connections(5);

        let pool = ConnectionPool::new(config).unwrap();
        let stats = pool.stats();

        assert_eq!(stats.total_connections, 2); // min_connections
        assert_eq!(stats.available_connections, 2);
    }

    #[test]
    fn test_get_connection() {
        let config = ConnectionConfig::new("admin", "admin")
            .max_connections(5);

        let pool = ConnectionPool::new(config).unwrap();
        let mut conn = pool.get_connection().unwrap();

        assert!(conn.connection().is_active());
    }

    #[test]
    fn test_connection_return_to_pool() {
        let config = ConnectionConfig::new("admin", "admin")
            .min_connections(1)
            .max_connections(5);

        let pool = ConnectionPool::new(config).unwrap();

        {
            let _conn = pool.get_connection().unwrap();
            let stats = pool.stats();
            assert_eq!(stats.active_connections, 1);
            assert_eq!(stats.available_connections, 0);
        } // Connection returned here

        // Wait a bit for the connection to be returned
        std::thread::sleep(Duration::from_millis(10));

        let stats = pool.stats();
        assert_eq!(stats.available_connections, 1);
    }

    #[test]
    fn test_max_connections_limit() {
        let config = ConnectionConfig::new("admin", "admin")
            .max_connections(2)
            .connect_timeout(Duration::from_millis(100));

        let pool = ConnectionPool::new(config).unwrap();

        let _conn1 = pool.get_connection().unwrap();
        let _conn2 = pool.get_connection().unwrap();

        // Third connection should timeout
        let result = pool.get_connection();
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_stats() {
        let config = ConnectionConfig::new("admin", "admin")
            .min_connections(2)
            .max_connections(10);

        let pool = ConnectionPool::new(config).unwrap();
        let stats = pool.stats();

        assert_eq!(stats.max_connections, 10);
        assert_eq!(stats.total_connections, 2);
    }
}
