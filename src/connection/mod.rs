pub mod auth;
pub mod pool;
pub mod config;

use crate::core::{DbError, Result};
use crate::facade::InMemoryDB;
use crate::result::QueryResult;
use std::sync::{Arc, RwLock};
use auth::{AuthManager, User};
use config::ConnectionConfig;

/// Database connection handle
///
/// Represents an authenticated connection to the database.
/// Similar to postgres::Connection or mysql::Conn
pub struct Connection {
    /// Unique connection ID
    id: u64,
    /// Authenticated user
    user: User,
    /// Shared database instance
    db: Arc<RwLock<InMemoryDB>>,
    /// Connection state
    state: ConnectionState,
    /// Active transaction ID (if any)
    transaction_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Active,
    InTransaction,
    Closed,
}

impl Connection {
    /// Create a new connection (internal use)
    pub(crate) fn new(id: u64, user: User, db: Arc<RwLock<InMemoryDB>>) -> Self {
        Self {
            id,
            user,
            db,
            state: ConnectionState::Active,
            transaction_id: None,
        }
    }

    /// Get connection ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get authenticated username
    pub fn username(&self) -> &str {
        &self.user.username()
    }

    /// Execute a SQL query
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let result = conn.execute("SELECT * FROM users WHERE age > 25")?;
    /// for row in result.rows() {
    ///     println!("{:?}", row);
    /// }
    /// ```
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        if self.state == ConnectionState::Closed {
            return Err(DbError::ExecutionError("Connection is closed".into()));
        }

        let mut db = self.db.write()
            .map_err(|_| DbError::LockError("Failed to acquire database lock".into()))?;

        db.execute(sql)
    }

    /// Execute a query and return the result
    ///
    /// Alias for execute() for compatibility with some SQL drivers
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute(sql)
    }

    /// Execute a statement that doesn't return results (INSERT, UPDATE, DELETE, CREATE, etc.)
    ///
    /// Returns the number of affected rows (for DML) or Ok(()) for DDL
    pub fn exec(&mut self, sql: &str) -> Result<u64> {
        let result = self.execute(sql)?;
        Ok(result.row_count() as u64)
    }

    /// Begin a new transaction
    ///
    /// # Examples
    ///
    /// ```ignore
    /// conn.begin()?;
    /// conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    /// conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)")?;
    /// conn.commit()?;
    /// ```
    pub fn begin(&mut self) -> Result<()> {
        if self.state == ConnectionState::Closed {
            return Err(DbError::ExecutionError("Connection is closed".into()));
        }

        if self.state == ConnectionState::InTransaction {
            return Err(DbError::ExecutionError("Transaction already active".into()));
        }

        self.state = ConnectionState::InTransaction;
        self.transaction_id = Some(0); // TODO: Real transaction ID from TransactionManager

        Ok(())
    }

    /// Commit the current transaction
    pub fn commit(&mut self) -> Result<()> {
        if self.state != ConnectionState::InTransaction {
            return Err(DbError::ExecutionError("No active transaction".into()));
        }

        // TODO: Call TransactionManager.commit()
        self.state = ConnectionState::Active;
        self.transaction_id = None;

        Ok(())
    }

    /// Rollback the current transaction
    pub fn rollback(&mut self) -> Result<()> {
        if self.state != ConnectionState::InTransaction {
            return Err(DbError::ExecutionError("No active transaction".into()));
        }

        // TODO: Call TransactionManager.rollback()
        self.state = ConnectionState::Active;
        self.transaction_id = None;

        Ok(())
    }

    /// Check if connection is in a transaction
    pub fn is_in_transaction(&self) -> bool {
        self.state == ConnectionState::InTransaction
    }

    /// Check if connection is active
    pub fn is_active(&self) -> bool {
        self.state != ConnectionState::Closed
    }

    /// Close the connection
    pub fn close(&mut self) -> Result<()> {
        if self.state == ConnectionState::InTransaction {
            self.rollback()?;
        }

        self.state = ConnectionState::Closed;
        Ok(())
    }

    /// Prepare a SQL statement (placeholder for future implementation)
    ///
    /// Currently returns a simple prepared statement wrapper
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        if self.state == ConnectionState::Closed {
            return Err(DbError::ExecutionError("Connection is closed".into()));
        }

        Ok(PreparedStatement {
            sql: sql.to_string(),
            db: Arc::clone(&self.db),
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Ensure connection is closed and transaction rolled back
        let _ = self.close();
    }
}

/// Prepared statement
///
/// Placeholder for future parameterized query support
pub struct PreparedStatement {
    sql: String,
    db: Arc<RwLock<InMemoryDB>>,
}

impl PreparedStatement {
    /// Execute prepared statement with parameters
    ///
    /// TODO: Implement parameter binding
    pub fn execute(&self, _params: &[&dyn std::fmt::Display]) -> Result<QueryResult> {
        let mut db = self.db.write()
            .map_err(|_| DbError::LockError("Failed to acquire database lock".into()))?;

        // For now, just execute the SQL as-is
        db.execute(&self.sql)
    }

    /// Get the SQL text of this prepared statement
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_connection() -> Connection {
        let db = Arc::new(RwLock::new(InMemoryDB::new()));
        let user = User::new("test_user".to_string(), "hash".to_string(),Vec::new());
        Connection::new(1, user, db)
    }

    #[test]
    fn test_connection_creation() {
        let conn = create_test_connection();
        assert_eq!(conn.id(), 1);
        assert_eq!(conn.username(), "test_user");
        assert!(conn.is_active());
        assert!(!conn.is_in_transaction());
    }

    #[test]
    fn test_transaction_lifecycle() {
        let mut conn = create_test_connection();

        assert!(conn.begin().is_ok());
        assert!(conn.is_in_transaction());

        assert!(conn.commit().is_ok());
        assert!(!conn.is_in_transaction());
    }

    #[test]
    fn test_transaction_rollback() {
        let mut conn = create_test_connection();

        assert!(conn.begin().is_ok());
        assert!(conn.is_in_transaction());

        assert!(conn.rollback().is_ok());
        assert!(!conn.is_in_transaction());
    }

    #[test]
    fn test_connection_close() {
        let mut conn = create_test_connection();

        assert!(conn.close().is_ok());
        assert!(!conn.is_active());

        // Should fail after close
        assert!(conn.execute("SELECT 1").is_err());
    }

    #[test]
    fn test_auto_rollback_on_drop() {
        let mut conn = create_test_connection();
        conn.begin().unwrap();

        // Drop should auto-rollback
        drop(conn);
    }
}
