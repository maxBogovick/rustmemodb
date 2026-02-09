pub mod auth;
pub mod pool;
pub mod config;

use crate::core::{DbError, Result};
use crate::facade::InMemoryDB;
use crate::result::QueryResult;
use crate::transaction::TransactionId;
use std::sync::{Arc};
use tokio::sync::RwLock;
use auth::{User, enforce_permissions};

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
    transaction_id: Option<TransactionId>,
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
        self.user.username()
    }

    /// Execute a SQL query
    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        if self.state == ConnectionState::Closed {
            return Err(DbError::ExecutionError("Connection is closed".into()));
        }

        // Handle transaction control statements specially
        let trimmed = sql.trim().to_uppercase();
        if trimmed == "BEGIN" || trimmed == "BEGIN TRANSACTION" || trimmed == "START TRANSACTION" {
            self.begin().await?;
            return Ok(QueryResult::empty_with_message("Transaction started".to_string()));
        }
        if trimmed == "COMMIT" || trimmed == "COMMIT TRANSACTION" {
            self.commit().await?;
            return Ok(QueryResult::empty_with_message("Transaction committed".to_string()));
        }
        if trimmed == "ROLLBACK" || trimmed == "ROLLBACK TRANSACTION" {
            self.rollback().await?;
            return Ok(QueryResult::empty_with_message("Transaction rolled back".to_string()));
        }

        let statement = {
            let db = self.db.read().await;
            db.parse_first(sql)?
        };

        enforce_permissions(&self.user, &statement)?;

        let result = {
            let db = self.db.read().await;
            if InMemoryDB::is_read_only_stmt(&statement) {
                db.execute_parsed_readonly_with_params(&statement, self.transaction_id, vec![]).await
            } else if !InMemoryDB::is_ddl_stmt(&statement) {
                db.execute_parsed_with_params_shared(&statement, self.transaction_id, vec![]).await
            } else {
                drop(db);
                let mut db = self.db.write().await;
                db.execute_parsed_with_params(&statement, self.transaction_id, vec![]).await
            }
        };

        match result {
            Ok(result) => Ok(result),
            Err(err) => {
                if self.state == ConnectionState::InTransaction {
                    let _ = self.rollback().await;
                }
                Err(err)
            }
        }
    }

    /// Execute a query and return the result
    ///
    /// Alias for execute() for compatibility with some SQL drivers
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute(sql).await
    }

    /// Execute a statement that doesn't return results (INSERT, UPDATE, DELETE, CREATE, etc.)
    ///
    /// Returns the number of affected rows (for DML) or Ok(()) for DDL
    pub async fn exec(&mut self, sql: &str) -> Result<u64> {
        let result = self.execute(sql).await?;
        Ok(result.row_count() as u64)
    }

    /// Begin a new transaction
    pub async fn begin(&mut self) -> Result<()> {
        if self.state == ConnectionState::Closed {
            return Err(DbError::ExecutionError("Connection is closed".into()));
        }

        if self.state == ConnectionState::InTransaction {
            return Err(DbError::ExecutionError("Transaction already active".into()));
        }

        // Begin transaction via TransactionManager
        let txn_id = {
            let db = self.db.read().await;
            db.transaction_manager().begin().await?
        };

        if let Some(persistence) = self.db.read().await.persistence() {
            let mut persistence_guard = persistence.lock().await;
            if let Err(err) = persistence_guard.log(&crate::storage::WalEntry::BeginTransaction(txn_id.0)) {
                let db = self.db.read().await;
                db.transaction_manager().rollback(txn_id).await?;
                return Err(err);
            }
        }

        self.state = ConnectionState::InTransaction;
        self.transaction_id = Some(txn_id);

        Ok(())
    }

    /// Commit the current transaction
    pub async fn commit(&mut self) -> Result<()> {
        if self.state != ConnectionState::InTransaction {
            return Err(DbError::ExecutionError("No active transaction".into()));
        }

        let txn_id = self.transaction_id.expect("Transaction ID must be set in InTransaction state");

        {
            let db = self.db.read().await;
            if db.transaction_manager().is_conflicted(txn_id).await {
                drop(db);
                let _ = self.rollback().await;
                return Err(DbError::ExecutionError("Write-write conflict detected".into()));
            }
        }

        // Commit transaction via TransactionManager
        {
            let db = self.db.write().await;
            let txn_mgr = Arc::clone(db.transaction_manager());
            txn_mgr.commit(txn_id).await?;
        }

        if let Some(persistence) = self.db.read().await.persistence() {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&crate::storage::WalEntry::Commit(txn_id.0))?;
        }

        self.state = ConnectionState::Active;
        self.transaction_id = None;

        Ok(())
    }

    /// Rollback the current transaction
    pub async fn rollback(&mut self) -> Result<()> {
        if self.state != ConnectionState::InTransaction {
            // SQL standard: rollback without transaction is a no-op
            return Ok(());
        }

        let txn_id = self.transaction_id.expect("Transaction ID must be set in InTransaction state");

        if let Some(persistence) = self.db.read().await.persistence() {
            let mut persistence_guard = persistence.lock().await;
            let _ = persistence_guard.log(&crate::storage::WalEntry::Rollback(txn_id.0));
        }

        // Rollback transaction via TransactionManager
        {
            let mut db = self.db.write().await;
            let txn_mgr = Arc::clone(db.transaction_manager());
            let storage = db.storage_mut();
            txn_mgr.rollback_with_storage(txn_id, storage).await?;
        }

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
    pub async fn close(&mut self) -> Result<()> {
        if self.state == ConnectionState::InTransaction {
            self.rollback().await?;
        }

        self.state = ConnectionState::Closed;
        Ok(())
    }

    /// Prepare a SQL statement (placeholder for future implementation)
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        if self.state == ConnectionState::Closed {
            return Err(DbError::ExecutionError("Connection is closed".into()));
        }

        Ok(PreparedStatement {
            sql: sql.to_string(),
            db: Arc::clone(&self.db),
            user: self.user.clone(),
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.state == ConnectionState::InTransaction {
            // We cannot rollback asynchronously in Drop without spawning, which is risky.
            // Users should call close() explicitly.
            if let Some(tx_id) = self.transaction_id {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    let db = Arc::clone(&self.db);
                    handle.spawn(async move {
                        if let Some(persistence) = db.read().await.persistence() {
                            let mut guard = persistence.lock().await;
                            let _ = guard.log(&crate::storage::WalEntry::Rollback(tx_id.0));
                        }
                        let txn_mgr = {
                            let db_guard = db.read().await;
                            Arc::clone(db_guard.transaction_manager())
                        };
                        let _ = txn_mgr.rollback(tx_id).await;
                    });
                } else {
                    eprintln!("Warning: Connection dropped in transaction without runtime; rollback skipped.");
                }
            }
        }
        self.state = ConnectionState::Closed;
    }
}

/// Prepared statement
///
/// Placeholder for future parameterized query support
pub struct PreparedStatement {
    sql: String,
    db: Arc<RwLock<InMemoryDB>>,
    user: User,
}

impl PreparedStatement {
    /// Execute prepared statement with parameters
    pub async fn execute(&self, params: &[&dyn std::fmt::Display]) -> Result<QueryResult> {
        let values = params
            .iter()
            .map(|p| parse_display_param(p))
            .collect::<Result<Vec<_>>>()?;
        self.execute_with_params(values).await
    }

    pub async fn execute_with_params(&self, params: Vec<crate::core::Value>) -> Result<QueryResult> {
        let statement = {
            let db_guard = self.db.read().await;
            db_guard.parse_first(&self.sql)?
        };

        {
            let db_guard = self.db.read().await;
            enforce_permissions(&self.user, &statement)?;
            if InMemoryDB::is_read_only_stmt(&statement) {
                return db_guard.execute_parsed_readonly_with_params(&statement, None, params).await;
            }
            if !InMemoryDB::is_ddl_stmt(&statement) {
                return db_guard.execute_parsed_with_params_shared(&statement, None, params).await;
            }
        }

        let mut db = self.db.write().await;
        db.execute_parsed_with_params(&statement, None, params).await
    }

    /// Get the SQL text of this prepared statement
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

fn parse_display_param(param: &dyn std::fmt::Display) -> Result<crate::core::Value> {
    let raw = param.to_string();
    let trimmed = raw.trim();

    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(crate::core::Value::Null);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(crate::core::Value::Boolean(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(crate::core::Value::Boolean(false));
    }

    if let Ok(number) = crate::core::Value::parse_number(trimmed) {
        return Ok(number);
    }

    Ok(crate::core::Value::Text(trimmed.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_test_connection() -> Connection {
        let db = Arc::new(RwLock::new(InMemoryDB::new()));
        let user = User::new("test_user".to_string(), "hash".to_string(), Vec::new());
        Connection::new(1, user, db)
    }

    #[tokio::test]
    async fn test_connection_creation() {
        let conn = create_test_connection().await;
        assert_eq!(conn.id(), 1);
        assert_eq!(conn.username(), "test_user");
        assert!(conn.is_active());
        assert!(!conn.is_in_transaction());
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let mut conn = create_test_connection().await;

        assert!(conn.begin().await.is_ok());
        assert!(conn.is_in_transaction());

        assert!(conn.commit().await.is_ok());
        assert!(!conn.is_in_transaction());
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let mut conn = create_test_connection().await;

        assert!(conn.begin().await.is_ok());
        assert!(conn.is_in_transaction());

        assert!(conn.rollback().await.is_ok());
        assert!(!conn.is_in_transaction());
    }

    #[tokio::test]
    async fn test_connection_close() {
        let mut conn = create_test_connection().await;

        assert!(conn.close().await.is_ok());
        assert!(!conn.is_active());

        // Should fail after close
        assert!(conn.execute("SELECT 1").await.is_err());
    }
}
