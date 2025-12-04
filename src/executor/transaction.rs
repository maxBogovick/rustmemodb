// ============================================================================
// Transaction Executors
// ============================================================================
//
// Implements executors for transaction control statements:
// - BEGIN/START TRANSACTION
// - COMMIT
// - ROLLBACK
//
// Uses the Facade Pattern to provide simple transaction management.
//
// ============================================================================

use crate::core::{DbError, Result};
use crate::executor::{ExecutionContext, Executor};
use crate::parser::ast::Statement;
use crate::result::QueryResult;

/// Executor for BEGIN/START TRANSACTION statement
///
/// Starts a new transaction for the current connection.
/// Subsequent operations will be isolated until COMMIT or ROLLBACK.
///
/// # Transaction Semantics
/// - Creates snapshot of current database state (MVCC)
/// - All reads see consistent snapshot
/// - All writes are buffered until commit
/// - Nested transactions not supported (error on nested BEGIN)
pub struct BeginExecutor;

impl Executor for BeginExecutor {
    fn name(&self) -> &'static str {
        "BEGIN"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Begin)
    }

    fn execute(&self, _stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        // Check for nested transaction
        if ctx.is_in_transaction() {
            return Err(DbError::ExecutionError(
                "Nested transactions are not supported. Use COMMIT or ROLLBACK first.".into()
            ));
        }

        // Transaction management is handled at the connection level
        // This executor just validates the statement is legal
        // The actual BEGIN is handled in the Client/Connection

        Ok(QueryResult::empty_with_message("Transaction started".to_string()))
    }
}

/// Executor for COMMIT statement
///
/// Commits the current transaction, making all changes permanent.
///
/// # Commit Semantics
/// - All changes are applied atomically
/// - Global version is incremented
/// - Transaction moves to Committed state
/// - On error, transaction remains active (can retry or rollback)
pub struct CommitExecutor;

impl Executor for CommitExecutor {
    fn name(&self) -> &'static str {
        "COMMIT"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Commit)
    }

    fn execute(&self, _stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        // Must be in a transaction to commit
        if !ctx.is_in_transaction() {
            return Err(DbError::ExecutionError(
                "No active transaction. Use BEGIN to start a transaction.".into()
            ));
        }

        // Actual commit is handled at the connection level
        // This executor just validates the statement is legal

        Ok(QueryResult::empty_with_message("Transaction committed".to_string()))
    }
}

/// Executor for ROLLBACK statement
///
/// Aborts the current transaction, discarding all changes.
///
/// # Rollback Semantics
/// - All changes are discarded
/// - Transaction moves to Aborted state
/// - Safe to call multiple times (idempotent)
/// - No effect if no transaction is active
pub struct RollbackExecutor;

impl Executor for RollbackExecutor {
    fn name(&self) -> &'static str {
        "ROLLBACK"
    }

    fn can_handle(&self, stmt: &Statement) -> bool {
        matches!(stmt, Statement::Rollback)
    }

    fn execute(&self, _stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        // Rollback without active transaction is a no-op (SQL standard behavior)
        if !ctx.is_in_transaction() {
            return Ok(QueryResult::empty_with_message(
                "No active transaction to rollback".to_string()
            ));
        }

        // Actual rollback is handled at the connection level
        // This executor just validates the statement is legal

        Ok(QueryResult::empty_with_message("Transaction rolled back".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryStorage;
    use crate::transaction::TransactionManager;
    use std::sync::Arc;

    #[test]
    fn test_begin_executor_can_handle() {
        let executor = BeginExecutor;
        assert!(executor.can_handle(&Statement::Begin));
        assert!(!executor.can_handle(&Statement::Commit));
    }

    #[test]
    fn test_commit_executor_can_handle() {
        let executor = CommitExecutor;
        assert!(executor.can_handle(&Statement::Commit));
        assert!(!executor.can_handle(&Statement::Rollback));
    }

    #[test]
    fn test_rollback_executor_can_handle() {
        let executor = RollbackExecutor;
        assert!(executor.can_handle(&Statement::Rollback));
        assert!(!executor.can_handle(&Statement::Begin));
    }

    #[test]
    fn test_begin_fails_in_transaction() {
        let executor = BeginExecutor;
        let storage = InMemoryStorage::new();
        let txn_mgr = Arc::new(TransactionManager::new());
        let txn_id = txn_mgr.begin().unwrap();

        let ctx = ExecutionContext::with_transaction(&storage, &txn_mgr, txn_id);

        let result = executor.execute(&Statement::Begin, &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Nested"));
    }

    #[test]
    fn test_commit_fails_without_transaction() {
        let executor = CommitExecutor;
        let storage = InMemoryStorage::new();
        let txn_mgr = Arc::new(TransactionManager::new());

        let ctx = ExecutionContext::new(&storage, &txn_mgr);

        let result = executor.execute(&Statement::Commit, &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No active transaction"));
    }

    #[test]
    fn test_rollback_without_transaction_is_noop() {
        let executor = RollbackExecutor;
        let storage = InMemoryStorage::new();
        let txn_mgr = Arc::new(TransactionManager::new());

        let ctx = ExecutionContext::new(&storage, &txn_mgr);

        let result = executor.execute(&Statement::Rollback, &ctx);
        assert!(result.is_ok());
    }
}
