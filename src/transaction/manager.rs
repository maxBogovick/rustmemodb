// ============================================================================
// Transaction Manager
// ============================================================================
//
// Central coordinator for all transactions using MVCC (Multi-Version
// Concurrency Control) with snapshot isolation.
//
// Design Patterns:
// - Singleton Pattern: Global transaction coordinator
// - Registry Pattern: Track all active transactions
// - Facade Pattern: Simple API for transaction operations
//
// Concurrency Model:
// - Each transaction gets a unique snapshot version
// - Reads see consistent snapshot (no dirty reads)
// - Writes are isolated until commit
// - Global version counter ensures total ordering
//
// ============================================================================

use super::{Change, Transaction, TransactionId, TransactionState};
use crate::core::{DbError, Result};
use crate::storage::memory::InMemoryStorage;
use crate::facade::InMemoryDB;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Global transaction coordinator
///
/// Manages the lifecycle of all transactions and ensures:
/// - Snapshot isolation (each transaction sees consistent snapshot)
/// - Atomic commits (all-or-nothing)
/// - Proper transaction cleanup
///
/// # Thread Safety
/// Uses RwLock for concurrent access from multiple connections.
/// Read operations (snapshot creation) use read lock.
/// Write operations (commit/rollback) use write lock.
pub struct TransactionManager {
    /// Active transactions indexed by ID
    transactions: Arc<RwLock<HashMap<TransactionId, Transaction>>>,

    /// Global version counter for MVCC
    /// Incremented on each commit to create new database version
    global_version: Arc<RwLock<u64>>,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            global_version: Arc::new(RwLock::new(0)),
        }
    }

    /// Begin a new transaction
    pub async fn begin(&self) -> Result<TransactionId> {
        let txn_id = TransactionId::new();

        // Get current version for snapshot isolation
        let read_version = *self.global_version.read().await;

        let transaction = Transaction::new(txn_id, read_version);

        let mut transactions = self.transactions.write().await;

        transactions.insert(txn_id, transaction);

        Ok(txn_id)
    }

    /// Record a change in a transaction
    pub async fn record_change(&self, txn_id: TransactionId, change: Change) -> Result<()> {
        let mut transactions = self.transactions.write().await;

        let transaction = transactions
            .get_mut(&txn_id)
            .ok_or_else(|| DbError::ExecutionError(format!("Transaction {} not found", txn_id)))?;

        transaction.record_change(change)
    }

    /// Commit a transaction
    ///
    /// Commits the transaction. Changes have already been applied to storage
    /// during execution, so we just mark the transaction as committed and
    /// increment the global version.
    ///
    /// On success:
    /// - Transaction is marked as committed
    /// - Global version is incremented
    ///
    /// # Errors
    /// - Transaction not found
    /// - Transaction not active
    pub async fn commit(&self, txn_id: TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write().await;

        let transaction = transactions
            .get_mut(&txn_id)
            .ok_or_else(|| DbError::ExecutionError(format!("Transaction {} not found", txn_id)))?;

        // Validate transaction can be committed
        if transaction.state() != TransactionState::Active {
            return Err(DbError::ExecutionError(format!(
                "Cannot commit transaction {}: state is {}",
                txn_id,
                transaction.state()
            )));
        }

        // Mark transaction as committed
        // (Changes have already been applied to storage during execution)
        transaction.commit()?;

        // Increment global version to create new snapshot
        let mut version = self.global_version.write().await;
        *version += 1;

        Ok(())
    }

    /// Rollback a transaction using a mutable storage reference (no database locking)
    pub async fn rollback_with_storage(&self, txn_id: TransactionId, storage: &mut InMemoryStorage) -> Result<()> {
        let mut transactions = self.transactions.write().await;

        let transaction = transactions
            .get_mut(&txn_id)
            .ok_or_else(|| DbError::ExecutionError(format!("Transaction {} not found", txn_id)))?;

        if transaction.state() == TransactionState::Active {
            let changes: Vec<Change> = transaction.changes().iter().cloned().collect();
            
            for change in changes.iter().rev() {
                self.undo_change(change, storage).await?;
            }

            transaction.rollback()?;
        }

        Ok(())
    }

    /// Rollback a transaction by acquiring a database lock (for background tasks/Drop)
    pub async fn rollback_database(&self, txn_id: TransactionId, db: Arc<RwLock<InMemoryDB>>) -> Result<()> {
        // We take the lock here and then call rollback_with_storage
        let mut db_guard = db.write().await;
        self.rollback_with_storage(txn_id, db_guard.storage_mut()).await
    }

    /// Rollback a transaction (without storage - for backwards compatibility)
    pub async fn rollback(&self, txn_id: TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write().await;

        let transaction = transactions
            .get_mut(&txn_id)
            .ok_or_else(|| DbError::ExecutionError(format!("Transaction {} not found", txn_id)))?;

        if transaction.state() == TransactionState::Active {
            transaction.rollback()?;
        }

        Ok(())
    }

    /// Get transaction information
    pub async fn get_transaction(&self, txn_id: TransactionId) -> Result<Option<TransactionInfo>> {
        let transactions = self.transactions.read().await;

        Ok(transactions.get(&txn_id).map(|txn| TransactionInfo {
            id: txn.id(),
            state: txn.state(),
            read_version: txn.read_version(),
            change_count: txn.change_count(),
            duration: txn.duration(),
        }))
    }

    /// Get the current global version
    pub async fn current_version(&self) -> Result<u64> {
        let version = self.global_version.read().await;
        Ok(*version)
    }

    /// Get count of active transactions
    pub async fn active_transaction_count(&self) -> Result<usize> {
        let transactions = self.transactions.read().await;

        Ok(transactions
            .values()
            .filter(|txn| txn.state() == TransactionState::Active)
            .count())
    }

    /// Cleanup completed transactions
    pub async fn cleanup(&self) -> Result<usize> {
        let mut transactions = self.transactions.write().await;

        let before_count = transactions.len();
        transactions.retain(|_, txn| txn.state() == TransactionState::Active);
        let removed = before_count - transactions.len();

        Ok(removed)
    }

    /// Apply a single change to storage
    async fn apply_change(&self, change: &Change, storage: &mut InMemoryStorage) -> Result<()> {
        match change {
            Change::InsertRow { table, row } => {
                storage.insert_row(table, row.clone()).await?;
            }
            Change::UpdateRow {
                table,
                row_index,
                new_row,
                ..
            } => {
                storage.update_row_at_index(table, *row_index, new_row.clone()).await?;
            }
            Change::DeleteRow {
                table, row_index, ..
            } => {
                storage.delete_row_at_index(table, *row_index).await?;
            }
            Change::CreateTable { table_schema } => {
                storage.create_table(table_schema.clone()).await?;
            }
            Change::DropTable { name, .. } => {
                storage.drop_table(name).await?;
            }
        }
        Ok(())
    }

    /// Undo a single change from storage (for rollback)
    async fn undo_change(&self, change: &Change, storage: &mut InMemoryStorage) -> Result<()> {
        match change {
            // Undo INSERT: remove the row
            // Note: We need to find and remove the exact row since indexes may have shifted
            Change::InsertRow { table, row } => {
                // Find the row and remove it
                let all_rows = storage.get_all_rows(table).await?;
                if let Some(index) = all_rows.iter().position(|r| r == row) {
                    storage.delete_row_at_index(table, index).await?;
                }
            }
            // Undo UPDATE: restore the old row
            Change::UpdateRow {
                table,
                row_index,
                old_row,
                ..
            } => {
                storage.update_row_at_index(table, *row_index, old_row.clone()).await?;
            }
            // Undo DELETE: insert the row back
            Change::DeleteRow {
                table,
                row_index,
                old_row,
            } => {
                storage.insert_row_at_index(table, *row_index, old_row.clone()).await?;
            }
            // Undo CREATE TABLE: drop the table
            Change::CreateTable { table_schema } => {
                storage.drop_table(table_schema.name()).await?;
            }
            // Undo DROP TABLE: recreate the table
            Change::DropTable { name, schema, rows } => {
                use crate::storage::TableSchema;
                let table_schema = TableSchema::new(name, schema.columns().to_vec());
                storage.create_table(table_schema).await?;
                for row in rows {
                    storage.insert_row(name, row.clone()).await?;
                }
            }
        }
        Ok(())
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of transaction information for monitoring
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub id: TransactionId,
    pub state: TransactionState,
    pub read_version: u64,
    pub change_count: usize,
    pub duration: std::time::Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Value};

    #[tokio::test]
    async fn test_begin_transaction() {
        let manager = TransactionManager::new();
        let txn_id = manager.begin().await.unwrap();

        let info = manager.get_transaction(txn_id).await.unwrap().unwrap();
        assert_eq!(info.state, TransactionState::Active);
        assert_eq!(info.change_count, 0);
    }

    #[tokio::test]
    async fn test_record_change() {
        let manager = TransactionManager::new();
        let txn_id = manager.begin().await.unwrap();

        let change = Change::InsertRow {
            table: "test".to_string(),
            row: vec![Value::Integer(1)],
        };

        manager.record_change(txn_id, change).await.unwrap();

        let info = manager.get_transaction(txn_id).await.unwrap().unwrap();
        assert_eq!(info.change_count, 1);
    }

    #[tokio::test]
    async fn test_commit_increments_version() {
        let manager = TransactionManager::new();
        let _db = Arc::new(RwLock::new(InMemoryDB::new()));

        let version_before = manager.current_version().await.unwrap();

        let txn_id = manager.begin().await.unwrap();
        manager.commit(txn_id).await.unwrap();

        let version_after = manager.current_version().await.unwrap();
        assert_eq!(version_after, version_before + 1);
    }

    #[tokio::test]
    async fn test_rollback() {
        let manager = TransactionManager::new();
        let txn_id = manager.begin().await.unwrap();

        let change = Change::InsertRow {
            table: "test".to_string(),
            row: vec![Value::Integer(1)],
        };
        manager.record_change(txn_id, change).await.unwrap();

        manager.rollback(txn_id).await.unwrap();

        let info = manager.get_transaction(txn_id).await.unwrap().unwrap();
        assert_eq!(info.state, TransactionState::Aborted);
        assert_eq!(info.change_count, 0);
    }

    #[tokio::test]
    async fn test_cannot_commit_twice() {
        let manager = TransactionManager::new();
        let _db = Arc::new(RwLock::new(InMemoryDB::new()));

        let txn_id = manager.begin().await.unwrap();
        manager.commit(txn_id).await.unwrap();

        let result = manager.commit(txn_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cleanup_removes_completed_transactions() {
        let manager = TransactionManager::new();
        let _db = Arc::new(RwLock::new(InMemoryDB::new()));

        let txn1 = manager.begin().await.unwrap();
        let txn2 = manager.begin().await.unwrap();
        let _txn3 = manager.begin().await.unwrap();

        manager.commit(txn1).await.unwrap();
        manager.rollback(txn2).await.unwrap();
        // txn3 remains active

        let removed = manager.cleanup().await.unwrap();
        assert_eq!(removed, 2);

        let active_count = manager.active_transaction_count().await.unwrap();
        assert_eq!(active_count, 1);
    }

    #[tokio::test]
    async fn test_snapshot_isolation_versions() {
        let manager = TransactionManager::new();
        let _db = Arc::new(RwLock::new(InMemoryDB::new()));

        let txn1 = manager.begin().await.unwrap();
        let info1 = manager.get_transaction(txn1).await.unwrap().unwrap();
        let version1 = info1.read_version;

        manager.commit(txn1).await.unwrap();

        let txn2 = manager.begin().await.unwrap();
        let info2 = manager.get_transaction(txn2).await.unwrap().unwrap();
        let version2 = info2.read_version;

        // Second transaction should see a newer version
        assert!(version2 > version1);
    }
}
