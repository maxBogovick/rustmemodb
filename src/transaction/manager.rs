// ============================================================================
// Transaction Manager
// ============================================================================

use super::{Change, Transaction, TransactionId, TransactionState};
use crate::core::{DbError, Result};
use crate::storage::memory::InMemoryStorage;
use crate::storage::table::Snapshot;
use crate::facade::InMemoryDB;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct TransactionManager {
    transactions: Arc<RwLock<HashMap<TransactionId, Transaction>>>,
    aborted: Arc<RwLock<HashSet<TransactionId>>>,
    global_version: Arc<RwLock<u64>>,
    next_transaction_id: Arc<RwLock<u64>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            aborted: Arc::new(RwLock::new(HashSet::new())),
            global_version: Arc::new(RwLock::new(0)),
            next_transaction_id: Arc::new(RwLock::new(1)),
        }
    }

    pub async fn begin(&self) -> Result<TransactionId> {
        let mut next_id = self.next_transaction_id.write().await;
        let txn_id = *next_id;
        *next_id += 1;

        let read_version = *self.global_version.read().await;
        let transaction_id = TransactionId(txn_id);
        let transaction = Transaction::new(transaction_id, read_version);

        let mut transactions = self.transactions.write().await;
        transactions.insert(transaction_id, transaction);

        Ok(transaction_id)
    }

    pub async fn get_snapshot(&self, txn_id: TransactionId) -> Result<Snapshot> {
        let transactions = self.transactions.read().await;
        let aborted = self.aborted.read().await;
        let next_id = *self.next_transaction_id.read().await;
        
        Ok(Snapshot {
            tx_id: txn_id.0,
            active: transactions.keys().map(|id| id.0).collect(),
            aborted: aborted.iter().map(|id| id.0).collect(),
            max_tx_id: next_id,
        })
    }

    pub async fn get_auto_commit_snapshot(&self) -> Result<Snapshot> {
        let transactions = self.transactions.read().await;
        let aborted = self.aborted.read().await;
        
        let mut next_id_guard = self.next_transaction_id.write().await;
        let this_id = *next_id_guard;
        *next_id_guard += 1;
        
        Ok(Snapshot {
            tx_id: this_id,
            active: transactions.keys().map(|id| id.0).collect(),
            aborted: aborted.iter().map(|id| id.0).collect(),
            max_tx_id: *next_id_guard,
        })
    }

    // Deprecated/No-op: Changes are written directly to MVCC storage
    pub async fn record_change(&self, _txn_id: TransactionId, _change: Change) -> Result<()> {
        Ok(())
    }

    pub async fn commit(&self, txn_id: TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write().await;
        let transaction = transactions
            .get_mut(&txn_id)
            .ok_or_else(|| DbError::ExecutionError(format!("Transaction {} not found", txn_id)))?;

        if transaction.state() != TransactionState::Active {
            return Err(DbError::ExecutionError("Transaction not active".into()));
        }

        transaction.commit()?;
        transactions.remove(&txn_id);

        let mut version = self.global_version.write().await;
        *version += 1;

        Ok(())
    }

    pub async fn rollback_database(&self, txn_id: TransactionId, _db: Arc<RwLock<InMemoryDB>>) -> Result<()> {
        self.rollback(txn_id).await
    }

    pub async fn rollback_with_storage(&self, txn_id: TransactionId, _storage: &mut InMemoryStorage) -> Result<()> {
        self.rollback(txn_id).await
    }

    pub async fn rollback(&self, txn_id: TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write().await;
        
        if let Some(transaction) = transactions.get_mut(&txn_id) {
             transaction.rollback()?;
             transactions.remove(&txn_id);
             
             // Track as aborted so MVCC visibility checks fail
             let mut aborted = self.aborted.write().await;
             aborted.insert(txn_id);
        }
        Ok(())
    }

    pub async fn get_transaction_info(&self, txn_id: TransactionId) -> Result<Option<TransactionInfo>> {
        let transactions = self.transactions.read().await;
        Ok(transactions.get(&txn_id).map(|txn| TransactionInfo {
            id: txn.id(),
            state: txn.state(),
            read_version: txn.read_version(),
            change_count: txn.change_count(),
            duration: txn.duration(),
        }))
    }
}

pub struct TransactionInfo {
    pub id: TransactionId,
    pub state: TransactionState,
    pub read_version: u64,
    pub change_count: usize,
    pub duration: std::time::Duration,
}