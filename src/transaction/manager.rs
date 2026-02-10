// ============================================================================
// Transaction Manager
// ============================================================================

use super::{Change, Transaction, TransactionId, TransactionState};
use crate::core::{DbError, Result, Snapshot};
use crate::facade::InMemoryDB;
use crate::storage::memory::InMemoryStorage;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct TransactionManager {
    // Stores full transaction state.
    transactions: Arc<RwLock<HashMap<TransactionId, Transaction>>>,

    // Optimization: Cache active transaction IDs for O(1) snapshot creation.
    // Uses Copy-on-Write (Arc) to allow lock-free reading in snapshots.
    active_ids: Arc<RwLock<Arc<HashSet<u64>>>>,

    // Optimization: Cache aborted IDs.
    // Uses Copy-on-Write (Arc).
    aborted_ids: Arc<RwLock<Arc<HashSet<u64>>>>,
    // Track transactions with write-write conflicts.
    conflicted_ids: Arc<RwLock<Arc<HashSet<u64>>>>,

    global_version: Arc<RwLock<u64>>,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())),
            active_ids: Arc::new(RwLock::new(Arc::new(HashSet::new()))),
            aborted_ids: Arc::new(RwLock::new(Arc::new(HashSet::new()))),
            conflicted_ids: Arc::new(RwLock::new(Arc::new(HashSet::new()))),
            global_version: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn begin(&self) -> Result<TransactionId> {
        let transaction_id = TransactionId::new();
        let txn_id_val = transaction_id.0;

        // Create snapshot for Repeatable Read
        let active = self.active_ids.read().await.clone();
        let aborted = self.aborted_ids.read().await.clone();

        let snapshot = Snapshot {
            tx_id: txn_id_val,
            active,
            aborted,
            max_tx_id: txn_id_val,
        };

        let transaction = Transaction::new(transaction_id, Some(snapshot));

        // Update active cache (COW)
        {
            let mut active_lock = self.active_ids.write().await;
            let mut new_set = (**active_lock).clone();
            new_set.insert(txn_id_val);
            *active_lock = Arc::new(new_set);
        }

        // Insert into transaction map
        let mut transactions = self.transactions.write().await;
        transactions.insert(transaction_id, transaction);

        Ok(transaction_id)
    }

    pub async fn get_snapshot(&self, txn_id: TransactionId) -> Result<Snapshot> {
        // Try to get snapshot from active transaction (Repeatable Read)
        {
            let transactions = self.transactions.read().await;
            if let Some(txn) = transactions.get(&txn_id) {
                if let Some(snapshot) = txn.snapshot() {
                    return Ok(snapshot.clone());
                }
            }
        }

        // Fast path: clone the Arcs (O(1))
        let active = self.active_ids.read().await.clone();
        let aborted = self.aborted_ids.read().await.clone();
        let next_id = TransactionId::next_raw();

        Ok(Snapshot {
            tx_id: txn_id.0,
            active,  // Arc<HashSet<u64>>
            aborted, // Arc<HashSet<u64>>
            max_tx_id: next_id,
        })
    }

    pub async fn get_auto_commit_snapshot(&self) -> Result<Snapshot> {
        let active = self.active_ids.read().await.clone();
        let aborted = self.aborted_ids.read().await.clone();

        let this_id = TransactionId::new().0;
        let next_id = TransactionId::next_raw();

        Ok(Snapshot {
            tx_id: this_id,
            active,
            aborted,
            max_tx_id: next_id,
        })
    }

    // Deprecated/No-op: Changes are written directly to MVCC storage
    pub async fn record_change(&self, _txn_id: TransactionId, _change: Change) -> Result<()> {
        Ok(())
    }

    pub async fn commit(&self, txn_id: TransactionId) -> Result<()> {
        {
            let conflicts = self.conflicted_ids.read().await;
            if conflicts.contains(&txn_id.0) {
                drop(conflicts);
                self.rollback(txn_id).await?;
                return Err(DbError::ExecutionError(
                    "Write-write conflict detected".into(),
                ));
            }
        }
        let mut transactions = self.transactions.write().await;
        let transaction = transactions
            .get_mut(&txn_id)
            .ok_or_else(|| DbError::ExecutionError(format!("Transaction {} not found", txn_id)))?;

        if transaction.state() != TransactionState::Active {
            return Err(DbError::ExecutionError("Transaction not active".into()));
        }

        transaction.commit()?;
        transactions.remove(&txn_id);
        {
            let mut conflicts = self.conflicted_ids.write().await;
            if conflicts.contains(&txn_id.0) {
                let mut new_set = (**conflicts).clone();
                new_set.remove(&txn_id.0);
                *conflicts = Arc::new(new_set);
            }
        }

        // Update active cache (COW)
        {
            let mut active_lock = self.active_ids.write().await;
            if active_lock.contains(&txn_id.0) {
                let mut new_set = (**active_lock).clone();
                new_set.remove(&txn_id.0);
                *active_lock = Arc::new(new_set);
            }
        }

        let mut version = self.global_version.write().await;
        *version += 1;

        Ok(())
    }

    pub async fn rollback_database(
        &self,
        txn_id: TransactionId,
        _db: Arc<RwLock<InMemoryDB>>,
    ) -> Result<()> {
        self.rollback(txn_id).await
    }

    pub async fn rollback_with_storage(
        &self,
        txn_id: TransactionId,
        _storage: &mut InMemoryStorage,
    ) -> Result<()> {
        self.rollback(txn_id).await
    }

    pub async fn rollback(&self, txn_id: TransactionId) -> Result<()> {
        let mut transactions = self.transactions.write().await;

        if let Some(transaction) = transactions.get_mut(&txn_id) {
            transaction.rollback()?;
            transactions.remove(&txn_id);

            // Update active cache (COW) - remove from active
            {
                let mut active_lock = self.active_ids.write().await;
                if active_lock.contains(&txn_id.0) {
                    let mut new_set = (**active_lock).clone();
                    new_set.remove(&txn_id.0);
                    *active_lock = Arc::new(new_set);
                }
            }

            // Update aborted cache (COW) - add to aborted
            {
                let mut aborted_lock = self.aborted_ids.write().await;
                let mut new_set = (**aborted_lock).clone();
                new_set.insert(txn_id.0);
                *aborted_lock = Arc::new(new_set);
            }
            {
                let mut conflicts = self.conflicted_ids.write().await;
                if conflicts.contains(&txn_id.0) {
                    let mut new_set = (**conflicts).clone();
                    new_set.remove(&txn_id.0);
                    *conflicts = Arc::new(new_set);
                }
            }
        }
        Ok(())
    }

    pub async fn mark_conflict(&self, txn_id: TransactionId) {
        let mut conflicts = self.conflicted_ids.write().await;
        if !conflicts.contains(&txn_id.0) {
            let mut new_set = (**conflicts).clone();
            new_set.insert(txn_id.0);
            *conflicts = Arc::new(new_set);
        }
    }

    pub async fn is_conflicted(&self, txn_id: TransactionId) -> bool {
        let conflicts = self.conflicted_ids.read().await;
        conflicts.contains(&txn_id.0)
    }

    pub async fn get_transaction_info(
        &self,
        txn_id: TransactionId,
    ) -> Result<Option<TransactionInfo>> {
        let transactions = self.transactions.read().await;
        Ok(transactions.get(&txn_id).map(|txn| TransactionInfo {
            id: txn.id(),
            state: txn.state(),
            read_version: txn.read_version(),
            change_count: txn.change_count(),
            duration: txn.duration(),
        }))
    }

    /// Fork the transaction manager
    /// Creates a new manager that inherits the history (aborted transactions) but has a clean active state.
    /// Active transactions in the parent are treated as aborted in the child to prevent uncommitted data leakage.
    pub async fn fork(&self) -> Self {
        let active_parent = self.active_ids.read().await;
        let aborted_parent = self.aborted_ids.read().await;

        // In the fork, any transaction that was active in the parent is effectively "lost" (connection broken).
        // We must mark them as aborted so their partial writes are ignored.
        let mut new_aborted = (**aborted_parent).clone();
        for active_id in (**active_parent).iter() {
            new_aborted.insert(*active_id);
        }

        Self {
            transactions: Arc::new(RwLock::new(HashMap::new())), // No active transactions in fork
            active_ids: Arc::new(RwLock::new(Arc::new(HashSet::new()))),
            aborted_ids: Arc::new(RwLock::new(Arc::new(new_aborted))),
            conflicted_ids: Arc::new(RwLock::new(Arc::new(HashSet::new()))),
            global_version: Arc::new(RwLock::new(*self.global_version.read().await)),
        }
    }
}

pub struct TransactionInfo {
    pub id: TransactionId,
    pub state: TransactionState,
    pub read_version: u64,
    pub change_count: usize,
    pub duration: std::time::Duration,
}
