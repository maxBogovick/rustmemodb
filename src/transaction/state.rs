// ============================================================================
// Transaction State Management
// ============================================================================
//
// Implements the State Pattern for transaction lifecycle management.
// Each transaction moves through defined states: Active -> Committed/Aborted
//
// Uses MVCC (Multi-Version Concurrency Control) with snapshot isolation:
// - Each transaction sees a consistent snapshot of the database
// - Read operations see data as of transaction start time
// - Write operations are isolated until commit
//
// ============================================================================

use super::Change;
use crate::core::Snapshot;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global transaction ID counter
static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

/// Unique identifier for a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionId(pub u64);

impl TransactionId {
    /// Generate a new unique transaction ID
    pub fn new() -> Self {
        TransactionId(NEXT_TXN_ID.fetch_add(1, Ordering::SeqCst))
    }

    pub(crate) fn next_raw() -> u64 {
        NEXT_TXN_ID.load(Ordering::SeqCst)
    }

    /// Get the raw ID value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "txn_{}", self.0)
    }
}

/// Transaction state following the State Pattern
///
/// State transitions:
/// ```text
/// Active ──commit──> Committed
///   │
///   └──rollback──> Aborted
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active and can execute operations
    Active,

    /// Transaction has been successfully committed
    Committed,

    /// Transaction has been aborted/rolled back
    Aborted,
}

impl TransactionState {
    /// Check if transaction can execute operations
    pub fn is_active(&self) -> bool {
        matches!(self, TransactionState::Active)
    }

    /// Check if transaction is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransactionState::Committed | TransactionState::Aborted
        )
    }
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionState::Active => write!(f, "ACTIVE"),
            TransactionState::Committed => write!(f, "COMMITTED"),
            TransactionState::Aborted => write!(f, "ABORTED"),
        }
    }
}

/// A database transaction with MVCC snapshot isolation
///
/// # Thread Safety
/// This structure is designed to be used from a single connection thread.
/// The TransactionManager handles synchronization across connections.
#[derive(Debug)]
pub struct Transaction {
    /// Unique transaction identifier
    id: TransactionId,

    /// Current state (Active, Committed, Aborted)
    state: TransactionState,

    /// Snapshot for read consistency (MVCC)
    /// All reads see data as of this snapshot
    snapshot: Option<Snapshot>,

    /// Changes made during this transaction (Command Pattern)
    changes: Vec<Change>,

    /// Start time for diagnostics
    start_time: std::time::Instant,
}

impl Transaction {
    /// Create a new transaction with the given ID and snapshot
    pub fn new(id: TransactionId, snapshot: Option<Snapshot>) -> Self {
        Self {
            id,
            state: TransactionState::Active,
            snapshot,
            changes: Vec::new(),
            start_time: std::time::Instant::now(),
        }
    }

    /// Get the transaction ID
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// Get the current state
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Get the snapshot
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// Get the read version (snapshot timestamp)
    /// Legacy: returns 0 or max_tx_id from snapshot
    pub fn read_version(&self) -> u64 {
        self.snapshot.as_ref().map(|s| s.max_tx_id).unwrap_or(0)
    }

    /// Get all changes recorded in this transaction
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }

    /// Get the number of changes
    pub fn change_count(&self) -> usize {
        self.changes.len()
    }

    /// Get transaction duration
    pub fn duration(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Record a change in this transaction
    ///
    /// # Errors
    /// Returns error if transaction is not active
    pub fn record_change(&mut self, change: Change) -> crate::core::Result<()> {
        use crate::core::DbError;

        if !self.state.is_active() {
            return Err(DbError::ExecutionError(format!(
                "Cannot record change: transaction {} is {}",
                self.id, self.state
            )));
        }

        self.changes.push(change);
        Ok(())
    }

    /// Mark transaction as committed
    ///
    /// # Errors
    /// Returns error if transaction is not active
    pub fn commit(&mut self) -> crate::core::Result<()> {
        use crate::core::DbError;

        if !self.state.is_active() {
            return Err(DbError::ExecutionError(format!(
                "Cannot commit: transaction {} is already {}",
                self.id, self.state
            )));
        }

        self.state = TransactionState::Committed;
        Ok(())
    }

    /// Mark transaction as aborted and discard changes
    ///
    /// # Errors
    /// Returns error if transaction is not active
    pub fn rollback(&mut self) -> crate::core::Result<()> {
        use crate::core::DbError;

        if !self.state.is_active() {
            return Err(DbError::ExecutionError(format!(
                "Cannot rollback: transaction {} is already {}",
                self.id, self.state
            )));
        }

        self.changes.clear();
        self.state = TransactionState::Aborted;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_id_generation() {
        let id1 = TransactionId::new();
        let id2 = TransactionId::new();
        assert!(id2.as_u64() > id1.as_u64());
    }

    #[test]
    fn test_transaction_lifecycle() {
        let id = TransactionId::new();
        let mut txn = Transaction::new(id, None);

        assert_eq!(txn.state(), TransactionState::Active);
        assert!(txn.state().is_active());
        assert!(!txn.state().is_terminal());

        txn.commit().unwrap();
        assert_eq!(txn.state(), TransactionState::Committed);
        assert!(txn.state().is_terminal());
    }

    #[test]
    fn test_cannot_commit_twice() {
        let id = TransactionId::new();
        let mut txn = Transaction::new(id, None);

        txn.commit().unwrap();
        assert!(txn.commit().is_err());
    }

    #[test]
    fn test_rollback_clears_changes() {
        let id = TransactionId::new();
        let mut txn = Transaction::new(id, None);

        let change = Change::InsertRow {
            table: "test".to_string(),
            row: vec![],
        };
        txn.record_change(change).unwrap();
        assert_eq!(txn.change_count(), 1);

        txn.rollback().unwrap();
        assert_eq!(txn.change_count(), 0);
        assert_eq!(txn.state(), TransactionState::Aborted);
    }

    #[test]
    fn test_cannot_record_change_after_commit() {
        let id = TransactionId::new();
        let mut txn = Transaction::new(id, None);

        txn.commit().unwrap();

        let change = Change::InsertRow {
            table: "test".to_string(),
            row: vec![],
        };
        assert!(txn.record_change(change).is_err());
    }
}
