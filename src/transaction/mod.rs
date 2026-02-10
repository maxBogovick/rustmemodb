// ============================================================================
// Transaction Management Module
// ============================================================================
//
// Implements ACID transactions with Snapshot Isolation using MVCC
// (Multi-Version Concurrency Control)
//
// Design Patterns Used:
// - State Pattern: Transaction state management (Active, Committed, Aborted)
// - Command Pattern: Reversible operations for rollback
// - Copy-on-Write: Snapshot isolation
//
// ============================================================================

pub mod change;
pub mod manager;
pub mod state;

pub use change::Change;
pub use manager::TransactionManager;
pub use state::{Transaction, TransactionId, TransactionState};
