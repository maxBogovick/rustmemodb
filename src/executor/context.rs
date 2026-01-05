use crate::storage::{InMemoryStorage, PersistenceManager};
use crate::transaction::{TransactionId, TransactionManager};
use std::sync::{Arc};
use tokio::sync::Mutex;

/// Execution context for query/statement execution
///
/// Provides access to storage and transaction management.
/// Supports both auto-commit (no transaction) and explicit transaction modes.
pub struct ExecutionContext<'a> {
    /// Reference to storage layer
    pub storage: &'a InMemoryStorage,

    /// Reference to transaction manager
    pub transaction_manager: &'a Arc<TransactionManager>,

    /// Current transaction ID (None = auto-commit mode)
    pub transaction_id: Option<TransactionId>,

    /// Reference to persistence manager for WAL logging (optional)
    pub persistence: Option<&'a Arc<Mutex<PersistenceManager>>>,
}

impl<'a> ExecutionContext<'a> {
    /// Create a new execution context (auto-commit mode)
    pub fn new(
        storage: &'a InMemoryStorage,
        transaction_manager: &'a Arc<TransactionManager>,
        persistence: Option<&'a Arc<Mutex<PersistenceManager>>>,
    ) -> Self {
        Self {
            storage,
            transaction_manager,
            transaction_id: None,
            persistence,
        }
    }

    /// Create execution context within a transaction
    pub fn with_transaction(
        storage: &'a InMemoryStorage,
        transaction_manager: &'a Arc<TransactionManager>,
        transaction_id: TransactionId,
        persistence: Option<&'a Arc<Mutex<PersistenceManager>>>,
    ) -> Self {
        Self {
            storage,
            transaction_manager,
            transaction_id: Some(transaction_id),
            persistence,
        }
    }

    /// Check if executing within a transaction
    pub fn is_in_transaction(&self) -> bool {
        self.transaction_id.is_some()
    }

    /// Get the current transaction ID
    pub fn get_transaction_id(&self) -> Option<TransactionId> {
        self.transaction_id
    }
}
