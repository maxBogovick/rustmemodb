use crate::storage::InMemoryStorage;
use crate::transaction::{TransactionId, TransactionManager};
use std::sync::Arc;

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
}

impl<'a> ExecutionContext<'a> {
    /// Create a new execution context (auto-commit mode)
    pub fn new(storage: &'a InMemoryStorage, transaction_manager: &'a Arc<TransactionManager>) -> Self {
        Self {
            storage,
            transaction_manager,
            transaction_id: None,
        }
    }

    /// Create execution context within a transaction
    pub fn with_transaction(
        storage: &'a InMemoryStorage,
        transaction_manager: &'a Arc<TransactionManager>,
        transaction_id: TransactionId,
    ) -> Self {
        Self {
            storage,
            transaction_manager,
            transaction_id: Some(transaction_id),
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
