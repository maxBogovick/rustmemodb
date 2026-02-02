use crate::storage::{InMemoryStorage, PersistenceManager};
use crate::core::{Snapshot, Value};
use crate::transaction::{TransactionId, TransactionManager};
use std::sync::{Arc};
use tokio::sync::Mutex;

pub struct ExecutionContext<'a> {
    pub storage: &'a InMemoryStorage,
    #[allow(dead_code)]
    pub transaction_manager: &'a Arc<TransactionManager>,
    pub transaction_id: Option<TransactionId>,
    pub persistence: Option<&'a Arc<Mutex<PersistenceManager>>>,
    pub snapshot: Snapshot,
    pub params: Vec<Value>,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(
        storage: &'a InMemoryStorage,
        transaction_manager: &'a Arc<TransactionManager>,
        persistence: Option<&'a Arc<Mutex<PersistenceManager>>>,
        snapshot: Snapshot,
    ) -> Self {
        Self {
            storage,
            transaction_manager,
            transaction_id: None,
            persistence,
            snapshot,
            params: Vec::new(),
        }
    }

    pub fn with_transaction(
        storage: &'a InMemoryStorage,
        transaction_manager: &'a Arc<TransactionManager>,
        transaction_id: TransactionId,
        persistence: Option<&'a Arc<Mutex<PersistenceManager>>>,
        snapshot: Snapshot,
    ) -> Self {
        Self {
            storage,
            transaction_manager,
            transaction_id: Some(transaction_id),
            persistence,
            snapshot,
            params: Vec::new(),
        }
    }

    pub fn with_params(mut self, params: Vec<Value>) -> Self {
        self.params = params;
        self
    }

    pub fn is_in_transaction(&self) -> bool {
        self.transaction_id.is_some()
    }

    #[allow(dead_code)]
    pub fn get_transaction_id(&self) -> Option<TransactionId> {
        self.transaction_id
    }
}
