use crate::core::{DbError, Result, Row};
use crate::storage::InMemoryStorage;
use std::collections::HashMap;
use std::sync::{Arc};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone)]
enum TransactionState {
    Active,
    Committed,
    Aborted,
}

#[derive(Clone)]
pub struct Transaction {
    id: u64,
    state: TransactionState,
    isolation_level: IsolationLevel,
    // Сохраняем изменения до commit
    pending_inserts: HashMap<String, Vec<Row>>,
    pending_updates: HashMap<String, Vec<(usize, Row)>>,
    pending_deletes: HashMap<String, Vec<usize>>,
}

impl Transaction {
    fn new(id: u64, isolation_level: IsolationLevel) -> Self {
        Self {
            id,
            state: TransactionState::Active,
            isolation_level,
            pending_inserts: HashMap::new(),
            pending_updates: HashMap::new(),
            pending_deletes: HashMap::new(),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, TransactionState::Active)
    }

    pub fn is_committed(&self) -> bool {
        matches!(self.state, TransactionState::Committed)
    }

    pub fn is_aborted(&self) -> bool {
        matches!(self.state, TransactionState::Aborted)
    }

    fn record_insert(&mut self, table: String, row: Row) {
        self.pending_inserts.entry(table).or_default().push(row);
    }

    fn record_update(&mut self, table: String, index: usize, row: Row) {
        self.pending_updates
            .entry(table)
            .or_default()
            .push((index, row));
    }

    fn record_delete(&mut self, table: String, index: usize) {
        self.pending_deletes.entry(table).or_default().push(index);
    }
}

pub struct TransactionManager {
    next_transaction_id: Arc<RwLock<u64>>,
    active_transactions: Arc<RwLock<HashMap<u64, Transaction>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_transaction_id: Arc::new(RwLock::new(1)),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Начать новую транзакцию
    pub async fn begin_transaction(&self, isolation_level: IsolationLevel) -> Result<u64> {
        let mut next_id = self.next_transaction_id.write().await;
        let tx_id = *next_id;
        *next_id += 1;

        let transaction = Transaction::new(tx_id, isolation_level);
        let mut active = self.active_transactions.write().await;
        active.insert(tx_id, transaction);

        Ok(tx_id)
    }

    /// Зафиксировать транзакцию
    pub async fn commit_transaction(&self, tx_id: u64, storage: &InMemoryStorage) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        let transaction = active
            .get_mut(&tx_id)
            .ok_or_else(|| DbError::ExecutionError("Transaction not found".into()))?;

        if !transaction.is_active() {
            return Err(DbError::ExecutionError("Transaction is not active".into()));
        }

        // 3. Вставки
        for (table, rows) in transaction.pending_inserts.drain() {
            for row in rows {
                storage.insert_row(&table, row).await?;
            }
        }

        transaction.state = TransactionState::Committed;
        active.remove(&tx_id);

        Ok(())
    }

    /// Откатить транзакцию
    pub async fn rollback_transaction(&self, tx_id: u64) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        let transaction = active
            .get_mut(&tx_id)
            .ok_or_else(|| DbError::ExecutionError("Transaction not found".into()))?;

        if !transaction.is_active() {
            return Err(DbError::ExecutionError("Transaction is not active".into()));
        }

        transaction.state = TransactionState::Aborted;
        active.remove(&tx_id);

        Ok(())
    }

    /// Получить транзакцию
    pub async fn get_transaction(&self, tx_id: u64) -> Result<Transaction> {
        let active = self.active_transactions.read().await;
        active
            .get(&tx_id)
            .cloned()
            .ok_or_else(|| DbError::ExecutionError("Transaction not found".into()))
    }

    /// Проверить, есть ли активная транзакция
    pub async fn has_active_transaction(&self, tx_id: u64) -> Result<bool> {
        let active = self.active_transactions.read().await;
        Ok(active.contains_key(&tx_id))
    }

    /// Записать операцию в транзакцию (для отложенного применения)
    pub async fn record_insert(&self, tx_id: u64, table: String, row: Row) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        let transaction = active
            .get_mut(&tx_id)
            .ok_or_else(|| DbError::ExecutionError("Transaction not found".into()))?;

        if !transaction.is_active() {
            return Err(DbError::ExecutionError("Transaction is not active".into()));
        }

        transaction.record_insert(table, row);
        Ok(())
    }

    pub async fn record_update(&self, tx_id: u64, table: String, index: usize, row: Row) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        let transaction = active
            .get_mut(&tx_id)
            .ok_or_else(|| DbError::ExecutionError("Transaction not found".into()))?;

        if !transaction.is_active() {
            return Err(DbError::ExecutionError("Transaction is not active".into()));
        }

        transaction.record_update(table, index, row);
        Ok(())
    }

    pub async fn record_delete(&self, tx_id: u64, table: String, index: usize) -> Result<()> {
        let mut active = self.active_transactions.write().await;
        let transaction = active
            .get_mut(&tx_id)
            .ok_or_else(|| DbError::ExecutionError("Transaction not found".into()))?;

        if !transaction.is_active() {
            return Err(DbError::ExecutionError("Transaction is not active".into()));
        }

        transaction.record_delete(table, index);
        Ok(())
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_begin_transaction() {
        let tm = TransactionManager::new();
        let tx_id = tm.begin_transaction(IsolationLevel::ReadCommitted).await.unwrap();
        assert!(tm.has_active_transaction(tx_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_rollback() {
        let tm = TransactionManager::new();
        let tx_id = tm.begin_transaction(IsolationLevel::ReadCommitted).await.unwrap();

        tm.rollback_transaction(tx_id).await.unwrap();
        assert!(!tm.has_active_transaction(tx_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_multiple_transactions() {
        let tm = TransactionManager::new();
        let tx1 = tm.begin_transaction(IsolationLevel::ReadCommitted).await.unwrap();
        let tx2 = tm.begin_transaction(IsolationLevel::Serializable).await.unwrap();

        assert!(tm.has_active_transaction(tx1).await.unwrap());
        assert!(tm.has_active_transaction(tx2).await.unwrap());
        assert_ne!(tx1, tx2);
    }
}