use super::{Table, TableSchema};
use crate::core::{DbError, Result, Row};
use crate::storage::table::Snapshot;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct InMemoryStorage {
    /// Таблицы с индивидуальными блокировками
    tables: HashMap<String, Arc<RwLock<Table>>>,
    /// Только метаданные (имена таблиц) под общим lock'ом - операции быстрые
    metadata_lock: RwLock<()>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            metadata_lock: RwLock::new(()),
        }
    }

    /// Создать таблицу
    pub async fn create_table(&mut self, schema: TableSchema) -> Result<()> {
        let name = schema.name().to_string();

        if self.tables.contains_key(&name) {
            return Err(DbError::TableExists(name));
        }

        let table = Arc::new(RwLock::new(Table::new(schema)));
        self.tables.insert(name, table);
        Ok(())
    }

    /// Удалить таблицу
    pub async fn drop_table(&mut self, table_name: &str) -> Result<()> {
        if self.tables.remove(table_name).is_none() {
            return Err(DbError::TableNotFound(table_name.to_string()));
        }
        Ok(())
    }

    /// Получить handle на таблицу для конкурентного доступа
    pub fn get_table(&self, name: &str) -> Result<Arc<RwLock<Table>>> {
        self.tables
            .get(name)
            .cloned()
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
    }

    /// Вставить строку (MVCC)
    pub async fn insert_row(&self, table_name: &str, row: Row, snapshot: &Snapshot) -> Result<()> {
        let table_handle = self.get_table(table_name)?;
        let mut table = table_handle.write().await;
        table.insert(row, snapshot)?;
        Ok(())
    }

    /// Сканировать таблицу (MVCC Snapshot)
    pub async fn scan_table(&self, table_name: &str, snapshot: &Snapshot) -> Result<Vec<Row>> {
        let table_handle = self.get_table(table_name)?;
        let table = table_handle.read().await;
        Ok(table.scan(snapshot))
    }

    /// Сканировать таблицу с ID (для Update/Delete)
    pub async fn scan_table_with_ids(&self, table_name: &str, snapshot: &Snapshot) -> Result<Vec<(usize, Row)>> {
        let table_handle = self.get_table(table_name)?;
        let table = table_handle.read().await;
        Ok(table.scan_with_ids(snapshot))
    }

    // Legacy/System scan
    pub async fn scan_table_all(&self, table_name: &str) -> Result<Vec<Row>> {
        let table_handle = self.get_table(table_name)?;
        let table = table_handle.read().await;
        let snapshot = Snapshot { tx_id: u64::MAX, active: std::collections::HashSet::new(), aborted: std::collections::HashSet::new(), max_tx_id: u64::MAX };
        Ok(table.scan(&snapshot))
    }

    /// Получить схему таблицы
    pub async fn get_schema(&self, table_name: &str) -> Result<TableSchema> {
        let table_handle = self.get_table(table_name)?;
        let table = table_handle.read().await;
        Ok(table.schema().clone())
    }

    /// Проверить существование таблицы
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Список таблиц
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Количество строк (Approximate)
    pub async fn row_count(&self, table_name: &str) -> Result<usize> {
        let table_handle = self.get_table(table_name)?;
        let table = table_handle.read().await;
        Ok(table.row_count())
    }

    /// Update row (MVCC)
    pub async fn update_row(&self, table_name: &str, id: usize, new_row: Row, snapshot: &Snapshot) -> Result<bool> {
        let table_handle = self.get_table(table_name)?;
        let mut table = table_handle.write().await;
        table.update(id, new_row, snapshot)
    }

    /// Delete row (MVCC)
    pub async fn delete_row(&self, table_name: &str, id: usize, tx_id: u64) -> Result<bool> {
        let table_handle = self.get_table(table_name)?;
        let mut table = table_handle.write().await;
        table.delete(id, tx_id)
    }

    /// Create an index on a column
    pub async fn create_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        let table_handle = self.get_table(table_name)?;
        let mut table = table_handle.write().await;
        table.create_index(column_name)
    }

    /// Scan a table using an index (MVCC)
    pub async fn scan_index(&self, table_name: &str, column_name: &str, value: &crate::core::Value, snapshot: &Snapshot) -> Result<Option<Vec<Row>>> {
        let table_handle = self.get_table(table_name)?;
        let table = table_handle.read().await;

        if let Some(index) = table.get_index(column_name) {
            if let Some(ids) = index.get(value) {
                let mut rows = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(row) = table.get_visible_row(*id, snapshot) {
                        rows.push(row);
                    }
                }
                return Ok(Some(rows));
            } else {
                return Ok(Some(Vec::new())); // Index exists but no match
            }
        }
        
        Ok(None) // Index does not exist
    }

    /// Get all tables (for persistence snapshots)
    pub async fn get_all_tables(&self) -> Result<std::collections::HashMap<String, Table>> {
        let mut tables = std::collections::HashMap::new();

        for (name, table_handle) in &self.tables {
            let table = table_handle.read().await;
            tables.insert(name.clone(), table.clone());
        }

        Ok(tables)
    }

    /// Restore tables from a snapshot (for crash recovery)
    pub async fn restore_tables(&mut self, tables: std::collections::HashMap<String, Table>) -> Result<()> {
        // Clear existing tables
        self.tables.clear();

        // Restore from snapshot
        for (name, table) in tables {
            self.tables.insert(name, Arc::new(RwLock::new(table)));
        }

        Ok(())
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}
