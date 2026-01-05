use super::{Table, TableSchema};
use crate::core::{DbError, Result, Row};
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

    /// Вставить строку - блокируется только одна таблица
    pub async fn insert_row(&self, table_name: &str, row: Row) -> Result<()> {
        let table_handle = self.get_table(table_name)?;

        let mut table = table_handle.write().await;

        table.insert(row)?;
        Ok(())
    }

    /// Сканировать таблицу - read lock только на одну таблицу
    pub async fn scan_table(&self, table_name: &str) -> Result<Vec<Row>> {
        let table_handle = self.get_table(table_name)?;

        let table = table_handle.read().await;

        Ok(table.rows_iter().map(|(_, r)| r.clone()).collect())
    }

    /// Insert a row at a specific index (for transaction rollback)
    pub async fn insert_row_at_index(&self, table_name: &str, index: usize, row: Row) -> Result<()> {
        let table_handle = self.get_table(table_name)?;

        let mut table = table_handle.write().await;

        table.insert_at_id(index, row)
    }

    /// Получить схему таблицы
    pub async fn get_schema(&self, table_name: &str) -> Result<TableSchema> {
        let table_handle = self.get_table(table_name)?;

        let table = table_handle.read().await;

        Ok(table.schema().clone())
    }

    /// Проверить существование таблицы - быстрая операция без lock'а на таблицы
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Список таблиц
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Количество строк
    pub async fn row_count(&self, table_name: &str) -> Result<usize> {
        let table_handle = self.get_table(table_name)?;

        let table = table_handle.read().await;

        Ok(table.row_count())
    }

    /// Update a row at a specific index (for transaction support)
    pub async fn update_row_at_index(&self, table_name: &str, index: usize, new_row: Row) -> Result<()> {
        let table_handle = self.get_table(table_name)?;

        let mut table = table_handle.write().await;

        table.update_row(index, new_row)
    }

    /// Delete a row at a specific index (for transaction support)
    pub async fn delete_row_at_index(&self, table_name: &str, index: usize) -> Result<()> {
        let table_handle = self.get_table(table_name)?;

        let mut table = table_handle.write().await;

        table.delete_rows(vec![index])?;
        Ok(())
    }

    /// Get all rows from a table (for transaction snapshotting)
    pub async fn get_all_rows(&self, table_name: &str) -> Result<Vec<Row>> {
        self.scan_table(table_name).await
    }

    /// Create an index on a column
    pub async fn create_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        let table_handle = self.get_table(table_name)?;
        let mut table = table_handle.write().await;
        
        table.create_index(column_name)
    }

    /// Scan a table using an index (exact match)
    pub async fn scan_index(&self, table_name: &str, column_name: &str, value: &crate::core::Value) -> Result<Option<Vec<Row>>> {
        let table_handle = self.get_table(table_name)?;
        let table = table_handle.read().await;

        if let Some(index) = table.get_index(column_name) {
            if let Some(ids) = index.get(value) {
                let mut rows = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(row) = table.get_row(*id) {
                        rows.push(row.clone());
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
