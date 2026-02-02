use super::{Table, TableSchema};
use crate::core::{Column, DbError, Result, Row, Snapshot};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct InMemoryStorage {
    /// Таблицы с индивидуальными блокировками
    tables: HashMap<String, Arc<RwLock<Table>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
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

    /// Добавить колонку в таблицу
    pub async fn add_column(&self, table_name: &str, column: Column) -> Result<()> {
        let table_handle = self.get_table(table_name)?;
        let mut table = table_handle.write().await;
        table.add_column(column)
    }

    /// Удалить колонку из таблицы
    pub async fn drop_column(&self, table_name: &str, column_name: &str) -> Result<()> {
        let table_handle = self.get_table(table_name)?;
        let mut table = table_handle.write().await;
        table.drop_column(column_name)
    }

    /// Переименовать колонку
    pub async fn rename_column(&self, table_name: &str, old_name: &str, new_name: &str) -> Result<()> {
        let table_handle = self.get_table(table_name)?;
        let mut table = table_handle.write().await;
        table.rename_column(old_name, new_name)
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
        let snapshot = Snapshot {
            tx_id: u64::MAX,
            active: Arc::new(std::collections::HashSet::new()),
            aborted: Arc::new(std::collections::HashSet::new()),
            max_tx_id: u64::MAX
        };
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

    /// Vacuum all tables to remove dead row versions
    pub async fn vacuum_all_tables(&self, min_active_tx_id: u64, aborted: &std::collections::HashSet<u64>) -> Result<usize> {
        let mut total_freed = 0;
        for table_handle in self.tables.values() {
            let mut table = table_handle.write().await;
            total_freed += table.vacuum(min_active_tx_id, aborted);
        }
        Ok(total_freed)
    }

    /// Fork the storage (Copy-On-Write)
    /// Creates a new storage instance that shares the underlying data structures
    /// with the current one. Writes to the new storage will not affect the old one,
    /// and vice versa, thanks to persistent data structures (im crate).
    pub async fn fork(&self) -> Result<Self> {
        let mut new_tables = HashMap::new();

        for (name, table_handle) in &self.tables {
            let table = table_handle.read().await;
            // Table::clone() is now O(1) due to im::OrdMap
            let new_table = table.clone();
            new_tables.insert(name.clone(), Arc::new(RwLock::new(new_table)));
        }

        Ok(Self {
            tables: new_tables,
        })
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}
