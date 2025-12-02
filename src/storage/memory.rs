use super::{Table, TableSchema};
use crate::core::{DbError, Result, Row};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
    pub fn create_table(&mut self, schema: TableSchema) -> Result<()> {
        let name = schema.name().to_string();

        if self.tables.contains_key(&name) {
            return Err(DbError::TableExists(name));
        }

        let table = Arc::new(RwLock::new(Table::new(schema)));
        self.tables.insert(name, table);
        Ok(())
    }

    /// Удалить таблицу
    pub fn drop_table(&mut self, table_name: &str) -> Result<()> {
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
    pub fn insert_row(&self, table_name: &str, row: Row) -> Result<()> {
        let table_handle = self.get_table(table_name)?;

        let mut table = table_handle
            .write()
            .map_err(|_| DbError::ExecutionError("Table lock poisoned".into()))?;

        table.insert(row)
    }

    /// Сканировать таблицу - read lock только на одну таблицу
    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Row>> {
        let table_handle = self.get_table(table_name)?;

        let table = table_handle
            .read()
            .map_err(|_| DbError::ExecutionError("Table lock poisoned".into()))?;

        Ok(table.rows().to_vec())
    }

    /// Получить схему таблицы
    pub fn get_schema(&self, table_name: &str) -> Result<TableSchema> {
        let table_handle = self.get_table(table_name)?;

        let table = table_handle
            .read()
            .map_err(|_| DbError::ExecutionError("Table lock poisoned".into()))?;

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
    pub fn row_count(&self, table_name: &str) -> Result<usize> {
        let table_handle = self.get_table(table_name)?;

        let table = table_handle
            .read()
            .map_err(|_| DbError::ExecutionError("Table lock poisoned".into()))?;

        Ok(table.row_count())
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}
