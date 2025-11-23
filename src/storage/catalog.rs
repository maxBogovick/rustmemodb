use std::collections::HashMap;
use std::sync::Arc;
use crate::core::{Result, DbError};
use super::TableSchema;

/// Catalog хранит только метаданные (схемы таблиц)
/// Immutable после создания - можно клонировать без блокировок
#[derive(Clone)]
pub struct Catalog {
    /// Arc позволяет cheap клонирование
    /// При изменении создаем новый HashMap (Copy-on-Write)
    tables: Arc<HashMap<String, TableSchema>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(HashMap::new()),
        }
    }

    /// Добавить таблицу - возвращает НОВЫЙ Catalog
    /// Старый Catalog остается неизменным
    pub fn with_table(self, schema: TableSchema) -> Result<Self> {
        let name = schema.name().to_string();

        if self.tables.contains_key(&name) {
            return Err(DbError::TableExists(name));
        }

        // Copy-on-Write: клонируем HashMap, добавляем таблицу
        let mut new_tables = (*self.tables).clone();
        new_tables.insert(name, schema);

        Ok(Self {
            tables: Arc::new(new_tables),
        })
    }

    /// Получить схему таблицы - БЕЗ блокировок!
    pub fn get_table(&self, name: &str) -> Result<&TableSchema> {
        self.tables
            .get(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Удалить таблицу - возвращает НОВЫЙ Catalog
    pub fn without_table(self, name: &str) -> Result<Self> {
        if !self.tables.contains_key(name) {
            return Err(DbError::TableNotFound(name.to_string()));
        }

        let mut new_tables = (*self.tables).clone();
        new_tables.remove(name);

        Ok(Self {
            tables: Arc::new(new_tables),
        })
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}