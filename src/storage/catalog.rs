use std::collections::HashMap;
use std::sync::Arc;
use crate::core::{Result, DbError};
use crate::parser::ast::QueryStmt;
use super::TableSchema;

/// Catalog хранит только метаданные (схемы таблиц и представления)
/// Immutable после создания - можно клонировать без блокировок
#[derive(Clone)]
pub struct Catalog {
    /// Arc позволяет cheap клонирование
    /// При изменении создаем новый HashMap (Copy-on-Write)
    tables: Arc<HashMap<String, TableSchema>>,
    views: Arc<HashMap<String, QueryStmt>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(HashMap::new()),
            views: Arc::new(HashMap::new()),
        }
    }

    /// Добавить таблицу - возвращает НОВЫЙ Catalog
    /// Старый Catalog остается неизменным
    pub fn with_table(self, schema: TableSchema) -> Result<Self> {
        let name = schema.name().to_string();

        if self.tables.contains_key(&name) {
            return Err(DbError::TableExists(name));
        }
        
        if self.views.contains_key(&name) {
            return Err(DbError::ExecutionError(format!("Relation '{}' already exists as a view", name)));
        }

        // Copy-on-Write: клонируем HashMap, добавляем таблицу
        let mut new_tables = (*self.tables).clone();
        new_tables.insert(name, schema);

        Ok(Self {
            tables: Arc::new(new_tables),
            views: self.views,
        })
    }
    
    /// Добавить представление
    pub fn with_view(self, name: String, query: QueryStmt) -> Result<Self> {
        if self.tables.contains_key(&name) {
             return Err(DbError::TableExists(name));
        }
        
        let mut new_views = (*self.views).clone();
        new_views.insert(name, query);
        
        Ok(Self {
            tables: self.tables,
            views: Arc::new(new_views),
        })
    }

    /// Получить схему таблицы - БЕЗ блокировок!
    pub fn get_table(&self, name: &str) -> Result<&TableSchema> {
        self.tables
            .get(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))
    }
    
    /// Получить определение представления
    pub fn get_view(&self, name: &str) -> Option<&QueryStmt> {
        self.views.get(name)
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
    
    pub fn view_exists(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }
    
    pub fn list_views(&self) -> Vec<&str> {
        self.views.keys().map(|s| s.as_str()).collect()
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
            views: self.views,
        })
    }
    
    /// Удалить представление
    pub fn without_view(self, name: &str) -> Result<Self> {
        if !self.views.contains_key(name) {
            return Err(DbError::ExecutionError(format!("View '{}' not found", name)));
        }
        
        let mut new_views = (*self.views).clone();
        new_views.remove(name);
        
        Ok(Self {
            tables: self.tables,
            views: Arc::new(new_views),
        })
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}