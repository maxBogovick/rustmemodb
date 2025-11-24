use crate::parser::ast::Statement;
use crate::result::QueryResult;
use crate::storage::Catalog;
use crate::core::Result;
use super::ExecutionContext;

pub trait Executor: Send + Sync {
    /// Имя executor'а для отладки
    fn name(&self) -> &'static str;

    fn can_handle(&self, stmt: &Statement) -> bool;
    fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult>;
}

pub struct ExecutorPipeline {
    pub executors: Vec<Box<dyn Executor>>,
}

impl ExecutorPipeline {
    pub fn new() -> Self {
        Self {
            executors: Vec::new(),
        }
    }

    pub fn register(&mut self, executor: Box<dyn Executor>) {
        self.executors.push(executor);
    }

    pub fn execute(&self, stmt: &Statement, ctx: &ExecutionContext) -> Result<QueryResult> {
        for executor in &self.executors {
            if executor.can_handle(stmt) {
                return executor.execute(stmt, ctx);
            }
        }

        Err(crate::core::DbError::UnsupportedOperation(
            "No executor found for statement".into()
        ))
    }

    /// Обновить catalog во всех executors (для DDL операций)
    pub fn update_catalog(&mut self, new_catalog: Catalog) {
        for executor in &mut self.executors {
            // Downcast к QueryExecutor и обновить catalog
            // В реальной реализации можно добавить метод в trait Executor
            // или использовать другой подход
        }
    }
}

impl Default for ExecutorPipeline {
    fn default() -> Self {
        Self::new()
    }
}