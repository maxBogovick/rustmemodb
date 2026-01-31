use crate::parser::ast::Statement;
use crate::result::QueryResult;
use crate::core::Result;
use super::{ExecutionContext, Executor};

/// Registry для автоматической регистрации executors
#[allow(dead_code)]
pub struct ExecutorRegistry {
    executors: Vec<Box<dyn Executor>>,
}

#[allow(dead_code)]
impl ExecutorRegistry {
    pub fn new() -> Self {
        Self {
            executors: Vec::new(),
        }
    }

    /// Зарегистрировать executor
    pub fn register(&mut self, executor: Box<dyn Executor>) {
        self.executors.push(executor);
    }

    /// Автоматическая регистрация всех встроенных executors
    pub fn with_default_executors(catalog: crate::storage::Catalog) -> Self {
        use crate::executor::ddl::CreateTableExecutor;
        use crate::executor::dml::InsertExecutor;
        use crate::executor::query::QueryExecutor;

        let mut registry = Self::new();

        // Автоматически регистрируем все executors
        registry.register(Box::new(CreateTableExecutor));
        registry.register(Box::new(InsertExecutor));
        registry.register(Box::new(QueryExecutor::new(catalog)));

        registry
    }

    /// Выполнить statement через подходящий executor
    pub async fn execute(&self, stmt: &Statement, ctx: &ExecutionContext<'_>) -> Result<QueryResult> {
        for executor in &self.executors {
            if executor.can_handle(stmt) {
                return executor.execute(stmt, ctx).await;
            }
        }

        Err(crate::core::DbError::UnsupportedOperation(
            "No executor found for statement".into()
        ))
    }

    /// Получить список зарегистрированных executors
    pub fn list_executors(&self) -> Vec<&str> {
        self.executors.iter().map(|e| e.name()).collect()
    }
}

impl Default for ExecutorRegistry {
    fn default() -> Self {
        Self::new()
    }
}