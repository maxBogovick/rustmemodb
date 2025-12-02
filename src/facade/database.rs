use crate::core::{Result, DbError, Column};
use crate::storage::{InMemoryStorage, Catalog, TableSchema};
use crate::parser::SqlParserAdapter;
use crate::executor::{ExecutorPipeline, ExecutionContext};
use crate::executor::ddl::{CreateTableExecutor, DropTableExecutor};
use crate::executor::dml::InsertExecutor;
use crate::executor::delete::DeleteExecutor;
use crate::executor::update::UpdateExecutor;
use crate::executor::query::QueryExecutor;
use crate::result::QueryResult;
use crate::parser::ast::{Statement, CreateTableStmt, DropTableStmt};
use std::sync::{Arc, RwLock};
use lazy_static::lazy_static;

// Global singleton instance of InMemoryDB
lazy_static! {
    static ref GLOBAL_DB: Arc<RwLock<InMemoryDB>> = Arc::new(RwLock::new(InMemoryDB::new()));
}

pub struct InMemoryDB {
    parser: SqlParserAdapter,
    storage: InMemoryStorage,
    /// Catalog - просто значение, не RwLock!
    catalog: Catalog,
    executor_pipeline: ExecutorPipeline,
}

impl InMemoryDB {
    /// Get the global InMemoryDB instance
    ///
    /// Returns a reference to the singleton database that is shared across all connections.
    /// This ensures that all clients see the same tables and data.
    pub fn global() -> &'static Arc<RwLock<InMemoryDB>> {
        &GLOBAL_DB
    }

    pub fn new() -> Self {
        let catalog = Catalog::new();

        let mut pipeline = ExecutorPipeline::new();
        pipeline.register(Box::new(CreateTableExecutor));
        pipeline.register(Box::new(DropTableExecutor));
        pipeline.register(Box::new(InsertExecutor));
        pipeline.register(Box::new(DeleteExecutor::new()));
        pipeline.register(Box::new(UpdateExecutor::new()));
        pipeline.register(Box::new(QueryExecutor::new(catalog.clone())));

        Self {
            parser: SqlParserAdapter::new(),
            storage: InMemoryStorage::new(),
            catalog,
            executor_pipeline: pipeline,
        }
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let statements = self.parser.parse(sql)?;

        if statements.is_empty() {
            return Err(DbError::ParseError("No statement found".into()));
        }

        // DDL operations (CREATE TABLE, DROP TABLE) обрабатываем отдельно
        match &statements[0] {
            Statement::CreateTable(create) => {
                return self.execute_create_table(create);
            }
            Statement::DropTable(drop) => {
                return self.execute_drop_table(drop);
            }
            _ => {}
        }

        // Остальное через pipeline (DML, DQL)
        let ctx = ExecutionContext::new(&self.storage);
        self.executor_pipeline.execute(&statements[0], &ctx)
    }

    fn execute_create_table(&mut self, create: &CreateTableStmt) -> Result<QueryResult> {
        let columns: Vec<Column> = create
            .columns
            .iter()
            .map(|col| {
                let mut column = Column::new(col.name.clone(), col.data_type.clone());
                if !col.nullable {
                    column = column.not_null();
                }
                column
            })
            .collect();

        let schema = TableSchema::new(create.table_name.clone(), columns);

        // 1. Создаем таблицу в storage
        self.storage.create_table(schema.clone())?;

        // 2. Обновляем catalog (Copy-on-Write)
        self.catalog = self.catalog.clone().with_table(schema)?;

        // 3. Создаем НОВЫЙ QueryExecutor с обновленным catalog
        // Удаляем старый QueryExecutor
        self.executor_pipeline.executors.retain(|e| !e.can_handle(&Statement::Query(
            crate::parser::ast::QueryStmt {
                projection: vec![],
                from: vec![],
                selection: None,
                order_by: vec![],
                limit: None,
            }
        )));

        // Добавляем новый с обновленным catalog
        self.executor_pipeline.register(Box::new(QueryExecutor::new(self.catalog.clone())));

        Ok(QueryResult::empty())
    }

    fn execute_drop_table(&mut self, drop: &DropTableStmt) -> Result<QueryResult> {
        // Check if table exists
        if !self.catalog.table_exists(&drop.table_name) {
            if drop.if_exists {
                return Ok(QueryResult::empty());
            }
            return Err(DbError::TableNotFound(drop.table_name.clone()));
        }

        // 1. Drop table from storage
        self.storage.drop_table(&drop.table_name)?;

        // 2. Update catalog (Copy-on-Write)
        self.catalog = self.catalog.clone().without_table(&drop.table_name)?;

        // 3. Recreate QueryExecutor with updated catalog
        self.executor_pipeline.executors.retain(|e| !e.can_handle(&Statement::Query(
            crate::parser::ast::QueryStmt {
                projection: vec![],
                from: vec![],
                selection: None,
                order_by: vec![],
                limit: None,
            }
        )));

        self.executor_pipeline.register(Box::new(QueryExecutor::new(self.catalog.clone())));

        Ok(QueryResult::empty())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.catalog.table_exists(name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables().into_iter().map(|s| s.to_string()).collect()
    }

    pub fn table_stats(&self, name: &str) -> Result<TableStats> {
        let row_count = self.storage.row_count(name)?;
        let schema = self.catalog.get_table(name)?;

        Ok(TableStats {
            name: name.to_string(),
            column_count: schema.schema().column_count(),
            row_count,
        })
    }
}

impl Default for InMemoryDB {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct TableStats {
    pub name: String,
    pub column_count: usize,
    pub row_count: usize,
}

impl std::fmt::Display for TableStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Table '{}': {} columns, {} rows",
            self.name, self.column_count, self.row_count
        )
    }
}