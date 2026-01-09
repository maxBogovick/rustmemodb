use crate::core::{Result, DbError, Column};
use crate::storage::{InMemoryStorage, Catalog, TableSchema};
use crate::storage::{PersistenceManager, DurabilityMode, WalEntry};
use crate::parser::SqlParserAdapter;
use crate::executor::{ExecutorPipeline, ExecutionContext};
use crate::executor::ddl::{CreateTableExecutor, DropTableExecutor};
use crate::executor::dml::InsertExecutor;
use crate::executor::delete::DeleteExecutor;
use crate::executor::update::UpdateExecutor;
use crate::executor::query::QueryExecutor;
use crate::executor::{BeginExecutor, CommitExecutor, RollbackExecutor};
use crate::result::QueryResult;
use crate::parser::ast::{Statement, CreateTableStmt, DropTableStmt};
use crate::transaction::TransactionManager;
use std::sync::{Arc};
use tokio::sync::{RwLock, Mutex};
use std::path::Path;
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
    /// Transaction manager for MVCC and transaction control
    transaction_manager: Arc<TransactionManager>,
    /// Persistence manager for WAL and snapshots (optional, Arc<Mutex> for shared mutable access)
    persistence: Option<Arc<Mutex<PersistenceManager>>>,
}

impl InMemoryDB {
    /// Get the global InMemoryDB instance
    pub fn global() -> &'static Arc<RwLock<InMemoryDB>> {
        &GLOBAL_DB
    }

    pub fn new() -> Self {
        let catalog = Catalog::new();
        let transaction_manager = Arc::new(TransactionManager::new());

        let mut pipeline = ExecutorPipeline::new();
        pipeline.register(Box::new(BeginExecutor));
        pipeline.register(Box::new(CommitExecutor));
        pipeline.register(Box::new(RollbackExecutor));

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
            transaction_manager,
            persistence: None,
        }
    }

    pub fn transaction_manager(&self) -> &Arc<TransactionManager> {
        &self.transaction_manager
    }

    pub(crate) fn storage_mut(&mut self) -> &mut InMemoryStorage {
        &mut self.storage
    }

    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute_with_transaction(sql, None).await
    }

    pub async fn execute_with_transaction(
        &mut self,
        sql: &str,
        transaction_id: Option<crate::transaction::TransactionId>,
    ) -> Result<QueryResult> {
        let statements = self.parser.parse(sql)?;

        if statements.is_empty() {
            return Err(DbError::ParseError("No statement found".into()));
        }

        match &statements[0] {
            Statement::CreateTable(create) => {
                return self.execute_create_table(create).await;
            }
            Statement::DropTable(drop) => {
                return self.execute_drop_table(drop).await;
            }
            Statement::CreateIndex(create_index) => {
                self.create_index(&create_index.table_name, &create_index.column).await?;
                return Ok(QueryResult::empty());
            }
            Statement::AlterTable(_) => {
                return Err(DbError::UnsupportedOperation("ALTER TABLE not implemented yet".into()));
            }
            _ => {}
        }

        let persistence_ref = self.persistence.as_ref();
        let ctx = if let Some(txn_id) = transaction_id {
            // Get snapshot for transaction
            let snapshot = self.transaction_manager.get_snapshot(txn_id).await?;
            ExecutionContext::with_transaction(&self.storage, &self.transaction_manager, txn_id, persistence_ref, snapshot)
        } else {
            // Auto-commit: Use a fresh snapshot
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            ExecutionContext::new(&self.storage, &self.transaction_manager, persistence_ref, snapshot)
        };
        
        self.executor_pipeline.execute(&statements[0], &ctx).await
    }

    async fn execute_create_table(&mut self, create: &CreateTableStmt) -> Result<QueryResult> {
        let columns: Vec<Column> = create
            .columns
            .iter()
            .map(|col| {
                let mut column = Column::new(col.name.clone(), col.data_type.clone());
                if !col.nullable {
                    column = column.not_null();
                }
                if col.primary_key {
                    column = column.primary_key();
                }
                if col.unique {
                    column = column.unique();
                }
                column
            })
            .collect();

        let schema = TableSchema::new(create.table_name.clone(), columns);

        if let Some(ref persistence) = self.persistence {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::CreateTable {
                name: create.table_name.clone(),
                schema: schema.clone(),
            })?;
        }

        self.storage.create_table(schema.clone()).await?;
        self.catalog = self.catalog.clone().with_table(schema)?;

        self.executor_pipeline.executors.retain(|e| !e.can_handle(&Statement::Query(
            crate::parser::ast::QueryStmt {
                projection: vec![],
                from: vec![],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
            }
        )));

        self.executor_pipeline.register(Box::new(QueryExecutor::new(self.catalog.clone())));
        self.maybe_checkpoint().await?;

        Ok(QueryResult::empty())
    }

    async fn execute_drop_table(&mut self, drop: &DropTableStmt) -> Result<QueryResult> {
        if !self.catalog.table_exists(&drop.table_name) {
            if drop.if_exists {
                return Ok(QueryResult::empty());
            }
            return Err(DbError::TableNotFound(drop.table_name.clone()));
        }

        let table = if self.persistence.is_some() {
            let tables = self.storage.get_all_tables().await?;
            tables.get(&drop.table_name).cloned()
        } else {
            None
        };

        if let Some(ref persistence) = self.persistence {
            if let Some(table) = table {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::DropTable {
                    name: drop.table_name.clone(),
                    table,
                })?;
            }
        }

        self.storage.drop_table(&drop.table_name).await?;
        self.catalog = self.catalog.clone().without_table(&drop.table_name)?;

        self.executor_pipeline.executors.retain(|e| !e.can_handle(&Statement::Query(
            crate::parser::ast::QueryStmt {
                projection: vec![],
                from: vec![],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
            }
        )));

        self.executor_pipeline.register(Box::new(QueryExecutor::new(self.catalog.clone())));
        self.maybe_checkpoint().await?;

        Ok(QueryResult::empty())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.catalog.table_exists(name)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables().into_iter().map(|s| s.to_string()).collect()
    }

    pub async fn table_stats(&self, name: &str) -> Result<TableStats> {
        let row_count = self.storage.row_count(name).await?;
        let schema = self.catalog.get_table(name)?;

        Ok(TableStats {
            name: name.to_string(),
            column_count: schema.schema().column_count(),
            row_count,
        })
    }

    pub async fn create_index(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::CreateIndex {
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
            })?;
        }

        self.storage.create_index(table_name, column_name).await?;
        
        let updated_schema = self.storage.get_schema(table_name).await?;
        self.catalog = self.catalog.clone().without_table(table_name)?.with_table(updated_schema)?;

        self.executor_pipeline.executors.retain(|e| !e.can_handle(&Statement::Query(
            crate::parser::ast::QueryStmt {
                projection: vec![],
                from: vec![],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
            }
        )));

        self.executor_pipeline.register(Box::new(QueryExecutor::new(self.catalog.clone())));
        self.maybe_checkpoint().await?;

        Ok(())
    }

    pub async fn enable_persistence<P: AsRef<Path>>(
        &mut self,
        data_dir: P,
        durability_mode: DurabilityMode,
    ) -> Result<()> {
        if self.persistence.is_some() {
            return Err(DbError::ExecutionError(
                "Persistence already enabled".to_string(),
            ));
        }

        let persistence = PersistenceManager::new(data_dir, durability_mode)?;
        self.persistence = Some(Arc::new(Mutex::new(persistence)));
        self.recover_if_needed().await?;

        Ok(())
    }

    pub fn disable_persistence(&mut self) -> Result<()> {
        self.persistence = None;
        Ok(())
    }

    pub async fn checkpoint(&mut self) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            let tables = self.storage.get_all_tables().await?;
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.checkpoint(&tables)?;
        }
        Ok(())
    }

    pub fn is_persistence_enabled(&self) -> bool {
        self.persistence.is_some()
    }

    pub fn durability_mode(&self) -> Option<DurabilityMode> {
        self.persistence.as_ref().and_then(|p| {
            p.try_lock().ok().map(|guard| guard.durability_mode())
        })
    }

    async fn recover_if_needed(&mut self) -> Result<()> {
        let tables = if let Some(ref persistence) = self.persistence {
            let persistence_guard = persistence.lock().await;
            persistence_guard.recover()?
        } else {
            None
        };

        if let Some(tables) = tables {
            self.storage.restore_tables(tables).await?;
            self.rebuild_catalog().await?;
            println!("Database recovered from persistence");
        }

        Ok(())
    }

    async fn rebuild_catalog(&mut self) -> Result<()> {
        let mut new_catalog = Catalog::new();

        for table_name in self.storage.list_tables() {
            let schema = self.storage.get_schema(&table_name).await?;
            new_catalog = new_catalog.with_table(schema)?;
        }

        self.catalog = new_catalog;

        self.executor_pipeline.executors.retain(|e| {
            !e.can_handle(&Statement::Query(crate::parser::ast::QueryStmt {
                projection: vec![],
                from: vec![],
                selection: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
            }))
        });

        self.executor_pipeline
            .register(Box::new(QueryExecutor::new(self.catalog.clone())));

        Ok(())
    }

    async fn maybe_checkpoint(&mut self) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            let mut persistence_guard = persistence.lock().await;
            if persistence_guard.needs_checkpoint() {
                let tables = self.storage.get_all_tables().await?;
                persistence_guard.checkpoint(&tables)?;
            }
        }
        Ok(())
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
