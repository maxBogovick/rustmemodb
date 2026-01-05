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
    ///
    /// Returns a reference to the singleton database that is shared across all connections.
    /// This ensures that all clients see the same tables and data.
    pub fn global() -> &'static Arc<RwLock<InMemoryDB>> {
        &GLOBAL_DB
    }

    pub fn new() -> Self {
        let catalog = Catalog::new();
        let transaction_manager = Arc::new(TransactionManager::new());

        let mut pipeline = ExecutorPipeline::new();
        // Register transaction control executors first
        pipeline.register(Box::new(BeginExecutor));
        pipeline.register(Box::new(CommitExecutor));
        pipeline.register(Box::new(RollbackExecutor));

        // Register DDL executors
        pipeline.register(Box::new(CreateTableExecutor));
        pipeline.register(Box::new(DropTableExecutor));

        // Register DML executors
        pipeline.register(Box::new(InsertExecutor));
        pipeline.register(Box::new(DeleteExecutor::new()));
        pipeline.register(Box::new(UpdateExecutor::new()));

        // Register query executor
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

    /// Get reference to transaction manager
    pub fn transaction_manager(&self) -> &Arc<TransactionManager> {
        &self.transaction_manager
    }

    /// Get mutable reference to storage (for transaction commits)
    pub(crate) fn storage_mut(&mut self) -> &mut InMemoryStorage {
        &mut self.storage
    }

    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute_with_transaction(sql, None).await
    }

    /// Execute SQL with an optional transaction context
    pub async fn execute_with_transaction(
        &mut self,
        sql: &str,
        transaction_id: Option<crate::transaction::TransactionId>,
    ) -> Result<QueryResult> {
        let statements = self.parser.parse(sql)?;

        if statements.is_empty() {
            return Err(DbError::ParseError("No statement found".into()));
        }

        // DDL operations (CREATE TABLE, DROP TABLE) обрабатываем отдельно
        match &statements[0] {
            Statement::CreateTable(create) => {
                return self.execute_create_table(create).await;
            }
            Statement::DropTable(drop) => {
                return self.execute_drop_table(drop).await;
            }
            _ => {}
        }

        // Остальное через pipeline (DML, DQL, Transaction Control)
        let persistence_ref = self.persistence.as_ref();
        let ctx = if let Some(txn_id) = transaction_id {
            ExecutionContext::with_transaction(&self.storage, &self.transaction_manager, txn_id, persistence_ref)
        } else {
            ExecutionContext::new(&self.storage, &self.transaction_manager, persistence_ref)
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
                column
            })
            .collect();

        let schema = TableSchema::new(create.table_name.clone(), columns);

        // 1. Log to WAL BEFORE making changes
        if let Some(ref persistence) = self.persistence {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::CreateTable {
                name: create.table_name.clone(),
                schema: schema.clone(),
            })?;
        }

        // 2. Создаем таблицу в storage
        self.storage.create_table(schema.clone()).await?;

        // 3. Обновляем catalog (Copy-on-Write)
        self.catalog = self.catalog.clone().with_table(schema)?;

        // 4. Создаем НОВЫЙ QueryExecutor с обновленным catalog
        // Удаляем старый QueryExecutor
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

        // Добавляем новый с обновленным catalog
        self.executor_pipeline.register(Box::new(QueryExecutor::new(self.catalog.clone())));

        // 5. Check if checkpoint is needed
        self.maybe_checkpoint().await?;

        Ok(QueryResult::empty())
    }

    async fn execute_drop_table(&mut self, drop: &DropTableStmt) -> Result<QueryResult> {
        // Check if table exists
        if !self.catalog.table_exists(&drop.table_name) {
            if drop.if_exists {
                return Ok(QueryResult::empty());
            }
            return Err(DbError::TableNotFound(drop.table_name.clone()));
        }

        // 1. Get table data before dropping (for WAL)
        let table = if self.persistence.is_some() {
            let tables = self.storage.get_all_tables().await?;
            tables.get(&drop.table_name).cloned()
        } else {
            None
        };

        // 2. Log to WAL BEFORE making changes
        if let Some(ref persistence) = self.persistence {
            if let Some(table) = table {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::DropTable {
                    name: drop.table_name.clone(),
                    table,
                })?;
            }
        }

        // 3. Drop table from storage
        self.storage.drop_table(&drop.table_name).await?;

        // 4. Update catalog (Copy-on-Write)
        self.catalog = self.catalog.clone().without_table(&drop.table_name)?;

        // 5. Recreate QueryExecutor with updated catalog
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

        // 6. Check if checkpoint is needed
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

    /// Create an index on a table column
    pub async fn create_index(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        // 1. Create index in storage
        self.storage.create_index(table_name, column_name).await?;
        
        // 2. Update catalog to reflect that column is indexed
        // We need to fetch the updated schema from storage because storage.create_index updates it
        let updated_schema = self.storage.get_schema(table_name).await?;
        
        // 3. Update catalog
        self.catalog = self.catalog.clone().without_table(table_name)?.with_table(updated_schema)?;

        // 4. Recreate QueryExecutor with updated catalog so planner sees the new index
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

        Ok(())
    }

    // ========================================================================
    // Persistence Management
    // ========================================================================

    /// Enable persistence with Write-Ahead Logging
    ///
    /// # Arguments
    /// * `data_dir` - Directory where WAL and snapshots will be stored
    /// * `durability_mode` - SYNC, ASYNC, or NONE
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustmemodb::{InMemoryDB, DurabilityMode};
    ///
    /// # tokio_test::block_on(async {
    /// let mut db = InMemoryDB::new();
    /// db.enable_persistence("./data", DurabilityMode::Async).await.unwrap();
    /// # });
    /// ```
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

        // Attempt recovery if snapshot/WAL files exist
        self.recover_if_needed().await?;

        Ok(())
    }

    /// Disable persistence (switch to in-memory mode)
    pub fn disable_persistence(&mut self) -> Result<()> {
        self.persistence = None;
        Ok(())
    }

    /// Manually trigger a checkpoint (snapshot + clear WAL)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use rustmemodb::InMemoryDB;
    /// # tokio_test::block_on(async {
    /// # let mut db = InMemoryDB::new();
    /// db.checkpoint().await.unwrap();
    /// # });
    /// ```
    pub async fn checkpoint(&mut self) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            let tables = self.storage.get_all_tables().await?;
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.checkpoint(&tables)?;
        }
        Ok(())
    }

    /// Check if persistence is enabled
    pub fn is_persistence_enabled(&self) -> bool {
        self.persistence.is_some()
    }

    /// Get durability mode (if persistence is enabled)
    pub fn durability_mode(&self) -> Option<DurabilityMode> {
        self.persistence.as_ref().and_then(|p| {
            p.try_lock().ok().map(|guard| guard.durability_mode())
        })
    }

    /// Recover database state from snapshot + WAL (if available)
    async fn recover_if_needed(&mut self) -> Result<()> {
        let tables = if let Some(ref persistence) = self.persistence {
            let persistence_guard = persistence.lock().await;
            persistence_guard.recover()?
        } else {
            None
        };

        if let Some(tables) = tables {
            // Restore tables to storage
            self.storage.restore_tables(tables).await?;

            // Rebuild catalog from storage
            self.rebuild_catalog().await?;

            println!("Database recovered from persistence");
        }

        Ok(())
    }

    /// Rebuild catalog from current storage state
    async fn rebuild_catalog(&mut self) -> Result<()> {
        let mut new_catalog = Catalog::new();

        for table_name in self.storage.list_tables() {
            let schema = self.storage.get_schema(&table_name).await?;
            new_catalog = new_catalog.with_table(schema)?;
        }

        self.catalog = new_catalog;

        // Recreate QueryExecutor with updated catalog
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

    /// Check if automatic checkpoint is needed and perform it
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