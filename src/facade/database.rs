use crate::core::{Result, DbError, Column, Schema, Value, DataType};
use crate::storage::{InMemoryStorage, Catalog, TableSchema};
use crate::storage::{PersistenceManager, DurabilityMode, WalEntry};
use crate::parser::SqlParserAdapter;
use crate::executor::{ExecutorPipeline, ExecutionContext};
use crate::executor::ddl::{AlterTableExecutor, CreateTableExecutor, DropTableExecutor};
use crate::executor::dml::InsertExecutor;
use crate::executor::delete::DeleteExecutor;
use crate::executor::update::UpdateExecutor;
use crate::executor::query::QueryExecutor;
use crate::executor::explain::ExplainExecutor;
use crate::executor::{BeginExecutor, CommitExecutor, RollbackExecutor};
use crate::result::QueryResult;
use crate::parser::ast::{Statement, CreateTableStmt, DropTableStmt, CreateViewStmt, DropViewStmt};
use crate::transaction::TransactionManager;
use crate::planner::{QueryPlanner};
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
        pipeline.register(Box::new(AlterTableExecutor));

        pipeline.register(Box::new(InsertExecutor));
        pipeline.register(Box::new(DeleteExecutor::new()));
        pipeline.register(Box::new(UpdateExecutor::new()));

        pipeline.register(Box::new(QueryExecutor::new(catalog.clone())));
        pipeline.register(Box::new(ExplainExecutor::new(catalog.clone())));

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

    fn refresh_catalog_executors(&mut self) {
        self.executor_pipeline.executors.retain(|e| {
            let name = e.name();
            name != "SELECT" && name != "EXPLAIN"
        });

        self.executor_pipeline.register(Box::new(QueryExecutor::new(self.catalog.clone())));
        self.executor_pipeline.register(Box::new(ExplainExecutor::new(self.catalog.clone())));
    }

    /// Parse and plan a query without executing it, returning the output schema.
    /// Useful for Describe messages in Postgres Wire Protocol.
    pub fn plan_query(&self, sql: &str) -> Result<Schema> {
        // Special case for version() - common during handshakes
        if sql.to_uppercase().contains("VERSION()") {
            return Ok(Schema::new(vec![Column::new("version", DataType::Text)]));
        }

        let statements = self.parser.parse(sql)?;

        if statements.is_empty() {
            return Err(DbError::ParseError("No statement found".into()));
        }

        let statement = &statements[0];
        match statement {
            Statement::Query(_) => {
                let planner = QueryPlanner::new();
                let plan = planner.plan(statement, &self.catalog)?;
                Ok(plan.schema().clone())
            }
            _ => Ok(Schema::new(vec![])), // Non-query statements have no output schema (usually)
        }
    }

    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.execute_with_transaction(sql, None).await
    }

    pub async fn execute_with_params(
        &mut self,
        sql: &str,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
    ) -> Result<QueryResult> {
        // Special case for version()
        if sql.to_uppercase().contains("VERSION()") {
            return Ok(QueryResult::new(
                vec![Column::new("version", DataType::Text)],
                vec![vec![Value::Text("PostgreSQL 14.0 (RustMemDB MVP)".to_string())]]
            ));
        }

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
            Statement::CreateView(create_view) => {
                return self.execute_create_view(create_view).await;
            }
            Statement::DropView(drop_view) => {
                return self.execute_drop_view(drop_view).await;
            }
            Statement::CreateIndex(create_index) => {
                self.create_index(&create_index.table_name, &create_index.column).await?;
                return Ok(QueryResult::empty());
            }
            Statement::AlterTable(alter) => {
                if let crate::parser::ast::AlterTableOperation::RenameTable(new_name) = &alter.operation {
                    self.execute_rename_table(&alter.table_name, new_name).await?;
                    return Ok(QueryResult::empty());
                }

                // Execute ALTER TABLE via pipeline
                // We need to update catalog after execution
                let persistence_ref = self.persistence.as_ref();
                let ctx = ExecutionContext::new(&self.storage, &self.transaction_manager, persistence_ref, self.transaction_manager.get_auto_commit_snapshot().await?);
                let result = self.executor_pipeline.execute(&statements[0], &ctx).await?;

                // Refresh catalog
                let schema = self.storage.get_schema(&alter.table_name).await?;
                self.catalog = self.catalog.clone().without_table(&alter.table_name)?.with_table(schema)?;

                self.refresh_catalog_executors();

                return Ok(result);
            }
            _ => {}
        }

        let persistence_ref = self.persistence.as_ref();
        let ctx = if let Some(txn_id) = transaction_id {
            // Get snapshot for transaction
            let snapshot = self.transaction_manager.get_snapshot(txn_id).await?;
            ExecutionContext::with_transaction(&self.storage, &self.transaction_manager, txn_id, persistence_ref, snapshot)
                .with_params(params)
        } else {
            // Auto-commit: Use a fresh snapshot
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            ExecutionContext::new(&self.storage, &self.transaction_manager, persistence_ref, snapshot)
                .with_params(params)
        };

        self.executor_pipeline.execute(&statements[0], &ctx).await
    }

    pub async fn execute_with_transaction(
        &mut self,
        sql: &str,
        transaction_id: Option<crate::transaction::TransactionId>,
    ) -> Result<QueryResult> {
        self.execute_with_params(sql, transaction_id, vec![]).await
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
                if let Some(ref fk) = col.references {
                    column = column.references(fk.table.clone(), fk.column.clone());
                }
                column
            })
            .collect();

        let mut schema = TableSchema::new(create.table_name.clone(), columns);

        // Pre-populate indexes metadata for PK/Unique columns
        // This ensures the Catalog knows about these indexes immediately for query planning
        let columns_iter = schema.schema().columns().to_vec();
        for col in columns_iter {
            if col.primary_key || col.unique {
                schema.indexes.push(col.name.clone());
            }
        }

        if let Some(ref persistence) = self.persistence {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::CreateTable {
                name: create.table_name.clone(),
                schema: schema.clone(),
            })?;
        }

        self.storage.create_table(schema.clone()).await?;
        self.catalog = self.catalog.clone().with_table(schema)?;

        self.refresh_catalog_executors();
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

        if let Some(ref persistence) = self.persistence
            && let Some(table) = table {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::DropTable {
                name: drop.table_name.clone(),
                table,
            })?;
        }

        self.storage.drop_table(&drop.table_name).await?;
        self.catalog = self.catalog.clone().without_table(&drop.table_name)?;

        self.refresh_catalog_executors();
        self.maybe_checkpoint().await?;

        Ok(QueryResult::empty())
    }

    async fn execute_create_view(&mut self, create: &CreateViewStmt) -> Result<QueryResult> {
        // Validation: ensure query plans correctly
        let _ = QueryPlanner::new().plan(&Statement::Query(*create.query.clone()), &self.catalog)?;

        if self.catalog.view_exists(&create.name) && !create.or_replace {
             return Err(DbError::ExecutionError(format!("View '{}' already exists", create.name)));
        }
        
        if self.catalog.table_exists(&create.name) {
             return Err(DbError::TableExists(create.name.clone()));
        }

        self.catalog = self.catalog.clone().with_view(create.name.clone(), *create.query.clone())?;
        
        // TODO: Add WAL support for Views
        
        self.refresh_catalog_executors();
        Ok(QueryResult::empty())
    }

    async fn execute_drop_view(&mut self, drop: &DropViewStmt) -> Result<QueryResult> {
         if !self.catalog.view_exists(&drop.name) {
             if drop.if_exists {
                 return Ok(QueryResult::empty());
             }
             return Err(DbError::ExecutionError(format!("View '{}' not found", drop.name)));
         }

         self.catalog = self.catalog.clone().without_view(&drop.name)?;
         self.refresh_catalog_executors();
         Ok(QueryResult::empty())
    }

    async fn execute_rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        if let Some(ref persistence) = self.persistence {
            // Log rename not supported in WAL yet properly (or reuse AlterTable?)
            // We'll skip WAL for RenameTable for MVP or need to add variant.
        }

        self.storage.rename_table(old_name, new_name).await?;

        // Update Catalog
        let schema = self.storage.get_schema(new_name).await?;
        self.catalog = self.catalog.clone().without_table(old_name)?.with_table(schema)?;

        self.refresh_catalog_executors();
        self.maybe_checkpoint().await?;
        Ok(())
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

        self.refresh_catalog_executors();
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

        self.refresh_catalog_executors();

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

    /// Run garbage collection to remove dead row versions
    pub async fn vacuum(&self) -> Result<usize> {
        let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
        // If there are no active transactions, min_active is the next transaction ID (max_tx_id)
        // All committed transactions < max_tx_id are visible to everyone.
        let min_active = snapshot.active.iter().min().cloned().unwrap_or(snapshot.max_tx_id);

        self.storage.vacuum_all_tables(min_active, &snapshot.aborted).await
    }

    /// Fork the database to create an isolated copy.
    ///
    /// This is an O(1) operation (Copy-On-Write).
    /// The new database instance shares the underlying data with the parent until modifications occur.
    /// Active transactions in the parent are treated as aborted in the child.
    pub async fn fork(&self) -> Result<Self> {
        let new_storage = self.storage.fork().await?;
        let new_txn_manager = self.transaction_manager.fork().await;

        // Clone catalog (metadata)
        let new_catalog = self.catalog.clone();

        // Re-create pipeline (stateless)
        let mut pipeline = ExecutorPipeline::new();
        pipeline.register(Box::new(BeginExecutor));
        pipeline.register(Box::new(CommitExecutor));
        pipeline.register(Box::new(RollbackExecutor));
        pipeline.register(Box::new(CreateTableExecutor));
        pipeline.register(Box::new(DropTableExecutor));
        pipeline.register(Box::new(AlterTableExecutor));
        pipeline.register(Box::new(InsertExecutor));
        pipeline.register(Box::new(DeleteExecutor::new()));
        pipeline.register(Box::new(UpdateExecutor::new()));
        pipeline.register(Box::new(QueryExecutor::new(new_catalog.clone())));

        Ok(Self {
            parser: SqlParserAdapter::new(),
            storage: new_storage,
            catalog: new_catalog,
            executor_pipeline: pipeline,
            transaction_manager: Arc::new(new_txn_manager),
            persistence: None, // Forks are ephemeral
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
