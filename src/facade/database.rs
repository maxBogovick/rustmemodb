use crate::core::{Column, DataType, DbError, Result, Schema, Value, estimated_row_bytes};
use crate::executor::ddl::{AlterTableExecutor, CreateTableExecutor, DropTableExecutor};
use crate::executor::delete::DeleteExecutor;
use crate::executor::dml::InsertExecutor;
use crate::executor::explain::ExplainExecutor;
use crate::executor::query::QueryExecutor;
use crate::executor::update::UpdateExecutor;
use crate::executor::{BeginExecutor, CommitExecutor, RollbackExecutor};
use crate::executor::{ExecutionContext, ExecutorPipeline};
use crate::parser::SqlParserAdapter;
use crate::parser::ast::{CreateTableStmt, CreateViewStmt, DropTableStmt, DropViewStmt, Statement};
use crate::planner::QueryPlanner;
use crate::result::QueryResult;
use crate::storage::{Catalog, InMemoryStorage, TableSchema};
use crate::storage::{DurabilityMode, PersistenceManager, WalEntry};
use crate::transaction::TransactionManager;
use lazy_static::lazy_static;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};

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
    autovac_threshold: Option<usize>,
    query_metrics: Arc<Mutex<QueryMetricsStore>>,
}

impl InMemoryDB {
    /// Get the global InMemoryDB instance
    pub fn global() -> &'static Arc<RwLock<InMemoryDB>> {
        &GLOBAL_DB
    }

    pub fn new() -> Self {
        let catalog = Catalog::new();
        let transaction_manager = Arc::new(TransactionManager::new());
        let autovac_threshold = std::env::var("RUSTMEMODB_AUTOVAC_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0);

        let mut pipeline = ExecutorPipeline::new();
        pipeline.register(Box::new(BeginExecutor));
        pipeline.register(Box::new(CommitExecutor));
        pipeline.register(Box::new(RollbackExecutor));

        pipeline.register(Box::new(CreateTableExecutor));
        pipeline.register(Box::new(DropTableExecutor));
        pipeline.register(Box::new(AlterTableExecutor));

        pipeline.register(Box::new(InsertExecutor::new(catalog.clone())));
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
            autovac_threshold,
            query_metrics: Arc::new(Mutex::new(QueryMetricsStore::new())),
        }
    }

    pub fn transaction_manager(&self) -> &Arc<TransactionManager> {
        &self.transaction_manager
    }

    pub fn persistence(&self) -> Option<Arc<Mutex<PersistenceManager>>> {
        self.persistence.clone()
    }

    pub(crate) fn storage_mut(&mut self) -> &mut InMemoryStorage {
        &mut self.storage
    }

    fn refresh_catalog_executors(&mut self) {
        self.executor_pipeline.executors.retain(|e| {
            let name = e.name();
            name != "SELECT" && name != "EXPLAIN" && name != "INSERT"
        });

        self.executor_pipeline
            .register(Box::new(QueryExecutor::new(self.catalog.clone())));
        self.executor_pipeline
            .register(Box::new(ExplainExecutor::new(self.catalog.clone())));
        self.executor_pipeline
            .register(Box::new(InsertExecutor::new(self.catalog.clone())));
    }

    async fn infer_parameters(
        &self,
        stmt: &Statement,
    ) -> std::collections::HashMap<usize, DataType> {
        let mut params = std::collections::HashMap::new();
        match stmt {
            Statement::Insert(insert) => {
                if let Ok(schema) = self.storage.get_schema(&insert.table_name).await {
                    let columns = schema.schema().columns();
                    if let crate::parser::ast::InsertSource::Values(rows) = &insert.source {
                        for row in rows {
                            for (i, expr) in row.iter().enumerate() {
                                if i < columns.len() {
                                    if let crate::parser::ast::Expr::Parameter(idx) = expr {
                                        params.insert(*idx, columns[i].data_type.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Statement::Update(update) => {
                if let Ok(schema) = self.storage.get_schema(&update.table_name).await {
                    for assign in &update.assignments {
                        if let Some(idx) = schema.schema().find_column_index(&assign.column) {
                            if let crate::parser::ast::Expr::Parameter(p_idx) = &assign.value {
                                params.insert(
                                    *p_idx,
                                    schema.schema().columns()[idx].data_type.clone(),
                                );
                            }
                        }
                    }
                    if let Some(selection) = &update.selection {
                        self.infer_from_expr(selection, &schema.schema(), &mut params);
                    }
                }
            }
            Statement::Delete(delete) => {
                if let Ok(schema) = self.storage.get_schema(&delete.table_name).await {
                    if let Some(selection) = &delete.selection {
                        self.infer_from_expr(selection, &schema.schema(), &mut params);
                    }
                }
            }
            Statement::Query(query) => {
                // println!("DEBUG: inferring query params. from len: {}", query.from.len());
                // Simple inference for single table SELECT
                if query.from.len() == 1 {
                    if let crate::parser::ast::TableFactor::Table { name, .. } =
                        &query.from[0].relation
                    {
                        // println!("DEBUG: inferring for table {}", name);
                        if let Ok(schema) = self.storage.get_schema(name).await {
                            if let Some(selection) = &query.selection {
                                // println!("DEBUG: inferring from selection {:?}", selection);
                                self.infer_from_expr(selection, &schema.schema(), &mut params);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        params
    }

    fn max_param_index(&self, stmt: &Statement) -> usize {
        let mut max = 0usize;
        self.collect_params_from_statement(stmt, &mut max);
        max
    }

    fn collect_params_from_statement(&self, stmt: &Statement, max: &mut usize) {
        match stmt {
            Statement::Insert(insert) => {
                if let crate::parser::ast::InsertSource::Values(rows) = &insert.source {
                    for row in rows {
                        for expr in row {
                            self.collect_params_from_expr(expr, max);
                        }
                    }
                }
                if let crate::parser::ast::InsertSource::Select(query) = &insert.source {
                    self.collect_params_from_query(query, max);
                }
            }
            Statement::Update(update) => {
                for assign in &update.assignments {
                    self.collect_params_from_expr(&assign.value, max);
                }
                if let Some(selection) = &update.selection {
                    self.collect_params_from_expr(selection, max);
                }
            }
            Statement::Delete(delete) => {
                if let Some(selection) = &delete.selection {
                    self.collect_params_from_expr(selection, max);
                }
            }
            Statement::Query(query) => {
                self.collect_params_from_query(query, max);
            }
            Statement::Explain(explain) => {
                self.collect_params_from_statement(&explain.statement, max);
            }
            _ => {}
        }
    }

    fn collect_params_from_query(&self, query: &crate::parser::ast::QueryStmt, max: &mut usize) {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.collect_params_from_query(&cte.query, max);
            }
        }

        for item in &query.projection {
            if let crate::parser::ast::SelectItem::Expr { expr, .. } = item {
                self.collect_params_from_expr(expr, max);
            }
        }

        for table in &query.from {
            self.collect_params_from_table_factor(&table.relation, max);
            for join in &table.joins {
                self.collect_params_from_table_factor(&join.relation, max);
                if let crate::parser::ast::JoinOperator::Inner(constraint)
                | crate::parser::ast::JoinOperator::LeftOuter(constraint)
                | crate::parser::ast::JoinOperator::RightOuter(constraint)
                | crate::parser::ast::JoinOperator::FullOuter(constraint) = &join.join_operator
                {
                    if let crate::parser::ast::JoinConstraint::On(expr) = constraint {
                        self.collect_params_from_expr(expr, max);
                    }
                }
            }
        }

        if let Some(selection) = &query.selection {
            self.collect_params_from_expr(selection, max);
        }
        for expr in &query.group_by {
            self.collect_params_from_expr(expr, max);
        }
        if let Some(having) = &query.having {
            self.collect_params_from_expr(having, max);
        }
        for order in &query.order_by {
            self.collect_params_from_expr(&order.expr, max);
        }

        if let Some(set_op) = &query.set_op {
            self.collect_params_from_set_op(set_op, max);
        }
    }

    fn collect_params_from_set_op(
        &self,
        set_op: &crate::parser::ast::SetOperation,
        max: &mut usize,
    ) {
        self.collect_params_from_query(&set_op.right, max);
        if let Some(next) = &set_op.right.set_op {
            self.collect_params_from_set_op(next, max);
        }
    }

    fn collect_params_from_table_factor(
        &self,
        factor: &crate::parser::ast::TableFactor,
        max: &mut usize,
    ) {
        if let crate::parser::ast::TableFactor::Derived { subquery, .. } = factor {
            self.collect_params_from_query(subquery, max);
        }
    }

    fn collect_params_from_expr(&self, expr: &crate::parser::ast::Expr, max: &mut usize) {
        use crate::parser::ast::Expr;
        match expr {
            Expr::Parameter(idx) => {
                if *idx > *max {
                    *max = *idx;
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_params_from_expr(left, max);
                self.collect_params_from_expr(right, max);
            }
            Expr::UnaryOp { expr, .. } => self.collect_params_from_expr(expr, max),
            Expr::Like { expr, pattern, .. } => {
                self.collect_params_from_expr(expr, max);
                self.collect_params_from_expr(pattern, max);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.collect_params_from_expr(expr, max);
                self.collect_params_from_expr(low, max);
                self.collect_params_from_expr(high, max);
            }
            Expr::In { expr, list, .. } => {
                self.collect_params_from_expr(expr, max);
                for item in list {
                    self.collect_params_from_expr(item, max);
                }
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.collect_params_from_expr(expr, max);
                self.collect_params_from_query(subquery, max);
            }
            Expr::Subquery(subquery) => {
                self.collect_params_from_query(subquery, max);
            }
            Expr::Exists { subquery, .. } => {
                self.collect_params_from_query(subquery, max);
            }
            Expr::IsNull { expr, .. } => self.collect_params_from_expr(expr, max),
            Expr::Not { expr } => self.collect_params_from_expr(expr, max),
            Expr::Function { args, over, .. } => {
                for arg in args {
                    self.collect_params_from_expr(arg, max);
                }
                if let Some(over) = over {
                    for expr in &over.partition_by {
                        self.collect_params_from_expr(expr, max);
                    }
                    for order in &over.order_by {
                        self.collect_params_from_expr(&order.expr, max);
                    }
                }
            }
            Expr::Cast { expr, .. } => self.collect_params_from_expr(expr, max),
            Expr::Array(list) => {
                for item in list {
                    self.collect_params_from_expr(item, max);
                }
            }
            Expr::ArrayIndex { obj, index } => {
                self.collect_params_from_expr(obj, max);
                self.collect_params_from_expr(index, max);
            }
            Expr::Column(_) | Expr::CompoundIdentifier(_) | Expr::Literal(_) => {}
        }
    }

    fn infer_from_expr(
        &self,
        expr: &crate::parser::ast::Expr,
        schema: &Schema,
        params: &mut std::collections::HashMap<usize, DataType>,
    ) {
        use crate::parser::ast::*;
        match expr {
            Expr::BinaryOp { left, op: _, right } => {
                if let (Expr::Column(col), Expr::Parameter(idx)) = (&**left, &**right) {
                    if let Some(c_idx) = schema.find_column_index(col) {
                        params.insert(*idx, schema.columns()[c_idx].data_type.clone());
                    }
                }
                if let (Expr::Parameter(idx), Expr::Column(col)) = (&**left, &**right) {
                    if let Some(c_idx) = schema.find_column_index(col) {
                        params.insert(*idx, schema.columns()[c_idx].data_type.clone());
                    }
                }
                self.infer_from_expr(left, schema, params);
                self.infer_from_expr(right, schema, params);
            }
            Expr::UnaryOp { expr, .. } => self.infer_from_expr(expr, schema, params),
            Expr::Not { expr } => self.infer_from_expr(expr, schema, params),
            _ => {}
        }
    }

    /// Parse and plan a query without executing it, returning the output schema.
    /// Useful for Describe messages in Postgres Wire Protocol.
    pub async fn plan_query(&self, sql: &str) -> Result<(Schema, Vec<DataType>)> {
        // Special case for version() - common during handshakes
        if sql.to_uppercase().contains("VERSION()") {
            return Ok((
                Schema::new(vec![Column::new("version", DataType::Text)]),
                vec![],
            ));
        }

        let statements = self.parser.parse(sql)?;

        if statements.is_empty() {
            return Err(DbError::ParseError("No statement found".into()));
        }

        let statement = &statements[0];

        let mut inferred = self.infer_parameters(statement).await;

        // Extract parameters from AST (no regex)
        let param_count = self.max_param_index(statement);

        let mut params = Vec::new();
        for i in 1..=param_count {
            params.push(inferred.remove(&i).unwrap_or(DataType::Unknown));
        }

        match statement {
            Statement::Query(_) => {
                let planner = QueryPlanner::new();
                let plan = planner.plan(statement, &self.catalog)?;
                Ok((plan.schema().clone(), params))
            }
            Statement::Explain(_) => Ok((
                Schema::new(vec![Column::new("QUERY PLAN", DataType::Text)]),
                params,
            )),
            // For DML and others, return empty schema (until RETURNING is supported)
            _ => Ok((Schema::new(vec![]), params)),
        }
    }

    pub fn parse_first(&self, sql: &str) -> Result<Statement> {
        let statements = self.parser.parse(sql)?;
        if statements.is_empty() {
            return Err(DbError::ParseError("No statement found".into()));
        }
        Ok(statements[0].clone())
    }

    pub fn is_read_only(&self, sql: &str) -> Result<bool> {
        let stmt = self.parse_first(sql)?;
        Ok(Self::is_read_only_stmt(&stmt))
    }

    pub fn is_read_only_stmt(stmt: &Statement) -> bool {
        matches!(stmt, Statement::Query(_) | Statement::Explain(_))
    }

    pub fn is_ddl_stmt(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::CreateTable(_)
                | Statement::DropTable(_)
                | Statement::CreateView(_)
                | Statement::DropView(_)
                | Statement::CreateIndex(_)
                | Statement::AlterTable(_)
        )
    }

    fn system_query_from_sql(sql: &str) -> Option<SystemQuery> {
        let trimmed = sql.trim().to_ascii_lowercase();
        match trimmed.as_str() {
            "select * from system_metrics" => Some(SystemQuery::Metrics),
            "select * from system_query_metrics" => Some(SystemQuery::QueryMetrics),
            "select * from system_memory_metrics" => Some(SystemQuery::MemoryMetrics),
            "select * from system_storage_metrics" => Some(SystemQuery::StorageMetrics),
            _ => None,
        }
    }

    fn system_query_from_statement(statement: &Statement) -> Option<SystemQuery> {
        let Statement::Query(query) = statement else {
            return None;
        };
        if query.with.is_some() || query.set_op.is_some() {
            return None;
        }
        if query.from.len() != 1 {
            return None;
        }
        if !matches!(
            query.projection.as_slice(),
            [crate::parser::ast::SelectItem::Wildcard]
        ) {
            return None;
        }
        let table = match &query.from[0].relation {
            crate::parser::ast::TableFactor::Table { name, .. } => name.as_str(),
            _ => return None,
        };
        match table.to_ascii_lowercase().as_str() {
            "system_metrics" => Some(SystemQuery::Metrics),
            "system_query_metrics" => Some(SystemQuery::QueryMetrics),
            "system_memory_metrics" => Some(SystemQuery::MemoryMetrics),
            "system_storage_metrics" => Some(SystemQuery::StorageMetrics),
            _ => None,
        }
    }

    async fn handle_system_query(&self, kind: SystemQuery) -> Result<QueryResult> {
        match kind {
            SystemQuery::Metrics => self.system_metrics().await,
            SystemQuery::QueryMetrics => self.system_query_metrics().await,
            SystemQuery::MemoryMetrics => self.system_memory_metrics().await,
            SystemQuery::StorageMetrics => self.system_storage_metrics().await,
        }
    }

    pub async fn execute_readonly_with_params(
        &self,
        sql: &str,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
    ) -> Result<QueryResult> {
        let statement = self.parse_first(sql)?;
        self.execute_parsed_readonly_with_params_internal(
            &statement,
            transaction_id,
            params,
            Some(sql),
        )
        .await
    }

    pub async fn execute_parsed_readonly_with_params(
        &self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
    ) -> Result<QueryResult> {
        self.execute_parsed_readonly_with_params_internal(statement, transaction_id, params, None)
            .await
    }

    pub(crate) async fn execute_parsed_readonly_with_params_tracked(
        &self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
        sql: &str,
    ) -> Result<QueryResult> {
        self.execute_parsed_readonly_with_params_internal(
            statement,
            transaction_id,
            params,
            Some(sql),
        )
        .await
    }

    async fn execute_parsed_readonly_with_params_internal(
        &self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
        sql: Option<&str>,
    ) -> Result<QueryResult> {
        if let Some(system_query) = Self::system_query_from_statement(statement) {
            return self.handle_system_query(system_query).await;
        }
        match statement {
            Statement::Query(_) | Statement::Explain(_) => {}
            _ => {
                return Err(DbError::UnsupportedOperation(
                    "Read-only execution supports only SELECT/EXPLAIN".into(),
                ));
            }
        }

        let persistence_ref = self.persistence.as_ref();
        let ctx = if let Some(txn_id) = transaction_id {
            let snapshot = self.transaction_manager.get_snapshot(txn_id).await?;
            ExecutionContext::with_transaction(
                &self.storage,
                &self.transaction_manager,
                txn_id,
                persistence_ref,
                snapshot,
            )
            .with_params(params)
        } else {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            ExecutionContext::new(
                &self.storage,
                &self.transaction_manager,
                persistence_ref,
                snapshot,
            )
            .with_params(params)
        };

        let start = Instant::now();
        let result = self.executor_pipeline.execute(statement, &ctx).await;
        self.record_query_metric(statement, sql, &result, start.elapsed())
            .await;
        result
    }

    pub async fn execute_readonly(
        &self,
        sql: &str,
        transaction_id: Option<crate::transaction::TransactionId>,
    ) -> Result<QueryResult> {
        self.execute_readonly_with_params(sql, transaction_id, vec![])
            .await
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
                vec![vec![Value::Text(
                    "PostgreSQL 14.0 (RustMemDB MVP)".to_string(),
                )]],
            ));
        }
        if let Some(system_query) = Self::system_query_from_sql(sql) {
            return self.handle_system_query(system_query).await;
        }

        let statement = self.parse_first(sql)?;
        self.execute_parsed_with_params_internal(&statement, transaction_id, params, Some(sql))
            .await
    }

    pub async fn execute_parsed_with_params(
        &mut self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
    ) -> Result<QueryResult> {
        self.execute_parsed_with_params_internal(statement, transaction_id, params, None)
            .await
    }

    pub(crate) async fn execute_parsed_with_params_tracked(
        &mut self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
        sql: &str,
    ) -> Result<QueryResult> {
        self.execute_parsed_with_params_internal(statement, transaction_id, params, Some(sql))
            .await
    }

    async fn execute_parsed_with_params_internal(
        &mut self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
        sql: Option<&str>,
    ) -> Result<QueryResult> {
        if let Some(system_query) = Self::system_query_from_statement(statement) {
            return self.handle_system_query(system_query).await;
        }

        let start = Instant::now();
        let special_result: Option<Result<QueryResult>> = match statement {
            Statement::CreateTable(create) => Some(self.execute_create_table(create).await),
            Statement::DropTable(drop) => Some(self.execute_drop_table(drop).await),
            Statement::CreateView(create_view) => Some(self.execute_create_view(create_view).await),
            Statement::DropView(drop_view) => Some(self.execute_drop_view(drop_view).await),
            Statement::CreateIndex(create_index) => {
                let result = self
                    .create_index_with_options(
                        &create_index.table_name,
                        &create_index.column,
                        create_index.if_not_exists,
                        create_index.unique,
                    )
                    .await
                    .map(|_| QueryResult::empty());
                Some(result)
            }
            Statement::AlterTable(alter) => {
                if let crate::parser::ast::AlterTableOperation::RenameTable(new_name) =
                    &alter.operation
                {
                    let result = self
                        .execute_rename_table(&alter.table_name, new_name)
                        .await
                        .map(|_| QueryResult::empty());
                    Some(result)
                } else {
                    // Execute ALTER TABLE via pipeline
                    // We need to update catalog after execution
                    let mut wal_tx_id = None;
                    if let Some(ref persistence) = self.persistence {
                        let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
                        wal_tx_id = Some(snapshot.tx_id);
                        let mut persistence_guard = persistence.lock().await;
                        persistence_guard.log(&WalEntry::BeginTransaction(snapshot.tx_id))?;
                        persistence_guard.log(&WalEntry::AlterTable {
                            tx_id: snapshot.tx_id,
                            table_name: alter.table_name.clone(),
                            operation: alter.operation.clone(),
                        })?;
                    }

                    let persistence_ref = self.persistence.as_ref();
                    let ctx = ExecutionContext::new(
                        &self.storage,
                        &self.transaction_manager,
                        persistence_ref,
                        self.transaction_manager.get_auto_commit_snapshot().await?,
                    );
                    let result = match self.executor_pipeline.execute(statement, &ctx).await {
                        Ok(result) => Ok(result),
                        Err(err) => {
                            if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id)
                            {
                                let mut persistence_guard = persistence.lock().await;
                                persistence_guard.log(&WalEntry::Rollback(tx_id))?;
                            }
                            Err(err)
                        }
                    };

                    match result {
                        Ok(result) => {
                            // Refresh catalog
                            let schema = self.storage.get_schema(&alter.table_name).await?;
                            self.catalog = self
                                .catalog
                                .clone()
                                .without_table(&alter.table_name)?
                                .with_table(schema)?;

                            self.refresh_catalog_executors();
                            if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id)
                            {
                                let mut persistence_guard = persistence.lock().await;
                                persistence_guard.log(&WalEntry::Commit(tx_id))?;
                            }

                            Some(Ok(result))
                        }
                        Err(err) => Some(Err(err)),
                    }
                }
            }
            _ => None,
        };
        if let Some(result) = special_result {
            self.record_query_metric(statement, sql, &result, start.elapsed())
                .await;
            return result;
        }

        let persistence_ref = self.persistence.as_ref();
        let ctx = if let Some(txn_id) = transaction_id {
            // Get snapshot for transaction
            let snapshot = self.transaction_manager.get_snapshot(txn_id).await?;
            ExecutionContext::with_transaction(
                &self.storage,
                &self.transaction_manager,
                txn_id,
                persistence_ref,
                snapshot,
            )
            .with_params(params)
        } else {
            // Auto-commit: Use a fresh snapshot
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            ExecutionContext::new(
                &self.storage,
                &self.transaction_manager,
                persistence_ref,
                snapshot,
            )
            .with_params(params)
        };

        let result = self.executor_pipeline.execute(statement, &ctx).await;
        self.record_query_metric(statement, sql, &result, start.elapsed())
            .await;
        let result = result?;
        if !InMemoryDB::is_read_only_stmt(statement) {
            self.maybe_autovacuum().await?;
        }
        Ok(result)
    }

    pub async fn execute_parsed_with_params_shared(
        &self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
    ) -> Result<QueryResult> {
        self.execute_parsed_with_params_shared_internal(statement, transaction_id, params, None)
            .await
    }

    pub(crate) async fn execute_parsed_with_params_shared_tracked(
        &self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
        sql: &str,
    ) -> Result<QueryResult> {
        self.execute_parsed_with_params_shared_internal(
            statement,
            transaction_id,
            params,
            Some(sql),
        )
        .await
    }

    async fn execute_parsed_with_params_shared_internal(
        &self,
        statement: &Statement,
        transaction_id: Option<crate::transaction::TransactionId>,
        params: Vec<Value>,
        sql: Option<&str>,
    ) -> Result<QueryResult> {
        if let Some(system_query) = Self::system_query_from_statement(statement) {
            return self.handle_system_query(system_query).await;
        }
        if Self::is_ddl_stmt(statement) {
            return Err(DbError::ExecutionError(
                "DDL requires exclusive access".into(),
            ));
        }

        let persistence_ref = self.persistence.as_ref();
        let ctx = if let Some(txn_id) = transaction_id {
            let snapshot = self.transaction_manager.get_snapshot(txn_id).await?;
            ExecutionContext::with_transaction(
                &self.storage,
                &self.transaction_manager,
                txn_id,
                persistence_ref,
                snapshot,
            )
            .with_params(params)
        } else {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            ExecutionContext::new(
                &self.storage,
                &self.transaction_manager,
                persistence_ref,
                snapshot,
            )
            .with_params(params)
        };

        let start = Instant::now();
        let result = self.executor_pipeline.execute(statement, &ctx).await;
        self.record_query_metric(statement, sql, &result, start.elapsed())
            .await;
        let result = result?;
        if !InMemoryDB::is_read_only_stmt(statement) {
            self.maybe_autovacuum().await?;
        }
        Ok(result)
    }

    async fn record_query_metric(
        &self,
        statement: &Statement,
        sql: Option<&str>,
        result: &Result<QueryResult>,
        duration: std::time::Duration,
    ) {
        let (enabled, capture_plan, plan_max_len) = {
            let store = self.query_metrics.lock().await;
            (store.enabled, store.capture_plan, store.plan_max_len)
        };
        if !enabled {
            return;
        }
        if Self::system_query_from_statement(statement).is_some() {
            return;
        }

        let (plan_text, uses_index) = if capture_plan {
            self.plan_info(statement, plan_max_len)
        } else {
            (None, false)
        };

        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let (rows, result_bytes, error) = match result {
            Ok(result) => {
                let rows = result.affected_rows().unwrap_or(result.row_count()) as i64;
                let result_bytes = result
                    .rows()
                    .iter()
                    .map(|row| estimated_row_bytes(row))
                    .sum::<usize>() as i64;
                (rows, result_bytes, None)
            }
            Err(err) => (0, 0, Some(err.to_string())),
        };

        let metric = QueryMetric {
            id: 0,
            timestamp_ms,
            duration_ms: duration.as_millis() as i64,
            rows,
            result_bytes,
            uses_index,
            statement_type: Self::statement_type(statement).to_string(),
            plan: plan_text,
            sql: sql.unwrap_or_else(|| "<unavailable>").to_string(),
            error,
        };

        let mut store = self.query_metrics.lock().await;
        let mut metric = metric;
        metric.id = store.next_id;
        store.next_id = store.next_id.saturating_add(1);
        store.record(metric);
    }

    fn plan_info(&self, statement: &Statement, plan_max_len: usize) -> (Option<String>, bool) {
        let Statement::Query(query) = statement else {
            return (None, false);
        };
        let planner = QueryPlanner::new();
        let planned = planner.plan(&Statement::Query(query.clone()), &self.catalog);
        let plan = match planned {
            Ok(plan) => plan,
            Err(_) => return (None, false),
        };
        let uses_index = Self::plan_uses_index(&plan);
        let mut plan_text = format!("{:?}", plan);
        if plan_text.len() > plan_max_len {
            plan_text.truncate(plan_max_len);
        }
        (Some(plan_text), uses_index)
    }

    fn plan_uses_index(plan: &crate::planner::LogicalPlan) -> bool {
        use crate::planner::LogicalPlan;
        match plan {
            LogicalPlan::TableScan(scan) => scan.index_scan.is_some(),
            LogicalPlan::Filter(node) => Self::plan_uses_index(&node.input),
            LogicalPlan::Projection(node) => Self::plan_uses_index(&node.input),
            LogicalPlan::Sort(node) => Self::plan_uses_index(&node.input),
            LogicalPlan::Limit(node) => Self::plan_uses_index(&node.input),
            LogicalPlan::Join(node) => {
                Self::plan_uses_index(&node.left) || Self::plan_uses_index(&node.right)
            }
            LogicalPlan::Aggregate(node) => Self::plan_uses_index(&node.input),
            LogicalPlan::Distinct(node) => Self::plan_uses_index(&node.input),
            LogicalPlan::Window(node) => Self::plan_uses_index(&node.input),
            LogicalPlan::Values(_) => false,
            LogicalPlan::RecursiveQuery(node) => {
                Self::plan_uses_index(&node.anchor_plan)
                    || Self::plan_uses_index(&node.recursive_plan)
                    || Self::plan_uses_index(&node.final_plan)
            }
        }
    }

    fn statement_type(statement: &Statement) -> &'static str {
        match statement {
            Statement::Query(_) => "QUERY",
            Statement::Insert(_) => "INSERT",
            Statement::Update(_) => "UPDATE",
            Statement::Delete(_) => "DELETE",
            Statement::CreateTable(_) => "CREATE_TABLE",
            Statement::DropTable(_) => "DROP_TABLE",
            Statement::AlterTable(_) => "ALTER_TABLE",
            Statement::CreateIndex(_) => "CREATE_INDEX",
            Statement::CreateView(_) => "CREATE_VIEW",
            Statement::DropView(_) => "DROP_VIEW",
            Statement::Explain(_) => "EXPLAIN",
            Statement::Begin => "BEGIN",
            Statement::Commit => "COMMIT",
            Statement::Rollback => "ROLLBACK",
        }
    }

    pub async fn execute_with_transaction(
        &mut self,
        sql: &str,
        transaction_id: Option<crate::transaction::TransactionId>,
    ) -> Result<QueryResult> {
        self.execute_with_params(sql, transaction_id, vec![]).await
    }

    async fn execute_create_table(&mut self, create: &CreateTableStmt) -> Result<QueryResult> {
        if self.catalog.table_exists(&create.table_name) {
            if create.if_not_exists {
                return Ok(QueryResult::empty());
            }
            return Err(DbError::TableExists(create.table_name.clone()));
        }

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
                column.default = col.default.clone();
                column
            })
            .collect();

        let mut schema = TableSchema::new(create.table_name.clone(), columns);
        for col in &create.columns {
            if let Some(expr) = &col.check {
                schema.checks.push(expr.clone());
            }
        }

        // Pre-populate indexes metadata for PK/Unique columns
        // This ensures the Catalog knows about these indexes immediately for query planning
        let columns_iter = schema.schema().columns().to_vec();
        for col in columns_iter {
            if col.primary_key || col.unique {
                schema.indexes.push(col.name.clone());
            }
        }

        let mut wal_tx_id = None;
        if let Some(ref persistence) = self.persistence {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            wal_tx_id = Some(snapshot.tx_id);
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::BeginTransaction(snapshot.tx_id))?;
            persistence_guard.log(&WalEntry::CreateTable {
                tx_id: snapshot.tx_id,
                name: create.table_name.clone(),
                schema: schema.clone(),
            })?;
        }

        if let Err(err) = self.storage.create_table(schema.clone()).await {
            if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Rollback(tx_id))?;
            }
            return Err(err);
        }

        if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::Commit(tx_id))?;
        }
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

        let mut wal_tx_id = None;
        if let Some(ref persistence) = self.persistence
            && let Some(table) = table
        {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            wal_tx_id = Some(snapshot.tx_id);
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::BeginTransaction(snapshot.tx_id))?;
            persistence_guard.log(&WalEntry::DropTable {
                tx_id: snapshot.tx_id,
                name: drop.table_name.clone(),
                table,
            })?;
        }

        if let Err(err) = self.storage.drop_table(&drop.table_name).await {
            if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Rollback(tx_id))?;
            }
            return Err(err);
        }

        if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::Commit(tx_id))?;
        }
        self.catalog = self.catalog.clone().without_table(&drop.table_name)?;

        self.refresh_catalog_executors();
        self.maybe_checkpoint().await?;

        Ok(QueryResult::empty())
    }

    async fn execute_create_view(&mut self, create: &CreateViewStmt) -> Result<QueryResult> {
        // Validation: ensure query plans correctly
        let _ =
            QueryPlanner::new().plan(&Statement::Query(*create.query.clone()), &self.catalog)?;

        if self.catalog.view_exists(&create.name) && !create.or_replace {
            return Err(DbError::ExecutionError(format!(
                "View '{}' already exists",
                create.name
            )));
        }

        if self.catalog.table_exists(&create.name) {
            return Err(DbError::TableExists(create.name.clone()));
        }

        let mut wal_tx_id = None;
        if let Some(ref persistence) = self.persistence {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            wal_tx_id = Some(snapshot.tx_id);
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::BeginTransaction(snapshot.tx_id))?;
            persistence_guard.log(&WalEntry::CreateView {
                tx_id: snapshot.tx_id,
                name: create.name.clone(),
                query: *create.query.clone(),
                columns: create.columns.clone(),
                or_replace: create.or_replace,
            })?;
        }

        self.catalog = self.catalog.clone().with_view(
            create.name.clone(),
            *create.query.clone(),
            create.columns.clone(),
        )?;
        if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::Commit(tx_id))?;
        }

        self.refresh_catalog_executors();
        self.maybe_checkpoint().await?;
        Ok(QueryResult::empty())
    }

    async fn execute_drop_view(&mut self, drop: &DropViewStmt) -> Result<QueryResult> {
        if !self.catalog.view_exists(&drop.name) {
            if drop.if_exists {
                return Ok(QueryResult::empty());
            }
            return Err(DbError::ExecutionError(format!(
                "View '{}' not found",
                drop.name
            )));
        }

        let mut wal_tx_id = None;
        if let Some(ref persistence) = self.persistence {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            wal_tx_id = Some(snapshot.tx_id);
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::BeginTransaction(snapshot.tx_id))?;
            persistence_guard.log(&WalEntry::DropView {
                tx_id: snapshot.tx_id,
                name: drop.name.clone(),
            })?;
        }

        self.catalog = self.catalog.clone().without_view(&drop.name)?;
        if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::Commit(tx_id))?;
        }
        self.refresh_catalog_executors();
        self.maybe_checkpoint().await?;
        Ok(QueryResult::empty())
    }

    async fn execute_rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let mut wal_tx_id = None;
        if let Some(ref persistence) = self.persistence {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            wal_tx_id = Some(snapshot.tx_id);
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::BeginTransaction(snapshot.tx_id))?;
            persistence_guard.log(&WalEntry::RenameTable {
                tx_id: snapshot.tx_id,
                old_name: old_name.to_string(),
                new_name: new_name.to_string(),
            })?;
        }

        if let Err(err) = self.storage.rename_table(old_name, new_name).await {
            if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Rollback(tx_id))?;
            }
            return Err(err);
        }

        // Update Catalog
        let schema = self.storage.get_schema(new_name).await?;
        self.catalog = self
            .catalog
            .clone()
            .without_table(old_name)?
            .with_table(schema)?;

        if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::Commit(tx_id))?;
        }

        self.refresh_catalog_executors();
        self.maybe_checkpoint().await?;
        Ok(())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.catalog.table_exists(name)
    }

    pub async fn get_table_schema(&self, name: &str) -> Result<TableSchema> {
        self.storage.get_schema(name).await
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.catalog
            .list_tables()
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    pub async fn table_stats(&self, name: &str) -> Result<TableStats> {
        let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
        let row_count = self.storage.row_count_visible(name, &snapshot).await?;
        let schema = self.catalog.get_table(name)?;

        Ok(TableStats {
            name: name.to_string(),
            column_count: schema.schema().column_count(),
            row_count,
        })
    }

    pub async fn create_index(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        self.create_index_with_options(table_name, column_name, false, false)
            .await
    }

    pub async fn create_index_with_options(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_not_exists: bool,
        unique: bool,
    ) -> Result<()> {
        let schema = self.storage.get_schema(table_name).await?;
        if schema.is_indexed(column_name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(DbError::ExecutionError(format!(
                "Index on '{}.{}' already exists",
                table_name, column_name
            )));
        }

        if unique {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            let rows = self.storage.scan_table(table_name, &snapshot).await?;
            let col_idx = schema
                .schema()
                .find_column_index(column_name)
                .ok_or_else(|| {
                    DbError::ColumnNotFound(column_name.to_string(), table_name.to_string())
                })?;
            let mut seen = std::collections::HashSet::new();
            for row in rows {
                let value = &row[col_idx];
                if value.is_null() {
                    continue;
                }
                if !seen.insert(value.clone()) {
                    return Err(DbError::ConstraintViolation(format!(
                        "Unique index violation on '{}.{}'",
                        table_name, column_name
                    )));
                }
            }
        }

        let mut wal_tx_id = None;
        if let Some(ref persistence) = self.persistence {
            let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
            wal_tx_id = Some(snapshot.tx_id);
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::BeginTransaction(snapshot.tx_id))?;
            persistence_guard.log(&WalEntry::CreateIndex {
                tx_id: snapshot.tx_id,
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
            })?;
        }

        if unique {
            self.storage
                .set_column_unique(table_name, column_name)
                .await?;
        }

        if let Err(err) = self.storage.create_index(table_name, column_name).await {
            if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
                let mut persistence_guard = persistence.lock().await;
                persistence_guard.log(&WalEntry::Rollback(tx_id))?;
            }
            return Err(err);
        }

        if let (Some(persistence), Some(tx_id)) = (&self.persistence, wal_tx_id) {
            let mut persistence_guard = persistence.lock().await;
            persistence_guard.log(&WalEntry::Commit(tx_id))?;
        }

        let updated_schema = self.storage.get_schema(table_name).await?;
        self.catalog = self
            .catalog
            .clone()
            .without_table(table_name)?
            .with_table(updated_schema)?;

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
            persistence_guard.checkpoint(&tables, &self.catalog.views_snapshot())?;
        }
        Ok(())
    }

    pub fn is_persistence_enabled(&self) -> bool {
        self.persistence.is_some()
    }

    pub fn durability_mode(&self) -> Option<DurabilityMode> {
        self.persistence
            .as_ref()
            .and_then(|p| p.try_lock().ok().map(|guard| guard.durability_mode()))
    }

    async fn recover_if_needed(&mut self) -> Result<()> {
        let snapshot = if let Some(ref persistence) = self.persistence {
            let persistence_guard = persistence.lock().await;
            persistence_guard.recover()?
        } else {
            None
        };

        if let Some(snapshot) = snapshot {
            self.storage.restore_tables(snapshot.tables).await?;
            self.rebuild_catalog_with_views(snapshot.views).await?;
        }

        Ok(())
    }

    async fn rebuild_catalog_with_views(
        &mut self,
        views: std::collections::HashMap<String, (crate::parser::ast::QueryStmt, Vec<String>)>,
    ) -> Result<()> {
        let mut new_catalog = Catalog::new();

        for table_name in self.storage.list_tables() {
            let schema = self.storage.get_schema(&table_name).await?;
            new_catalog = new_catalog.with_table(schema)?;
        }

        for (name, (query, columns)) in views {
            new_catalog = new_catalog.with_view(name, query, columns)?;
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
                persistence_guard.checkpoint(&tables, &self.catalog.views_snapshot())?;
            }
        }
        Ok(())
    }

    async fn maybe_autovacuum(&self) -> Result<()> {
        let Some(threshold) = self.autovac_threshold else {
            return Ok(());
        };

        let total_versions = self.storage.version_count().await;
        if total_versions < threshold {
            return Ok(());
        }

        let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
        let min_active = snapshot
            .active
            .iter()
            .min()
            .cloned()
            .unwrap_or(snapshot.max_tx_id);
        let _ = self
            .storage
            .vacuum_all_tables(min_active, &snapshot.aborted)
            .await?;
        Ok(())
    }

    async fn system_metrics(&self) -> Result<QueryResult> {
        let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
        let mut total_rows = 0usize;
        for table in self.catalog.list_tables() {
            total_rows += self.storage.row_count_visible(table, &snapshot).await?;
        }
        let total_versions = self.storage.version_count().await;
        let active_tx = snapshot.active.len();

        Ok(QueryResult::new(
            vec![
                Column::new("name", DataType::Text),
                Column::new("value", DataType::Text),
            ],
            vec![
                vec![
                    Value::Text("table_count".to_string()),
                    Value::Text(self.catalog.list_tables().len().to_string()),
                ],
                vec![
                    Value::Text("row_count".to_string()),
                    Value::Text(total_rows.to_string()),
                ],
                vec![
                    Value::Text("version_count".to_string()),
                    Value::Text(total_versions.to_string()),
                ],
                vec![
                    Value::Text("active_transactions".to_string()),
                    Value::Text(active_tx.to_string()),
                ],
            ],
        ))
    }

    async fn system_query_metrics(&self) -> Result<QueryResult> {
        let entries = {
            let store = self.query_metrics.lock().await;
            store.entries.iter().cloned().collect::<Vec<_>>()
        };

        let columns = vec![
            Column::new("id", DataType::Integer),
            Column::new("timestamp_ms", DataType::Integer),
            Column::new("duration_ms", DataType::Integer),
            Column::new("rows", DataType::Integer),
            Column::new("result_bytes", DataType::Integer),
            Column::new("uses_index", DataType::Boolean),
            Column::new("statement_type", DataType::Text),
            Column::new("plan", DataType::Text),
            Column::new("sql", DataType::Text),
            Column::new("error", DataType::Text),
        ];

        let rows = entries
            .into_iter()
            .map(|entry| {
                vec![
                    Value::Integer(entry.id as i64),
                    Value::Integer(entry.timestamp_ms),
                    Value::Integer(entry.duration_ms),
                    Value::Integer(entry.rows),
                    Value::Integer(entry.result_bytes),
                    Value::Boolean(entry.uses_index),
                    Value::Text(entry.statement_type),
                    entry.plan.map(Value::Text).unwrap_or(Value::Null),
                    Value::Text(entry.sql),
                    entry.error.map(Value::Text).unwrap_or(Value::Null),
                ]
            })
            .collect::<Vec<_>>();

        Ok(QueryResult::new(columns, rows))
    }

    async fn system_memory_metrics(&self) -> Result<QueryResult> {
        let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
        let sample_limit = Self::storage_sample_limit();
        let estimates = self
            .storage
            .storage_estimates(&snapshot, sample_limit)
            .await;

        let table_count = estimates.len() as i64;
        let visible_rows = estimates.iter().map(|e| e.visible_rows as i64).sum::<i64>();
        let version_count = estimates
            .iter()
            .map(|e| e.version_count as i64)
            .sum::<i64>();
        let index_count = estimates.iter().map(|e| e.index_count as i64).sum::<i64>();
        let index_entries = estimates
            .iter()
            .map(|e| e.index_entry_count as i64)
            .sum::<i64>();
        let estimated_row_bytes = estimates
            .iter()
            .map(|e| e.estimated_row_bytes as i64)
            .sum::<i64>();
        let estimated_index_bytes = estimates
            .iter()
            .map(|e| e.estimated_index_bytes as i64)
            .sum::<i64>();
        let estimated_total_bytes = estimates
            .iter()
            .map(|e| e.estimated_total_bytes as i64)
            .sum::<i64>();
        let estimated_visible_bytes = estimates
            .iter()
            .map(|e| (e.avg_row_bytes as i64) * (e.visible_rows as i64))
            .sum::<i64>();
        let avg_row_bytes = if visible_rows > 0 {
            estimated_visible_bytes / visible_rows
        } else {
            0
        };

        let heap_bytes = Self::jemalloc_allocated_bytes().map(|v| v as i64);

        let mut rows = vec![
            vec![
                Value::Text("table_count".to_string()),
                Value::Text(table_count.to_string()),
            ],
            vec![
                Value::Text("visible_rows".to_string()),
                Value::Text(visible_rows.to_string()),
            ],
            vec![
                Value::Text("mvcc_versions".to_string()),
                Value::Text(version_count.to_string()),
            ],
            vec![
                Value::Text("index_count".to_string()),
                Value::Text(index_count.to_string()),
            ],
            vec![
                Value::Text("index_entries".to_string()),
                Value::Text(index_entries.to_string()),
            ],
            vec![
                Value::Text("avg_row_bytes_estimate".to_string()),
                Value::Text(avg_row_bytes.to_string()),
            ],
            vec![
                Value::Text("estimated_row_bytes".to_string()),
                Value::Text(estimated_row_bytes.to_string()),
            ],
            vec![
                Value::Text("estimated_index_bytes".to_string()),
                Value::Text(estimated_index_bytes.to_string()),
            ],
            vec![
                Value::Text("estimated_total_bytes".to_string()),
                Value::Text(estimated_total_bytes.to_string()),
            ],
        ];

        if let Some(heap_bytes) = heap_bytes {
            rows.push(vec![
                Value::Text("jemalloc_allocated_bytes".to_string()),
                Value::Text(heap_bytes.to_string()),
            ]);
        } else {
            rows.push(vec![
                Value::Text("jemalloc_allocated_bytes".to_string()),
                Value::Null,
            ]);
        }

        Ok(QueryResult::new(
            vec![
                Column::new("name", DataType::Text),
                Column::new("value", DataType::Text),
            ],
            rows,
        ))
    }

    async fn system_storage_metrics(&self) -> Result<QueryResult> {
        let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
        let sample_limit = Self::storage_sample_limit();
        let estimates = self
            .storage
            .storage_estimates(&snapshot, sample_limit)
            .await;

        let columns = vec![
            Column::new("table_name", DataType::Text),
            Column::new("visible_rows", DataType::Integer),
            Column::new("version_count", DataType::Integer),
            Column::new("avg_row_bytes", DataType::Integer),
            Column::new("estimated_row_bytes", DataType::Integer),
            Column::new("index_count", DataType::Integer),
            Column::new("index_entry_count", DataType::Integer),
            Column::new("estimated_index_bytes", DataType::Integer),
            Column::new("estimated_total_bytes", DataType::Integer),
        ];

        let rows = estimates
            .into_iter()
            .map(|estimate| {
                vec![
                    Value::Text(estimate.table_name),
                    Value::Integer(estimate.visible_rows as i64),
                    Value::Integer(estimate.version_count as i64),
                    Value::Integer(estimate.avg_row_bytes as i64),
                    Value::Integer(estimate.estimated_row_bytes as i64),
                    Value::Integer(estimate.index_count as i64),
                    Value::Integer(estimate.index_entry_count as i64),
                    Value::Integer(estimate.estimated_index_bytes as i64),
                    Value::Integer(estimate.estimated_total_bytes as i64),
                ]
            })
            .collect::<Vec<_>>();

        Ok(QueryResult::new(columns, rows))
    }

    fn storage_sample_limit() -> usize {
        std::env::var("RUSTMEMODB_STORAGE_ESTIMATE_SAMPLE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(200)
    }

    #[cfg(feature = "jemalloc")]
    fn jemalloc_allocated_bytes() -> Option<u64> {
        jemalloc_ctl::stats::allocated::read().ok()
    }

    #[cfg(not(feature = "jemalloc"))]
    fn jemalloc_allocated_bytes() -> Option<u64> {
        None
    }

    /// Run garbage collection to remove dead row versions
    pub async fn vacuum(&self) -> Result<usize> {
        let snapshot = self.transaction_manager.get_auto_commit_snapshot().await?;
        // If there are no active transactions, min_active is the next transaction ID (max_tx_id)
        // All committed transactions < max_tx_id are visible to everyone.
        let min_active = snapshot
            .active
            .iter()
            .min()
            .cloned()
            .unwrap_or(snapshot.max_tx_id);

        self.storage
            .vacuum_all_tables(min_active, &snapshot.aborted)
            .await
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
        pipeline.register(Box::new(InsertExecutor::new(new_catalog.clone())));
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
            autovac_threshold: self.autovac_threshold,
            query_metrics: Arc::new(Mutex::new(QueryMetricsStore::new())),
        })
    }
}

impl Default for InMemoryDB {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemQuery {
    Metrics,
    QueryMetrics,
    MemoryMetrics,
    StorageMetrics,
}

#[derive(Debug, Clone)]
struct QueryMetric {
    id: u64,
    timestamp_ms: i64,
    duration_ms: i64,
    rows: i64,
    result_bytes: i64,
    uses_index: bool,
    statement_type: String,
    plan: Option<String>,
    sql: String,
    error: Option<String>,
}

#[derive(Debug)]
struct QueryMetricsStore {
    entries: VecDeque<QueryMetric>,
    next_id: u64,
    max_entries: usize,
    enabled: bool,
    capture_plan: bool,
    plan_max_len: usize,
}

impl QueryMetricsStore {
    fn new() -> Self {
        let enabled = std::env::var("RUSTMEMODB_QUERY_METRICS")
            .ok()
            .map(|v| v != "0")
            .unwrap_or(true);
        let max_entries = std::env::var("RUSTMEMODB_QUERY_METRICS_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(1000);
        let capture_plan = std::env::var("RUSTMEMODB_QUERY_METRICS_PLAN")
            .ok()
            .map(|v| v != "0")
            .unwrap_or(true);
        let plan_max_len = std::env::var("RUSTMEMODB_QUERY_METRICS_PLAN_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2048);

        Self {
            entries: VecDeque::with_capacity(max_entries),
            next_id: 1,
            max_entries,
            enabled,
            capture_plan,
            plan_max_len,
        }
    }

    fn record(&mut self, metric: QueryMetric) {
        if !self.enabled {
            return;
        }
        if self.entries.len() == self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(metric);
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
