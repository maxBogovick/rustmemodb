use crate::core::{DbError, Result, Value};
use crate::facade::InMemoryDB;
use crate::transaction::TransactionId;
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub mod app;
pub mod cluster;
mod macros;
pub mod runtime;

pub const PERSIST_SCHEMA_REGISTRY_TABLE: &str = "__persist_schema_versions";

pub const fn default_schema_version() -> u32 {
    1
}

#[derive(Clone)]
pub struct PersistSession {
    db: Arc<Mutex<InMemoryDB>>,
    transaction_id: Option<TransactionId>,
}

impl PersistSession {
    pub fn new(db: InMemoryDB) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            transaction_id: None,
        }
    }

    pub fn from_shared(db: Arc<Mutex<InMemoryDB>>) -> Self {
        Self {
            db,
            transaction_id: None,
        }
    }

    pub fn shared_db(&self) -> Arc<Mutex<InMemoryDB>> {
        self.db.clone()
    }

    pub fn with_transaction_id(&self, transaction_id: TransactionId) -> Self {
        Self {
            db: self.db.clone(),
            transaction_id: Some(transaction_id),
        }
    }

    pub fn transaction_id(&self) -> Option<TransactionId> {
        self.transaction_id
    }

    pub async fn execute(&self, sql: &str) -> Result<crate::result::QueryResult> {
        let mut db = self.db.lock().await;
        db.execute_with_transaction(sql, self.transaction_id).await
    }

    pub async fn query(&self, sql: &str) -> Result<crate::result::QueryResult> {
        self.execute(sql).await
    }

    pub async fn persist_row_exists(&self, table_name: &str, persist_id: &str) -> Result<bool> {
        let sql = format!(
            "SELECT __persist_id FROM {} WHERE __persist_id = '{}'",
            table_name,
            sql_escape_string(persist_id)
        );
        let result = self.query(&sql).await?;
        Ok(result.row_count() > 0)
    }

    pub async fn delete_persist_row(&self, table_name: &str, persist_id: &str) -> Result<()> {
        let sql = format!(
            "DELETE FROM {} WHERE __persist_id = '{}'",
            table_name,
            sql_escape_string(persist_id)
        );
        self.execute(&sql).await?;
        Ok(())
    }

    pub async fn ensure_schema_registry_table(&self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (table_name TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, updated_at TIMESTAMP NOT NULL)",
            PERSIST_SCHEMA_REGISTRY_TABLE
        );
        self.execute(&sql).await?;
        Ok(())
    }

    pub async fn get_table_schema_version(&self, table_name: &str) -> Result<Option<u32>> {
        self.ensure_schema_registry_table().await?;
        let sql = format!(
            "SELECT schema_version FROM {} WHERE table_name = '{}'",
            PERSIST_SCHEMA_REGISTRY_TABLE,
            sql_escape_string(table_name)
        );
        let result = self.query(&sql).await?;
        if result.row_count() == 0 {
            return Ok(None);
        }

        let Some(first_row) = result.rows().first() else {
            return Ok(None);
        };

        let Some(first_col) = first_row.first() else {
            return Ok(None);
        };

        match first_col {
            Value::Integer(v) if *v >= 0 => Ok(Some(*v as u32)),
            other => Err(DbError::ExecutionError(format!(
                "Invalid schema version value for table '{}': {}",
                table_name,
                other.type_name()
            ))),
        }
    }

    pub async fn set_table_schema_version(
        &self,
        table_name: &str,
        schema_version: u32,
    ) -> Result<()> {
        self.ensure_schema_registry_table().await?;
        let now = Utc::now().to_rfc3339();
        let escaped_table = sql_escape_string(table_name);

        let exists_sql = format!(
            "SELECT table_name FROM {} WHERE table_name = '{}'",
            PERSIST_SCHEMA_REGISTRY_TABLE, escaped_table
        );
        let exists = self.query(&exists_sql).await?.row_count() > 0;

        if exists {
            let update_sql = format!(
                "UPDATE {} SET schema_version = {}, updated_at = '{}' WHERE table_name = '{}'",
                PERSIST_SCHEMA_REGISTRY_TABLE, schema_version, now, escaped_table
            );
            self.execute(&update_sql).await?;
        } else {
            let insert_sql = format!(
                "INSERT INTO {} (table_name, schema_version, updated_at) VALUES ('{}', {}, '{}')",
                PERSIST_SCHEMA_REGISTRY_TABLE, escaped_table, schema_version, now
            );
            self.execute(&insert_sql).await?;
        }

        Ok(())
    }

    pub async fn begin_transaction(&self) -> Result<TransactionId> {
        let db = self.db.lock().await;
        db.transaction_manager().begin().await
    }

    pub async fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let db = self.db.lock().await;
        db.transaction_manager().commit(transaction_id).await
    }

    pub async fn rollback_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let db = self.db.lock().await;
        db.transaction_manager().rollback(transaction_id).await
    }

    pub async fn with_transaction<F, Fut, T>(&self, op: F) -> Result<T>
    where
        F: FnOnce(PersistSession) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let transaction_id = self.begin_transaction().await?;
        let tx_session = self.with_transaction_id(transaction_id);

        let op_result = op(tx_session).await;
        match op_result {
            Ok(value) => {
                if let Err(err) = self.commit_transaction(transaction_id).await {
                    let _ = self.rollback_transaction(transaction_id).await;
                    return Err(err);
                }
                Ok(value)
            }
            Err(err) => {
                let _ = self.rollback_transaction(transaction_id).await;
                Err(err)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistMetadata {
    pub version: i64,
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_touch_at: DateTime<Utc>,
    pub touch_count: u64,
    pub persisted: bool,
}

impl PersistMetadata {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            version: 0,
            schema_version: default_schema_version(),
            created_at: now,
            updated_at: now,
            last_touch_at: now,
            touch_count: 0,
            persisted: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDescriptor {
    pub name: String,
    pub arg_count: usize,
    pub mutates_state: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDescriptor {
    pub type_name: String,
    pub table_name: String,
    pub functions: Vec<FunctionDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistState {
    pub persist_id: String,
    pub type_name: String,
    pub table_name: String,
    pub metadata: PersistMetadata,
    pub fields: serde_json::Value,
}

impl PersistState {
    pub fn fields_object(&self) -> Result<&serde_json::Map<String, serde_json::Value>> {
        self.fields.as_object().ok_or_else(|| {
            DbError::ExecutionError("Persist state fields must be a JSON object".to_string())
        })
    }

    pub fn fields_object_mut(&mut self) -> Result<&mut serde_json::Map<String, serde_json::Value>> {
        self.fields.as_object_mut().ok_or_else(|| {
            DbError::ExecutionError("Persist state fields must be a JSON object".to_string())
        })
    }

    pub fn set_json_field(
        &mut self,
        name: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<()> {
        let fields = self.fields_object_mut()?;
        fields.insert(name.into(), value);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicFieldDef {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicSchema {
    pub table_name: String,
    pub fields: Vec<DynamicFieldDef>,
    pub source_kind: String,
    pub source: String,
}

impl DynamicSchema {
    pub fn create_table_sql(&self) -> String {
        let mut columns = vec![
            "__persist_id TEXT PRIMARY KEY".to_string(),
            "__version INTEGER NOT NULL".to_string(),
            "__schema_version INTEGER NOT NULL".to_string(),
            "__touch_count INTEGER NOT NULL".to_string(),
            "__created_at TIMESTAMP NOT NULL".to_string(),
            "__updated_at TIMESTAMP NOT NULL".to_string(),
            "__last_touch_at TIMESTAMP NOT NULL".to_string(),
        ];

        for field in &self.fields {
            let mut col = format!("{} {}", field.name, field.sql_type);
            if !field.nullable {
                col.push_str(" NOT NULL");
            }
            columns.push(col);
        }

        format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            self.table_name,
            columns.join(", ")
        )
    }

    pub fn field(&self, name: &str) -> Option<&DynamicFieldDef> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.field(name).is_some()
    }

    pub fn default_value_map(&self) -> BTreeMap<String, Value> {
        let mut values = BTreeMap::new();
        for field in &self.fields {
            values.insert(field.name.clone(), Value::Null);
        }
        values
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SnapshotMode {
    SchemaOnly,
    WithData,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RestoreConflictPolicy {
    FailFast,
    SkipExisting,
    OverwriteExisting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistVecSnapshot {
    pub format_version: u16,
    pub created_at_unix_ms: i64,
    pub mode: SnapshotMode,
    pub vec_name: String,
    pub object_type: String,
    pub table_name: String,
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub states: Vec<PersistState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeteroTypeSnapshot {
    pub type_name: String,
    pub table_name: String,
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeteroPersistVecSnapshot {
    pub format_version: u16,
    pub created_at_unix_ms: i64,
    pub mode: SnapshotMode,
    pub vec_name: String,
    pub types: Vec<HeteroTypeSnapshot>,
    pub states: Vec<PersistState>,
}

#[derive(Debug, Clone)]
pub enum InvokeStatus {
    Invoked,
    SkippedUnsupported,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct InvokeOutcome {
    pub persist_id: String,
    pub function: String,
    pub status: InvokeStatus,
    pub result: Option<Value>,
}

pub type StateMigrationFn = Arc<dyn Fn(&mut PersistState) -> Result<()> + Send + Sync>;

#[derive(Clone)]
pub struct PersistMigrationStep {
    pub from_version: u32,
    pub to_version: u32,
    pub sql_statements: Vec<String>,
    state_migrator: Option<StateMigrationFn>,
}

impl std::fmt::Debug for PersistMigrationStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistMigrationStep")
            .field("from_version", &self.from_version)
            .field("to_version", &self.to_version)
            .field("sql_statements", &self.sql_statements)
            .field("has_state_migrator", &self.state_migrator.is_some())
            .finish()
    }
}

impl PersistMigrationStep {
    pub fn new(from_version: u32, to_version: u32) -> Self {
        Self {
            from_version,
            to_version,
            sql_statements: Vec::new(),
            state_migrator: None,
        }
    }

    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql_statements.push(sql.into());
        self
    }

    pub fn with_sql_many<I, S>(mut self, sql_statements: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for sql in sql_statements {
            self.sql_statements.push(sql.into());
        }
        self
    }

    pub fn with_state_migrator<F>(mut self, migrator: F) -> Self
    where
        F: Fn(&mut PersistState) -> Result<()> + Send + Sync + 'static,
    {
        self.state_migrator = Some(Arc::new(migrator));
        self
    }
}

#[derive(Debug, Clone)]
pub struct PersistMigrationPlan {
    current_version: u32,
    steps: Vec<PersistMigrationStep>,
}

impl PersistMigrationPlan {
    pub fn new(current_version: u32) -> Self {
        Self {
            current_version,
            steps: Vec::new(),
        }
    }

    pub fn current_version(&self) -> u32 {
        self.current_version
    }

    pub fn steps(&self) -> &[PersistMigrationStep] {
        &self.steps
    }

    pub fn add_step(&mut self, step: PersistMigrationStep) -> Result<()> {
        self.steps.push(step);
        self.validate()
    }

    pub fn with_step(mut self, step: PersistMigrationStep) -> Result<Self> {
        self.add_step(step)?;
        Ok(self)
    }

    pub fn add_sql_step(
        &mut self,
        from_version: u32,
        to_version: u32,
        sql: impl Into<String>,
    ) -> Result<()> {
        self.add_step(PersistMigrationStep::new(from_version, to_version).with_sql(sql))
    }

    pub fn add_state_step<F>(
        &mut self,
        from_version: u32,
        to_version: u32,
        migrator: F,
    ) -> Result<()>
    where
        F: Fn(&mut PersistState) -> Result<()> + Send + Sync + 'static,
    {
        self.add_step(
            PersistMigrationStep::new(from_version, to_version).with_state_migrator(migrator),
        )
    }

    pub fn validate(&self) -> Result<()> {
        if self.current_version == 0 {
            return Err(DbError::ExecutionError(
                "Schema version must be >= 1".to_string(),
            ));
        }

        let mut seen_from = HashSet::<u32>::new();
        for step in &self.steps {
            if step.from_version == 0 {
                return Err(DbError::ExecutionError(
                    "Migration 'from_version' must be >= 1".to_string(),
                ));
            }
            if step.to_version <= step.from_version {
                return Err(DbError::ExecutionError(format!(
                    "Migration step {} -> {} is invalid",
                    step.from_version, step.to_version
                )));
            }
            if step.to_version > self.current_version {
                return Err(DbError::ExecutionError(format!(
                    "Migration step {} -> {} exceeds current schema version {}",
                    step.from_version, step.to_version, self.current_version
                )));
            }
            if !seen_from.insert(step.from_version) {
                return Err(DbError::ExecutionError(format!(
                    "Duplicate migration step starting at version {}",
                    step.from_version
                )));
            }
        }

        Ok(())
    }

    fn resolve_chain(&self, from_version: u32) -> Result<Vec<&PersistMigrationStep>> {
        if from_version > self.current_version {
            return Err(DbError::ExecutionError(format!(
                "Cannot migrate down from schema version {} to {}",
                from_version, self.current_version
            )));
        }

        if from_version == self.current_version {
            return Ok(Vec::new());
        }

        let mut by_from = HashMap::<u32, &PersistMigrationStep>::new();
        for step in &self.steps {
            by_from.insert(step.from_version, step);
        }

        let mut cursor = from_version;
        let mut chain = Vec::new();
        while cursor < self.current_version {
            let step = by_from.get(&cursor).copied().ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Missing migration step starting at version {} for target {}",
                    cursor, self.current_version
                ))
            })?;

            if step.to_version <= cursor || step.to_version > self.current_version {
                return Err(DbError::ExecutionError(format!(
                    "Invalid migration chain edge {} -> {}",
                    step.from_version, step.to_version
                )));
            }

            chain.push(step);
            cursor = step.to_version;
        }

        Ok(chain)
    }

    pub fn migrate_state_to_current(&self, state: &mut PersistState) -> Result<()> {
        self.validate()?;

        let from_version = state.metadata.schema_version;
        if from_version == self.current_version {
            return Ok(());
        }

        let chain = self.resolve_chain(from_version)?;
        for step in chain {
            if let Some(migrator) = &step.state_migrator {
                migrator(state)?;
            }
            state.metadata.schema_version = step.to_version;
        }

        Ok(())
    }

    pub async fn migrate_table_from(
        &self,
        session: &PersistSession,
        table_name: &str,
        from_version: u32,
    ) -> Result<()> {
        self.validate()?;

        let chain = self.resolve_chain(from_version)?;
        for step in chain {
            for sql in &step.sql_statements {
                let rendered_sql = sql.replace("{table}", table_name);
                session.execute(&rendered_sql).await?;
            }
        }

        session
            .set_table_schema_version(table_name, self.current_version)
            .await?;
        Ok(())
    }

    pub async fn ensure_table_schema_version(
        &self,
        session: &PersistSession,
        table_name: &str,
    ) -> Result<()> {
        self.validate()?;

        if let Some(current_table_version) = session.get_table_schema_version(table_name).await? {
            // Forward-compatible mode: a table can be ahead of the current runtime plan.
            // This allows restoring snapshots migrated by a newer plan while still operating
            // on known columns/fields.
            if current_table_version > self.current_version {
                return Ok(());
            }
            if current_table_version < self.current_version {
                return self
                    .migrate_table_from(session, table_name, current_table_version)
                    .await;
            }
            return Ok(());
        }

        session
            .set_table_schema_version(table_name, self.current_version)
            .await
    }
}

#[async_trait]
pub trait PersistEntity: Send + Sync {
    fn type_name(&self) -> &'static str;
    fn table_name(&self) -> &str;
    fn persist_id(&self) -> &str;
    fn metadata(&self) -> &PersistMetadata;
    fn metadata_mut(&mut self) -> &mut PersistMetadata;
    fn descriptor(&self) -> ObjectDescriptor;
    fn state(&self) -> PersistState;
    fn supports_function(&self, function: &str) -> bool;
    fn available_functions(&self) -> Vec<FunctionDescriptor>;
    async fn ensure_table(&mut self, session: &PersistSession) -> Result<()>;
    async fn save(&mut self, session: &PersistSession) -> Result<()>;
    async fn delete(&mut self, session: &PersistSession) -> Result<()>;
    async fn invoke(
        &mut self,
        function: &str,
        args: Vec<Value>,
        session: &PersistSession,
    ) -> Result<Value>;
}

#[async_trait]
pub trait PersistEntityFactory: PersistEntity + Sized {
    fn entity_type_name() -> &'static str;
    fn default_table_name() -> String;
    fn create_table_sql(table_name: &str) -> String;
    fn from_state(state: &PersistState) -> Result<Self>;

    fn schema_version() -> u32 {
        default_schema_version()
    }

    fn migration_plan() -> PersistMigrationPlan {
        PersistMigrationPlan::new(Self::schema_version())
    }

    async fn restore_into_db(&mut self, session: &PersistSession) -> Result<()> {
        self.save(session).await
    }
}

pub trait PersistModelExt: Sized {
    type Persisted: PersistEntityFactory + Send + Sync + 'static;

    fn into_persisted(self) -> Self::Persisted;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistPatchContract {
    pub field: String,
    pub rust_type: String,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistCommandFieldContract {
    pub name: String,
    pub rust_type: String,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistCommandContract {
    pub name: String,
    pub fields: Vec<PersistCommandFieldContract>,
    pub mutates_state: bool,
}

pub trait PersistCommandModel: PersistEntity + Sized {
    type Draft: Send + Sync + 'static;
    type Patch: Send + Sync + 'static;
    type Command: Send + Sync + 'static;

    fn from_draft(draft: Self::Draft) -> Self;
    fn try_from_draft(draft: Self::Draft) -> Result<Self> {
        Ok(Self::from_draft(draft))
    }
    fn apply_patch_model(&mut self, patch: Self::Patch) -> Result<bool>;
    fn apply_command_model(&mut self, command: Self::Command) -> Result<bool>;

    fn validate_draft_payload(_draft: &Self::Draft) -> Result<()> {
        Ok(())
    }

    fn validate_patch_payload(_patch: &Self::Patch) -> Result<()> {
        Ok(())
    }

    fn validate_command_payload(_command: &Self::Command) -> Result<()> {
        Ok(())
    }

    fn patch_contract() -> Vec<PersistPatchContract>;
    fn command_contract() -> Vec<PersistCommandContract>;
}

pub struct PersistVec<T: PersistEntityFactory> {
    name: String,
    items: Vec<T>,
}

impl<T: PersistEntityFactory> PersistVec<T> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            items: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn items_mut(&mut self) -> &mut [T] {
        &mut self.items
    }

    pub fn add_one(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn add_many<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.items.extend(items);
    }

    pub fn remove_by_persist_id(&mut self, persist_id: &str) -> Option<T> {
        let position = self
            .items
            .iter()
            .position(|item| item.persist_id() == persist_id)?;
        Some(self.items.remove(position))
    }

    pub fn states(&self) -> Vec<PersistState> {
        self.items.iter().map(|item| item.state()).collect()
    }

    pub fn descriptors(&self) -> Vec<ObjectDescriptor> {
        self.items.iter().map(|item| item.descriptor()).collect()
    }

    pub fn functions_catalog(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for item in &self.items {
            for function in item.available_functions() {
                *counts.entry(function.name).or_insert(0) += 1;
            }
        }
        counts
    }

    pub async fn ensure_all_tables(&mut self, session: &PersistSession) -> Result<()> {
        for item in &mut self.items {
            item.ensure_table(session).await?;
        }
        Ok(())
    }

    pub async fn save_all(&mut self, session: &PersistSession) -> Result<()> {
        for item in &mut self.items {
            item.save(session).await?;
        }
        Ok(())
    }

    pub async fn invoke_supported(
        &mut self,
        function: &str,
        args: Vec<Value>,
        session: &PersistSession,
    ) -> Result<Vec<InvokeOutcome>> {
        let mut outcomes = Vec::with_capacity(self.items.len());

        for item in &mut self.items {
            if !item.supports_function(function) {
                outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::SkippedUnsupported,
                    result: None,
                });
                continue;
            }

            match item.invoke(function, args.clone(), session).await {
                Ok(result) => outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::Invoked,
                    result: Some(result),
                }),
                Err(err) => outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::Failed(err.to_string()),
                    result: None,
                }),
            }
        }

        Ok(outcomes)
    }

    pub async fn prune_stale(
        &mut self,
        max_age: Duration,
        session: &PersistSession,
    ) -> Result<usize> {
        let now = Utc::now();
        let mut kept = Vec::with_capacity(self.items.len());
        let mut removed = 0usize;

        for mut item in self.items.drain(..) {
            let metadata = item.metadata().clone();
            let is_stale = metadata.touch_count == 0 && (now - metadata.created_at) > max_age;

            if is_stale {
                let _ = item.delete(session).await;
                removed += 1;
            } else {
                kept.push(item);
            }
        }

        self.items = kept;
        Ok(removed)
    }

    pub fn snapshot(&self, mode: SnapshotMode) -> PersistVecSnapshot {
        let object_type = self
            .items
            .first()
            .map(|item| item.type_name().to_string())
            .unwrap_or_else(|| T::entity_type_name().to_string());

        let table_name = self
            .items
            .first()
            .map(|item| item.table_name().to_string())
            .unwrap_or_else(T::default_table_name);

        PersistVecSnapshot {
            format_version: 1,
            created_at_unix_ms: Utc::now().timestamp_millis(),
            mode: mode.clone(),
            vec_name: self.name.clone(),
            object_type,
            table_name,
            schema_version: self
                .items
                .first()
                .map(|item| item.metadata().schema_version)
                .unwrap_or_else(T::schema_version),
            states: if mode == SnapshotMode::WithData {
                self.states()
            } else {
                Vec::new()
            },
        }
    }

    pub async fn restore(
        &mut self,
        snapshot: PersistVecSnapshot,
        session: &PersistSession,
    ) -> Result<()> {
        self.restore_with_policy(snapshot, session, RestoreConflictPolicy::FailFast)
            .await
    }

    pub async fn restore_with_policy(
        &mut self,
        snapshot: PersistVecSnapshot,
        session: &PersistSession,
        conflict_policy: RestoreConflictPolicy,
    ) -> Result<()> {
        self.restore_with_custom_migration_plan(
            snapshot,
            session,
            conflict_policy,
            T::migration_plan(),
        )
        .await
    }

    pub async fn restore_with_custom_migration_plan(
        &mut self,
        snapshot: PersistVecSnapshot,
        session: &PersistSession,
        conflict_policy: RestoreConflictPolicy,
        migration_plan: PersistMigrationPlan,
    ) -> Result<()> {
        migration_plan.validate()?;
        let create_sql = T::create_table_sql(&snapshot.table_name);
        session.execute(&create_sql).await?;
        migration_plan
            .ensure_table_schema_version(session, &snapshot.table_name)
            .await?;

        self.name = snapshot.vec_name;
        self.items.clear();

        if snapshot.mode == SnapshotMode::WithData {
            for mut state in snapshot.states {
                if state.metadata.schema_version == 0 {
                    state.metadata.schema_version =
                        snapshot.schema_version.max(default_schema_version());
                }
                migration_plan.migrate_state_to_current(&mut state)?;

                let exists = session
                    .persist_row_exists(&state.table_name, &state.persist_id)
                    .await?;

                if exists {
                    match conflict_policy {
                        RestoreConflictPolicy::FailFast => {
                            return Err(DbError::ExecutionError(format!(
                                "Restore conflict: row {} already exists in table {}",
                                state.persist_id, state.table_name
                            )));
                        }
                        RestoreConflictPolicy::SkipExisting => {
                            continue;
                        }
                        RestoreConflictPolicy::OverwriteExisting => {
                            session
                                .delete_persist_row(&state.table_name, &state.persist_id)
                                .await?;
                        }
                    }
                }

                let mut entity = T::from_state(&state)?;
                entity.restore_into_db(session).await?;
                self.items.push(entity);
            }
        }

        Ok(())
    }
}

type DynamicCreateTableSql = Arc<dyn Fn(&str) -> String + Send + Sync>;
type DynamicFromState = Arc<dyn Fn(&PersistState) -> Result<Box<dyn PersistEntity>> + Send + Sync>;
type DynamicDefaultTableName = Arc<dyn Fn() -> String + Send + Sync>;
type DynamicMigrationPlan = Arc<dyn Fn() -> PersistMigrationPlan + Send + Sync>;
type DynamicSchemaVersion = Arc<dyn Fn() -> u32 + Send + Sync>;

#[derive(Clone)]
struct PersistTypeRegistration {
    default_table_name: DynamicDefaultTableName,
    create_table_sql: DynamicCreateTableSql,
    from_state: DynamicFromState,
    migration_plan: DynamicMigrationPlan,
    schema_version: DynamicSchemaVersion,
}

pub struct HeteroPersistVec {
    name: String,
    items: Vec<Box<dyn PersistEntity>>,
    registrations: HashMap<String, PersistTypeRegistration>,
}

impl HeteroPersistVec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            items: Vec::new(),
            registrations: HashMap::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn registered_types(&self) -> Vec<String> {
        let mut names = self.registrations.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn register_type<T>(&mut self)
    where
        T: PersistEntityFactory + 'static,
    {
        self.register_type_with_migration_plan::<T>(T::migration_plan());
    }

    pub fn register_type_with_migration_plan<T>(&mut self, migration_plan: PersistMigrationPlan)
    where
        T: PersistEntityFactory + 'static,
    {
        let type_name = T::entity_type_name().to_string();
        let schema_version = migration_plan.current_version();
        let plan_clone = migration_plan.clone();

        let registration = PersistTypeRegistration {
            default_table_name: Arc::new(T::default_table_name),
            create_table_sql: Arc::new(T::create_table_sql),
            from_state: Arc::new(|state| {
                let item = T::from_state(state)?;
                Ok(Box::new(item) as Box<dyn PersistEntity>)
            }),
            migration_plan: Arc::new(move || plan_clone.clone()),
            schema_version: Arc::new(move || schema_version),
        };
        self.registrations.insert(type_name, registration);
    }

    pub fn add_boxed(&mut self, item: Box<dyn PersistEntity>) -> Result<()> {
        let type_name = item.type_name().to_string();
        if !self.registrations.contains_key(&type_name) {
            return Err(DbError::ExecutionError(format!(
                "Type '{}' is not registered in hetero persist vec",
                type_name
            )));
        }

        self.items.push(item);
        Ok(())
    }

    pub fn add_one<T>(&mut self, item: T) -> Result<()>
    where
        T: PersistEntity + 'static,
    {
        self.add_boxed(Box::new(item))
    }

    pub fn add_many_boxed<I>(&mut self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = Box<dyn PersistEntity>>,
    {
        for item in items {
            self.add_boxed(item)?;
        }
        Ok(())
    }

    pub fn add_many<T, I>(&mut self, items: I) -> Result<()>
    where
        T: PersistEntity + 'static,
        I: IntoIterator<Item = T>,
    {
        for item in items {
            self.add_one(item)?;
        }
        Ok(())
    }

    pub fn states(&self) -> Vec<PersistState> {
        self.items.iter().map(|item| item.state()).collect()
    }

    pub fn descriptors(&self) -> Vec<ObjectDescriptor> {
        self.items.iter().map(|item| item.descriptor()).collect()
    }

    pub fn functions_catalog(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for item in &self.items {
            for function in item.available_functions() {
                *counts.entry(function.name).or_insert(0) += 1;
            }
        }
        counts
    }

    pub async fn ensure_all_tables(&mut self, session: &PersistSession) -> Result<()> {
        for item in &mut self.items {
            item.ensure_table(session).await?;
        }
        Ok(())
    }

    pub async fn save_all(&mut self, session: &PersistSession) -> Result<()> {
        for item in &mut self.items {
            item.save(session).await?;
        }
        Ok(())
    }

    pub async fn invoke_supported(
        &mut self,
        function: &str,
        args: Vec<Value>,
        session: &PersistSession,
    ) -> Result<Vec<InvokeOutcome>> {
        let mut outcomes = Vec::with_capacity(self.items.len());
        for item in &mut self.items {
            if !item.supports_function(function) {
                outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::SkippedUnsupported,
                    result: None,
                });
                continue;
            }

            match item.invoke(function, args.clone(), session).await {
                Ok(value) => outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::Invoked,
                    result: Some(value),
                }),
                Err(err) => outcomes.push(InvokeOutcome {
                    persist_id: item.persist_id().to_string(),
                    function: function.to_string(),
                    status: InvokeStatus::Failed(err.to_string()),
                    result: None,
                }),
            }
        }

        Ok(outcomes)
    }

    pub async fn prune_stale(
        &mut self,
        max_age: Duration,
        session: &PersistSession,
    ) -> Result<usize> {
        let now = Utc::now();
        let mut kept = Vec::with_capacity(self.items.len());
        let mut removed = 0usize;

        for mut item in self.items.drain(..) {
            let metadata = item.metadata().clone();
            let is_stale = metadata.touch_count == 0 && (now - metadata.created_at) > max_age;
            if is_stale {
                let _ = item.delete(session).await;
                removed += 1;
            } else {
                kept.push(item);
            }
        }

        self.items = kept;
        Ok(removed)
    }

    pub fn snapshot(&self, mode: SnapshotMode) -> HeteroPersistVecSnapshot {
        let mut table_and_version_by_type = HashMap::<String, (String, u32)>::new();
        for item in &self.items {
            table_and_version_by_type
                .entry(item.type_name().to_string())
                .or_insert_with(|| {
                    (
                        item.table_name().to_string(),
                        item.metadata().schema_version.max(default_schema_version()),
                    )
                });
        }

        for (type_name, registration) in &self.registrations {
            table_and_version_by_type
                .entry(type_name.clone())
                .or_insert_with(|| {
                    (
                        (registration.default_table_name)(),
                        (registration.schema_version)(),
                    )
                });
        }

        let mut types = table_and_version_by_type
            .into_iter()
            .map(
                |(type_name, (table_name, schema_version))| HeteroTypeSnapshot {
                    type_name,
                    table_name,
                    schema_version,
                },
            )
            .collect::<Vec<_>>();
        types.sort_by(|a, b| a.type_name.cmp(&b.type_name));

        HeteroPersistVecSnapshot {
            format_version: 1,
            created_at_unix_ms: Utc::now().timestamp_millis(),
            mode: mode.clone(),
            vec_name: self.name.clone(),
            types,
            states: if mode == SnapshotMode::WithData {
                self.states()
            } else {
                Vec::new()
            },
        }
    }

    pub async fn restore(
        &mut self,
        snapshot: HeteroPersistVecSnapshot,
        session: &PersistSession,
    ) -> Result<()> {
        self.restore_with_policy(snapshot, session, RestoreConflictPolicy::FailFast)
            .await
    }

    pub async fn restore_with_policy(
        &mut self,
        snapshot: HeteroPersistVecSnapshot,
        session: &PersistSession,
        conflict_policy: RestoreConflictPolicy,
    ) -> Result<()> {
        self.name = snapshot.vec_name.clone();
        self.items.clear();

        let mut created_pairs = HashSet::<(String, String)>::new();
        for t in &snapshot.types {
            created_pairs.insert((t.type_name.clone(), t.table_name.clone()));
        }
        if snapshot.mode == SnapshotMode::WithData {
            for state in &snapshot.states {
                created_pairs.insert((state.type_name.clone(), state.table_name.clone()));
            }
        }

        for (type_name, table_name) in &created_pairs {
            let registration = self.registrations.get(type_name).ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Type '{}' is not registered for hetero restore",
                    type_name
                ))
            })?;

            let ddl = (registration.create_table_sql)(table_name);
            session.execute(&ddl).await?;
            let migration_plan = (registration.migration_plan)();
            migration_plan
                .ensure_table_schema_version(session, table_name)
                .await?;
        }

        if snapshot.mode == SnapshotMode::WithData {
            let type_version_hints = snapshot
                .types
                .iter()
                .map(|item| (item.type_name.clone(), item.schema_version))
                .collect::<HashMap<_, _>>();

            for mut state in snapshot.states {
                let registration = self.registrations.get(&state.type_name).ok_or_else(|| {
                    DbError::ExecutionError(format!(
                        "Type '{}' is not registered for hetero restore",
                        state.type_name
                    ))
                })?;

                if state.metadata.schema_version == 0 {
                    state.metadata.schema_version = type_version_hints
                        .get(&state.type_name)
                        .copied()
                        .unwrap_or(default_schema_version());
                }

                let migration_plan = (registration.migration_plan)();
                migration_plan.migrate_state_to_current(&mut state)?;

                let exists = session
                    .persist_row_exists(&state.table_name, &state.persist_id)
                    .await?;

                if exists {
                    match conflict_policy {
                        RestoreConflictPolicy::FailFast => {
                            return Err(DbError::ExecutionError(format!(
                                "Restore conflict: row {} already exists in table {}",
                                state.persist_id, state.table_name
                            )));
                        }
                        RestoreConflictPolicy::SkipExisting => {
                            continue;
                        }
                        RestoreConflictPolicy::OverwriteExisting => {
                            session
                                .delete_persist_row(&state.table_name, &state.persist_id)
                                .await?;
                        }
                    }
                }

                let mut entity = (registration.from_state)(&state)?;
                entity.save(session).await?;
                self.items.push(entity);
            }
        }

        Ok(())
    }
}

pub fn default_table_name(type_name: &str, line: u32, column: u32) -> String {
    let mut sanitized = String::with_capacity(type_name.len());
    for ch in type_name.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
        } else {
            sanitized.push('_');
        }
    }

    format!("persist_{}_{}_{}", sanitized, line, column)
}

pub fn default_table_name_stable(type_name: &str) -> String {
    let mut sanitized = String::with_capacity(type_name.len());
    for ch in type_name.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
        } else {
            sanitized.push('_');
        }
    }

    format!("persist_{}", sanitized)
}

pub fn new_persist_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn sql_escape_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub trait PersistValue:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn sql_type() -> &'static str;
    fn to_sql_literal(&self) -> String;
}

impl PersistValue for i64 {
    fn sql_type() -> &'static str {
        "INTEGER"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for i32 {
    fn sql_type() -> &'static str {
        "INTEGER"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for u64 {
    fn sql_type() -> &'static str {
        "INTEGER"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for usize {
    fn sql_type() -> &'static str {
        "INTEGER"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for f64 {
    fn sql_type() -> &'static str {
        "FLOAT"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for f32 {
    fn sql_type() -> &'static str {
        "FLOAT"
    }

    fn to_sql_literal(&self) -> String {
        self.to_string()
    }
}

impl PersistValue for bool {
    fn sql_type() -> &'static str {
        "BOOLEAN"
    }

    fn to_sql_literal(&self) -> String {
        if *self {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }
    }
}

impl PersistValue for String {
    fn sql_type() -> &'static str {
        "TEXT"
    }

    fn to_sql_literal(&self) -> String {
        format!("'{}'", sql_escape_string(self))
    }
}

impl PersistValue for Uuid {
    fn sql_type() -> &'static str {
        "UUID"
    }

    fn to_sql_literal(&self) -> String {
        format!("'{}'", self)
    }
}

impl PersistValue for DateTime<Utc> {
    fn sql_type() -> &'static str {
        "TIMESTAMP"
    }

    fn to_sql_literal(&self) -> String {
        format!("'{}'", self.to_rfc3339())
    }
}

impl PersistValue for NaiveDate {
    fn sql_type() -> &'static str {
        "DATE"
    }

    fn to_sql_literal(&self) -> String {
        format!("'{}'", self.format("%Y-%m-%d"))
    }
}

impl<T: PersistValue> PersistValue for Option<T> {
    fn sql_type() -> &'static str {
        T::sql_type()
    }

    fn to_sql_literal(&self) -> String {
        match self {
            Some(value) => value.to_sql_literal(),
            None => "NULL".to_string(),
        }
    }
}

pub fn serde_to_db_error(context: &str, err: serde_json::Error) -> DbError {
    DbError::ExecutionError(format!("{}: {}", context, err))
}

pub fn value_to_sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Text(v) => format!("'{}'", sql_escape_string(v)),
        Value::Boolean(v) => {
            if *v {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Timestamp(v) => format!("'{}'", v.to_rfc3339()),
        Value::Date(v) => format!("'{}'", v.format("%Y-%m-%d")),
        Value::Uuid(v) => format!("'{}'", v),
        Value::Array(v) => {
            let json = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
            format!("'{}'", sql_escape_string(&json))
        }
        Value::Json(v) => {
            let json = v.to_string();
            format!("'{}'", sql_escape_string(&json))
        }
    }
}

pub fn value_matches_sql_type(value: &Value, sql_type: &str) -> bool {
    if matches!(value, Value::Null) {
        return true;
    }

    let upper = sql_type.to_ascii_uppercase();
    let base = upper
        .split(['(', ' ', '\t'])
        .next()
        .unwrap_or_default()
        .to_string();

    match base.as_str() {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => matches!(value, Value::Integer(_)),
        "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => {
            matches!(value, Value::Integer(_) | Value::Float(_))
        }
        "TEXT" | "STRING" | "CHAR" | "VARCHAR" => matches!(value, Value::Text(_)),
        "BOOL" | "BOOLEAN" => matches!(value, Value::Boolean(_)),
        "TIMESTAMP" | "DATETIME" => matches!(value, Value::Timestamp(_) | Value::Text(_)),
        "DATE" => matches!(value, Value::Date(_) | Value::Text(_)),
        "UUID" => matches!(value, Value::Uuid(_) | Value::Text(_)),
        "JSON" | "JSONB" => matches!(value, Value::Json(_) | Value::Text(_)),
        _ => true,
    }
}

pub fn dynamic_schema_from_ddl(ddl: &str, table_name: impl Into<String>) -> Result<DynamicSchema> {
    let ddl = ddl.trim().trim_end_matches(';').trim();
    if ddl.is_empty() {
        return Err(DbError::ParseError("DDL is empty".to_string()));
    }

    let open_idx = ddl.find('(').ok_or_else(|| {
        DbError::ParseError("DDL must contain '(' with column declarations".to_string())
    })?;
    let close_idx = ddl.rfind(')').ok_or_else(|| {
        DbError::ParseError("DDL must contain ')' with column declarations".to_string())
    })?;
    if close_idx <= open_idx {
        return Err(DbError::ParseError(
            "DDL has invalid parenthesis order".to_string(),
        ));
    }

    let columns_body = &ddl[open_idx + 1..close_idx];
    let segments = split_top_level_commas(columns_body);
    let mut fields = Vec::new();

    for raw_segment in segments {
        let segment = raw_segment.trim();
        if segment.is_empty() {
            continue;
        }

        let upper = segment.to_ascii_uppercase();
        if upper.starts_with("PRIMARY KEY")
            || upper.starts_with("FOREIGN KEY")
            || upper.starts_with("UNIQUE")
            || upper.starts_with("CHECK")
            || upper.starts_with("CONSTRAINT")
        {
            continue;
        }

        let mut parts = segment.split_whitespace();
        let Some(raw_name) = parts.next() else {
            continue;
        };
        let col_name = trim_sql_identifier(raw_name);

        let modifiers = [
            "NOT",
            "NULL",
            "PRIMARY",
            "KEY",
            "UNIQUE",
            "REFERENCES",
            "CHECK",
            "DEFAULT",
            "CONSTRAINT",
        ];

        let mut type_tokens = Vec::new();
        for token in parts {
            let token_upper = token.to_ascii_uppercase();
            if modifiers.contains(&token_upper.as_str()) {
                break;
            }
            type_tokens.push(token);
        }

        if type_tokens.is_empty() {
            return Err(DbError::ParseError(format!(
                "DDL column '{}' has no SQL type",
                col_name
            )));
        }

        let sql_type = type_tokens.join(" ");
        let nullable = !upper.contains("NOT NULL");
        fields.push(DynamicFieldDef {
            name: col_name,
            sql_type,
            nullable,
        });
    }

    if fields.is_empty() {
        return Err(DbError::ParseError(
            "DDL does not contain any parseable columns".to_string(),
        ));
    }

    Ok(DynamicSchema {
        table_name: table_name.into(),
        fields,
        source_kind: "ddl".to_string(),
        source: ddl.to_string(),
    })
}

pub fn dynamic_schema_from_json_schema(
    json_schema: &str,
    table_name: impl Into<String>,
) -> Result<DynamicSchema> {
    let root: serde_json::Value = serde_json::from_str(json_schema)
        .map_err(|err| DbError::ParseError(format!("Invalid JSON schema: {}", err)))?;

    let obj = root
        .as_object()
        .ok_or_else(|| DbError::ParseError("JSON schema root must be an object".to_string()))?;

    let properties = obj
        .get("properties")
        .and_then(|value| value.as_object())
        .ok_or_else(|| {
            DbError::ParseError("JSON schema must contain object 'properties'".to_string())
        })?;

    let required_fields: BTreeSet<String> = obj
        .get("required")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let mut fields = Vec::new();
    for (name, prop) in properties {
        let prop_obj = prop.as_object().ok_or_else(|| {
            DbError::ParseError(format!(
                "Property '{}' in JSON schema must be an object",
                name
            ))
        })?;

        let (sql_type, nullable_from_type) = json_property_to_sql_type(prop_obj)?;
        let nullable = nullable_from_type || !required_fields.contains(name);

        fields.push(DynamicFieldDef {
            name: name.clone(),
            sql_type,
            nullable,
        });
    }

    if fields.is_empty() {
        return Err(DbError::ParseError(
            "JSON schema contains no properties".to_string(),
        ));
    }

    fields.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(DynamicSchema {
        table_name: table_name.into(),
        fields,
        source_kind: "json_schema".to_string(),
        source: json_schema.to_string(),
    })
}

fn split_top_level_commas(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;

    for ch in input.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                result.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        result.push(current.trim().to_string());
    }

    result
}

fn trim_sql_identifier(value: &str) -> String {
    value
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}

fn json_property_to_sql_type(
    prop: &serde_json::Map<String, serde_json::Value>,
) -> Result<(String, bool)> {
    let mut nullable = false;

    let json_type_value = prop
        .get("type")
        .ok_or_else(|| DbError::ParseError("JSON schema property missing 'type'".to_string()))?;

    let json_type = if let Some(type_name) = json_type_value.as_str() {
        type_name.to_string()
    } else if let Some(type_array) = json_type_value.as_array() {
        let mut chosen = None;
        for item in type_array {
            if let Some(type_name) = item.as_str() {
                if type_name == "null" {
                    nullable = true;
                } else if chosen.is_none() {
                    chosen = Some(type_name.to_string());
                }
            }
        }
        chosen.ok_or_else(|| {
            DbError::ParseError("JSON schema type array must contain non-null type".to_string())
        })?
    } else {
        return Err(DbError::ParseError(
            "JSON schema 'type' must be string or array".to_string(),
        ));
    };

    if json_type == "string" {
        if let Some(format) = prop.get("format").and_then(|v| v.as_str()) {
            match format {
                "date-time" => return Ok(("TIMESTAMP".to_string(), nullable)),
                "date" => return Ok(("DATE".to_string(), nullable)),
                "uuid" => return Ok(("UUID".to_string(), nullable)),
                _ => {}
            }
        }
    }

    let sql_type = match json_type.as_str() {
        "string" => "TEXT".to_string(),
        "integer" => "INTEGER".to_string(),
        "number" => "FLOAT".to_string(),
        "boolean" => "BOOLEAN".to_string(),
        "object" | "array" => "JSONB".to_string(),
        other => {
            return Err(DbError::ParseError(format!(
                "Unsupported JSON schema type '{}'",
                other
            )));
        }
    };

    Ok((sql_type, nullable))
}
