/// Determines what is included in a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Only the schema definitions are included.
    SchemaOnly,
    /// Both schema definitions and data are included.
    WithData,
}

/// Defines the policy for handling conflicts during data restoration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RestoreConflictPolicy {
    /// Fail the operation if a conflict occurs.
    FailFast,
    /// Skip items that already exist.
    SkipExisting,
    /// Overwrite existing items with data from the snapshot.
    OverwriteExisting,
}

/// A snapshot of a `PersistVec` (homogenous collection).
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

/// Snapshot metadata for a single type within a `HeteroPersistVec`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeteroTypeSnapshot {
    pub type_name: String,
    pub table_name: String,
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
}

/// A snapshot of a `HeteroPersistVec` (heterogeneous collection).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeteroPersistVecSnapshot {
    pub format_version: u16,
    pub created_at_unix_ms: i64,
    pub mode: SnapshotMode,
    pub vec_name: String,
    pub types: Vec<HeteroTypeSnapshot>,
    pub states: Vec<PersistState>,
}

/// The execution status of a dynamic function invocation.
#[derive(Debug, Clone)]
pub enum InvokeStatus {
    Invoked,
    SkippedUnsupported,
    Failed(String),
}

/// The outcome of invoking a function on a persisted item.
#[derive(Debug, Clone)]
pub struct InvokeOutcome {
    pub persist_id: String,
    pub function: String,
    pub status: InvokeStatus,
    pub result: Option<Value>,
}

/// A function type responsible for migrating the state of an item during schema migration.
pub type StateMigrationFn = Arc<dyn Fn(&mut PersistState) -> Result<()> + Send + Sync>;

/// Represents a single step in a migration plan.
#[derive(Clone)]
pub struct PersistMigrationStep {
    pub from_version: u32,
    pub to_version: u32,
    pub sql_statements: Vec<String>,
    pub(crate) state_migrator: Option<StateMigrationFn>,
}

/// A plan describing how to migrate data effectively between versions.
#[derive(Debug, Clone)]
pub struct PersistMigrationPlan {
    pub(crate) current_version: u32,
    pub(crate) steps: Vec<PersistMigrationStep>,
}
