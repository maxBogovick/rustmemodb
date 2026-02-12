use super::{PersistMetadata, PersistState, new_persist_id};
use crate::core::{DbError, Result, Value};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration as TokioDuration, sleep, timeout};
use tracing::{Level, event, info_span};
use uuid::Uuid;

const RUNTIME_SNAPSHOT_FILE: &str = "runtime_snapshot.json";
const RUNTIME_JOURNAL_FILE: &str = "runtime_journal.log";
const RUNTIME_FORMAT_VERSION: u16 = 1;

pub type DeterministicCommandHandler =
    Arc<dyn Fn(&mut PersistState, &serde_json::Value) -> Result<()> + Send + Sync>;
pub type DeterministicEnvelopeCommandHandler = Arc<
    dyn Fn(&mut PersistState, &RuntimeCommandEnvelope) -> Result<Vec<RuntimeSideEffectSpec>>
        + Send
        + Sync,
>;
pub type DeterministicContextCommandHandler = Arc<
    dyn Fn(
            &mut PersistState,
            &serde_json::Value,
            &RuntimeDeterministicContext,
        ) -> Result<Vec<RuntimeSideEffectSpec>>
        + Send
        + Sync,
>;
pub type RuntimeClosureHandler =
    Arc<dyn Fn(&mut PersistState, Vec<Value>) -> Result<Value> + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeCommandEnvelope {
    pub envelope_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub expected_version: Option<u64>,
    pub command_name: String,
    pub payload_json: serde_json::Value,
    pub payload_version: u32,
    pub created_at: DateTime<Utc>,
    pub idempotency_key: Option<String>,
    pub actor_id: Option<String>,
}

impl RuntimeCommandEnvelope {
    pub fn new(
        entity_type: impl Into<String>,
        entity_id: impl Into<String>,
        command_name: impl Into<String>,
        payload_json: serde_json::Value,
    ) -> Self {
        Self {
            envelope_id: Uuid::new_v4(),
            entity_type: entity_type.into(),
            entity_id: entity_id.into(),
            causation_id: None,
            correlation_id: None,
            expected_version: None,
            command_name: command_name.into(),
            payload_json,
            payload_version: 1,
            created_at: Utc::now(),
            idempotency_key: None,
            actor_id: None,
        }
    }

    pub fn with_expected_version(mut self, expected_version: u64) -> Self {
        self.expected_version = Some(expected_version);
        self
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn with_actor_id(mut self, actor_id: impl Into<String>) -> Self {
        self.actor_id = Some(actor_id.into());
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_causation_id(mut self, causation_id: Uuid) -> Self {
        self.causation_id = Some(causation_id);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeDeterministicContext {
    pub envelope_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub command_name: String,
    pub payload_version: u32,
    pub created_at: DateTime<Utc>,
    pub expected_version: Option<u64>,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub idempotency_key: Option<String>,
    pub actor_id: Option<String>,
}

impl RuntimeDeterministicContext {
    fn from_envelope(envelope: &RuntimeCommandEnvelope) -> Self {
        Self {
            envelope_id: envelope.envelope_id,
            entity_type: envelope.entity_type.clone(),
            entity_id: envelope.entity_id.clone(),
            command_name: envelope.command_name.clone(),
            payload_version: envelope.payload_version,
            created_at: envelope.created_at,
            expected_version: envelope.expected_version,
            causation_id: envelope.causation_id,
            correlation_id: envelope.correlation_id,
            idempotency_key: envelope.idempotency_key.clone(),
            actor_id: envelope.actor_id.clone(),
        }
    }

    pub fn deterministic_uuid(&self, namespace: &str) -> Uuid {
        Uuid::new_v5(&self.envelope_id, namespace.as_bytes())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSideEffectSpec {
    pub effect_type: String,
    pub payload_json: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeOutboxStatus {
    Pending,
    Dispatched,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeOutboxRecord {
    pub outbox_id: String,
    pub envelope_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub effect_type: String,
    pub payload_json: serde_json::Value,
    pub status: RuntimeOutboxStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeIdempotencyReceipt {
    pub envelope_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub command_name: String,
    pub state: PersistState,
    pub outbox: Vec<RuntimeOutboxRecord>,
}

#[derive(Debug, Clone)]
pub struct RuntimeEnvelopeApplyResult {
    pub envelope_id: Uuid,
    pub state: PersistState,
    pub idempotent_replay: bool,
    pub outbox: Vec<RuntimeOutboxRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimePayloadType {
    Null,
    Boolean,
    Integer,
    Float,
    Text,
    Array,
    Object,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimePayloadFieldContract {
    pub name: String,
    pub payload_type: RuntimePayloadType,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeCommandPayloadSchema {
    pub root_type: RuntimePayloadType,
    pub fields: Vec<RuntimePayloadFieldContract>,
    pub allow_extra_fields: bool,
}

impl Default for RuntimeCommandPayloadSchema {
    fn default() -> Self {
        Self {
            root_type: RuntimePayloadType::Object,
            fields: Vec::new(),
            allow_extra_fields: true,
        }
    }
}

impl RuntimeCommandPayloadSchema {
    pub fn object() -> Self {
        Self::default()
    }

    pub fn with_root_type(mut self, root_type: RuntimePayloadType) -> Self {
        self.root_type = root_type;
        self
    }

    pub fn require_field(
        mut self,
        name: impl Into<String>,
        payload_type: RuntimePayloadType,
    ) -> Self {
        self.fields.push(RuntimePayloadFieldContract {
            name: name.into(),
            payload_type,
            required: true,
        });
        self
    }

    pub fn optional_field(
        mut self,
        name: impl Into<String>,
        payload_type: RuntimePayloadType,
    ) -> Self {
        self.fields.push(RuntimePayloadFieldContract {
            name: name.into(),
            payload_type,
            required: false,
        });
        self
    }

    pub fn allow_extra_fields(mut self, allow: bool) -> Self {
        self.allow_extra_fields = allow;
        self
    }

    fn validate(&self, payload: &serde_json::Value) -> Result<()> {
        if !payload_matches_type(payload, &self.root_type) {
            return Err(DbError::ExecutionError(format!(
                "Payload root type mismatch: expected {:?}, got {}",
                self.root_type,
                json_type_name(payload)
            )));
        }

        if self.root_type != RuntimePayloadType::Object {
            return Ok(());
        }

        let object = payload
            .as_object()
            .ok_or_else(|| DbError::ExecutionError("Payload must be a JSON object".to_string()))?;

        let mut declared_names = HashSet::new();
        for field in &self.fields {
            declared_names.insert(field.name.as_str());

            match object.get(field.name.as_str()) {
                Some(value) => {
                    if !payload_matches_type(value, &field.payload_type) {
                        return Err(DbError::ExecutionError(format!(
                            "Field '{}' type mismatch: expected {:?}, got {}",
                            field.name,
                            field.payload_type,
                            json_type_name(value)
                        )));
                    }
                }
                None if field.required => {
                    return Err(DbError::ExecutionError(format!(
                        "Missing required field '{}'",
                        field.name
                    )));
                }
                None => {}
            }
        }

        if !self.allow_extra_fields {
            for key in object.keys() {
                if !declared_names.contains(key.as_str()) {
                    return Err(DbError::ExecutionError(format!(
                        "Unexpected field '{}' in payload",
                        key
                    )));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeDurabilityMode {
    Strict,
    Eventual { sync_interval_ms: u64 },
}

impl Default for RuntimeDurabilityMode {
    fn default() -> Self {
        Self::Strict
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for RuntimeRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 5,
            max_backoff_ms: 250,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeBackpressurePolicy {
    pub max_inflight: usize,
    pub acquire_timeout_ms: u64,
}

impl Default for RuntimeBackpressurePolicy {
    fn default() -> Self {
        Self {
            max_inflight: 64,
            acquire_timeout_ms: 2_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshotPolicy {
    pub snapshot_every_ops: usize,
    pub compact_if_journal_exceeds_bytes: u64,
    pub background_worker_interval_ms: Option<u64>,
}

impl Default for RuntimeSnapshotPolicy {
    fn default() -> Self {
        Self {
            snapshot_every_ops: 128,
            compact_if_journal_exceeds_bytes: 8 * 1024 * 1024,
            background_worker_interval_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeReplicationMode {
    Sync,
    AsyncBestEffort,
}

impl Default for RuntimeReplicationMode {
    fn default() -> Self {
        Self::Sync
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeReplicationPolicy {
    pub mode: RuntimeReplicationMode,
    pub replica_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeLifecyclePolicy {
    pub passivate_after_ms: u64,
    pub gc_after_ms: u64,
    pub max_hot_objects: usize,
    pub gc_only_if_never_touched: bool,
}

impl Default for RuntimeLifecyclePolicy {
    fn default() -> Self {
        Self {
            passivate_after_ms: 60_000,
            gc_after_ms: 15 * 60_000,
            max_hot_objects: 10_000,
            gc_only_if_never_touched: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeDeterminismPolicy {
    Permissive,
    StrictContextOnly,
}

impl Default for RuntimeDeterminismPolicy {
    fn default() -> Self {
        Self::Permissive
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeConsistencyMode {
    Strong,
    LocalDurable,
    Eventual,
}

impl Default for RuntimeConsistencyMode {
    fn default() -> Self {
        Self::LocalDurable
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeOperationalPolicy {
    #[serde(default)]
    pub consistency: RuntimeConsistencyMode,
    pub durability: RuntimeDurabilityMode,
    pub retry: RuntimeRetryPolicy,
    pub backpressure: RuntimeBackpressurePolicy,
    pub snapshot: RuntimeSnapshotPolicy,
    pub replication: RuntimeReplicationPolicy,
    pub lifecycle: RuntimeLifecyclePolicy,
    pub determinism: RuntimeDeterminismPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RuntimeEntityKey {
    pub entity_type: String,
    pub persist_id: String,
}

impl RuntimeEntityKey {
    pub fn new(entity_type: impl Into<String>, persist_id: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            persist_id: persist_id.into(),
        }
    }

    fn from_state(state: &PersistState) -> Self {
        Self {
            entity_type: state.type_name.clone(),
            persist_id: state.persist_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCommandInvocation {
    pub command: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStoredEntity {
    pub state: PersistState,
    pub last_access_at: DateTime<Utc>,
    pub access_count: u64,
    pub resident: bool,
}

impl RuntimeStoredEntity {
    fn new(state: PersistState, resident: bool) -> Self {
        Self {
            state,
            last_access_at: Utc::now(),
            access_count: 1,
            resident,
        }
    }

    fn touch(&mut self) {
        self.last_access_at = Utc::now();
        self.access_count = self.access_count.saturating_add(1);
        self.state.metadata.last_touch_at = self.last_access_at;
        self.state.metadata.touch_count = self.state.metadata.touch_count.saturating_add(1);
        self.state.metadata.updated_at = self.last_access_at;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuntimeJournalOp {
    Upsert {
        entity: RuntimeStoredEntity,
        reason: String,
        command: Option<RuntimeCommandInvocation>,
        #[serde(default)]
        envelope: Option<RuntimeCommandEnvelope>,
        #[serde(default)]
        outbox: Vec<RuntimeOutboxRecord>,
        #[serde(default)]
        idempotency_scope_key: Option<String>,
    },
    Delete {
        key: RuntimeEntityKey,
        reason: String,
    },
    OutboxUpsert {
        record: RuntimeOutboxRecord,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeJournalRecord {
    pub seq: u64,
    pub ts_unix_ms: i64,
    pub op: RuntimeJournalOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshotFile {
    pub format_version: u16,
    pub created_at_unix_ms: i64,
    pub last_seq: u64,
    pub entities: Vec<RuntimeStoredEntity>,
    #[serde(default)]
    pub outbox: Vec<RuntimeOutboxRecord>,
    #[serde(default)]
    pub idempotency_index: HashMap<String, RuntimeIdempotencyReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeProjectionField {
    pub state_field: String,
    pub column_name: String,
    pub payload_type: RuntimePayloadType,
    pub indexed: bool,
}

impl RuntimeProjectionField {
    pub fn new(
        state_field: impl Into<String>,
        column_name: impl Into<String>,
        payload_type: RuntimePayloadType,
    ) -> Self {
        Self {
            state_field: state_field.into(),
            column_name: column_name.into(),
            payload_type,
            indexed: false,
        }
    }

    pub fn indexed(mut self, indexed: bool) -> Self {
        self.indexed = indexed;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeProjectionContract {
    pub entity_type: String,
    pub table_name: String,
    pub schema_version: u32,
    pub fields: Vec<RuntimeProjectionField>,
}

impl RuntimeProjectionContract {
    pub fn new(entity_type: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            table_name: table_name.into(),
            schema_version: 1,
            fields: Vec::new(),
        }
    }

    pub fn with_schema_version(mut self, schema_version: u32) -> Self {
        self.schema_version = schema_version.max(1);
        self
    }

    pub fn with_field(mut self, field: RuntimeProjectionField) -> Self {
        self.fields.push(field);
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.entity_type.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Projection contract entity_type must not be empty".to_string(),
            ));
        }

        if self.table_name.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Projection contract table_name must not be empty".to_string(),
            ));
        }

        if self.fields.is_empty() {
            return Err(DbError::ExecutionError(format!(
                "Projection contract '{}' must declare at least one field",
                self.entity_type
            )));
        }

        let mut state_fields = HashSet::<String>::new();
        let mut column_names = HashSet::<String>::new();
        for field in &self.fields {
            if field.state_field.trim().is_empty() {
                return Err(DbError::ExecutionError(format!(
                    "Projection contract '{}' has empty state_field",
                    self.entity_type
                )));
            }
            if field.column_name.trim().is_empty() {
                return Err(DbError::ExecutionError(format!(
                    "Projection contract '{}' has empty column_name",
                    self.entity_type
                )));
            }

            if !state_fields.insert(field.state_field.clone()) {
                return Err(DbError::ExecutionError(format!(
                    "Projection contract '{}' has duplicate state_field '{}'",
                    self.entity_type, field.state_field
                )));
            }
            if !column_names.insert(field.column_name.clone()) {
                return Err(DbError::ExecutionError(format!(
                    "Projection contract '{}' has duplicate column_name '{}'",
                    self.entity_type, field.column_name
                )));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeProjectionRow {
    pub entity_id: String,
    pub values: serde_json::Map<String, serde_json::Value>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct RuntimeProjectionTable {
    contract: RuntimeProjectionContract,
    rows: HashMap<String, RuntimeProjectionRow>,
    indexes: HashMap<String, HashMap<String, HashSet<String>>>,
}

impl RuntimeProjectionTable {
    fn new(contract: RuntimeProjectionContract) -> Self {
        let mut indexes = HashMap::new();
        for field in &contract.fields {
            if field.indexed {
                indexes.insert(field.column_name.clone(), HashMap::new());
            }
        }

        Self {
            contract,
            rows: HashMap::new(),
            indexes,
        }
    }

    fn upsert_state(&mut self, state: &PersistState) -> Result<Option<RuntimeProjectionRow>> {
        let row = build_projection_row(&self.contract, state)?;
        let entity_id = state.persist_id.clone();
        let previous = self.rows.insert(entity_id.clone(), row.clone());
        if let Some(prev) = &previous {
            self.remove_from_indexes(prev);
        }
        self.add_to_indexes(&row);
        Ok(previous)
    }

    fn remove_entity(&mut self, entity_id: &str) -> Option<RuntimeProjectionRow> {
        let previous = self.rows.remove(entity_id);
        if let Some(prev) = &previous {
            self.remove_from_indexes(prev);
        }
        previous
    }

    fn restore_entity(&mut self, entity_id: &str, previous: Option<RuntimeProjectionRow>) {
        self.remove_entity(entity_id);
        if let Some(previous) = previous {
            self.add_to_indexes(&previous);
            self.rows.insert(entity_id.to_string(), previous);
        }
    }

    fn rows_sorted(&self) -> Vec<RuntimeProjectionRow> {
        let mut rows = self.rows.values().cloned().collect::<Vec<_>>();
        rows.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));
        rows
    }

    fn find_entity_ids_by_index(&self, column: &str, value: &serde_json::Value) -> Vec<String> {
        let key = projection_index_key(value);
        let mut ids = self
            .indexes
            .get(column)
            .and_then(|entries| entries.get(&key))
            .map(|set| set.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        ids.sort();
        ids
    }

    fn add_to_indexes(&mut self, row: &RuntimeProjectionRow) {
        for field in &self.contract.fields {
            if !field.indexed {
                continue;
            }
            let Some(value) = row.values.get(field.column_name.as_str()) else {
                continue;
            };
            let key = projection_index_key(value);
            let bucket = self
                .indexes
                .entry(field.column_name.clone())
                .or_default()
                .entry(key)
                .or_default();
            bucket.insert(row.entity_id.clone());
        }
    }

    fn remove_from_indexes(&mut self, row: &RuntimeProjectionRow) {
        for field in &self.contract.fields {
            if !field.indexed {
                continue;
            }
            let Some(value) = row.values.get(field.column_name.as_str()) else {
                continue;
            };
            let key = projection_index_key(value);
            if let Some(entries) = self.indexes.get_mut(field.column_name.as_str()) {
                if let Some(bucket) = entries.get_mut(&key) {
                    bucket.remove(row.entity_id.as_str());
                    if bucket.is_empty() {
                        entries.remove(&key);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RuntimeProjectionUndo {
    entity_type: String,
    entity_id: String,
    previous_row: Option<RuntimeProjectionRow>,
}

#[derive(Debug, Clone)]
struct RuntimeEntityMailbox {
    pending_commands: u64,
    inflight: bool,
    last_command_at: DateTime<Utc>,
}

impl RuntimeEntityMailbox {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            pending_commands: 0,
            inflight: false,
            last_command_at: now,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeLifecycleReport {
    pub passivated: usize,
    pub resurrected: usize,
    pub gc_deleted: usize,
}

#[derive(Debug, Clone)]
pub struct RuntimeStats {
    pub hot_entities: usize,
    pub cold_entities: usize,
    pub registered_types: usize,
    pub registered_deterministic_commands: usize,
    pub deterministic_commands_with_payload_contracts: usize,
    pub registered_runtime_closures: usize,
    pub registered_projections: usize,
    pub projection_rows: usize,
    pub projection_index_columns: usize,
    pub projection_lag_entities: usize,
    pub replication_targets: usize,
    pub replication_failures: u64,
    pub durability_lag_ms: u64,
    pub snapshot_worker_running: bool,
    pub snapshot_worker_errors: u64,
    pub next_seq: u64,
    pub ops_since_snapshot: usize,
    pub outbox_total: usize,
    pub outbox_pending: usize,
    pub idempotency_entries: usize,
    pub mailbox_entities: usize,
    pub mailbox_busy_entities: usize,
    pub lifecycle_passivated_total: u64,
    pub lifecycle_resurrected_total: u64,
    pub lifecycle_gc_deleted_total: u64,
    pub lifecycle_churn_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSloMetrics {
    pub durability_lag_ms: u64,
    pub projection_lag_entities: usize,
    pub lifecycle_churn_total: u64,
    pub outbox_pending: usize,
    pub replication_failures: u64,
    pub mailbox_busy_entities: usize,
}

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub root_dir: PathBuf,
    pub snapshot_file: PathBuf,
    pub journal_file: PathBuf,
}

#[derive(Clone)]
enum RegisteredDeterministicCommandHandler {
    Legacy(DeterministicCommandHandler),
    Envelope(DeterministicEnvelopeCommandHandler),
    Context(DeterministicContextCommandHandler),
}

#[derive(Clone)]
struct RegisteredDeterministicCommand {
    handler: RegisteredDeterministicCommandHandler,
    payload_schema: Option<RuntimeCommandPayloadSchema>,
}

pub struct PersistEntityRuntime {
    root_dir: PathBuf,
    policy: RuntimeOperationalPolicy,
    hot_entities: HashMap<RuntimeEntityKey, RuntimeStoredEntity>,
    cold_entities: HashMap<RuntimeEntityKey, RuntimeStoredEntity>,
    deterministic_registry: HashMap<String, HashMap<String, RegisteredDeterministicCommand>>,
    runtime_closure_registry: HashMap<String, HashMap<String, RuntimeClosureHandler>>,
    projection_registry: HashMap<String, RuntimeProjectionContract>,
    projection_tables: HashMap<String, RuntimeProjectionTable>,
    entity_mailboxes: HashMap<RuntimeEntityKey, RuntimeEntityMailbox>,
    outbox_records: HashMap<String, RuntimeOutboxRecord>,
    idempotency_index: HashMap<String, RuntimeIdempotencyReceipt>,
    seq_next: u64,
    ops_since_snapshot: usize,
    last_sync_unix_ms: i64,
    inflight: Arc<Semaphore>,
    resurrected_since_last_report: usize,
    lifecycle_passivated_total: u64,
    lifecycle_resurrected_total: u64,
    lifecycle_gc_deleted_total: u64,
    snapshot_worker_running: bool,
    snapshot_worker_errors: Arc<AtomicU64>,
    replica_targets: Vec<RuntimePaths>,
    replication_failures: Arc<AtomicU64>,
}

impl PersistEntityRuntime {
    pub async fn open(
        root_dir: impl Into<PathBuf>,
        policy: RuntimeOperationalPolicy,
    ) -> Result<Self> {
        let root_dir = root_dir.into();
        let policy = normalize_runtime_policy(policy);
        fs::create_dir_all(&root_dir)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let max_inflight = policy.backpressure.max_inflight.max(1);
        let replica_targets = runtime_replica_targets(&root_dir, &policy.replication.replica_roots);
        for replica in &replica_targets {
            fs::create_dir_all(&replica.root_dir)
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        let mut runtime = Self {
            root_dir,
            policy,
            hot_entities: HashMap::new(),
            cold_entities: HashMap::new(),
            deterministic_registry: HashMap::new(),
            runtime_closure_registry: HashMap::new(),
            projection_registry: HashMap::new(),
            projection_tables: HashMap::new(),
            entity_mailboxes: HashMap::new(),
            outbox_records: HashMap::new(),
            idempotency_index: HashMap::new(),
            seq_next: 1,
            ops_since_snapshot: 0,
            last_sync_unix_ms: Utc::now().timestamp_millis(),
            inflight: Arc::new(Semaphore::new(max_inflight)),
            resurrected_since_last_report: 0,
            lifecycle_passivated_total: 0,
            lifecycle_resurrected_total: 0,
            lifecycle_gc_deleted_total: 0,
            snapshot_worker_running: false,
            snapshot_worker_errors: Arc::new(AtomicU64::new(0)),
            replica_targets,
            replication_failures: Arc::new(AtomicU64::new(0)),
        };

        runtime.load_from_disk().await?;
        Ok(runtime)
    }

    pub fn policy(&self) -> &RuntimeOperationalPolicy {
        &self.policy
    }

    pub fn paths(&self) -> RuntimePaths {
        RuntimePaths {
            root_dir: self.root_dir.clone(),
            snapshot_file: self.snapshot_path(),
            journal_file: self.journal_path(),
        }
    }

    pub fn stats(&self) -> RuntimeStats {
        let command_count = self
            .deterministic_registry
            .values()
            .map(|commands| commands.len())
            .sum();
        let command_schema_count = self
            .deterministic_registry
            .values()
            .flat_map(|commands| commands.values())
            .filter(|command| command.payload_schema.is_some())
            .count();

        let closure_count = self
            .runtime_closure_registry
            .values()
            .map(|commands| commands.len())
            .sum();

        let projection_rows = self
            .projection_tables
            .values()
            .map(|table| table.rows.len())
            .sum();
        let projection_index_columns = self
            .projection_tables
            .values()
            .map(|table| table.indexes.len())
            .sum();
        let projection_lag_entities = self.projection_lag_entities_count();
        let durability_lag_ms =
            (Utc::now().timestamp_millis() - self.last_sync_unix_ms).max(0) as u64;
        let mailbox_busy_entities = self
            .entity_mailboxes
            .values()
            .filter(|mailbox| mailbox.inflight || mailbox.pending_commands > 0)
            .count();
        let lifecycle_churn_total = self
            .lifecycle_passivated_total
            .saturating_add(self.lifecycle_resurrected_total)
            .saturating_add(self.lifecycle_gc_deleted_total);

        RuntimeStats {
            hot_entities: self.hot_entities.len(),
            cold_entities: self.cold_entities.len(),
            registered_types: self
                .deterministic_registry
                .keys()
                .chain(self.runtime_closure_registry.keys())
                .chain(self.projection_registry.keys())
                .collect::<HashSet<_>>()
                .len(),
            registered_deterministic_commands: command_count,
            deterministic_commands_with_payload_contracts: command_schema_count,
            registered_runtime_closures: closure_count,
            registered_projections: self.projection_registry.len(),
            projection_rows,
            projection_index_columns,
            projection_lag_entities,
            replication_targets: self.replica_targets.len(),
            replication_failures: self.replication_failures.load(AtomicOrdering::Relaxed),
            durability_lag_ms,
            snapshot_worker_running: self.snapshot_worker_running,
            snapshot_worker_errors: self.snapshot_worker_errors.load(AtomicOrdering::Relaxed),
            next_seq: self.seq_next,
            ops_since_snapshot: self.ops_since_snapshot,
            outbox_total: self.outbox_records.len(),
            outbox_pending: self
                .outbox_records
                .values()
                .filter(|record| record.status == RuntimeOutboxStatus::Pending)
                .count(),
            idempotency_entries: self.idempotency_index.len(),
            mailbox_entities: self.entity_mailboxes.len(),
            mailbox_busy_entities,
            lifecycle_passivated_total: self.lifecycle_passivated_total,
            lifecycle_resurrected_total: self.lifecycle_resurrected_total,
            lifecycle_gc_deleted_total: self.lifecycle_gc_deleted_total,
            lifecycle_churn_total,
        }
    }

    pub fn slo_metrics(&self) -> RuntimeSloMetrics {
        let stats = self.stats();
        RuntimeSloMetrics {
            durability_lag_ms: stats.durability_lag_ms,
            projection_lag_entities: stats.projection_lag_entities,
            lifecycle_churn_total: stats.lifecycle_churn_total,
            outbox_pending: stats.outbox_pending,
            replication_failures: stats.replication_failures,
            mailbox_busy_entities: stats.mailbox_busy_entities,
        }
    }

    pub fn register_deterministic_command(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        handler: DeterministicCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Legacy(handler),
                payload_schema: None,
            },
        );
    }

    pub fn register_deterministic_command_with_schema(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        payload_schema: RuntimeCommandPayloadSchema,
        handler: DeterministicCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Legacy(handler),
                payload_schema: Some(payload_schema),
            },
        );
    }

    pub fn register_deterministic_envelope_command(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        handler: DeterministicEnvelopeCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Envelope(handler),
                payload_schema: None,
            },
        );
    }

    pub fn register_deterministic_envelope_command_with_schema(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        payload_schema: RuntimeCommandPayloadSchema,
        handler: DeterministicEnvelopeCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Envelope(handler),
                payload_schema: Some(payload_schema),
            },
        );
    }

    pub fn register_deterministic_context_command(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        handler: DeterministicContextCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Context(handler),
                payload_schema: None,
            },
        );
    }

    pub fn register_deterministic_context_command_with_schema(
        &mut self,
        entity_type: impl Into<String>,
        command: impl Into<String>,
        payload_schema: RuntimeCommandPayloadSchema,
        handler: DeterministicContextCommandHandler,
    ) {
        let entry = self
            .deterministic_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(
            command.into(),
            RegisteredDeterministicCommand {
                handler: RegisteredDeterministicCommandHandler::Context(handler),
                payload_schema: Some(payload_schema),
            },
        );
    }

    pub fn register_runtime_closure(
        &mut self,
        entity_type: impl Into<String>,
        function: impl Into<String>,
        handler: RuntimeClosureHandler,
    ) {
        let entry = self
            .runtime_closure_registry
            .entry(entity_type.into())
            .or_default();
        entry.insert(function.into(), handler);
    }

    pub fn register_projection_contract(
        &mut self,
        contract: RuntimeProjectionContract,
    ) -> Result<()> {
        contract.validate()?;
        let entity_type = contract.entity_type.clone();
        self.projection_registry
            .insert(entity_type.clone(), contract.clone());
        self.projection_tables
            .insert(entity_type.clone(), RuntimeProjectionTable::new(contract));
        self.rebuild_projection_for_entity_type(&entity_type)
    }

    pub fn projection_contract(&self, entity_type: &str) -> Option<&RuntimeProjectionContract> {
        self.projection_registry.get(entity_type)
    }

    pub fn list_projection_rows(&self, entity_type: &str) -> Result<Vec<RuntimeProjectionRow>> {
        let table = self.projection_tables.get(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection contract is not registered for entity type '{}'",
                entity_type
            ))
        })?;
        Ok(table.rows_sorted())
    }

    pub fn find_projection_entity_ids_by_index(
        &self,
        entity_type: &str,
        column: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<String>> {
        let contract = self.projection_registry.get(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection contract is not registered for entity type '{}'",
                entity_type
            ))
        })?;

        let indexed = contract
            .fields
            .iter()
            .any(|field| field.column_name == column && field.indexed);
        if !indexed {
            return Err(DbError::ExecutionError(format!(
                "Projection column '{}.{}' is not indexed",
                entity_type, column
            )));
        }

        let table = self.projection_tables.get(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection table is not initialized for entity type '{}'",
                entity_type
            ))
        })?;

        Ok(table.find_entity_ids_by_index(column, value))
    }

    pub fn find_projection_rows_by_index(
        &self,
        entity_type: &str,
        column: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<RuntimeProjectionRow>> {
        let table = self.projection_tables.get(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection table is not initialized for entity type '{}'",
                entity_type
            ))
        })?;

        let ids = self.find_projection_entity_ids_by_index(entity_type, column, value)?;
        let mut rows = ids
            .into_iter()
            .filter_map(|entity_id| table.rows.get(&entity_id).cloned())
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));
        Ok(rows)
    }

    pub fn rebuild_registered_projections(&mut self) -> Result<()> {
        let entity_types = self.projection_registry.keys().cloned().collect::<Vec<_>>();
        for entity_type in entity_types {
            self.rebuild_projection_for_entity_type(&entity_type)?;
        }
        Ok(())
    }

    pub async fn create_entity(
        &mut self,
        entity_type: impl Into<String>,
        table_name: impl Into<String>,
        fields: serde_json::Value,
        schema_version: u32,
    ) -> Result<String> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;

        let now = Utc::now();
        let persist_id = new_persist_id();
        let mut metadata = PersistMetadata::new(now);
        metadata.schema_version = schema_version.max(1);
        metadata.version = 1;
        metadata.touch_count = 1;
        metadata.persisted = true;

        let state = PersistState {
            persist_id: persist_id.clone(),
            type_name: entity_type.into(),
            table_name: table_name.into(),
            metadata,
            fields,
        };

        let managed = RuntimeStoredEntity::new(state, true);
        self.apply_upsert(managed, "create", None).await?;
        Ok(persist_id)
    }

    pub async fn upsert_state(
        &mut self,
        state: PersistState,
        reason: impl Into<String>,
    ) -> Result<()> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;

        let mut managed = RuntimeStoredEntity::new(state, true);
        managed.state.metadata.persisted = true;
        self.apply_upsert(managed, reason, None).await
    }

    pub async fn delete_entity(
        &mut self,
        entity_type: &str,
        persist_id: &str,
        reason: impl Into<String>,
    ) -> Result<()> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;
        let delete_reason = reason.into();
        let span = info_span!(
            "runtime.entity.delete",
            entity_type = %entity_type,
            entity_id = %persist_id,
            reason = %delete_reason
        );
        let _enter = span.enter();

        let key = RuntimeEntityKey::new(entity_type, persist_id);
        let projection_undo = self.apply_projection_delete(&key);

        let append_result = self
            .append_record(RuntimeJournalOp::Delete {
                key,
                reason: delete_reason,
            })
            .await;
        if let Err(err) = append_result {
            self.rollback_projection_undo(projection_undo);
            event!(Level::ERROR, error = %err, "runtime entity delete append failed");
            return Err(err);
        }

        self.hot_entities
            .remove(&RuntimeEntityKey::new(entity_type, persist_id));
        self.cold_entities
            .remove(&RuntimeEntityKey::new(entity_type, persist_id));
        self.mailbox_drop_entity(&RuntimeEntityKey::new(entity_type, persist_id));

        self.maybe_snapshot_and_compact().await?;
        event!(Level::DEBUG, "runtime entity deleted");
        Ok(())
    }

    pub fn get_state(&mut self, entity_type: &str, persist_id: &str) -> Result<PersistState> {
        let key = RuntimeEntityKey::new(entity_type, persist_id);

        if let Some(hot) = self.hot_entities.get_mut(&key) {
            hot.touch();
            return Ok(hot.state.clone());
        }

        if let Some(mut cold) = self.cold_entities.remove(&key) {
            cold.resident = true;
            cold.touch();
            let state = cold.state.clone();
            self.hot_entities.insert(key, cold);
            self.record_resurrection();
            return Ok(state);
        }

        Err(DbError::ExecutionError(format!(
            "Entity not found: {}:{}",
            entity_type, persist_id
        )))
    }

    pub fn list_states(&self) -> Vec<PersistState> {
        let mut states = Vec::with_capacity(self.hot_entities.len() + self.cold_entities.len());
        states.extend(self.hot_entities.values().map(|m| m.state.clone()));
        states.extend(self.cold_entities.values().map(|m| m.state.clone()));
        states
    }

    pub fn list_outbox_records(&self) -> Vec<RuntimeOutboxRecord> {
        let mut records = self.outbox_records.values().cloned().collect::<Vec<_>>();
        records.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then(a.outbox_id.cmp(&b.outbox_id))
        });
        records
    }

    pub fn list_pending_outbox_records(&self) -> Vec<RuntimeOutboxRecord> {
        self.list_outbox_records()
            .into_iter()
            .filter(|record| record.status == RuntimeOutboxStatus::Pending)
            .collect()
    }

    pub async fn mark_outbox_dispatched(&mut self, outbox_id: &str) -> Result<()> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;
        let span = info_span!("runtime.outbox.dispatch", outbox_id = %outbox_id);
        let _enter = span.enter();

        let Some(current) = self.outbox_records.get(outbox_id).cloned() else {
            return Err(DbError::ExecutionError(format!(
                "Outbox record not found: {}",
                outbox_id
            )));
        };

        if current.status == RuntimeOutboxStatus::Dispatched {
            event!(Level::DEBUG, "outbox already dispatched");
            return Ok(());
        }

        let mut updated = current;
        updated.status = RuntimeOutboxStatus::Dispatched;
        let persisted = updated.clone();
        self.append_record(RuntimeJournalOp::OutboxUpsert { record: updated })
            .await?;
        self.outbox_records
            .insert(outbox_id.to_string(), persisted.clone());
        self.update_idempotency_outbox_status(&persisted);
        self.maybe_snapshot_and_compact().await?;
        event!(Level::INFO, envelope_id = %persisted.envelope_id, "outbox dispatched");
        Ok(())
    }

    pub async fn apply_deterministic_command(
        &mut self,
        entity_type: &str,
        persist_id: &str,
        command: &str,
        payload: serde_json::Value,
    ) -> Result<PersistState> {
        let envelope = RuntimeCommandEnvelope::new(entity_type, persist_id, command, payload);
        let result = self.apply_command_envelope(envelope).await?;
        Ok(result.state)
    }

    pub async fn apply_command_envelope(
        &mut self,
        envelope: RuntimeCommandEnvelope,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;
        let span = info_span!(
            "runtime.command.envelope",
            envelope_id = %envelope.envelope_id,
            entity_type = %envelope.entity_type,
            entity_id = %envelope.entity_id,
            command = %envelope.command_name
        );
        let _enter = span.enter();

        validate_command_envelope(&envelope)?;
        event!(
            Level::DEBUG,
            payload_version = envelope.payload_version,
            "runtime envelope accepted"
        );

        let command_handler = self
            .deterministic_registry
            .get(envelope.entity_type.as_str())
            .and_then(|commands| commands.get(envelope.command_name.as_str()))
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Deterministic command '{}' is not registered for entity type '{}'",
                    envelope.command_name, envelope.entity_type
                ))
            })?;
        if let Some(payload_schema) = &command_handler.payload_schema {
            payload_schema
                .validate(&envelope.payload_json)
                .map_err(|err| {
                    DbError::ExecutionError(format!(
                        "Payload validation for command '{}': {}",
                        envelope.command_name, err
                    ))
                })?;
        }

        let idempotency_scope_key = build_idempotency_scope_key(&envelope);
        if let Some(scope_key) = &idempotency_scope_key {
            if let Some(existing) = self.idempotency_index.get(scope_key) {
                event!(Level::INFO, "runtime envelope idempotent replay");
                return Ok(RuntimeEnvelopeApplyResult {
                    envelope_id: existing.envelope_id,
                    state: existing.state.clone(),
                    idempotent_replay: true,
                    outbox: existing.outbox.clone(),
                });
            }
        }

        if self.policy.determinism == RuntimeDeterminismPolicy::StrictContextOnly
            && !matches!(
                &command_handler.handler,
                RegisteredDeterministicCommandHandler::Context(_)
            )
        {
            return Err(DbError::ExecutionError(format!(
                "Determinism policy is StrictContextOnly; command '{}::{}' must be registered via register_deterministic_context_command[_with_schema]",
                envelope.entity_type, envelope.command_name
            )));
        }

        let key = RuntimeEntityKey::new(envelope.entity_type.clone(), envelope.entity_id.clone());
        let base = self.take_entity_for_mutation(&key)?;
        self.mailbox_start_command(&key);
        if let Some(expected_version) = envelope.expected_version {
            let actual_version = base.state.metadata.version.max(0) as u64;
            if expected_version != actual_version {
                self.hot_entities.insert(key.clone(), base);
                self.mailbox_complete_command(&key);
                event!(
                    Level::WARN,
                    expected_version,
                    actual_version,
                    "runtime envelope expected version mismatch"
                );
                return Err(DbError::ExecutionError(format!(
                    "Expected version mismatch for {}:{} (expected {}, actual {})",
                    envelope.entity_type, envelope.entity_id, expected_version, actual_version
                )));
            }
        }

        let max_attempts = self.policy.retry.max_attempts.max(1);
        let mut last_err: Option<DbError> = None;
        let deterministic_ctx = RuntimeDeterministicContext::from_envelope(&envelope);

        for attempt in 1..=max_attempts {
            let mut working = base.clone();
            let result = invoke_registered_handler(
                &command_handler.handler,
                &mut working.state,
                &envelope,
                &deterministic_ctx,
            );

            match result {
                Ok(side_effects) => {
                    working.state.metadata.persisted = true;
                    working.touch();
                    let outbox_records = side_effects
                        .into_iter()
                        .enumerate()
                        .map(|(index, effect)| RuntimeOutboxRecord {
                            outbox_id: format!("{}:{}", envelope.envelope_id, index),
                            envelope_id: envelope.envelope_id,
                            entity_type: envelope.entity_type.clone(),
                            entity_id: envelope.entity_id.clone(),
                            effect_type: effect.effect_type,
                            payload_json: effect.payload_json,
                            status: RuntimeOutboxStatus::Pending,
                            created_at: envelope.created_at,
                        })
                        .collect::<Vec<_>>();

                    let invocation = RuntimeCommandInvocation {
                        command: envelope.command_name.clone(),
                        payload: envelope.payload_json.clone(),
                    };

                    let projection_undo = match self.apply_projection_upsert(&working.state) {
                        Ok(undo) => undo,
                        Err(err) => {
                            event!(Level::ERROR, error = %err, "runtime projection upsert failed");
                            last_err = Some(err);
                            continue;
                        }
                    };

                    match self
                        .append_record(RuntimeJournalOp::Upsert {
                            entity: working.clone(),
                            reason: "command".to_string(),
                            command: Some(invocation),
                            envelope: Some(envelope.clone()),
                            outbox: outbox_records.clone(),
                            idempotency_scope_key: idempotency_scope_key.clone(),
                        })
                        .await
                    {
                        Ok(()) => {
                            self.hot_entities.insert(key.clone(), working.clone());
                            for record in &outbox_records {
                                self.outbox_records
                                    .insert(record.outbox_id.clone(), record.clone());
                            }
                            if let Some(scope_key) = idempotency_scope_key.as_ref() {
                                self.idempotency_index.insert(
                                    scope_key.clone(),
                                    RuntimeIdempotencyReceipt {
                                        envelope_id: envelope.envelope_id,
                                        entity_type: envelope.entity_type.clone(),
                                        entity_id: envelope.entity_id.clone(),
                                        command_name: envelope.command_name.clone(),
                                        state: working.state.clone(),
                                        outbox: outbox_records.clone(),
                                    },
                                );
                            }
                            let snapshot_result = self.maybe_snapshot_and_compact().await;
                            self.mailbox_complete_command(&key);
                            if let Err(err) = snapshot_result {
                                event!(
                                    Level::ERROR,
                                    error = %err,
                                    "runtime post-commit snapshot/compaction failed"
                                );
                                return Err(err);
                            }
                            event!(
                                Level::INFO,
                                attempt,
                                outbox_records = outbox_records.len(),
                                "runtime envelope applied"
                            );
                            return Ok(RuntimeEnvelopeApplyResult {
                                envelope_id: envelope.envelope_id,
                                state: working.state,
                                idempotent_replay: false,
                                outbox: outbox_records,
                            });
                        }
                        Err(err) => {
                            self.rollback_projection_undo(projection_undo);
                            event!(Level::ERROR, error = %err, "runtime journal append failed");
                            last_err = Some(err);
                        }
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, error = %err, "runtime deterministic handler failed");
                    last_err = Some(err);
                }
            }

            if attempt < max_attempts {
                sleep(TokioDuration::from_millis(self.retry_backoff_ms(attempt))).await;
            }
        }

        self.hot_entities.insert(key.clone(), base);
        self.mailbox_complete_command(&key);
        let err = last_err.unwrap_or_else(|| {
            DbError::ExecutionError("Failed to apply deterministic command".to_string())
        });
        event!(Level::ERROR, error = %err, "runtime envelope apply failed");
        Err(err)
    }

    pub async fn invoke_runtime_closure(
        &mut self,
        entity_type: &str,
        persist_id: &str,
        function: &str,
        args: Vec<Value>,
    ) -> Result<Value> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;
        let span = info_span!(
            "runtime.closure.invoke",
            entity_type = %entity_type,
            entity_id = %persist_id,
            function = %function
        );
        let _enter = span.enter();

        let runtime_handler = self
            .runtime_closure_registry
            .get(entity_type)
            .and_then(|commands| commands.get(function))
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Runtime closure '{}' is not registered for entity type '{}'",
                    function, entity_type
                ))
            })?;

        let key = RuntimeEntityKey::new(entity_type, persist_id);
        let mut entity = self.take_entity_for_mutation(&key)?;
        self.mailbox_start_command(&key);
        let base = entity.clone();
        let result = runtime_handler(&mut entity.state, args);
        let result = match result {
            Ok(value) => value,
            Err(err) => {
                self.hot_entities.insert(key.clone(), base);
                self.mailbox_complete_command(&key);
                event!(Level::ERROR, error = %err, "runtime closure handler failed");
                return Err(err);
            }
        };

        // Runtime closures are intentionally not deterministic/serializable.
        // We keep them available for local runtime behavior, and persist only
        // the final state snapshot as an upsert event.
        entity.touch();
        if let Err(err) = self.apply_upsert(entity, "runtime_closure", None).await {
            self.hot_entities.insert(key.clone(), base);
            self.mailbox_complete_command(&key);
            event!(Level::ERROR, error = %err, "runtime closure persist failed");
            return Err(err);
        }
        self.mailbox_complete_command(&key);
        event!(Level::DEBUG, "runtime closure applied");

        Ok(result)
    }

    pub async fn run_lifecycle_maintenance(&mut self) -> Result<RuntimeLifecycleReport> {
        let now = Utc::now();
        let passivate_after = TokioDuration::from_millis(self.policy.lifecycle.passivate_after_ms);
        let gc_after = TokioDuration::from_millis(self.policy.lifecycle.gc_after_ms);

        let mut passivated = 0usize;
        let mut gc_deleted = 0usize;
        let mut passivated_in_this_cycle = HashSet::new();

        let mut to_passivate = Vec::new();
        for (key, entity) in &self.hot_entities {
            if self.mailbox_is_busy(key) {
                continue;
            }
            let idle_ms = (now - entity.last_access_at).num_milliseconds();
            if idle_ms >= passivate_after.as_millis() as i64 {
                to_passivate.push(key.clone());
            }
        }

        if self.hot_entities.len() > self.policy.lifecycle.max_hot_objects {
            let mut candidates = self
                .hot_entities
                .iter()
                .map(|(k, v)| (k.clone(), v.last_access_at))
                .collect::<Vec<_>>();
            candidates.sort_by(|a, b| a.1.cmp(&b.1));

            let extra = self.hot_entities.len() - self.policy.lifecycle.max_hot_objects;
            for (candidate, _) in candidates.into_iter().take(extra) {
                if !to_passivate.contains(&candidate) {
                    to_passivate.push(candidate);
                }
            }
        }

        for key in to_passivate {
            if let Some(mut entity) = self.hot_entities.remove(&key) {
                entity.resident = false;
                self.cold_entities.insert(key.clone(), entity);
                passivated = passivated.saturating_add(1);
                passivated_in_this_cycle.insert(key);
            }
        }

        let mut to_gc = Vec::new();
        for (key, entity) in &self.cold_entities {
            if passivated_in_this_cycle.contains(key) {
                continue;
            }

            let idle_ms = (now - entity.last_access_at).num_milliseconds();
            let old_enough = idle_ms >= gc_after.as_millis() as i64;
            let eligible_by_touch = if self.policy.lifecycle.gc_only_if_never_touched {
                entity.state.metadata.touch_count == 0
            } else {
                true
            };

            if old_enough && eligible_by_touch && !self.mailbox_is_busy(key) {
                to_gc.push(key.clone());
            }
        }

        for key in to_gc {
            if let Some(removed) = self.cold_entities.remove(&key) {
                let projection_undo = self.apply_projection_delete(&key);
                let append = self
                    .append_record(RuntimeJournalOp::Delete {
                        key: key.clone(),
                        reason: "lifecycle_gc".to_string(),
                    })
                    .await;
                if let Err(err) = append {
                    self.rollback_projection_undo(projection_undo);
                    self.cold_entities.insert(key, removed);
                    return Err(err);
                }
                gc_deleted = gc_deleted.saturating_add(1);
                self.mailbox_drop_entity(&key);
            }
        }

        self.maybe_snapshot_and_compact().await?;
        self.lifecycle_passivated_total = self
            .lifecycle_passivated_total
            .saturating_add(passivated as u64);
        self.lifecycle_gc_deleted_total = self
            .lifecycle_gc_deleted_total
            .saturating_add(gc_deleted as u64);

        let resurrected = self.resurrected_since_last_report;
        self.resurrected_since_last_report = 0;

        Ok(RuntimeLifecycleReport {
            passivated,
            resurrected,
            gc_deleted,
        })
    }

    pub async fn force_snapshot_and_compact(&mut self) -> Result<()> {
        self.write_snapshot_and_compact().await
    }

    pub async fn run_snapshot_tick(&mut self) -> Result<bool> {
        let snapshot_due =
            self.ops_since_snapshot >= self.policy.snapshot.snapshot_every_ops.max(1);
        let journal_too_large = self
            .journal_size_bytes()
            .await?
            .cmp(&self.policy.snapshot.compact_if_journal_exceeds_bytes)
            == Ordering::Greater;

        if snapshot_due || journal_too_large {
            self.write_snapshot_and_compact().await?;
            return Ok(true);
        }

        Ok(false)
    }

    pub fn export_snapshot(&self) -> RuntimeSnapshotFile {
        let mut entities = Vec::with_capacity(self.hot_entities.len() + self.cold_entities.len());
        entities.extend(self.hot_entities.values().cloned());
        entities.extend(self.cold_entities.values().cloned());
        let mut outbox = self.outbox_records.values().cloned().collect::<Vec<_>>();
        outbox.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then(a.outbox_id.cmp(&b.outbox_id))
        });

        RuntimeSnapshotFile {
            format_version: RUNTIME_FORMAT_VERSION,
            created_at_unix_ms: Utc::now().timestamp_millis(),
            last_seq: self.seq_next.saturating_sub(1),
            entities,
            outbox,
            idempotency_index: self.idempotency_index.clone(),
        }
    }

    fn snapshot_path(&self) -> PathBuf {
        self.root_dir.join(RUNTIME_SNAPSHOT_FILE)
    }

    fn journal_path(&self) -> PathBuf {
        self.root_dir.join(RUNTIME_JOURNAL_FILE)
    }

    async fn load_from_disk(&mut self) -> Result<()> {
        let mut last_seq = 0u64;

        if let Some(snapshot) = self.read_snapshot_file().await? {
            if snapshot.format_version != RUNTIME_FORMAT_VERSION {
                return Err(DbError::ExecutionError(format!(
                    "Unsupported runtime snapshot format version {}",
                    snapshot.format_version
                )));
            }

            for mut entity in snapshot.entities {
                if entity.state.metadata.schema_version == 0 {
                    entity.state.metadata.schema_version = 1;
                }
                let key = RuntimeEntityKey::from_state(&entity.state);
                if entity.resident {
                    self.hot_entities.insert(key, entity);
                } else {
                    self.cold_entities.insert(key, entity);
                }
            }
            for record in snapshot.outbox {
                self.outbox_records.insert(record.outbox_id.clone(), record);
            }
            self.idempotency_index = snapshot.idempotency_index;
            last_seq = snapshot.last_seq;
        }

        let records = self.read_journal_records(last_seq).await?;
        let mut max_seq = last_seq;
        for record in records {
            max_seq = max_seq.max(record.seq);
            self.apply_journal_record_to_memory(record);
        }

        self.seq_next = max_seq.saturating_add(1).max(1);
        Ok(())
    }

    fn apply_journal_record_to_memory(&mut self, record: RuntimeJournalRecord) {
        match record.op {
            RuntimeJournalOp::Upsert {
                entity,
                envelope,
                outbox,
                idempotency_scope_key,
                ..
            } => {
                let key = RuntimeEntityKey::from_state(&entity.state);
                if entity.resident {
                    self.cold_entities.remove(&key);
                    self.hot_entities.insert(key, entity);
                } else {
                    self.hot_entities.remove(&key);
                    self.cold_entities.insert(key, entity);
                }

                for record in &outbox {
                    self.outbox_records
                        .insert(record.outbox_id.clone(), record.clone());
                }

                if let (Some(scope_key), Some(envelope)) = (idempotency_scope_key, envelope) {
                    let state = self
                        .hot_entities
                        .get(&RuntimeEntityKey::new(
                            envelope.entity_type.clone(),
                            envelope.entity_id.clone(),
                        ))
                        .map(|stored| stored.state.clone())
                        .or_else(|| {
                            self.cold_entities
                                .get(&RuntimeEntityKey::new(
                                    envelope.entity_type.clone(),
                                    envelope.entity_id.clone(),
                                ))
                                .map(|stored| stored.state.clone())
                        });

                    if let Some(state) = state {
                        self.idempotency_index.insert(
                            scope_key,
                            RuntimeIdempotencyReceipt {
                                envelope_id: envelope.envelope_id,
                                entity_type: envelope.entity_type,
                                entity_id: envelope.entity_id,
                                command_name: envelope.command_name,
                                state,
                                outbox,
                            },
                        );
                    }
                }
            }
            RuntimeJournalOp::Delete { key, .. } => {
                self.hot_entities.remove(&key);
                self.cold_entities.remove(&key);
                self.mailbox_drop_entity(&key);
            }
            RuntimeJournalOp::OutboxUpsert { record } => {
                self.outbox_records
                    .insert(record.outbox_id.clone(), record.clone());
                self.update_idempotency_outbox_status(&record);
            }
        }
    }

    fn rebuild_projection_for_entity_type(&mut self, entity_type: &str) -> Result<()> {
        let contract = self
            .projection_registry
            .get(entity_type)
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Projection contract is not registered for entity type '{}'",
                    entity_type
                ))
            })?;

        let states = self
            .hot_entities
            .values()
            .chain(self.cold_entities.values())
            .filter(|entity| entity.state.type_name == entity_type)
            .map(|entity| entity.state.clone())
            .collect::<Vec<_>>();

        self.projection_tables.insert(
            entity_type.to_string(),
            RuntimeProjectionTable::new(contract),
        );
        let table = self.projection_tables.get_mut(entity_type).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Projection table is not initialized for entity type '{}'",
                entity_type
            ))
        })?;

        for state in states {
            table.upsert_state(&state)?;
        }
        Ok(())
    }

    fn apply_projection_upsert(
        &mut self,
        state: &PersistState,
    ) -> Result<Option<RuntimeProjectionUndo>> {
        let Some(table) = self.projection_tables.get_mut(state.type_name.as_str()) else {
            return Ok(None);
        };

        let previous_row = table.upsert_state(state)?;
        Ok(Some(RuntimeProjectionUndo {
            entity_type: state.type_name.clone(),
            entity_id: state.persist_id.clone(),
            previous_row,
        }))
    }

    fn apply_projection_delete(&mut self, key: &RuntimeEntityKey) -> Option<RuntimeProjectionUndo> {
        let table = self.projection_tables.get_mut(key.entity_type.as_str())?;
        let previous_row = table.remove_entity(key.persist_id.as_str());
        Some(RuntimeProjectionUndo {
            entity_type: key.entity_type.clone(),
            entity_id: key.persist_id.clone(),
            previous_row,
        })
    }

    fn rollback_projection_undo(&mut self, undo: Option<RuntimeProjectionUndo>) {
        let Some(undo) = undo else {
            return;
        };
        if let Some(table) = self.projection_tables.get_mut(undo.entity_type.as_str()) {
            table.restore_entity(undo.entity_id.as_str(), undo.previous_row);
        }
    }

    fn projection_lag_entities_count(&self) -> usize {
        let mut lag = 0usize;

        for (entity_type, table) in &self.projection_tables {
            let states = self
                .hot_entities
                .values()
                .chain(self.cold_entities.values())
                .filter(|entity| &entity.state.type_name == entity_type)
                .map(|entity| &entity.state)
                .collect::<Vec<_>>();

            let mut expected_ids = HashSet::<&str>::new();
            for state in states {
                expected_ids.insert(state.persist_id.as_str());
                let Some(row) = table.rows.get(state.persist_id.as_str()) else {
                    lag = lag.saturating_add(1);
                    continue;
                };

                match build_projection_row(&table.contract, state) {
                    Ok(expected_row) => {
                        if row.values != expected_row.values
                            || row.updated_at != expected_row.updated_at
                        {
                            lag = lag.saturating_add(1);
                        }
                    }
                    Err(_) => {
                        lag = lag.saturating_add(1);
                    }
                }
            }

            for entity_id in table.rows.keys() {
                if !expected_ids.contains(entity_id.as_str()) {
                    lag = lag.saturating_add(1);
                }
            }
        }

        lag
    }

    fn mailbox_start_command(&mut self, key: &RuntimeEntityKey) {
        let now = Utc::now();
        let entry = self
            .entity_mailboxes
            .entry(key.clone())
            .or_insert_with(|| RuntimeEntityMailbox::new(now));
        entry.pending_commands = entry.pending_commands.saturating_add(1);
        entry.inflight = true;
        entry.last_command_at = now;
    }

    fn mailbox_complete_command(&mut self, key: &RuntimeEntityKey) {
        let Some(entry) = self.entity_mailboxes.get_mut(key) else {
            return;
        };
        entry.pending_commands = entry.pending_commands.saturating_sub(1);
        entry.inflight = false;
        entry.last_command_at = Utc::now();
    }

    fn mailbox_is_busy(&self, key: &RuntimeEntityKey) -> bool {
        self.entity_mailboxes
            .get(key)
            .map(|entry| entry.inflight || entry.pending_commands > 0)
            .unwrap_or(false)
    }

    fn mailbox_drop_entity(&mut self, key: &RuntimeEntityKey) {
        self.entity_mailboxes.remove(key);
    }

    fn record_resurrection(&mut self) {
        self.resurrected_since_last_report = self.resurrected_since_last_report.saturating_add(1);
        self.lifecycle_resurrected_total = self.lifecycle_resurrected_total.saturating_add(1);
    }

    fn take_entity_for_mutation(&mut self, key: &RuntimeEntityKey) -> Result<RuntimeStoredEntity> {
        if let Some(entity) = self.hot_entities.remove(key) {
            return Ok(entity);
        }

        if let Some(mut entity) = self.cold_entities.remove(key) {
            entity.resident = true;
            self.record_resurrection();
            return Ok(entity);
        }

        Err(DbError::ExecutionError(format!(
            "Entity not found: {}:{}",
            key.entity_type, key.persist_id
        )))
    }

    async fn apply_upsert(
        &mut self,
        mut managed: RuntimeStoredEntity,
        reason: impl Into<String>,
        command: Option<RuntimeCommandInvocation>,
    ) -> Result<()> {
        managed.state.metadata.persisted = true;
        managed.resident = true;

        let projection_undo = self.apply_projection_upsert(&managed.state)?;

        let append_result = self
            .append_record(RuntimeJournalOp::Upsert {
                entity: managed.clone(),
                reason: reason.into(),
                command,
                envelope: None,
                outbox: Vec::new(),
                idempotency_scope_key: None,
            })
            .await;
        if let Err(err) = append_result {
            self.rollback_projection_undo(projection_undo);
            return Err(err);
        }

        let key = RuntimeEntityKey::from_state(&managed.state);
        self.cold_entities.remove(&key);
        self.hot_entities.insert(key, managed);

        self.maybe_snapshot_and_compact().await
    }

    async fn append_record(&mut self, op: RuntimeJournalOp) -> Result<()> {
        let seq = self.seq_next;
        self.seq_next = self.seq_next.saturating_add(1);

        let record = RuntimeJournalRecord {
            seq,
            ts_unix_ms: Utc::now().timestamp_millis(),
            op,
        };

        let line = serde_json::to_string(&record).map_err(|err| {
            DbError::ExecutionError(format!("serialize runtime journal: {}", err))
        })?;

        let journal_path = self.journal_path();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        file.write_all(line.as_bytes())
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        file.write_all(b"\n")
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        file.flush()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let now_ms = Utc::now().timestamp_millis();
        match self.policy.durability {
            RuntimeDurabilityMode::Strict => {
                file.sync_data()
                    .await
                    .map_err(|err| DbError::IoError(err.to_string()))?;
                self.last_sync_unix_ms = now_ms;
            }
            RuntimeDurabilityMode::Eventual { sync_interval_ms } => {
                if now_ms - self.last_sync_unix_ms >= sync_interval_ms as i64 {
                    file.sync_data()
                        .await
                        .map_err(|err| DbError::IoError(err.to_string()))?;
                    self.last_sync_unix_ms = now_ms;
                }
            }
        }

        self.ship_journal_line_to_replicas(&line).await;

        self.ops_since_snapshot = self.ops_since_snapshot.saturating_add(1);
        Ok(())
    }

    async fn maybe_snapshot_and_compact(&mut self) -> Result<()> {
        if self.policy.snapshot.background_worker_interval_ms.is_some()
            && self.snapshot_worker_running
        {
            return Ok(());
        }

        let _ = self.run_snapshot_tick().await?;
        Ok(())
    }

    async fn write_snapshot_and_compact(&mut self) -> Result<()> {
        let snapshot = self.export_snapshot();
        self.write_snapshot_file(&snapshot).await?;
        self.compact_journal_at(self.journal_path(), snapshot.last_seq)
            .await?;
        self.ship_snapshot_to_replicas(&snapshot).await;

        self.ops_since_snapshot = 0;
        Ok(())
    }

    async fn read_snapshot_file(&self) -> Result<Option<RuntimeSnapshotFile>> {
        let path = self.snapshot_path();
        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(&path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let snapshot = serde_json::from_slice::<RuntimeSnapshotFile>(&bytes)
            .map_err(|err| DbError::ExecutionError(format!("parse runtime snapshot: {}", err)))?;

        Ok(Some(snapshot))
    }

    async fn write_snapshot_file(&self, snapshot: &RuntimeSnapshotFile) -> Result<()> {
        let path = self.snapshot_path();
        let tmp_path = path.with_extension("tmp");

        let json = serde_json::to_vec_pretty(snapshot).map_err(|err| {
            DbError::ExecutionError(format!("serialize runtime snapshot: {}", err))
        })?;

        fs::write(&tmp_path, json)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        fs::rename(&tmp_path, &path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        Ok(())
    }

    async fn compact_journal_at(&self, journal_path: PathBuf, keep_after_seq: u64) -> Result<()> {
        Self::compact_journal_path(journal_path, keep_after_seq).await
    }

    async fn compact_journal_path(journal_path: PathBuf, keep_after_seq: u64) -> Result<()> {
        if !journal_path.exists() {
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&journal_path)
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
            return Ok(());
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&journal_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        let mut reader = BufReader::new(file).lines();
        let mut retained = Vec::new();

        while let Some(line) = reader
            .next_line()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?
        {
            if line.trim().is_empty() {
                continue;
            }

            let record = serde_json::from_str::<RuntimeJournalRecord>(&line).map_err(|err| {
                DbError::ExecutionError(format!("parse runtime journal record: {}", err))
            })?;

            if record.seq > keep_after_seq {
                let serialized = serde_json::to_string(&record).map_err(|err| {
                    DbError::ExecutionError(format!("serialize runtime journal record: {}", err))
                })?;
                retained.push(serialized);
            }
        }

        let tmp_path = journal_path.with_extension("tmp");
        let mut tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        for line in retained {
            tmp.write_all(line.as_bytes())
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
            tmp.write_all(b"\n")
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        tmp.flush()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        fs::rename(&tmp_path, &journal_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        Ok(())
    }

    async fn ship_journal_line_to_replicas(&self, line: &str) {
        if self.replica_targets.is_empty() {
            return;
        }

        match self.policy.replication.mode {
            RuntimeReplicationMode::Sync => {
                for replica in &self.replica_targets {
                    if let Err(err) = Self::append_line_to_journal_path(
                        replica.journal_file.clone(),
                        line.to_string(),
                        true,
                    )
                    .await
                    {
                        self.record_replication_error(
                            &format!("sync journal shipping to {}", replica.root_dir.display()),
                            &err,
                        );
                    }
                }
            }
            RuntimeReplicationMode::AsyncBestEffort => {
                let line = line.to_string();
                for replica in self.replica_targets.clone() {
                    let failures = self.replication_failures.clone();
                    let target_path = replica.journal_file.clone();
                    let target_root = replica.root_dir.clone();
                    let line_copy = line.clone();
                    tokio::spawn(async move {
                        if let Err(err) = PersistEntityRuntime::append_line_to_journal_path(
                            target_path,
                            line_copy,
                            false,
                        )
                        .await
                        {
                            failures.fetch_add(1, AtomicOrdering::Relaxed);
                            eprintln!(
                                "runtime replication (async journal) failed for {}: {}",
                                target_root.display(),
                                err
                            );
                        }
                    });
                }
            }
        }
    }

    async fn ship_snapshot_to_replicas(&self, snapshot: &RuntimeSnapshotFile) {
        if self.replica_targets.is_empty() {
            return;
        }

        let snapshot_bytes = match serde_json::to_vec_pretty(snapshot) {
            Ok(bytes) => bytes,
            Err(err) => {
                self.record_replication_error(
                    "serialize snapshot for replication",
                    &DbError::ExecutionError(err.to_string()),
                );
                return;
            }
        };

        match self.policy.replication.mode {
            RuntimeReplicationMode::Sync => {
                for replica in &self.replica_targets {
                    let write_result = Self::write_snapshot_path(
                        replica.snapshot_file.clone(),
                        snapshot_bytes.clone(),
                    )
                    .await;
                    if let Err(err) = write_result {
                        self.record_replication_error(
                            &format!("sync snapshot shipping to {}", replica.root_dir.display()),
                            &err,
                        );
                        continue;
                    }

                    if let Err(err) =
                        Self::compact_journal_path(replica.journal_file.clone(), snapshot.last_seq)
                            .await
                    {
                        self.record_replication_error(
                            &format!(
                                "sync journal compaction on replica {}",
                                replica.root_dir.display()
                            ),
                            &err,
                        );
                    }
                }
            }
            RuntimeReplicationMode::AsyncBestEffort => {
                for replica in self.replica_targets.clone() {
                    let failures = self.replication_failures.clone();
                    let bytes = snapshot_bytes.clone();
                    let snapshot_path = replica.snapshot_file.clone();
                    let replica_root = replica.root_dir.clone();
                    tokio::spawn(async move {
                        if let Err(err) =
                            PersistEntityRuntime::write_snapshot_path(snapshot_path, bytes).await
                        {
                            failures.fetch_add(1, AtomicOrdering::Relaxed);
                            eprintln!(
                                "runtime replication (async snapshot) failed for {}: {}",
                                replica_root.display(),
                                err
                            );
                        }
                    });
                }
            }
        }
    }

    async fn append_line_to_journal_path(
        journal_path: PathBuf,
        line: String,
        force_sync: bool,
    ) -> Result<()> {
        if let Some(parent) = journal_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        file.write_all(line.as_bytes())
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        file.write_all(b"\n")
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        file.flush()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        if force_sync {
            file.sync_data()
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        Ok(())
    }

    async fn write_snapshot_path(snapshot_path: PathBuf, bytes: Vec<u8>) -> Result<()> {
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|err| DbError::IoError(err.to_string()))?;
        }

        let tmp_path = snapshot_path.with_extension("tmp");
        fs::write(&tmp_path, bytes)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        fs::rename(&tmp_path, &snapshot_path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        Ok(())
    }

    fn record_replication_error(&self, context: &str, err: &DbError) {
        self.replication_failures
            .fetch_add(1, AtomicOrdering::Relaxed);
        eprintln!("runtime replication error ({context}): {err}");
    }

    fn record_snapshot_worker_error(&self, err: &DbError) {
        self.snapshot_worker_errors
            .fetch_add(1, AtomicOrdering::Relaxed);
        eprintln!("runtime snapshot worker error: {err}");
    }

    async fn read_journal_records(
        &self,
        greater_than_seq: u64,
    ) -> Result<Vec<RuntimeJournalRecord>> {
        let path = self.journal_path();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;

        let mut records = Vec::new();
        let mut lines = BufReader::new(file).lines();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?
        {
            if line.trim().is_empty() {
                continue;
            }

            let record = serde_json::from_str::<RuntimeJournalRecord>(&line).map_err(|err| {
                DbError::ExecutionError(format!("parse runtime journal record: {}", err))
            })?;

            if record.seq > greater_than_seq {
                records.push(record);
            }
        }

        records.sort_by(|a, b| a.seq.cmp(&b.seq));
        Ok(records)
    }

    async fn journal_size_bytes(&self) -> Result<u64> {
        let path = self.journal_path();
        if !path.exists() {
            return Ok(0);
        }

        let metadata = fs::metadata(&path)
            .await
            .map_err(|err| DbError::IoError(err.to_string()))?;
        Ok(metadata.len())
    }

    async fn acquire_inflight_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        let timeout_ms = self.policy.backpressure.acquire_timeout_ms;
        let fut = self.inflight.clone().acquire_owned();

        timeout(TokioDuration::from_millis(timeout_ms), fut)
            .await
            .map_err(|_| {
                DbError::ExecutionError(format!(
                    "Backpressure: could not acquire operation slot within {}ms",
                    timeout_ms
                ))
            })?
            .map_err(|_| DbError::ExecutionError("Backpressure semaphore closed".to_string()))
    }

    fn retry_backoff_ms(&self, attempt: u32) -> u64 {
        let base = self.policy.retry.initial_backoff_ms.max(1);
        let max = self.policy.retry.max_backoff_ms.max(base);
        let factor = 2u64.saturating_pow(attempt.saturating_sub(1));
        base.saturating_mul(factor).min(max)
    }

    fn update_idempotency_outbox_status(&mut self, updated: &RuntimeOutboxRecord) {
        for receipt in self.idempotency_index.values_mut() {
            for outbox in &mut receipt.outbox {
                if outbox.outbox_id == updated.outbox_id {
                    outbox.status = updated.status.clone();
                }
            }
        }
    }
}

fn invoke_registered_handler(
    handler: &RegisteredDeterministicCommandHandler,
    state: &mut PersistState,
    envelope: &RuntimeCommandEnvelope,
    context: &RuntimeDeterministicContext,
) -> Result<Vec<RuntimeSideEffectSpec>> {
    let apply = || match handler {
        RegisteredDeterministicCommandHandler::Legacy(handler) => {
            handler(state, &envelope.payload_json).map(|_| Vec::new())
        }
        RegisteredDeterministicCommandHandler::Envelope(handler) => handler(state, envelope),
        RegisteredDeterministicCommandHandler::Context(handler) => {
            handler(state, &envelope.payload_json, context)
        }
    };

    catch_unwind(AssertUnwindSafe(apply)).map_err(|_| {
        DbError::ExecutionError(format!(
            "Deterministic handler panicked for '{}::{}'",
            envelope.entity_type, envelope.command_name
        ))
    })?
}

fn validate_command_envelope(envelope: &RuntimeCommandEnvelope) -> Result<()> {
    if envelope.entity_type.trim().is_empty() {
        return Err(DbError::ExecutionError(
            "Command envelope entity_type must not be empty".to_string(),
        ));
    }
    if envelope.entity_id.trim().is_empty() {
        return Err(DbError::ExecutionError(
            "Command envelope entity_id must not be empty".to_string(),
        ));
    }
    if envelope.command_name.trim().is_empty() {
        return Err(DbError::ExecutionError(
            "Command envelope command_name must not be empty".to_string(),
        ));
    }
    if envelope.payload_version == 0 {
        return Err(DbError::ExecutionError(
            "Command envelope payload_version must be >= 1".to_string(),
        ));
    }
    Ok(())
}

fn build_idempotency_scope_key(envelope: &RuntimeCommandEnvelope) -> Option<String> {
    envelope.idempotency_key.as_ref().map(|key| {
        format!(
            "{}:{}:{}:{}",
            envelope.entity_type, envelope.entity_id, envelope.command_name, key
        )
    })
}

fn build_projection_row(
    contract: &RuntimeProjectionContract,
    state: &PersistState,
) -> Result<RuntimeProjectionRow> {
    let fields = state.fields_object()?;
    let mut values = serde_json::Map::with_capacity(contract.fields.len());

    for projection_field in &contract.fields {
        let value = fields
            .get(projection_field.state_field.as_str())
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Projection field '{}.{}' is missing in state '{}'",
                    contract.entity_type, projection_field.state_field, state.persist_id
                ))
            })?;

        if !payload_matches_type(&value, &projection_field.payload_type) {
            return Err(DbError::ExecutionError(format!(
                "Projection field '{}.{}' type mismatch: expected {:?}, got {}",
                contract.entity_type,
                projection_field.state_field,
                projection_field.payload_type,
                json_type_name(&value)
            )));
        }

        values.insert(projection_field.column_name.clone(), value);
    }

    Ok(RuntimeProjectionRow {
        entity_id: state.persist_id.clone(),
        values,
        updated_at: state.metadata.updated_at,
    })
}

fn projection_index_key(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

fn payload_matches_type(value: &serde_json::Value, payload_type: &RuntimePayloadType) -> bool {
    match payload_type {
        RuntimePayloadType::Null => value.is_null(),
        RuntimePayloadType::Boolean => value.is_boolean(),
        RuntimePayloadType::Integer => value.as_i64().is_some(),
        RuntimePayloadType::Float => value.as_f64().is_some(),
        RuntimePayloadType::Text => value.as_str().is_some(),
        RuntimePayloadType::Array => value.as_array().is_some(),
        RuntimePayloadType::Object => value.as_object().is_some(),
    }
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    if value.is_null() {
        return "null";
    }
    if value.is_boolean() {
        return "boolean";
    }
    if value.as_i64().is_some() {
        return "integer";
    }
    if value.as_f64().is_some() {
        return "float";
    }
    if value.is_string() {
        return "string";
    }
    if value.is_array() {
        return "array";
    }
    if value.is_object() {
        return "object";
    }
    "unknown"
}

fn normalize_runtime_policy(mut policy: RuntimeOperationalPolicy) -> RuntimeOperationalPolicy {
    match policy.consistency {
        RuntimeConsistencyMode::Strong => {
            policy.durability = RuntimeDurabilityMode::Strict;
            policy.replication.mode = RuntimeReplicationMode::Sync;
        }
        RuntimeConsistencyMode::LocalDurable => {
            policy.durability = RuntimeDurabilityMode::Strict;
        }
        RuntimeConsistencyMode::Eventual => {
            policy.durability = RuntimeDurabilityMode::Eventual {
                sync_interval_ms: 250,
            };
            policy.replication.mode = RuntimeReplicationMode::AsyncBestEffort;
        }
    }
    policy
}

fn runtime_replica_targets(root_dir: &Path, replica_roots: &[PathBuf]) -> Vec<RuntimePaths> {
    let mut result = Vec::new();
    for replica_root in replica_roots {
        if replica_root == root_dir {
            continue;
        }
        result.push(RuntimePaths {
            root_dir: replica_root.clone(),
            snapshot_file: replica_root.join(RUNTIME_SNAPSHOT_FILE),
            journal_file: replica_root.join(RUNTIME_JOURNAL_FILE),
        });
    }
    result
}

pub struct RuntimeSnapshotWorker {
    runtime: Arc<Mutex<PersistEntityRuntime>>,
    stop_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl RuntimeSnapshotWorker {
    pub async fn stop(mut self) -> Result<()> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }

        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .await
                .map_err(|err| DbError::ExecutionError(format!("snapshot worker join: {}", err)))?;
        }

        let mut runtime = self.runtime.lock().await;
        runtime.snapshot_worker_running = false;
        Ok(())
    }
}

impl Drop for RuntimeSnapshotWorker {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort();
        }
    }
}

pub async fn spawn_runtime_snapshot_worker(
    runtime: Arc<Mutex<PersistEntityRuntime>>,
) -> Result<RuntimeSnapshotWorker> {
    let interval_ms = {
        let mut guard = runtime.lock().await;
        let interval = guard
            .policy
            .snapshot
            .background_worker_interval_ms
            .ok_or_else(|| {
                DbError::ExecutionError(
                    "snapshot.background_worker_interval_ms must be configured to start worker"
                        .to_string(),
                )
            })?;
        guard.snapshot_worker_running = true;
        interval.max(10)
    };

    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let runtime_for_worker = runtime.clone();

    let join_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    break;
                }
                _ = sleep(TokioDuration::from_millis(interval_ms)) => {
                    let mut guard = runtime_for_worker.lock().await;
                    if let Err(err) = guard.run_snapshot_tick().await {
                        guard.record_snapshot_worker_error(&err);
                    }
                }
            }
        }
    });

    Ok(RuntimeSnapshotWorker {
        runtime,
        stop_tx: Some(stop_tx),
        join_handle: Some(join_handle),
    })
}

pub fn runtime_snapshot_compat_check(
    snapshot_path: impl AsRef<Path>,
    current_version: u32,
) -> Result<RuntimeCompatReport> {
    let path = snapshot_path.as_ref();
    let bytes = std::fs::read(path).map_err(|err| DbError::IoError(err.to_string()))?;

    let snapshot = serde_json::from_slice::<RuntimeSnapshotFile>(&bytes).map_err(|err| {
        DbError::ExecutionError(format!(
            "Failed to parse runtime snapshot file '{}': {}",
            path.display(),
            err
        ))
    })?;

    let mut incompatible = Vec::new();
    for entity in snapshot.entities {
        if entity.state.metadata.schema_version > current_version {
            incompatible.push(RuntimeCompatIssue {
                entity_type: entity.state.type_name,
                persist_id: entity.state.persist_id,
                schema_version: entity.state.metadata.schema_version,
                reason: format!(
                    "Entity schema version {} is newer than runtime {}",
                    entity.state.metadata.schema_version, current_version
                ),
            });
        }
    }

    Ok(RuntimeCompatReport {
        snapshot_path: path.to_string_lossy().to_string(),
        current_version,
        compatible: incompatible.is_empty(),
        issues: incompatible,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCompatIssue {
    pub entity_type: String,
    pub persist_id: String,
    pub schema_version: u32,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCompatReport {
    pub snapshot_path: String,
    pub current_version: u32,
    pub compatible: bool,
    pub issues: Vec<RuntimeCompatIssue>,
}
