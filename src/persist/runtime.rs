use super::{PersistMetadata, PersistState, new_persist_id};
use crate::core::{DbError, Result, Value};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration as TokioDuration, sleep, timeout};

const RUNTIME_SNAPSHOT_FILE: &str = "runtime_snapshot.json";
const RUNTIME_JOURNAL_FILE: &str = "runtime_journal.log";
const RUNTIME_FORMAT_VERSION: u16 = 1;

pub type DeterministicCommandHandler =
    Arc<dyn Fn(&mut PersistState, &serde_json::Value) -> Result<()> + Send + Sync>;
pub type RuntimeClosureHandler =
    Arc<dyn Fn(&mut PersistState, Vec<Value>) -> Result<Value> + Send + Sync>;

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeOperationalPolicy {
    pub durability: RuntimeDurabilityMode,
    pub retry: RuntimeRetryPolicy,
    pub backpressure: RuntimeBackpressurePolicy,
    pub snapshot: RuntimeSnapshotPolicy,
    pub replication: RuntimeReplicationPolicy,
    pub lifecycle: RuntimeLifecyclePolicy,
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
    },
    Delete {
        key: RuntimeEntityKey,
        reason: String,
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
    pub replication_targets: usize,
    pub replication_failures: u64,
    pub snapshot_worker_running: bool,
    pub snapshot_worker_errors: u64,
    pub next_seq: u64,
    pub ops_since_snapshot: usize,
}

#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub root_dir: PathBuf,
    pub snapshot_file: PathBuf,
    pub journal_file: PathBuf,
}

#[derive(Clone)]
struct RegisteredDeterministicCommand {
    handler: DeterministicCommandHandler,
    payload_schema: Option<RuntimeCommandPayloadSchema>,
}

pub struct PersistEntityRuntime {
    root_dir: PathBuf,
    policy: RuntimeOperationalPolicy,
    hot_entities: HashMap<RuntimeEntityKey, RuntimeStoredEntity>,
    cold_entities: HashMap<RuntimeEntityKey, RuntimeStoredEntity>,
    deterministic_registry: HashMap<String, HashMap<String, RegisteredDeterministicCommand>>,
    runtime_closure_registry: HashMap<String, HashMap<String, RuntimeClosureHandler>>,
    seq_next: u64,
    ops_since_snapshot: usize,
    last_sync_unix_ms: i64,
    inflight: Arc<Semaphore>,
    resurrected_since_last_report: usize,
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
            seq_next: 1,
            ops_since_snapshot: 0,
            last_sync_unix_ms: Utc::now().timestamp_millis(),
            inflight: Arc::new(Semaphore::new(max_inflight)),
            resurrected_since_last_report: 0,
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

        RuntimeStats {
            hot_entities: self.hot_entities.len(),
            cold_entities: self.cold_entities.len(),
            registered_types: self
                .deterministic_registry
                .keys()
                .chain(self.runtime_closure_registry.keys())
                .collect::<HashSet<_>>()
                .len(),
            registered_deterministic_commands: command_count,
            deterministic_commands_with_payload_contracts: command_schema_count,
            registered_runtime_closures: closure_count,
            replication_targets: self.replica_targets.len(),
            replication_failures: self.replication_failures.load(AtomicOrdering::Relaxed),
            snapshot_worker_running: self.snapshot_worker_running,
            snapshot_worker_errors: self.snapshot_worker_errors.load(AtomicOrdering::Relaxed),
            next_seq: self.seq_next,
            ops_since_snapshot: self.ops_since_snapshot,
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
                handler,
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
                handler,
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

        let key = RuntimeEntityKey::new(entity_type, persist_id);
        self.hot_entities.remove(&key);
        self.cold_entities.remove(&key);

        self.append_record(RuntimeJournalOp::Delete {
            key,
            reason: reason.into(),
        })
        .await?;

        self.maybe_snapshot_and_compact().await?;
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
            self.resurrected_since_last_report =
                self.resurrected_since_last_report.saturating_add(1);
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

    pub async fn apply_deterministic_command(
        &mut self,
        entity_type: &str,
        persist_id: &str,
        command: &str,
        payload: serde_json::Value,
    ) -> Result<PersistState> {
        let permit = self.acquire_inflight_permit().await?;
        let _keep_permit_until_drop = permit;

        let command_handler = self
            .deterministic_registry
            .get(entity_type)
            .and_then(|commands| commands.get(command))
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Deterministic command '{}' is not registered for entity type '{}'",
                    command, entity_type
                ))
            })?;
        if let Some(payload_schema) = &command_handler.payload_schema {
            payload_schema.validate(&payload).map_err(|err| {
                DbError::ExecutionError(format!(
                    "Payload validation for command '{}': {}",
                    command, err
                ))
            })?;
        }

        let key = RuntimeEntityKey::new(entity_type, persist_id);
        let base = self.take_entity_for_mutation(&key)?;

        let max_attempts = self.policy.retry.max_attempts.max(1);
        let mut last_err: Option<DbError> = None;

        for attempt in 1..=max_attempts {
            let mut working = base.clone();
            let result = (command_handler.handler)(&mut working.state, &payload);
            match result {
                Ok(()) => {
                    working.state.metadata.persisted = true;
                    working.touch();

                    let invocation = RuntimeCommandInvocation {
                        command: command.to_string(),
                        payload: payload.clone(),
                    };

                    match self
                        .append_record(RuntimeJournalOp::Upsert {
                            entity: working.clone(),
                            reason: "command".to_string(),
                            command: Some(invocation),
                        })
                        .await
                    {
                        Ok(()) => {
                            self.hot_entities.insert(key.clone(), working.clone());
                            self.maybe_snapshot_and_compact().await?;
                            return Ok(working.state);
                        }
                        Err(err) => {
                            last_err = Some(err);
                        }
                    }
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }

            if attempt < max_attempts {
                sleep(TokioDuration::from_millis(self.retry_backoff_ms(attempt))).await;
            }
        }

        self.hot_entities.insert(key, base);
        Err(last_err.unwrap_or_else(|| {
            DbError::ExecutionError("Failed to apply deterministic command".to_string())
        }))
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
        let result = runtime_handler(&mut entity.state, args)?;

        // Runtime closures are intentionally not deterministic/serializable.
        // We keep them available for local runtime behavior, and persist only
        // the final state snapshot as an upsert event.
        entity.touch();
        self.apply_upsert(entity, "runtime_closure", None).await?;

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

            if old_enough && eligible_by_touch {
                to_gc.push(key.clone());
            }
        }

        for key in to_gc {
            if self.cold_entities.remove(&key).is_some() {
                gc_deleted = gc_deleted.saturating_add(1);
                self.append_record(RuntimeJournalOp::Delete {
                    key,
                    reason: "lifecycle_gc".to_string(),
                })
                .await?;
            }
        }

        self.maybe_snapshot_and_compact().await?;

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

        RuntimeSnapshotFile {
            format_version: RUNTIME_FORMAT_VERSION,
            created_at_unix_ms: Utc::now().timestamp_millis(),
            last_seq: self.seq_next.saturating_sub(1),
            entities,
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
            RuntimeJournalOp::Upsert { entity, .. } => {
                let key = RuntimeEntityKey::from_state(&entity.state);
                if entity.resident {
                    self.cold_entities.remove(&key);
                    self.hot_entities.insert(key, entity);
                } else {
                    self.hot_entities.remove(&key);
                    self.cold_entities.insert(key, entity);
                }
            }
            RuntimeJournalOp::Delete { key, .. } => {
                self.hot_entities.remove(&key);
                self.cold_entities.remove(&key);
            }
        }
    }

    fn take_entity_for_mutation(&mut self, key: &RuntimeEntityKey) -> Result<RuntimeStoredEntity> {
        if let Some(entity) = self.hot_entities.remove(key) {
            return Ok(entity);
        }

        if let Some(mut entity) = self.cold_entities.remove(key) {
            entity.resident = true;
            self.resurrected_since_last_report =
                self.resurrected_since_last_report.saturating_add(1);
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

        self.append_record(RuntimeJournalOp::Upsert {
            entity: managed.clone(),
            reason: reason.into(),
            command,
        })
        .await?;

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
