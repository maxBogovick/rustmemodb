/// Unique identifier for a stored entity, consisting of its type and ID.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RuntimeEntityKey {
    pub entity_type: String,
    pub persist_id: String,
}

impl RuntimeEntityKey {
    /// Creates a new entity key.
    pub fn new(entity_type: impl Into<String>, persist_id: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            persist_id: persist_id.into(),
        }
    }

    /// Extracts the key from an entity's persistent state.
    fn from_state(state: &PersistState) -> Self {
        Self {
            entity_type: state.type_name.clone(),
            persist_id: state.persist_id.clone(),
        }
    }
}

/// Record of a command invocation, stored for debugging and audit logs.
///
/// This captures the command name and the payload that was executed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCommandInvocation {
    pub command: String,
    pub payload: serde_json::Value,
}

/// Internal representation of an entity managed by the runtime.
///
/// Wraps the user-facing `PersistState` with runtime-specific metadata like
/// access patterns and residency status (hot/cold).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeStoredEntity {
    /// The actual entity data.
    pub state: PersistState,
    /// Timestamp of the last access (read or write).
    pub last_access_at: DateTime<Utc>,
    /// Number of times this entity has been accessed since being loaded.
    pub access_count: u64,
    /// Whether the entity is currently considered "resident" (hot) in memory.
    pub resident: bool,
}

impl RuntimeStoredEntity {
    /// Creates a new stored entity wrapper.
    fn new(state: PersistState, resident: bool) -> Self {
        Self {
            state,
            last_access_at: Utc::now(),
            access_count: 1,
            resident,
        }
    }

    /// Updates access statistics for LRU/eviction policies.
    fn touch(&mut self) {
        self.last_access_at = Utc::now();
        self.access_count = self.access_count.saturating_add(1);
        self.state.metadata.last_touch_at = self.last_access_at;
        self.state.metadata.touch_count = self.state.metadata.touch_count.saturating_add(1);
        self.state.metadata.updated_at = self.last_access_at;
    }
}

/// Marker for a deleted entity (Soft Delete / Tombstone).
///
/// Used to prevent immediate resurrection of deleted entities and to handle
/// distributed deletes in an eventually consistent system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEntityTombstone {
    pub key: RuntimeEntityKey,
    pub reason: String,
    pub deleted_at_unix_ms: i64,
    /// Optional expiry time for the tombstone itself (for physical deletion/GC).
    #[serde(default)]
    pub expires_at_unix_ms: Option<i64>,
}

impl RuntimeEntityTombstone {
    /// Checks if this tombstone has expired and can be physically removed.
    fn is_expired_at(&self, now_unix_ms: i64) -> bool {
        self.expires_at_unix_ms
            .map(|expires_at| now_unix_ms >= expires_at)
            .unwrap_or(false)
    }
}

/// Operations recorded in the runtime journal.
///
/// The journal acts as the Write-Ahead Log (WAL) for the runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuntimeJournalOp {
    /// Update or Insert an entity state.
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
    /// Delete an entity.
    Delete {
        key: RuntimeEntityKey,
        reason: String,
        #[serde(default)]
        expires_at_unix_ms: Option<i64>,
    },
    /// Upsert an outbox record (independent of entity update).
    /// Used for updating side-effect status (e.g. marking as dispatched).
    OutboxUpsert { record: RuntimeOutboxRecord },
}

/// A serialized record in the immutable journal log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeJournalRecord {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// Timestamp of the operation.
    pub ts_unix_ms: i64,
    /// The operation performed.
    pub op: RuntimeJournalOp,
}

/// Structure of the runtime snapshot file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshotFile {
    pub format_version: u16,
    pub created_at_unix_ms: i64,
    pub last_seq: u64,
    pub entities: Vec<RuntimeStoredEntity>,
    #[serde(default)]
    pub tombstones: Vec<RuntimeEntityTombstone>,
    #[serde(default)]
    pub outbox: Vec<RuntimeOutboxRecord>,
    #[serde(default)]
    pub idempotency_index: HashMap<String, RuntimeIdempotencyReceipt>,
}
