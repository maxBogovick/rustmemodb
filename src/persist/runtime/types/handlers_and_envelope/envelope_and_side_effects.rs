/// A complete message envelope for a command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeCommandEnvelope {
    /// Unique ID of the envelope.
    pub envelope_id: Uuid,
    /// Type of the target entity.
    pub entity_type: String,
    /// ID of the target entity.
    pub entity_id: String,
    /// ID of the command or event that caused this command (traceability).
    pub causation_id: Option<Uuid>,
    /// ID used to correlate related commands/events (e.g., a session ID).
    pub correlation_id: Option<Uuid>,
    /// Optional expected version of the entity state for optimistic concurrency control.
    pub expected_version: Option<u64>,
    /// Name of the command to execute.
    pub command_name: String,
    /// Use-case specific payload data.
    pub payload_json: serde_json::Value,
    /// Version of the payload schema.
    pub payload_version: u32,
    /// Timestamp when the envelope was created.
    pub created_at: DateTime<Utc>,
    /// Optional key for idempotency; duplicates will be ignored.
    pub idempotency_key: Option<String>,
    /// Optional ID of the actor (user or system) initiating the command.
    pub actor_id: Option<String>,
}

impl RuntimeCommandEnvelope {
    /// Creates a new command envelope with a unique ID and current timestamp.
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

    /// Sets the expected version for optimistic concurrency control.
    pub fn with_expected_version(mut self, expected_version: u64) -> Self {
        self.expected_version = Some(expected_version);
        self
    }

    /// Sets an idempotency key.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Sets the actor ID.
    pub fn with_actor_id(mut self, actor_id: impl Into<String>) -> Self {
        self.actor_id = Some(actor_id.into());
        self
    }

    /// Sets the correlation ID.
    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Sets the causation ID.
    pub fn with_causation_id(mut self, causation_id: Uuid) -> Self {
        self.causation_id = Some(causation_id);
        self
    }
}

/// Context passed to deterministic command handlers.
///
/// Provides access to metadata about the command invocation without exposing mutable state
/// or non-deterministic capabilities.
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

    /// Generates a deterministic UUID based on the envelope ID and a namespace.
    pub fn deterministic_uuid(&self, namespace: &str) -> Uuid {
        Uuid::new_v5(&self.envelope_id, namespace.as_bytes())
    }
}

/// Specification for a side effect to be executed after a command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSideEffectSpec {
    pub effect_type: String,
    pub payload_json: serde_json::Value,
}

/// Status of an outbox record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeOutboxStatus {
    /// The side effect is waiting to be dispatched.
    Pending,
    /// The side effect has been successfully dispatched.
    Dispatched,
}

/// A persistent record of a side effect that needs to be guaranteed (at-least-once).
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

/// Receipt stored to ensure idempotency of commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeIdempotencyReceipt {
    pub envelope_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub command_name: String,
    pub state: PersistState,
    pub outbox: Vec<RuntimeOutboxRecord>,
}

/// The result of applying a command envelope to an entity.
#[derive(Debug, Clone)]
pub struct RuntimeEnvelopeApplyResult {
    pub envelope_id: Uuid,
    pub state: PersistState,
    pub idempotent_replay: bool,
    pub outbox: Vec<RuntimeOutboxRecord>,
}

/// Descriptor for migrating command payloads between versions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeCommandMigrationDescriptor {
    pub from_command: String,
    pub from_payload_version: u32,
    pub to_command: String,
    pub to_payload_version: u32,
}
