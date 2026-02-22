/// Report of lifecycle operations performed during a maintenance cycle.
#[derive(Debug, Clone)]
pub struct RuntimeLifecycleReport {
    /// Number of entities passivated (unloaded from memory).
    pub passivated: usize,
    /// Number of entities resurrected (loaded from disk).
    pub resurrected: usize,
    /// Number of entities garbage collected.
    pub gc_deleted: usize,
    /// Number of tombstones pruned.
    pub tombstones_pruned: usize,
}

/// Comprehensive runtime statistics.
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    /// Number of entities currently in memory (hot).
    pub hot_entities: usize,
    /// Number of entities tracked but not in memory (cold).
    pub cold_entities: usize,
    /// Number of tombstones tracked.
    pub tombstones: usize,
    /// Count of registered entity types.
    pub registered_types: usize,
    /// Count of registered deterministic command handlers.
    pub registered_deterministic_commands: usize,
    /// Count of registered command migration rules.
    pub registered_command_migrations: usize,
    /// Count of deterministic commands with payload schema contracts.
    pub deterministic_commands_with_payload_contracts: usize,
    /// Count of registered runtime closures (dynamic logic).
    pub registered_runtime_closures: usize,
    /// Count of registered active projections.
    pub registered_projections: usize,
    /// Total number of rows across all projection tables.
    pub projection_rows: usize,
    /// Total number of indexed columns across all projection tables.
    pub projection_index_columns: usize,
    /// Number of entities waiting for projection updates.
    pub projection_lag_entities: usize,
    /// Number of configured replication targets.
    pub replication_targets: usize,
    /// Total count of replication failures.
    pub replication_failures: u64,
    /// Estimated lag in durability (time since last sync).
    pub durability_lag_ms: u64,
    /// Whether the background snapshot worker is currently running.
    pub snapshot_worker_running: bool,
    /// Total count of errors encountered by the snapshot worker.
    pub snapshot_worker_errors: u64,
    /// The next sequence number to be assigned.
    pub next_seq: u64,
    /// Number of operations performed since the last snapshot.
    pub ops_since_snapshot: usize,
    /// Total number of outbox records tracked.
    pub outbox_total: usize,
    /// Number of pending outbox records.
    pub outbox_pending: usize,
    /// Number of idempotency keys tracked.
    pub idempotency_entries: usize,
    /// Number of entities with active mailboxes.
    pub mailbox_entities: usize,
    /// Number of entities currently processing commands.
    pub mailbox_busy_entities: usize,
    /// Total entities passivated over the runtime's lifetime.
    pub lifecycle_passivated_total: u64,
    /// Total entities resurrected over the runtime's lifetime.
    pub lifecycle_resurrected_total: u64,
    /// Total entities garbage collected over the runtime's lifetime.
    pub lifecycle_gc_deleted_total: u64,
    /// Total tombstones pruned over the runtime's lifetime.
    pub tombstones_pruned_total: u64,
    /// Total churn (passivations + resurrections).
    pub lifecycle_churn_total: u64,
}

/// Subset of metrics focused on Service Level Objectives (SLOs).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSloMetrics {
    pub durability_lag_ms: u64,
    pub projection_lag_entities: usize,
    pub lifecycle_churn_total: u64,
    pub outbox_pending: usize,
    pub replication_failures: u64,
    pub mailbox_busy_entities: usize,
}

/// Paths to critical runtime files.
#[derive(Debug, Clone)]
pub struct RuntimePaths {
    pub root_dir: PathBuf,
    pub snapshot_file: PathBuf,
    pub journal_file: PathBuf,
}

#[derive(Clone)]
pub(crate) enum RegisteredDeterministicCommandHandler {
    Legacy(DeterministicCommandHandler),
    Envelope(DeterministicEnvelopeCommandHandler),
    Context(DeterministicContextCommandHandler),
}

#[derive(Clone)]
pub(crate) struct RegisteredDeterministicCommand {
    handler: RegisteredDeterministicCommandHandler,
    payload_schema: Option<RuntimeCommandPayloadSchema>,
}

#[derive(Clone)]
struct RuntimeCommandMigrationRule {
    descriptor: RuntimeCommandMigrationDescriptor,
    transform: RuntimeCommandPayloadMigration,
}

/// The core runtime state struct.
///
/// This struct holds all in-memory state for the `PersistRuntime` actor, including
/// loaded entities, indexes, configuration, and metrics.
pub struct PersistEntityRuntime {
    root_dir: PathBuf,
    policy: RuntimeOperationalPolicy,
    hot_entities: HashMap<RuntimeEntityKey, RuntimeStoredEntity>,
    cold_entities: HashMap<RuntimeEntityKey, RuntimeStoredEntity>,
    tombstones: HashMap<RuntimeEntityKey, RuntimeEntityTombstone>,
    deterministic_registry: HashMap<String, HashMap<String, RegisteredDeterministicCommand>>,
    command_migration_registry: HashMap<String, Vec<RuntimeCommandMigrationRule>>,
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
    tombstones_pruned_total: u64,
    snapshot_worker_running: bool,
    snapshot_worker_errors: Arc<AtomicU64>,
    replica_targets: Vec<RuntimePaths>,
    replication_failures: Arc<AtomicU64>,
}
