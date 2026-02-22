/// Durability mode for persistence operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeDurabilityMode {
    /// Commits flushed to disk before acknowledging success (fsync).
    Strict,
    /// Commits acknowledged in memory, flushed asynchronously every `sync_interval_ms`.
    Eventual { sync_interval_ms: u64 },
}

impl Default for RuntimeDurabilityMode {
    fn default() -> Self {
        Self::Strict
    }
}

/// Configuration for retry behavior on transient failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRetryPolicy {
    /// Maximum number of retry attempts.
    pub max_attempts: u32,
    /// Initial backoff duration in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds.
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

/// Configuration for handling system load and backpressure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeBackpressurePolicy {
    /// Maximum number of concurrent in-flight operations.
    pub max_inflight: usize,
    /// Timeout in milliseconds to wait for a slot when the system is overloaded.
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

/// policies controlling when snapshots are taken and compacted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshotPolicy {
    /// Create a new snapshot after this many operations.
    pub snapshot_every_ops: usize,
    /// Compact the journal if it exceeds this size in bytes.
    pub compact_if_journal_exceeds_bytes: u64,
    /// Optional interval in milliseconds for a background snapshot worker.
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

/// Mode for replicating data to other locations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeReplicationMode {
    /// Replicate synchronously; failure to replicate fails the operation.
    Sync,
    /// Replicate asynchronously on a best-effort basis.
    AsyncBestEffort,
}

impl Default for RuntimeReplicationMode {
    fn default() -> Self {
        Self::Sync
    }
}

/// Configuration for data replication.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeReplicationPolicy {
    /// The replication mode (Sync or Async).
    pub mode: RuntimeReplicationMode,
    /// List of root paths to replicate data to.
    pub replica_roots: Vec<PathBuf>,
}

/// Configuration for managing the lifecycle of in-memory entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeLifecyclePolicy {
    /// Time in milliseconds after which an untouched entity is passivated (removed from memory).
    pub passivate_after_ms: u64,
    /// Time in milliseconds after which to run garbage collection.
    pub gc_after_ms: u64,
    /// Maximum number of hot (active) entities to keep in memory.
    pub max_hot_objects: usize,
    /// If true, only GC entities that have never been touched.
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

/// Configuration for handling tombstones (deleted entities).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeTombstonePolicy {
    /// Time-to-live for tombstones in milliseconds.
    pub ttl_ms: u64,
    /// Whether to retain tombstones during lifecycle garbage collection.
    pub retain_for_lifecycle_gc: bool,
}

impl Default for RuntimeTombstonePolicy {
    fn default() -> Self {
        Self {
            ttl_ms: 5 * 60_000,
            retain_for_lifecycle_gc: true,
        }
    }
}

/// Policy for enforcing determinism in command execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeDeterminismPolicy {
    /// Allow non-deterministic behavior.
    Permissive,
    /// Enforce strict determinism based on context constraints.
    StrictContextOnly,
}

impl Default for RuntimeDeterminismPolicy {
    fn default() -> Self {
        Self::Permissive
    }
}

/// Consistency mode for read/write operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeConsistencyMode {
    /// Strong consistency (linearizability).
    Strong,
    /// Strong consistency reading from local durable state (may be stale if replication lags).
    LocalDurable,
    /// Eventual consistency.
    Eventual,
}

impl Default for RuntimeConsistencyMode {
    fn default() -> Self {
        Self::LocalDurable
    }
}

/// Comprehensive operational policy for the persistence runtime.
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
    #[serde(default)]
    pub tombstone: RuntimeTombstonePolicy,
    pub determinism: RuntimeDeterminismPolicy,
}
