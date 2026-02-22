/// Runtime statistics for a managed vector.
#[derive(Debug, Clone)]
pub struct ManagedPersistVecStats {
    /// Logical name of the vector.
    pub vec_name: String,
    /// Total number of items managed.
    pub item_count: usize,
    /// Configured snapshot interval.
    pub snapshot_every_ops: usize,
    /// Operations performed since the last snapshot.
    pub ops_since_snapshot: usize,
    /// File path where the snapshot is stored.
    pub snapshot_path: String,
    /// Active replication mode description.
    pub replication_mode: String,
    /// Number of replication targets.
    pub replication_targets: usize,
    /// Total count of replication failures encountered.
    pub replication_failures: u64,
    /// Timestamp of the last successful snapshot (RFC3339).
    pub last_snapshot_at: Option<String>,
}

/// A page of results from an aggregate query.
#[derive(Debug, Clone)]
pub struct PersistAggregatePage<T> {
    /// The items in the current page.
    pub items: Vec<T>,
    /// The current page number (1-based).
    pub page: u32,
    /// The number of items per page.
    pub per_page: u32,
    /// Total number of items matching the query.
    pub total: u64,
    /// Total number of pages available.
    pub total_pages: u32,
}

/// A persistence collection managed by `PersistApp`.
///
/// Wraps a `PersistCollection` with runtime services:
/// - Session binding
/// - Automatic snapshotting
/// - Metric collection
/// - Replication hooks
pub struct ManagedPersistVec<V: PersistCollection> {
    pub(crate) name: String,
    pub(crate) collection: V,
    pub(crate) session: PersistSession,
    pub(crate) snapshot_path: PathBuf,
    pub(crate) snapshot_every_ops: usize,
    pub(crate) ops_since_snapshot: usize,
    pub(crate) replication: PersistReplicationPolicy,
    pub(crate) replication_failures: u64,
    pub(crate) last_snapshot_at: Option<String>,
}

/// A wrapper that adds aggregate-level query capabilities to a managed collection.
/// A high-level wrapper around `ManagedPersistVec` that provides domain-specific access patterns.
///
/// This struct is the primary entry point for application logic to interact with persisted collections.
/// It aggregates functionality for:
/// - Basic CRUD operations
/// - Indexed queries
/// - Command execution (optimistic locking)
/// - Audit logging
/// - Workflow execution
pub struct PersistAggregateStore<V: PersistCollection> {
    pub(crate) managed: ManagedPersistVec<V>,
}

/// An autonomous store that manages both data and its audit history.
///
/// This is the primary high-level construct for domain-driven persistence,
/// handling version conflicts and audit logging automatically.
pub struct PersistAutonomousAggregate<V: PersistCollection> {
    pub(crate) aggregate: PersistAggregateStore<V>,
    pub(crate) audits: PersistAggregateStore<PersistAuditRecordVec>,
    pub(crate) rest_idempotency: PersistAggregateStore<PersistRestIdempotencyRecordVec>,
    pub(crate) conflict_retry: PersistConflictRetryPolicy,
}

/// Adapter for legacy compatibility using `PersistSession` explicitly.
pub struct LegacyPersistVecAdapter<V: PersistCollection> {
    pub(crate) managed: ManagedPersistVec<V>,
}

/// Alias for `PersistAutonomousAggregate`, representing a typical domain store.
pub type PersistDomainStore<V> = PersistAutonomousAggregate<V>;

/// Thread-safe handle to a domain store with `&self` async methods.
///
/// This is the preferred app-facing shape when business code should avoid
/// explicit `Arc<Mutex<...>>` orchestration around persistence collections.
pub struct PersistDomainHandle<V: PersistCollection> {
    pub(crate) inner: Arc<Mutex<PersistDomainStore<V>>>,
}

impl<V: PersistCollection> Clone for PersistDomainHandle<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Source-model record returned by autonomous derive handle operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistAutonomousRecord<M> {
    /// Stable persistence id of the aggregate.
    pub persist_id: String,
    /// Domain model payload.
    pub model: M,
    /// Current persisted version.
    pub version: i64,
}

/// High-level autonomous handle for source-model-first development.
///
/// Obtained via `PersistApp::open_autonomous_model::<Model>(...)`.
pub struct PersistAutonomousModelHandle<M: PersistAutonomousModel> {
    pub(crate) inner: PersistDomainHandle<M::Collection>,
    pub(crate) marker: PhantomData<M>,
}

/// Result of executing a REST command with automatic idempotency handling.
#[derive(Debug, Clone)]
pub enum PersistIdempotentCommandResult<T> {
    /// Command was executed normally in this request.
    Applied(T),
    /// Command was already executed for this idempotency key; response is replayed.
    Replayed {
        status_code: u16,
        body: serde_json::Value,
    },
}

impl<M: PersistAutonomousModel> Clone for PersistAutonomousModelHandle<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            marker: PhantomData,
        }
    }
}
