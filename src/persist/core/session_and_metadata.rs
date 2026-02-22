/// Represents a persistence session, holding a database connection and an optional transaction context.
#[derive(Clone)]
pub struct PersistSession {
    pub(crate) db: Arc<Mutex<InMemoryDB>>,
    pub(crate) transaction_id: Option<TransactionId>,
}

/// Metadata associated with a persisted item.
///
/// Tracks versioning, potential schema evolution, and timestamps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistMetadata {
    /// The opportunistic locking version.
    pub version: i64,
    /// The schema version of the stored data.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// Timestamp of creation.
    pub created_at: DateTime<Utc>,
    /// Timestamp of last update.
    pub updated_at: DateTime<Utc>,
    /// Timestamp of last access or touch.
    pub last_touch_at: DateTime<Utc>,
    /// Counter of how many times the item has been unwrapped/accessed.
    pub touch_count: u64,
    /// Whether the item is currently persisted in the database.
    pub persisted: bool,
}

impl PersistMetadata {
    /// Creates new metadata initialized with the current time.
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
