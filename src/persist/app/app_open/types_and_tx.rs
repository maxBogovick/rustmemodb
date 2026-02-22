/// The main entry point for the persistence application runtime.
///
/// `PersistApp` owns persistence session lifecycle, filesystem root management,
/// and global operation policies (snapshotting, replication and conflict retry).
#[derive(Clone)]
pub struct PersistApp {
    session: PersistSession,
    root: PathBuf,
    policy: PersistAppPolicy,
}

/// A handle for a multi-collection transaction.
///
/// This type intentionally exposes only session-bound execution methods so
/// application code can compose atomic operations without opening a new session.
#[derive(Clone)]
pub struct PersistTx {
    session: PersistSession,
}

impl PersistTx {
    fn new(session: PersistSession) -> Self {
        Self { session }
    }

    /// Returns a clone of the underlying transaction-scoped session.
    ///
    /// This is the low-level escape hatch for APIs that require direct
    /// `PersistSession` access.
    pub fn session(&self) -> PersistSession {
        self.session.clone()
    }

    /// Executes a raw SQL statement inside the active transaction.
    pub async fn execute(&self, sql: &str) -> Result<crate::result::QueryResult> {
        self.session.execute(sql).await
    }

    /// Executes a raw SQL query inside the active transaction.
    pub async fn query(&self, sql: &str) -> Result<crate::result::QueryResult> {
        self.session.query(sql).await
    }
}
