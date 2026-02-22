impl PersistApp {
    /// Opens a `PersistApp` with automatic defaults at `root`.
    pub async fn open_auto(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_auto_with(root, PersistAppAutoPolicy::default()).await
    }

    /// Opens a `PersistApp` with a coarse-grained auto policy profile.
    pub async fn open_auto_with(
        root: impl Into<PathBuf>,
        policy: PersistAppAutoPolicy,
    ) -> Result<Self> {
        Self::open(root, policy.into()).await
    }

    /// Opens a `PersistApp` with fully explicit policy settings.
    ///
    /// This method creates the root directory if it does not exist and
    /// initializes a fresh `PersistSession`.
    pub async fn open(root: impl Into<PathBuf>, policy: PersistAppPolicy) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).await.map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to create persist app root '{}': {}",
                root.display(),
                err
            ))
        })?;

        Ok(Self {
            session: PersistSession::new(InMemoryDB::new()),
            root,
            policy,
        })
    }

    /// Returns the active policy used by this `PersistApp` instance.
    pub fn policy(&self) -> &PersistAppPolicy {
        &self.policy
    }

    /// Returns `true` when a transaction-level conflict should be retried.
    ///
    /// Retries are intentionally limited to write-write conflicts to avoid
    /// hiding business-level optimistic lock outcomes.
    fn should_retry_transaction_conflict(&self, attempt: usize, err: &DbError) -> bool {
        let retry = &self.policy.conflict_retry;
        if attempt >= retry.max_attempts.max(1) {
            return false;
        }

        let Some(kind) = classify_managed_conflict(err) else {
            return false;
        };

        matches!(kind, ManagedConflictKind::WriteWrite) && retry.retry_write_write
    }

    /// Computes exponential retry backoff in milliseconds.
    fn retry_backoff_ms(&self, attempt: usize) -> u64 {
        let retry = &self.policy.conflict_retry;
        let base = retry.base_backoff_ms.max(1);
        let cap = retry.max_backoff_ms.max(base);

        let mut backoff = base;
        for _ in 1..attempt {
            backoff = backoff.saturating_mul(2).min(cap);
        }
        backoff
    }
}
