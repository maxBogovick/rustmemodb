impl<V: PersistCollection> ManagedPersistVec<V> {
    /// Returns the logical name of this collection.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Access the underlying raw collection (read-only).
    pub fn collection(&self) -> &V {
        &self.collection
    }

    /// Access the underlying raw collection (mutable).
    ///
    /// Warning: Direct mutation bypasses some runtime services. Use `mutate` or `mutate_async`
    /// to ensure changes are persisted and metrics/snapshots are updated correctly.
    pub fn collection_mut(&mut self) -> &mut V {
        &mut self.collection
    }

    /// Returns current runtime statistics.
    pub fn stats(&self) -> ManagedPersistVecStats {
        ManagedPersistVecStats {
            vec_name: self.name.clone(),
            item_count: self.collection.len(),
            snapshot_every_ops: self.snapshot_every_ops,
            ops_since_snapshot: self.ops_since_snapshot,
            snapshot_path: self.snapshot_path.to_string_lossy().to_string(),
            replication_mode: match self.replication.mode {
                PersistReplicationMode::Sync => "sync".to_string(),
                PersistReplicationMode::AsyncBestEffort => "async".to_string(),
            },
            replication_targets: self.replication.replica_roots.len(),
            replication_failures: self.replication_failures,
            last_snapshot_at: self.last_snapshot_at.clone(),
        }
    }

    /// Persists all current changes to the underlying storage.
    ///
    /// Triggers the `on_mutation_committed` hook (e.g., for snapshotting).
    pub async fn save(&mut self) -> Result<()> {
        self.collection.save_all(&self.session).await?;
        self.on_mutation_committed().await
    }

    /// Executes a closure that mutates the collection, then saves changes.
    ///
    /// This ensures atomic persistence of the mutation block.
    pub async fn mutate<F>(&mut self, f: F) -> Result<()>
    where
        F: FnOnce(&mut V) -> Result<()>,
    {
        f(&mut self.collection)?;
        self.save().await
    }

    /// Executes an async closure that mutates the collection, then saves changes.
    ///
    /// Useful when the mutation logic itself requires await points.
    pub async fn mutate_async<F>(&mut self, f: F) -> Result<()>
    where
        F: for<'a> FnOnce(
            &'a mut V,
            &'a PersistSession,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>,
    {
        f(&mut self.collection, &self.session).await?;
        self.save().await
    }

    /// Executes an atomic operation spanning two managed collections.
    ///
    /// This provides AC (Atomicity, Consistency) across two aggregates using a shared transaction.
    /// It handles distributed transaction logic (commit/rollback) and in-memory state restoration (rewind)
    /// if the transaction fails, ensuring both memory and disk remain consistent.
    pub async fn atomic_with<U, F, T>(
        &mut self,
        other: &mut ManagedPersistVec<U>,
        operation: F,
    ) -> Result<T>
    where
        U: PersistCollection,
        F: for<'a> FnOnce(
            PersistTx,
            &'a mut ManagedPersistVec<V>,
            &'a mut ManagedPersistVec<U>,
        ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
    {
        let left_snapshot = self.snapshot_for_external_transaction();
        let right_snapshot = other.snapshot_for_external_transaction();
        let shared_session = self.shared_session();

        let transaction_result = shared_session
            .with_transaction(|tx_session| operation(PersistTx::new(tx_session), self, other))
            .await;

        match transaction_result {
            Ok(value) => {
                self.on_external_mutation_committed().await?;
                other.on_external_mutation_committed().await?;
                Ok(value)
            }
            Err(operation_err) => {
                let left_rewind = self
                    .restore_snapshot_for_external_transaction(left_snapshot)
                    .await;
                let right_rewind = other
                    .restore_snapshot_for_external_transaction(right_snapshot)
                    .await;

                match (left_rewind, right_rewind) {
                    (Ok(()), Ok(())) => {
                        Err(map_managed_conflict_error("atomic_with", operation_err))
                    }
                    (left, right) => Err(DbError::ExecutionError(format!(
                        "Managed operation 'atomic_with' failed and state rewind failed: operation_error='{}'; left_rewind_error='{}'; right_rewind_error='{}'",
                        operation_err,
                        left.err()
                            .map(|err| err.to_string())
                            .unwrap_or_else(|| "none".to_string()),
                        right
                            .err()
                            .map(|err| err.to_string())
                            .unwrap_or_else(|| "none".to_string())
                    ))),
                }
            }
        }
    }

    /// Forces a full snapshot of the collection state to disk.
    ///
    /// Also triggers replication of the snapshot.
    pub async fn force_snapshot(&mut self) -> Result<()> {
        let snapshot = self.collection.snapshot(SnapshotMode::WithData);
        let bytes = serde_json::to_vec_pretty(&snapshot).map_err(|err| {
            DbError::ExecutionError(format!(
                "Failed to encode snapshot for vec '{}': {}",
                self.name, err
            ))
        })?;
        atomic_write(&self.snapshot_path, &bytes).await?;
        self.replicate_snapshot(&bytes).await?;
        self.ops_since_snapshot = 0;
        self.last_snapshot_at = Some(Utc::now().to_rfc3339());
        Ok(())
    }

    /// Returns a clone of the current session, sharing the underlying DB connection.
    pub fn shared_session(&self) -> PersistSession {
        self.session.clone()
    }

    /// Captures a snapshot state for rollback purposes during external transactions.
    pub fn snapshot_for_external_transaction(&self) -> V::Snapshot {
        self.collection.snapshot(SnapshotMode::WithData)
    }

    /// Restores the collection state from a snapshot (rollback).
    pub async fn restore_snapshot_for_external_transaction(
        &mut self,
        snapshot: V::Snapshot,
    ) -> Result<()> {
        let rewind_session = PersistSession::new(InMemoryDB::new());
        self.collection
            .restore_with_policy(snapshot, &rewind_session, RestoreConflictPolicy::FailFast)
            .await
    }

    /// Hook called after an external transaction commits successfully.
    ///
    /// Updates snapshot counters and triggers snapshotting if threshold is reached.
    pub async fn on_external_mutation_committed(&mut self) -> Result<()> {
        self.on_mutation_committed().await
    }

    async fn on_mutation_committed(&mut self) -> Result<()> {
        self.ops_since_snapshot += 1;
        if self.ops_since_snapshot >= self.snapshot_every_ops {
            self.force_snapshot().await?;
        }
        Ok(())
    }

    async fn begin_atomic_scope(&mut self) -> Result<(V::Snapshot, TransactionId, PersistSession)> {
        let rollback_snapshot = self.collection.snapshot(SnapshotMode::WithData);
        let transaction_id = self.session.begin_transaction().await?;
        let tx_session = self.session.with_transaction_id(transaction_id);
        Ok((rollback_snapshot, transaction_id, tx_session))
    }

    async fn finalize_atomic_scope<T>(
        &mut self,
        operation: &str,
        rollback_snapshot: V::Snapshot,
        transaction_id: TransactionId,
        operation_result: Result<T>,
    ) -> Result<T> {
        match operation_result {
            Ok(value) => {
                if let Err(commit_err) = self.session.commit_transaction(transaction_id).await {
                    let _ = self.session.rollback_transaction(transaction_id).await;
                    let rewind_session = PersistSession::new(InMemoryDB::new());
                    let rewind_result = self
                        .collection
                        .restore_with_policy(
                            rollback_snapshot,
                            &rewind_session,
                            RestoreConflictPolicy::FailFast,
                        )
                        .await;
                    return match rewind_result {
                        Ok(_) => Err(map_managed_conflict_error(operation, commit_err)),
                        Err(rewind_err) => Err(DbError::ExecutionError(format!(
                            "Managed operation '{}' failed to commit and failed to rewind state: commit_error='{}'; rewind_error='{}'",
                            operation, commit_err, rewind_err
                        ))),
                    };
                }
                Ok(value)
            }
            Err(operation_err) => {
                let rollback_result = self.session.rollback_transaction(transaction_id).await;
                let rewind_session = PersistSession::new(InMemoryDB::new());
                let rewind_result = self
                    .collection
                    .restore_with_policy(
                        rollback_snapshot,
                        &rewind_session,
                        RestoreConflictPolicy::FailFast,
                    )
                    .await;

                if let Err(rewind_err) = rewind_result {
                    return Err(DbError::ExecutionError(format!(
                        "Managed operation '{}' failed and rollback state rewind failed: operation_error='{}'; rewind_error='{}'",
                        operation, operation_err, rewind_err
                    )));
                }
                if let Err(rollback_err) = rollback_result {
                    return Err(DbError::ExecutionError(format!(
                        "Managed operation '{}' failed and transaction rollback failed: operation_error='{}'; rollback_error='{}'",
                        operation, operation_err, rollback_err
                    )));
                }

                Err(map_managed_conflict_error(operation, operation_err))
            }
        }
    }

    /// Aborts an in-flight atomic scope because user-domain validation failed.
    ///
    /// This path intentionally keeps user error `E` intact while still ensuring:
    /// - SQL transaction rollback
    /// - in-memory state rewind from the pre-operation snapshot
    async fn abort_atomic_scope_with_user_error<T, E>(
        &mut self,
        operation: &str,
        rollback_snapshot: V::Snapshot,
        transaction_id: TransactionId,
        user_error: E,
    ) -> Result<std::result::Result<T, E>> {
        let rollback_result = self.session.rollback_transaction(transaction_id).await;
        let rewind_session = PersistSession::new(InMemoryDB::new());
        let rewind_result = self
            .collection
            .restore_with_policy(
                rollback_snapshot,
                &rewind_session,
                RestoreConflictPolicy::FailFast,
            )
            .await;

        if let Err(rewind_err) = rewind_result {
            return Err(DbError::ExecutionError(format!(
                "Managed operation '{}' failed with user mutation error and rewind failed: rewind_error='{}'",
                operation, rewind_err
            )));
        }
        if let Err(rollback_err) = rollback_result {
            return Err(DbError::ExecutionError(format!(
                "Managed operation '{}' failed with user mutation error and transaction rollback failed: rollback_error='{}'",
                operation, rollback_err
            )));
        }

        Ok(Err(user_error))
    }

    async fn replicate_snapshot(&mut self, bytes: &[u8]) -> Result<()> {
        if self.replication.replica_roots.is_empty() {
            return Ok(());
        }

        let mode = self.replication.mode.clone();
        let mut failures = 0u64;

        for root in self.replication.replica_roots.clone() {
            let target = root.join(
                self.snapshot_path
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("snapshot.json")),
            );
            if let Err(err) = atomic_write(&target, bytes).await {
                failures += 1;
                if matches!(mode, PersistReplicationMode::Sync) {
                    self.replication_failures += failures;
                    return Err(err);
                }
                warn!(
                    "async snapshot replication failed: vec='{}' replica='{}' error='{}'",
                    self.name,
                    root.display(),
                    err
                );
            }
        }

        self.replication_failures += failures;
        Ok(())
    }
}
