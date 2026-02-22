impl PersistEntityRuntime {
    /// Opens the runtime, initializing storage and loading stated from disk.
    ///
    /// This method will create the root directory if it does not exist, and traverse
    /// the snapshot and journal files to rebuild the memory state.
    pub async fn open(
        root_dir: impl Into<PathBuf>,
        policy: RuntimeOperationalPolicy,
    ) -> Result<Self> {
        let root_dir = root_dir.into();
        let policy = normalize_runtime_policy(policy);
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
            tombstones: HashMap::new(),
            deterministic_registry: HashMap::new(),
            command_migration_registry: HashMap::new(),
            runtime_closure_registry: HashMap::new(),
            projection_registry: HashMap::new(),
            projection_tables: HashMap::new(),
            entity_mailboxes: HashMap::new(),
            outbox_records: HashMap::new(),
            idempotency_index: HashMap::new(),
            seq_next: 1,
            ops_since_snapshot: 0,
            last_sync_unix_ms: Utc::now().timestamp_millis(),
            inflight: Arc::new(Semaphore::new(max_inflight)),
            resurrected_since_last_report: 0,
            lifecycle_passivated_total: 0,
            lifecycle_resurrected_total: 0,
            lifecycle_gc_deleted_total: 0,
            tombstones_pruned_total: 0,
            snapshot_worker_running: false,
            snapshot_worker_errors: Arc::new(AtomicU64::new(0)),
            replica_targets,
            replication_failures: Arc::new(AtomicU64::new(0)),
        };

        runtime.load_from_disk().await?;
        Ok(runtime)
    }

    /// Returns the current active operational policy.
    pub fn policy(&self) -> &RuntimeOperationalPolicy {
        &self.policy
    }

    /// Returns the file paths used by this runtime instance.
    pub fn paths(&self) -> RuntimePaths {
        RuntimePaths {
            root_dir: self.root_dir.clone(),
            snapshot_file: self.snapshot_path(),
            journal_file: self.journal_path(),
        }
    }

    /// Generates a comprehensive statistics report for the runtime.
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
        let command_migration_count = self
            .command_migration_registry
            .values()
            .map(|rules| rules.len())
            .sum();

        let closure_count = self
            .runtime_closure_registry
            .values()
            .map(|commands| commands.len())
            .sum();

        let projection_rows = self
            .projection_tables
            .values()
            .map(|table| table.rows.len())
            .sum();
        let projection_index_columns = self
            .projection_tables
            .values()
            .map(|table| table.indexes.len())
            .sum();
        let projection_lag_entities = self.projection_lag_entities_count();
        let durability_lag_ms =
            (Utc::now().timestamp_millis() - self.last_sync_unix_ms).max(0) as u64;
        let mailbox_busy_entities = self
            .entity_mailboxes
            .values()
            .filter(|mailbox| mailbox.inflight || mailbox.pending_commands > 0)
            .count();
        let lifecycle_churn_total = self
            .lifecycle_passivated_total
            .saturating_add(self.lifecycle_resurrected_total)
            .saturating_add(self.lifecycle_gc_deleted_total);

        RuntimeStats {
            hot_entities: self.hot_entities.len(),
            cold_entities: self.cold_entities.len(),
            tombstones: self.tombstones.len(),
            registered_types: self
                .deterministic_registry
                .keys()
                .chain(self.command_migration_registry.keys())
                .chain(self.runtime_closure_registry.keys())
                .chain(self.projection_registry.keys())
                .collect::<HashSet<_>>()
                .len(),
            registered_deterministic_commands: command_count,
            registered_command_migrations: command_migration_count,
            deterministic_commands_with_payload_contracts: command_schema_count,
            registered_runtime_closures: closure_count,
            registered_projections: self.projection_registry.len(),
            projection_rows,
            projection_index_columns,
            projection_lag_entities,
            replication_targets: self.replica_targets.len(),
            replication_failures: self.replication_failures.load(AtomicOrdering::Relaxed),
            durability_lag_ms,
            snapshot_worker_running: self.snapshot_worker_running,
            snapshot_worker_errors: self.snapshot_worker_errors.load(AtomicOrdering::Relaxed),
            next_seq: self.seq_next,
            ops_since_snapshot: self.ops_since_snapshot,
            outbox_total: self.outbox_records.len(),
            outbox_pending: self
                .outbox_records
                .values()
                .filter(|record| record.status == RuntimeOutboxStatus::Pending)
                .count(),
            idempotency_entries: self.idempotency_index.len(),
            mailbox_entities: self.entity_mailboxes.len(),
            mailbox_busy_entities,
            lifecycle_passivated_total: self.lifecycle_passivated_total,
            lifecycle_resurrected_total: self.lifecycle_resurrected_total,
            lifecycle_gc_deleted_total: self.lifecycle_gc_deleted_total,
            tombstones_pruned_total: self.tombstones_pruned_total,
            lifecycle_churn_total,
        }
    }

    /// Returns a subset of metrics specifically focused on Service Level Objectives.
    pub fn slo_metrics(&self) -> RuntimeSloMetrics {
        let stats = self.stats();
        RuntimeSloMetrics {
            durability_lag_ms: stats.durability_lag_ms,
            projection_lag_entities: stats.projection_lag_entities,
            lifecycle_churn_total: stats.lifecycle_churn_total,
            outbox_pending: stats.outbox_pending,
            replication_failures: stats.replication_failures,
            mailbox_busy_entities: stats.mailbox_busy_entities,
        }
    }
}
