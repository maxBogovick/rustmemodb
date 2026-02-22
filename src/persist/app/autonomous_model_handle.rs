impl<M: PersistAutonomousModel> PersistAutonomousModelHandle<M> {
    pub(crate) fn new(inner: PersistDomainHandle<M::Collection>) -> Self {
        Self {
            inner,
            marker: PhantomData,
        }
    }

    fn into_record(persisted: M::Persisted) -> PersistAutonomousRecord<M> {
        let persist_id = persisted.persist_id().to_string();
        let version = persisted.metadata().version;
        let model = M::from_persisted(persisted);
        PersistAutonomousRecord {
            persist_id,
            model,
            version,
        }
    }

    async fn append_system_audit_best_effort(
        &self,
        persist_id: &str,
        operation_name: &str,
        resulting_version: i64,
    ) {
        let event_type = default_audit_event_type(operation_name);
        let message = format!("system: command '{}' applied", event_type);
        if let Err(err) = self
            .inner
            .append_audit_for(persist_id, event_type.clone(), message, resulting_version)
            .await
        {
            warn!(
                "PersistAutonomousModelHandle audit append failed for '{}' op '{}': {}",
                persist_id, operation_name, err
            );
        }
    }

    /// Creates one aggregate from source model and returns source-model record.
    pub async fn create_one(
        &self,
        model: M,
    ) -> std::result::Result<PersistAutonomousRecord<M>, PersistDomainError> {
        let persisted = self.inner.create_one(M::into_persisted(model)).await?;
        let record = Self::into_record(persisted);
        self.append_system_audit_best_effort(&record.persist_id, "create_one", record.version)
            .await;
        Ok(record)
    }

    /// Returns one aggregate by id as source-model record.
    pub async fn get_one(&self, persist_id: impl AsRef<str>) -> Option<PersistAutonomousRecord<M>>
    where
        M::Persisted: Clone,
    {
        let persist_id = persist_id.as_ref();
        self.inner.get_one(persist_id).await.map(Self::into_record)
    }

    /// Lists aggregates as source-model records.
    pub async fn list(&self) -> Vec<PersistAutonomousRecord<M>>
    where
        M::Persisted: Clone,
    {
        self.inner
            .list()
            .await
            .into_iter()
            .map(Self::into_record)
            .collect()
    }

    /// Applies source-model mutation and persists it atomically.
    pub async fn mutate_one_with<F, E>(
        &self,
        persist_id: impl AsRef<str>,
        mutator: F,
    ) -> std::result::Result<PersistAutonomousRecord<M>, PersistDomainMutationError<E>>
    where
        F: FnOnce(&mut M) -> std::result::Result<(), E>,
    {
        let persist_id = persist_id.as_ref();
        let updated = self
            .inner
            .mutate_one_with(persist_id, move |persisted| mutator(persisted.model_mut()))
            .await?;
        let record = Self::into_record(updated);
        self.append_system_audit_best_effort(&record.persist_id, "mutate_one", record.version)
            .await;
        Ok(record)
    }

    /// Applies source-model mutation atomically and returns both updated record and mutator output.
    ///
    /// This keeps app code focused on business intent when a mutation needs to
    /// return a domain value (for example, an id of a nested object created in
    /// the same mutation).
    pub async fn mutate_one_with_result<F, E, R>(
        &self,
        persist_id: impl AsRef<str>,
        mutator: F,
    ) -> std::result::Result<(PersistAutonomousRecord<M>, R), PersistDomainMutationError<E>>
    where
        F: FnOnce(&mut M) -> std::result::Result<R, E>,
    {
        let persist_id = persist_id.as_ref();
        self.mutate_one_with_result_named(persist_id, "mutate_one_with_result", mutator)
            .await
    }

    /// Applies a named source-model mutation atomically and records an audit event.
    ///
    /// `operation_name` is used to derive a stable system audit event key.
    pub async fn mutate_one_with_result_named<F, E, R>(
        &self,
        persist_id: impl AsRef<str>,
        operation_name: &str,
        mutator: F,
    ) -> std::result::Result<(PersistAutonomousRecord<M>, R), PersistDomainMutationError<E>>
    where
        F: FnOnce(&mut M) -> std::result::Result<R, E>,
    {
        let persist_id = persist_id.as_ref();
        let result = std::sync::Arc::new(std::sync::Mutex::new(None));
        let result_ref = std::sync::Arc::clone(&result);
        let updated = self
            .inner
            .mutate_one_with(persist_id, move |persisted| {
                let output = mutator(persisted.model_mut())?;
                let mut guard = result_ref
                    .lock()
                    .expect("mutator result lock poisoned in mutate_one_with_result");
                *guard = Some(output);
                Ok(())
            })
            .await?;

        let output = result
            .lock()
            .expect("mutator result lock poisoned after mutate_one_with_result")
            .take()
            .expect("mutator output must exist on successful mutation");
        let record = Self::into_record(updated);
        self.append_system_audit_best_effort(&record.persist_id, operation_name, record.version)
            .await;
        Ok((record, output))
    }

    /// Executes one REST command with optional automatic idempotency replay.
    ///
    /// If `idempotency_key` is present, persist stores command response under a
    /// stable scope key (`aggregate_id:operation:idempotency_key`) and replays it
    /// for duplicate requests without invoking model mutation again.
    pub async fn execute_rest_command_with_idempotency<F, E, R>(
        &self,
        persist_id: impl AsRef<str>,
        operation_name: &str,
        idempotency_key: Option<String>,
        success_status: u16,
        mutator: F,
    ) -> std::result::Result<PersistIdempotentCommandResult<R>, PersistDomainMutationError<E>>
    where
        F: FnOnce(&mut M) -> std::result::Result<R, E>,
        R: Serialize,
    {
        let persist_id = persist_id.as_ref();
        let Some(idempotency_key) = idempotency_key else {
            let (_, output) = self
                .mutate_one_with_result_named(persist_id, operation_name, mutator)
                .await?;
            return Ok(PersistIdempotentCommandResult::Applied(output));
        };

        let scope_key = format!("{persist_id}:{operation_name}:{idempotency_key}");
        let mut store = self.inner.inner.lock().await;

        if let Some(existing) = store
            .rest_idempotency
            .find_first(|entry| entry.scope_key() == &scope_key)
        {
            let body = serde_json::from_str::<serde_json::Value>(existing.response_body_json())
                .map_err(|err| {
                    PersistDomainMutationError::Domain(PersistDomainError::Internal(format!(
                        "failed to decode idempotency payload for scope '{}': {}",
                        scope_key, err
                    )))
                })?;
            let status_code = status_code_from_persist(*existing.status_code());
            return Ok(PersistIdempotentCommandResult::Replayed { status_code, body });
        }

        let aggregate_snapshot = store.aggregate.managed.snapshot_for_external_transaction();
        let idempotency_snapshot = store
            .rest_idempotency
            .managed
            .snapshot_for_external_transaction();
        let shared_session = store.aggregate.managed.shared_session();
        let tx_id = shared_session.begin_transaction().await.map_err(|err| {
            PersistDomainMutationError::Domain(PersistDomainError::from(err))
        })?;
        let tx_session = shared_session.with_transaction_id(tx_id);

        let update_result = store
            .aggregate
            .managed
            .update_with_result_with_session(&tx_session, persist_id, move |persisted| {
                mutator(persisted.model_mut())
            })
            .await
            .map_err(|err| PersistDomainMutationError::Domain(PersistDomainError::from(err)))?;

        let output = match update_result {
            Ok(Some(output)) => output,
            Ok(None) => {
                let _ = shared_session.rollback_transaction(tx_id).await;
                let _ = store
                    .aggregate
                    .managed
                    .restore_snapshot_for_external_transaction(aggregate_snapshot)
                    .await;
                let _ = store
                    .rest_idempotency
                    .managed
                    .restore_snapshot_for_external_transaction(idempotency_snapshot)
                    .await;
                return Err(PersistDomainMutationError::Domain(PersistDomainError::NotFound));
            }
            Err(user_error) => {
                let _ = shared_session.rollback_transaction(tx_id).await;
                let _ = store
                    .aggregate
                    .managed
                    .restore_snapshot_for_external_transaction(aggregate_snapshot)
                    .await;
                let _ = store
                    .rest_idempotency
                    .managed
                    .restore_snapshot_for_external_transaction(idempotency_snapshot)
                    .await;
                return Err(PersistDomainMutationError::User(user_error));
            }
        };

        let response_body_json = serde_json::to_string(&output).map_err(|err| {
            PersistDomainMutationError::Domain(PersistDomainError::Internal(format!(
                "failed to serialize idempotent response payload for '{}': {}",
                operation_name, err
            )))
        })?;

        let resulting_version = store
            .aggregate
            .managed
            .get(persist_id)
            .map(|item| item.metadata().version)
            .ok_or_else(|| {
                PersistDomainMutationError::Domain(PersistDomainError::Internal(format!(
                    "entity '{}' missing after command '{}' in '{}'",
                    persist_id,
                    operation_name,
                    store.aggregate.name()
                )))
            })?;

        let receipt = PersistRestIdempotencyRecord::new(
            scope_key,
            persist_id.to_string(),
            operation_name.to_string(),
            idempotency_key,
            i64::from(success_status),
            response_body_json,
        );

        if let Err(err) = store
            .rest_idempotency
            .managed
            .create_with_session(&tx_session, receipt)
            .await
        {
            let _ = shared_session.rollback_transaction(tx_id).await;
            let _ = store
                .aggregate
                .managed
                .restore_snapshot_for_external_transaction(aggregate_snapshot)
                .await;
            let _ = store
                .rest_idempotency
                .managed
                .restore_snapshot_for_external_transaction(idempotency_snapshot)
                .await;
            return Err(PersistDomainMutationError::Domain(PersistDomainError::from(err)));
        }

        if let Err(err) = shared_session.commit_transaction(tx_id).await {
            let _ = shared_session.rollback_transaction(tx_id).await;
            let _ = store
                .aggregate
                .managed
                .restore_snapshot_for_external_transaction(aggregate_snapshot)
                .await;
            let _ = store
                .rest_idempotency
                .managed
                .restore_snapshot_for_external_transaction(idempotency_snapshot)
                .await;
            return Err(PersistDomainMutationError::Domain(PersistDomainError::from(err)));
        }

        store.aggregate.managed.on_external_mutation_committed().await.map_err(
            |err| PersistDomainMutationError::Domain(PersistDomainError::from(err)),
        )?;
        store
            .rest_idempotency
            .managed
            .on_external_mutation_committed()
            .await
            .map_err(|err| PersistDomainMutationError::Domain(PersistDomainError::from(err)))?;

        drop(store);
        self.append_system_audit_best_effort(persist_id, operation_name, resulting_version)
            .await;
        Ok(PersistIdempotentCommandResult::Applied(output))
    }

    /// Deletes one aggregate by id.
    pub async fn remove_one(
        &self,
        persist_id: impl AsRef<str>,
    ) -> std::result::Result<(), PersistDomainError>
    where
        <M::Persisted as PersistCommandModel>::Command: PersistCommandName,
    {
        let persist_id = persist_id.as_ref();
        self.inner.remove_one(persist_id).await
    }

    /// Returns the underlying domain handle for advanced scenarios.
    pub fn domain_handle(&self) -> &PersistDomainHandle<M::Collection> {
        &self.inner
    }
}

fn status_code_from_persist(stored: i64) -> u16 {
    if (100..=599).contains(&stored) {
        stored as u16
    } else {
        200
    }
}
