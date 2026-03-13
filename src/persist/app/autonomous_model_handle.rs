impl<M> PersistAutonomousModelHandle<M>
where
    M: PersistAutonomousModel,
    M::Persisted: PersistEntityFactory,
{
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

    /// Returns one aggregate by id from in-memory cache.
    pub async fn get_one_cached(
        &self,
        persist_id: impl AsRef<str>,
    ) -> Option<PersistAutonomousRecord<M>>
    where
        M::Persisted: Clone,
    {
        let persist_id = persist_id.as_ref();
        self.inner.get_one_cached(persist_id).await.map(Self::into_record)
    }

    /// Returns one aggregate by id using DB-first lookup.
    pub async fn get_one_db(
        &self,
        persist_id: impl AsRef<str>,
    ) -> std::result::Result<Option<PersistAutonomousRecord<M>>, PersistDomainError>
    where
        M::Persisted: Clone,
    {
        let persist_id = persist_id.as_ref();
        self.inner
            .get_one_db(persist_id)
            .await
            .map(|item| item.map(Self::into_record))
            .map_err(PersistDomainError::from)
    }

    /// Returns current persisted version for one aggregate via DB-first lookup.
    pub async fn get_version_db(
        &self,
        persist_id: impl AsRef<str>,
    ) -> std::result::Result<Option<i64>, PersistDomainError> {
        let persist_id = persist_id.as_ref();
        self.inner
            .get_version_db(persist_id)
            .await
            .map_err(PersistDomainError::from)
    }

    /// Returns one aggregate by id.
    ///
    /// Uses DB-first path and falls back to cache only if DB read fails.
    pub async fn get_one(&self, persist_id: impl AsRef<str>) -> Option<PersistAutonomousRecord<M>>
    where
        M::Persisted: Clone,
    {
        let persist_id = persist_id.as_ref();
        match self.get_one_db(persist_id).await {
            Ok(record) => record,
            Err(err) => {
                warn!(
                    "PersistAutonomousModelHandle.get_one DB read failed for '{}'; fallback to cache: {}",
                    persist_id, err
                );
                self.get_one_cached(persist_id).await
            }
        }
    }

    /// Creates a typed view handle bound to this model handle.
    ///
    /// Keeping view calls on the same handle avoids stale state between
    /// independently opened collections.
    pub fn view<V>(&self) -> PersistViewHandle<M, V>
    where
        V: PersistView<M>,
        M::Persisted: Clone,
    {
        PersistViewHandle::new(self.clone())
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
        M::Persisted: PersistEntityFactory,
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

        if let Some((status_code, body)) =
            load_existing_rest_idempotency_replay(&mut store.rest_idempotency, &scope_key)
                .await
                .map_err(|err| PersistDomainMutationError::Domain(PersistDomainError::from(err)))?
        {
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

        let receipt = PersistRestIdempotencyRecord::new(
            scope_key.clone(),
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
            if let Some((status_code, body)) = load_existing_rest_idempotency_replay(
                &mut store.rest_idempotency,
                &scope_key,
            )
            .await
            .map_err(|reload_err| {
                PersistDomainMutationError::Domain(PersistDomainError::Internal(format!(
                    "failed to reload idempotency receipt for scope '{}' after insert error '{}': {}",
                    scope_key, err, reload_err
                )))
            })? {
                return Ok(PersistIdempotentCommandResult::Replayed { status_code, body });
            }
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

        let aggregate_name = store.aggregate.name().to_string();
        let resulting_version = store
            .aggregate
            .managed
            .get_version_db(persist_id)
            .await
            .map_err(|err| PersistDomainMutationError::Domain(PersistDomainError::from(err)))?
            .ok_or_else(|| {
                PersistDomainMutationError::Domain(PersistDomainError::Internal(format!(
                    "entity '{}' missing after command '{}' in '{}'",
                    persist_id, operation_name, aggregate_name
                )))
            })?;

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

/// Sort direction for high-level autonomous query DSL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PersistQuerySortDirection {
    #[default]
    Asc,
    Desc,
}

/// Supported filter operators for high-level autonomous query DSL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistQueryOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
}

/// One filter condition in high-level autonomous query DSL.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PersistQueryFilter {
    pub field: String,
    pub op: PersistQueryOp,
    pub value: serde_json::Value,
}

/// Sorting options in high-level autonomous query DSL.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PersistQuerySort {
    pub field: String,
    #[serde(default)]
    pub direction: PersistQuerySortDirection,
}

/// Declarative list/filter/sort/page query descriptor for autonomous models.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PersistQuerySpec {
    #[serde(default = "default_query_page")]
    pub page: u32,
    #[serde(default = "default_query_per_page")]
    pub per_page: u32,
    #[serde(default)]
    pub filters: Vec<PersistQueryFilter>,
    #[serde(default)]
    pub sort: Option<PersistQuerySort>,
}

impl Default for PersistQuerySpec {
    fn default() -> Self {
        Self {
            page: default_query_page(),
            per_page: default_query_per_page(),
            filters: Vec::new(),
            sort: None,
        }
    }
}

fn default_query_page() -> u32 {
    1
}

fn default_query_per_page() -> u32 {
    50
}

/// Chainable query builder over `PersistAutonomousModelHandle`.
///
/// This is the default high-level path for list/filter/sort/pagination
/// without manual in-app scans.
#[derive(Clone)]
pub struct PersistQueryBuilder<M: PersistAutonomousModel> {
    handle: PersistAutonomousModelHandle<M>,
    spec: PersistQuerySpec,
}

impl<M: PersistAutonomousModel> PersistQueryBuilder<M> {
    fn new(handle: PersistAutonomousModelHandle<M>) -> Self {
        Self {
            handle,
            spec: PersistQuerySpec::default(),
        }
    }

    pub fn page(mut self, page: u32) -> Self {
        self.spec.page = page;
        self
    }

    pub fn per_page(mut self, per_page: u32) -> Self {
        self.spec.per_page = per_page;
        self
    }

    pub fn sort_by(mut self, field: impl Into<String>, direction: PersistQuerySortDirection) -> Self {
        self.spec.sort = Some(PersistQuerySort {
            field: field.into(),
            direction,
        });
        self
    }

    pub fn sort_asc(self, field: impl Into<String>) -> Self {
        self.sort_by(field, PersistQuerySortDirection::Asc)
    }

    pub fn sort_desc(self, field: impl Into<String>) -> Self {
        self.sort_by(field, PersistQuerySortDirection::Desc)
    }

    pub fn where_eq<V: Serialize>(self, field: impl Into<String>, value: V) -> Self {
        self.push_filter(field, PersistQueryOp::Eq, value)
    }

    pub fn where_ne<V: Serialize>(self, field: impl Into<String>, value: V) -> Self {
        self.push_filter(field, PersistQueryOp::Ne, value)
    }

    pub fn where_gt<V: Serialize>(self, field: impl Into<String>, value: V) -> Self {
        self.push_filter(field, PersistQueryOp::Gt, value)
    }

    pub fn where_gte<V: Serialize>(self, field: impl Into<String>, value: V) -> Self {
        self.push_filter(field, PersistQueryOp::Gte, value)
    }

    pub fn where_lt<V: Serialize>(self, field: impl Into<String>, value: V) -> Self {
        self.push_filter(field, PersistQueryOp::Lt, value)
    }

    pub fn where_lte<V: Serialize>(self, field: impl Into<String>, value: V) -> Self {
        self.push_filter(field, PersistQueryOp::Lte, value)
    }

    pub fn where_contains<V: Serialize>(self, field: impl Into<String>, value: V) -> Self {
        self.push_filter(field, PersistQueryOp::Contains, value)
    }

    fn push_filter<V: Serialize>(
        mut self,
        field: impl Into<String>,
        op: PersistQueryOp,
        value: V,
    ) -> Self {
        let encoded_value = serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
        self.spec.filters.push(PersistQueryFilter {
            field: field.into(),
            op,
            value: encoded_value,
        });
        self
    }

    pub fn spec(&self) -> &PersistQuerySpec {
        &self.spec
    }

    pub async fn fetch(self) -> PersistAggregatePage<PersistAutonomousRecord<M>>
    where
        M::Persisted: Clone + PersistEntityFactory,
        M: Serialize,
    {
        self.handle.query_with_spec(self.spec).await
    }
}

impl<M> PersistAutonomousModelHandle<M>
where
    M: PersistAutonomousModel,
    M::Persisted: PersistEntityFactory,
{
    /// Starts high-level declarative query building for list/filter/sort/page.
    pub fn query(&self) -> PersistQueryBuilder<M> {
        PersistQueryBuilder::new(self.clone())
    }

    /// Executes one declarative query spec.
    pub async fn query_with_spec(
        &self,
        spec: PersistQuerySpec,
    ) -> PersistAggregatePage<PersistAutonomousRecord<M>>
    where
        M::Persisted: Clone + PersistEntityFactory,
        M: Serialize,
    {
        match self.query_with_spec_via_storage(&spec).await {
            Ok(Some(page)) => page,
            Ok(None) => self.query_with_spec_in_memory(spec).await,
            Err(err) => {
                warn!(
                    "PersistAutonomousModelHandle query_with_spec storage path failed; fallback to in-memory query: {}",
                    err
                );
                self.query_with_spec_in_memory(spec).await
            }
        }
    }

    async fn query_with_spec_via_storage(
        &self,
        spec: &PersistQuerySpec,
    ) -> std::result::Result<Option<PersistAggregatePage<PersistAutonomousRecord<M>>>, PersistDomainError>
    where
        M::Persisted: Clone + PersistEntityFactory,
        M: Serialize,
    {
        let page = spec.page.max(1);
        let per_page = spec.per_page.clamp(1, 500);
        let offset = page.saturating_sub(1).saturating_mul(per_page);

        let (table_name, session) = {
            let store = self.inner.inner.lock().await;
            (
                M::Persisted::default_table_name(),
                store.aggregate.managed.session.clone(),
            )
        };

        let where_clause = match build_storage_where_clause(&spec.filters) {
            Some(clause) => clause,
            None => return Ok(None),
        };

        let order_clause = match build_storage_order_clause(spec.sort.as_ref()) {
            Some(clause) => clause,
            None => return Ok(None),
        };

        let count_sql = format!(
            "SELECT COUNT(*) FROM {}{}",
            table_name,
            where_clause
                .as_ref()
                .map(|clause| format!(" WHERE {clause}"))
                .unwrap_or_default()
        );
        let total = query_total_from_session(&session, &count_sql)
            .await
            .map_err(PersistDomainError::from)?;
        let total_pages = if total == 0 {
            0
        } else {
            let rounded = total.saturating_add(u64::from(per_page).saturating_sub(1));
            u32::try_from(rounded / u64::from(per_page)).unwrap_or(u32::MAX)
        };

        let select_sql = format!(
            "SELECT __persist_id FROM {}{} ORDER BY {} LIMIT {} OFFSET {}",
            table_name,
            where_clause
                .as_ref()
                .map(|clause| format!(" WHERE {clause}"))
                .unwrap_or_default(),
            order_clause,
            per_page,
            offset,
        );
        let persist_ids = query_ids_from_session(&session, &select_sql)
            .await
            .map_err(PersistDomainError::from)?;

        let mut items = Vec::with_capacity(persist_ids.len());
        for persist_id in persist_ids {
            if let Some(record) = self.get_one_db(persist_id.as_str()).await? {
                items.push(record);
            }
        }

        Ok(Some(PersistAggregatePage {
            items,
            page,
            per_page,
            total,
            total_pages,
        }))
    }

    async fn query_with_spec_in_memory(
        &self,
        spec: PersistQuerySpec,
    ) -> PersistAggregatePage<PersistAutonomousRecord<M>>
    where
        M::Persisted: Clone + PersistEntityFactory,
        M: Serialize,
    {
        let page = spec.page.max(1);
        let per_page = spec.per_page.clamp(1, 500);
        let mut records = self
            .list()
            .await
            .into_iter()
            .filter_map(|record| {
                let json = serde_json::to_value(&record.model).ok()?;
                if query_filters_match(&json, &spec.filters) {
                    Some((record, json))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if let Some(sort) = spec.sort {
            let field = sort.field;
            let direction = sort.direction;
            records.sort_by(|left, right| {
                let left_value = query_json_path(&left.1, field.as_str());
                let right_value = query_json_path(&right.1, field.as_str());
                query_compare_values(left_value, right_value, direction)
            });
        }

        let total = u64::try_from(records.len()).unwrap_or(u64::MAX);
        let per_page_usize = usize::try_from(per_page).unwrap_or(usize::MAX);
        let offset = page.saturating_sub(1).saturating_mul(per_page);
        let offset = usize::try_from(offset).unwrap_or(usize::MAX);
        let total_pages = if total == 0 {
            0
        } else {
            let rounded = total.saturating_add(u64::from(per_page).saturating_sub(1));
            u32::try_from(rounded / u64::from(per_page)).unwrap_or(u32::MAX)
        };
        let items = records
            .into_iter()
            .skip(offset)
            .take(per_page_usize)
            .map(|(record, _)| record)
            .collect::<Vec<_>>();

        PersistAggregatePage {
            items,
            page,
            per_page,
            total,
            total_pages,
        }
    }

    /// Appends one item into a nested array field (by dot path) and persists atomically.
    ///
    /// Example paths:
    /// - `columns`
    /// - `columns.0.tasks`
    pub async fn nested_push<T>(
        &self,
        persist_id: impl AsRef<str>,
        array_path: impl Into<String>,
        item: T,
    ) -> std::result::Result<PersistAutonomousRecord<M>, PersistDomainError>
    where
        M::Persisted: Clone + PersistEntityFactory,
        M: Serialize + DeserializeOwned,
        T: Serialize,
    {
        let persist_id = persist_id.as_ref().to_string();
        let array_path = array_path.into();
        let item = serde_json::to_value(item).map_err(|err| {
            PersistDomainError::Validation(format!(
                "nested_push payload serialization failed for '{}': {}",
                array_path, err
            ))
        })?;
        let operation_name = format!("nested_push:{}", array_path);
        let result = self
            .mutate_one_with_result_named(&persist_id, &operation_name, move |model| {
                let mut root = serde_json::to_value(&*model).map_err(|err| {
                    PersistDomainError::Internal(format!(
                        "nested_push model serialization failed for '{}': {}",
                        array_path, err
                    ))
                })?;
                let target = query_json_path_mut(&mut root, &array_path).ok_or_else(|| {
                    PersistDomainError::Validation(format!(
                        "nested_push path '{}' not found",
                        array_path
                    ))
                })?;
                let array = target.as_array_mut().ok_or_else(|| {
                    PersistDomainError::Validation(format!(
                        "nested_push path '{}' is not an array",
                        array_path
                    ))
                })?;
                array.push(item);
                *model = serde_json::from_value(root).map_err(|err| {
                    PersistDomainError::Internal(format!(
                        "nested_push model deserialization failed for '{}': {}",
                        array_path, err
                    ))
                })?;
                Ok(())
            })
            .await;

        map_nested_mutation_result(result)
    }

    /// Removes the first nested array element where `match_field_path == match_value`.
    pub async fn nested_remove_where_eq<V: Serialize>(
        &self,
        persist_id: impl AsRef<str>,
        array_path: impl Into<String>,
        match_field_path: impl Into<String>,
        match_value: V,
    ) -> std::result::Result<PersistAutonomousRecord<M>, PersistDomainError>
    where
        M::Persisted: Clone + PersistEntityFactory,
        M: Serialize + DeserializeOwned,
    {
        let persist_id = persist_id.as_ref().to_string();
        let array_path = array_path.into();
        let match_field_path = match_field_path.into();
        let match_value = serde_json::to_value(match_value).map_err(|err| {
            PersistDomainError::Validation(format!(
                "nested_remove_where_eq value serialization failed for '{}': {}",
                array_path, err
            ))
        })?;
        let operation_name = format!("nested_remove_where_eq:{}:{}", array_path, match_field_path);
        let result = self
            .mutate_one_with_result_named(&persist_id, &operation_name, move |model| {
                let mut root = serde_json::to_value(&*model).map_err(|err| {
                    PersistDomainError::Internal(format!(
                        "nested_remove_where_eq model serialization failed for '{}': {}",
                        array_path, err
                    ))
                })?;
                let target = query_json_path_mut(&mut root, &array_path).ok_or_else(|| {
                    PersistDomainError::Validation(format!(
                        "nested_remove_where_eq path '{}' not found",
                        array_path
                    ))
                })?;
                let array = target.as_array_mut().ok_or_else(|| {
                    PersistDomainError::Validation(format!(
                        "nested_remove_where_eq path '{}' is not an array",
                        array_path
                    ))
                })?;
                let index = array
                    .iter()
                    .position(|entry| query_json_path(entry, &match_field_path) == Some(&match_value))
                    .ok_or_else(|| {
                        PersistDomainError::Validation(format!(
                            "nested_remove_where_eq could not find item in '{}' where '{}' == {}",
                            array_path, match_field_path, match_value
                        ))
                    })?;
                array.remove(index);
                *model = serde_json::from_value(root).map_err(|err| {
                    PersistDomainError::Internal(format!(
                        "nested_remove_where_eq model deserialization failed for '{}': {}",
                        array_path, err
                    ))
                })?;
                Ok(())
            })
            .await;

        map_nested_mutation_result(result)
    }

    /// Applies object patch to the first nested array element where `match_field_path == match_value`.
    ///
    /// `patch` must be a JSON object. Object merge is recursive.
    pub async fn nested_patch_where_eq<V: Serialize>(
        &self,
        persist_id: impl AsRef<str>,
        array_path: impl Into<String>,
        match_field_path: impl Into<String>,
        match_value: V,
        patch: serde_json::Value,
    ) -> std::result::Result<PersistAutonomousRecord<M>, PersistDomainError>
    where
        M::Persisted: Clone + PersistEntityFactory,
        M: Serialize + DeserializeOwned,
    {
        let persist_id = persist_id.as_ref().to_string();
        let array_path = array_path.into();
        let match_field_path = match_field_path.into();
        let match_value = serde_json::to_value(match_value).map_err(|err| {
            PersistDomainError::Validation(format!(
                "nested_patch_where_eq value serialization failed for '{}': {}",
                array_path, err
            ))
        })?;
        if !patch.is_object() {
            return Err(PersistDomainError::Validation(
                "nested_patch_where_eq patch must be a JSON object".to_string(),
            ));
        }
        let operation_name = format!("nested_patch_where_eq:{}:{}", array_path, match_field_path);
        let result = self
            .mutate_one_with_result_named(&persist_id, &operation_name, move |model| {
                let mut root = serde_json::to_value(&*model).map_err(|err| {
                    PersistDomainError::Internal(format!(
                        "nested_patch_where_eq model serialization failed for '{}': {}",
                        array_path, err
                    ))
                })?;
                let target = query_json_path_mut(&mut root, &array_path).ok_or_else(|| {
                    PersistDomainError::Validation(format!(
                        "nested_patch_where_eq path '{}' not found",
                        array_path
                    ))
                })?;
                let array = target.as_array_mut().ok_or_else(|| {
                    PersistDomainError::Validation(format!(
                        "nested_patch_where_eq path '{}' is not an array",
                        array_path
                    ))
                })?;
                let index = array
                    .iter()
                    .position(|entry| query_json_path(entry, &match_field_path) == Some(&match_value))
                    .ok_or_else(|| {
                        PersistDomainError::Validation(format!(
                            "nested_patch_where_eq could not find item in '{}' where '{}' == {}",
                            array_path, match_field_path, match_value
                        ))
                    })?;
                merge_json_values(&mut array[index], &patch);
                *model = serde_json::from_value(root).map_err(|err| {
                    PersistDomainError::Internal(format!(
                        "nested_patch_where_eq model deserialization failed for '{}': {}",
                        array_path, err
                    ))
                })?;
                Ok(())
            })
            .await;

        map_nested_mutation_result(result)
    }

    /// Moves one nested array element from one path to another atomically.
    pub async fn nested_move_where_eq<V: Serialize>(
        &self,
        persist_id: impl AsRef<str>,
        from_array_path: impl Into<String>,
        to_array_path: impl Into<String>,
        match_field_path: impl Into<String>,
        match_value: V,
    ) -> std::result::Result<PersistAutonomousRecord<M>, PersistDomainError>
    where
        M::Persisted: Clone + PersistEntityFactory,
        M: Serialize + DeserializeOwned,
    {
        let persist_id = persist_id.as_ref().to_string();
        let from_array_path = from_array_path.into();
        let to_array_path = to_array_path.into();
        let match_field_path = match_field_path.into();
        let match_value = serde_json::to_value(match_value).map_err(|err| {
            PersistDomainError::Validation(format!(
                "nested_move_where_eq value serialization failed for '{}': {}",
                from_array_path, err
            ))
        })?;
        let operation_name = format!(
            "nested_move_where_eq:{}:{}:{}",
            from_array_path, to_array_path, match_field_path
        );
        let result = self
            .mutate_one_with_result_named(&persist_id, &operation_name, move |model| {
                let mut root = serde_json::to_value(&*model).map_err(|err| {
                    PersistDomainError::Internal(format!(
                        "nested_move_where_eq model serialization failed for '{}' -> '{}': {}",
                        from_array_path, to_array_path, err
                    ))
                })?;

                let moved_item = {
                    let source = query_json_path_mut(&mut root, &from_array_path).ok_or_else(|| {
                        PersistDomainError::Validation(format!(
                            "nested_move_where_eq source path '{}' not found",
                            from_array_path
                        ))
                    })?;
                    let source_array = source.as_array_mut().ok_or_else(|| {
                        PersistDomainError::Validation(format!(
                            "nested_move_where_eq source path '{}' is not an array",
                            from_array_path
                        ))
                    })?;
                    let index = source_array
                        .iter()
                        .position(|entry| query_json_path(entry, &match_field_path) == Some(&match_value))
                        .ok_or_else(|| {
                            PersistDomainError::Validation(format!(
                                "nested_move_where_eq could not find item in '{}' where '{}' == {}",
                                from_array_path, match_field_path, match_value
                            ))
                        })?;
                    source_array.remove(index)
                };

                let destination = query_json_path_mut(&mut root, &to_array_path).ok_or_else(|| {
                    PersistDomainError::Validation(format!(
                        "nested_move_where_eq destination path '{}' not found",
                        to_array_path
                    ))
                })?;
                let destination_array = destination.as_array_mut().ok_or_else(|| {
                    PersistDomainError::Validation(format!(
                        "nested_move_where_eq destination path '{}' is not an array",
                        to_array_path
                    ))
                })?;
                destination_array.push(moved_item);

                *model = serde_json::from_value(root).map_err(|err| {
                    PersistDomainError::Internal(format!(
                        "nested_move_where_eq model deserialization failed for '{}' -> '{}': {}",
                        from_array_path, to_array_path, err
                    ))
                })?;
                Ok(())
            })
            .await;

        map_nested_mutation_result(result)
    }
}

fn map_nested_mutation_result<M>(
    result: std::result::Result<
        (PersistAutonomousRecord<M>, ()),
        PersistDomainMutationError<PersistDomainError>,
    >,
) -> std::result::Result<PersistAutonomousRecord<M>, PersistDomainError> {
    result.map(|(record, ())| record).map_err(|error| match error {
        PersistDomainMutationError::Domain(domain) => domain,
        PersistDomainMutationError::User(user) => user,
    })
}

fn build_storage_where_clause(filters: &[PersistQueryFilter]) -> Option<Option<String>> {
    if filters.is_empty() {
        return Some(None);
    }

    let mut clauses = Vec::with_capacity(filters.len());
    for filter in filters {
        let field = sanitize_query_identifier(&filter.field)?;
        let clause = storage_filter_clause(field.as_str(), filter)?;
        clauses.push(clause);
    }

    Some(Some(clauses.join(" AND ")))
}

fn build_storage_order_clause(sort: Option<&PersistQuerySort>) -> Option<String> {
    match sort {
        Some(sort) => {
            let field = sanitize_query_identifier(&sort.field)?;
            let direction = match sort.direction {
                PersistQuerySortDirection::Asc => "ASC",
                PersistQuerySortDirection::Desc => "DESC",
            };
            Some(format!("{field} {direction}, __persist_id ASC"))
        }
        None => Some("__created_at ASC, __persist_id ASC".to_string()),
    }
}

fn sanitize_query_identifier(identifier: &str) -> Option<String> {
    if identifier.is_empty() {
        return None;
    }
    if identifier
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        Some(identifier.to_string())
    } else {
        None
    }
}

fn storage_filter_clause(field: &str, filter: &PersistQueryFilter) -> Option<String> {
    match filter.op {
        PersistQueryOp::Eq => {
            if filter.value.is_null() {
                Some(format!("{field} IS NULL"))
            } else {
                let literal = storage_sql_literal(&filter.value)?;
                Some(format!("{field} = {literal}"))
            }
        }
        PersistQueryOp::Ne => {
            if filter.value.is_null() {
                Some(format!("{field} IS NOT NULL"))
            } else {
                let literal = storage_sql_literal(&filter.value)?;
                Some(format!("{field} != {literal}"))
            }
        }
        PersistQueryOp::Gt => {
            let literal = storage_sql_literal(&filter.value)?;
            Some(format!("{field} > {literal}"))
        }
        PersistQueryOp::Gte => {
            let literal = storage_sql_literal(&filter.value)?;
            Some(format!("{field} >= {literal}"))
        }
        PersistQueryOp::Lt => {
            let literal = storage_sql_literal(&filter.value)?;
            Some(format!("{field} < {literal}"))
        }
        PersistQueryOp::Lte => {
            let literal = storage_sql_literal(&filter.value)?;
            Some(format!("{field} <= {literal}"))
        }
        PersistQueryOp::Contains => {
            let needle = filter.value.as_str()?;
            let escaped = crate::persist::sql_escape_string(needle);
            Some(format!("LOWER({field}) LIKE LOWER('%{escaped}%')"))
        }
    }
}

fn storage_sql_literal(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => Some("NULL".to_string()),
        serde_json::Value::Bool(flag) => Some(if *flag { "TRUE" } else { "FALSE" }.to_string()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::String(text) => Some(format!(
            "'{}'",
            crate::persist::sql_escape_string(text.as_str())
        )),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
}

async fn query_total_from_session(session: &PersistSession, sql: &str) -> Result<u64> {
    let result = session.query(sql).await?;
    let Some(row) = result.rows().first() else {
        return Ok(0);
    };
    let Some(value) = row.first() else {
        return Ok(0);
    };
    Ok(value
        .as_i64()
        .and_then(|count| u64::try_from(count).ok())
        .unwrap_or(0))
}

async fn query_ids_from_session(session: &PersistSession, sql: &str) -> Result<Vec<String>> {
    let result = session.query(sql).await?;
    let mut ids = Vec::with_capacity(result.row_count());
    for row in result.rows() {
        let Some(value) = row.first() else {
            continue;
        };
        match value {
            crate::core::Value::Text(id) => ids.push(id.clone()),
            crate::core::Value::Uuid(id) => ids.push(id.to_string()),
            other => ids.push(other.to_string()),
        }
    }
    Ok(ids)
}

async fn load_existing_rest_idempotency_replay(
    store: &mut PersistAggregateStore<PersistRestIdempotencyRecordVec>,
    scope_key: &str,
) -> Result<Option<(u16, serde_json::Value)>> {
    if let Some(existing) = store.find_first(|entry| entry.scope_key() == scope_key) {
        let body = serde_json::from_str::<serde_json::Value>(existing.response_body_json())
            .map_err(|err| {
                DbError::ExecutionError(format!(
                    "failed to decode cached idempotency payload for scope '{}': {}",
                    scope_key, err
                ))
            })?;
        return Ok(Some((status_code_from_persist(*existing.status_code()), body)));
    }

    let session = store.managed.session.clone();
    let mut candidate_tables = Vec::with_capacity(2);
    candidate_tables.push(<PersistRestIdempotencyRecord as PersistEntityFactory>::default_table_name());
    let logical_name = store.name().to_string();
    if !candidate_tables.iter().any(|table| table == &logical_name) {
        candidate_tables.push(logical_name);
    }

    for table_name in candidate_tables {
        let lookup_sql = format!(
            "SELECT status_code, response_body_json FROM {} WHERE scope_key = '{}' ORDER BY __updated_at DESC LIMIT 1",
            table_name,
            crate::persist::sql_escape_string(scope_key)
        );
        let query = match session.query(&lookup_sql).await {
            Ok(query) => query,
            Err(err) if is_missing_table_error(&err) => continue,
            Err(err) => return Err(err),
        };
        let Some(row) = query.rows().first() else {
            continue;
        };
        let Some(status_value) = row.first() else {
            continue;
        };
        let Some(body_value) = row.get(1) else {
            continue;
        };

        let status_code = status_value.as_i64().ok_or_else(|| {
            DbError::ExecutionError(format!(
                "idempotency receipt row in '{}' missing integer status_code for scope '{}'",
                table_name, scope_key
            ))
        })?;
        let response_body_json = match body_value {
            crate::core::Value::Text(text) => text.clone(),
            other => other.to_string(),
        };
        let body = serde_json::from_str::<serde_json::Value>(&response_body_json).map_err(
            |err| {
                DbError::ExecutionError(format!(
                    "failed to decode idempotency payload for scope '{}' from table '{}': {}",
                    scope_key, table_name, err
                ))
            },
        )?;
        return Ok(Some((status_code_from_persist(status_code), body)));
    }

    Ok(None)
}

fn is_missing_table_error(err: &DbError) -> bool {
    let message = err.to_string();
    message.contains("Table '") && message.contains("' not found")
}

fn query_filters_match(root: &serde_json::Value, filters: &[PersistQueryFilter]) -> bool {
    filters
        .iter()
        .all(|filter| query_filter_matches(root, filter))
}

fn query_filter_matches(root: &serde_json::Value, filter: &PersistQueryFilter) -> bool {
    let Some(actual) = query_json_path(root, filter.field.as_str()) else {
        return false;
    };
    match filter.op {
        PersistQueryOp::Eq => actual == &filter.value,
        PersistQueryOp::Ne => actual != &filter.value,
        PersistQueryOp::Contains => query_contains(actual, &filter.value),
        PersistQueryOp::Gt => query_compare_scalar(actual, &filter.value)
            .is_some_and(|ordering| ordering == Ordering::Greater),
        PersistQueryOp::Gte => query_compare_scalar(actual, &filter.value)
            .is_some_and(|ordering| ordering != Ordering::Less),
        PersistQueryOp::Lt => query_compare_scalar(actual, &filter.value)
            .is_some_and(|ordering| ordering == Ordering::Less),
        PersistQueryOp::Lte => query_compare_scalar(actual, &filter.value)
            .is_some_and(|ordering| ordering != Ordering::Greater),
    }
}

fn query_json_path<'a>(
    root: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = root;
    for segment in path.split('.').filter(|segment| !segment.is_empty()) {
        if let Ok(index) = segment.parse::<usize>() {
            current = current.as_array()?.get(index)?;
        } else {
            current = current.get(segment)?;
        }
    }
    Some(current)
}

fn query_json_path_mut<'a>(
    root: &'a mut serde_json::Value,
    path: &str,
) -> Option<&'a mut serde_json::Value> {
    let mut current = root;
    for segment in path.split('.').filter(|segment| !segment.is_empty()) {
        if let Ok(index) = segment.parse::<usize>() {
            current = current.as_array_mut()?.get_mut(index)?;
        } else {
            current = current.get_mut(segment)?;
        }
    }
    Some(current)
}

fn query_contains(actual: &serde_json::Value, needle: &serde_json::Value) -> bool {
    match (actual, needle) {
        (serde_json::Value::String(actual), serde_json::Value::String(needle)) => {
            actual.to_lowercase().contains(&needle.to_lowercase())
        }
        (serde_json::Value::Array(items), value) => items.iter().any(|item| item == value),
        _ => actual == needle,
    }
}

fn query_compare_scalar(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> Option<Ordering> {
    match (left, right) {
        (serde_json::Value::Number(left), serde_json::Value::Number(right)) => {
            left.as_f64()?.partial_cmp(&right.as_f64()?)
        }
        (serde_json::Value::String(left), serde_json::Value::String(right)) => Some(left.cmp(right)),
        (serde_json::Value::Bool(left), serde_json::Value::Bool(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn query_compare_values(
    left: Option<&serde_json::Value>,
    right: Option<&serde_json::Value>,
    direction: PersistQuerySortDirection,
) -> Ordering {
    let base = match (left, right) {
        (Some(left), Some(right)) => query_compare_scalar(left, right)
            .unwrap_or_else(|| left.to_string().cmp(&right.to_string())),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    };
    match direction {
        PersistQuerySortDirection::Asc => base,
        PersistQuerySortDirection::Desc => base.reverse(),
    }
}

fn merge_json_values(target: &mut serde_json::Value, patch: &serde_json::Value) {
    match (target, patch) {
        (serde_json::Value::Object(target_obj), serde_json::Value::Object(patch_obj)) => {
            for (key, patch_value) in patch_obj {
                if let Some(target_value) = target_obj.get_mut(key) {
                    merge_json_values(target_value, patch_value);
                } else {
                    target_obj.insert(key.clone(), patch_value.clone());
                }
            }
        }
        (target, patch) => {
            *target = patch.clone();
        }
    }
}
