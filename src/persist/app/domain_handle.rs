impl<V: PersistCollection> PersistDomainHandle<V> {
    pub(crate) fn new(store: PersistDomainStore<V>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(store)),
        }
    }

    /// Returns the logical collection name.
    pub async fn name(&self) -> String {
        let store = self.inner.lock().await;
        store.name().to_string()
    }

    /// Returns runtime statistics for the underlying collection.
    pub async fn stats(&self) -> ManagedPersistVecStats {
        let store = self.inner.lock().await;
        store.stats()
    }

    /// Appends an audit event for one aggregate instance.
    ///
    /// This helper lets high-level APIs publish operation history without exposing
    /// application code to the underlying audit collection mechanics.
    pub async fn append_audit_for(
        &self,
        aggregate_persist_id: impl AsRef<str>,
        event_type: impl Into<String>,
        message: impl Into<String>,
        resulting_version: i64,
    ) -> Result<()> {
        let aggregate_persist_id = aggregate_persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store
            .audits
            .create(PersistAuditRecord::new(
                aggregate_persist_id.to_string(),
                event_type.into(),
                message.into(),
                resulting_version,
            ))
            .await
    }
}

impl<V> PersistDomainHandle<V>
where
    V: PersistIndexedCollection,
{
    /// Returns a cloned list of all entities.
    pub async fn list(&self) -> Vec<V::Item>
    where
        V::Item: Clone,
    {
        let store = self.inner.lock().await;
        store.list().to_vec()
    }

    /// Returns one entity by id as an owned value.
    pub async fn get_one(&self, persist_id: impl AsRef<str>) -> Option<V::Item>
    where
        V::Item: Clone,
    {
        let persist_id = persist_id.as_ref();
        let store = self.inner.lock().await;
        store.get(persist_id).cloned()
    }

    /// Finds the first entity matching the predicate.
    pub async fn find_first<F>(&self, predicate: F) -> Option<V::Item>
    where
        V::Item: Clone,
        F: FnMut(&V::Item) -> bool,
    {
        let store = self.inner.lock().await;
        store.find_first(predicate)
    }

    /// Returns a paginated, filtered, and sorted page as owned entities.
    pub async fn query_page_filtered_sorted<F, C>(
        &self,
        page: u32,
        per_page: u32,
        filter: F,
        compare: C,
    ) -> PersistAggregatePage<V::Item>
    where
        V::Item: Clone,
        F: Fn(&V::Item) -> bool,
        C: FnMut(&V::Item, &V::Item) -> Ordering,
    {
        let store = self.inner.lock().await;
        store.query_page_filtered_sorted(page, per_page, filter, compare)
    }

    /// Returns audits for a specific entity as owned records.
    pub async fn list_audits_for(
        &self,
        aggregate_persist_id: impl AsRef<str>,
    ) -> Vec<PersistAuditRecord> {
        let aggregate_persist_id = aggregate_persist_id.as_ref();
        let store = self.inner.lock().await;
        store
            .list_audits_for(aggregate_persist_id)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Creates one entity.
    pub async fn create(&self, item: V::Item) -> Result<()> {
        let mut store = self.inner.lock().await;
        store.create(item).await
    }

    /// Creates multiple entities.
    pub async fn create_many(&self, items: Vec<V::Item>) -> Result<usize> {
        let mut store = self.inner.lock().await;
        store.create_many(items).await
    }

    /// Creates one entity and returns domain-level outcome.
    pub async fn create_one(
        &self,
        item: V::Item,
    ) -> std::result::Result<V::Item, PersistDomainError>
    where
        V::Item: Clone,
    {
        let mut store = self.inner.lock().await;
        store.create_one(item).await
    }

    /// Applies a closure mutation to one entity and returns updated state.
    pub async fn mutate_one<F>(
        &self,
        persist_id: impl AsRef<str>,
        mutator: F,
    ) -> std::result::Result<V::Item, PersistDomainError>
    where
        V::Item: Clone,
        F: FnOnce(&mut V::Item) -> Result<()>,
    {
        let persist_id = persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store.mutate_one(persist_id, mutator).await
    }

    /// Applies a closure mutation to one entity and preserves user-level mutator errors.
    pub async fn mutate_one_with<F, E>(
        &self,
        persist_id: impl AsRef<str>,
        mutator: F,
    ) -> std::result::Result<V::Item, PersistDomainMutationError<E>>
    where
        V::Item: Clone,
        F: FnOnce(&mut V::Item) -> std::result::Result<(), E>,
    {
        let persist_id = persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store.mutate_one_with(persist_id, mutator).await
    }

    /// Applies a closure mutation to multiple entities.
    pub async fn mutate_many<F>(
        &self,
        persist_ids: &[String],
        mutator: F,
    ) -> std::result::Result<u64, PersistDomainError>
    where
        F: Fn(&mut V::Item) -> Result<()>,
    {
        let mut store = self.inner.lock().await;
        store.mutate_many(persist_ids, mutator).await
    }

    /// Applies a closure mutation to multiple entities and preserves user-level mutator errors.
    pub async fn mutate_many_with<F, E>(
        &self,
        persist_ids: &[String],
        mutator: F,
    ) -> std::result::Result<u64, PersistDomainMutationError<E>>
    where
        F: Fn(&mut V::Item) -> std::result::Result<(), E>,
    {
        let mut store = self.inner.lock().await;
        store.mutate_many_with(persist_ids, mutator).await
    }
}

impl<V> PersistDomainHandle<V>
where
    V: PersistIndexedCollection,
    V::Item: PersistCommandModel + Clone,
    <V::Item as PersistCommandModel>::Command: PersistCommandName,
{
    /// Executes one intent and returns domain-level outcome.
    pub async fn intent_one<C>(
        &self,
        persist_id: impl AsRef<str>,
        command: C,
    ) -> std::result::Result<V::Item, PersistDomainError>
    where
        C: PersistAutonomousCommand<V::Item>,
    {
        let persist_id = persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store.intent_one(persist_id, command).await
    }

    /// Executes one intent and returns optional entity when found.
    pub async fn intent<C>(
        &self,
        persist_id: impl AsRef<str>,
        command: C,
    ) -> Result<Option<V::Item>>
    where
        C: PersistAutonomousCommand<V::Item>,
    {
        let persist_id = persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store.intent(persist_id, command).await
    }

    /// Executes one patch and returns domain-level outcome.
    pub async fn patch_one(
        &self,
        persist_id: impl AsRef<str>,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> std::result::Result<V::Item, PersistDomainError> {
        let persist_id = persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store.patch_one(persist_id, patch).await
    }

    /// Executes one patch and returns optional entity when found.
    pub async fn patch(
        &self,
        persist_id: impl AsRef<str>,
        patch: <V::Item as PersistCommandModel>::Patch,
    ) -> Result<Option<V::Item>> {
        let persist_id = persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store.patch(persist_id, patch).await
    }

    /// Executes one delete and returns domain-level outcome.
    pub async fn remove_one(
        &self,
        persist_id: impl AsRef<str>,
    ) -> std::result::Result<(), PersistDomainError> {
        let persist_id = persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store.remove_one(persist_id).await
    }

    /// Executes one delete and returns `true` when entity existed.
    pub async fn remove(&self, persist_id: impl AsRef<str>) -> Result<bool> {
        let persist_id = persist_id.as_ref();
        let mut store = self.inner.lock().await;
        store.remove(persist_id).await
    }
}
