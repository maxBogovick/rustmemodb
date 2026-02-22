impl<V: PersistCollection> PersistAutonomousAggregate<V> {
    pub(super) fn new(
        aggregate: PersistAggregateStore<V>,
        audits: PersistAggregateStore<PersistAuditRecordVec>,
        rest_idempotency: PersistAggregateStore<PersistRestIdempotencyRecordVec>,
        conflict_retry: PersistConflictRetryPolicy,
    ) -> Self {
        Self {
            aggregate,
            audits,
            rest_idempotency,
            conflict_retry,
        }
    }

    /// Returns the name of the underlying collection.
    pub fn name(&self) -> &str {
        self.aggregate.name()
    }

    /// Returns runtime statistics for the aggregate collection.
    pub fn stats(&self) -> ManagedPersistVecStats {
        self.aggregate.stats()
    }
}

impl<V> PersistAutonomousAggregate<V>
where
    V: PersistIndexedCollection,
{
    /// Returns a list of all items in the aggregate.
    pub fn list(&self) -> &[V::Item] {
        self.aggregate.list()
    }

    /// Retrieves an item by its persistence ID.
    pub fn get(&self, persist_id: &str) -> Option<&V::Item> {
        self.aggregate.get(persist_id)
    }

    /// Finds the first item matching the predicate.
    pub fn find_first<F>(&self, predicate: F) -> Option<V::Item>
    where
        V::Item: Clone,
        F: FnMut(&V::Item) -> bool,
    {
        self.aggregate.find_first(predicate)
    }

    /// Returns a paginated, filtered, and sorted result set.
    ///
    /// Delegates to `PersistAggregateStore::query_page_filtered_sorted`.
    pub fn query_page_filtered_sorted<F, C>(
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
        self.aggregate
            .query_page_filtered_sorted(page, per_page, filter, compare)
    }

    /// Retrieves audit records associated with a specific aggregate instance.
    pub fn list_audits_for(&self, aggregate_persist_id: &str) -> Vec<&PersistAuditRecord> {
        self.audits
            .list_filtered(|event| event.aggregate_persist_id() == aggregate_persist_id)
    }

    /// creating a new aggregate instance.
    ///
    /// This is a direct creation and does not generate an audit record by default
    /// unless the item itself manages that (which is rare for `create`).
    /// For audited creation, standard usage patterns typically prefer explicit commands or factory methods.
    pub async fn create(&mut self, item: V::Item) -> Result<()> {
        self.aggregate.create(item).await
    }

    /// Creates multiple aggregate instances in a batch.
    pub async fn create_many(&mut self, items: Vec<V::Item>) -> Result<usize> {
        self.aggregate.create_many(items).await
    }

    /// Creates an aggregate and returns the persisted entity as a domain outcome.
    ///
    /// Compared to `create(...) -> Result<()>`, this API is designed for
    /// application services that should not perform follow-up `get(...)` calls
    /// or inspect `DbError` internals.
    pub async fn create_one(
        &mut self,
        item: V::Item,
    ) -> std::result::Result<V::Item, PersistDomainError>
    where
        V::Item: Clone,
    {
        let persist_id = item.persist_id().to_string();
        self.create(item).await.map_err(PersistDomainError::from)?;
        self.get(&persist_id).cloned().ok_or_else(|| {
            PersistDomainError::Internal(format!(
                "entity '{}' missing after successful create in '{}'",
                persist_id,
                self.name()
            ))
        })
    }
}
