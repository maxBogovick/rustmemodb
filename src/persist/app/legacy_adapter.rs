use super::*;

/// Adapter for legacy persistence operations.
///
/// Wraps a `ManagedPersistVec` and exposes a simpler, but less feature-rich interface
/// compatible with older parts of the codebase.
/// NOTE: Direct use of `ManagedPersistVec` is preferred for new code.
impl<V: PersistCollection> LegacyPersistVecAdapter<V> {
    pub(super) fn new(managed: ManagedPersistVec<V>) -> Self {
        Self { managed }
    }

    /// Returns the name of the collection.
    pub fn name(&self) -> &str {
        self.managed.name()
    }

    /// Returns the number of items in the collection.
    pub fn len(&self) -> usize {
        self.managed.collection().len()
    }

    /// Returns true if the collection is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns statistics about the managed collection.
    pub fn stats(&self) -> ManagedPersistVecStats {
        self.managed.stats()
    }

    /// Takes a snapshot of the current state.
    pub fn snapshot(&self, mode: SnapshotMode) -> V::Snapshot {
        self.managed.collection().snapshot(mode)
    }

    /// Persists all changes to storage.
    pub async fn save_all(&mut self) -> Result<()> {
        self.managed.save().await
    }

    /// Forces a snapshot and replication.
    pub async fn force_snapshot(&mut self) -> Result<()> {
        self.managed.force_snapshot().await
    }

    /// Restores state from a snapshot using FailFast conflict policy.
    pub async fn restore(&mut self, snapshot: V::Snapshot) -> Result<()> {
        self.restore_with_policy(snapshot, RestoreConflictPolicy::FailFast)
            .await
    }

    /// Restores state from a snapshot with a specific conflict policy.
    pub async fn restore_with_policy(
        &mut self,
        snapshot: V::Snapshot,
        conflict_policy: RestoreConflictPolicy,
    ) -> Result<()> {
        self.managed
            .collection
            .restore_with_policy(snapshot, &self.managed.session, conflict_policy)
            .await?;
        self.managed.force_snapshot().await
    }

    /// Consumes the adapter and returns the underlying `ManagedPersistVec`.
    pub fn into_managed(self) -> ManagedPersistVec<V> {
        self.managed
    }
}

impl<V> LegacyPersistVecAdapter<V>
where
    V: PersistIndexedCollection,
{
    /// Adds a single item directly to the collection.
    ///
    /// WARNING: This bypasses managed transaction safety checks (though `save_all` will still validate).
    pub fn add_one(&mut self, item: V::Item) {
        self.managed.collection.add_one(item);
    }

    /// Adds multiple items directly to the collection.
    pub fn add_many(&mut self, items: Vec<V::Item>) {
        self.managed.collection.add_many(items);
    }

    /// Removes an item by its persistent ID.
    pub fn remove_by_persist_id(&mut self, persist_id: &str) -> Option<V::Item> {
        self.managed.collection.remove_by_persist_id(persist_id)
    }

    /// Returns a reference to the items slice.
    pub fn items(&self) -> &[V::Item] {
        self.managed.collection.items()
    }

    /// Returns a mutable reference to the items slice.
    ///
    /// WARNING: Direct mutation bypasses managed tracking.
    pub fn items_mut(&mut self) -> &mut [V::Item] {
        self.managed.collection.items_mut()
    }
}
