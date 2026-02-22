impl<V: PersistCollection> PersistAggregateStore<V> {
    /// Creates a new `PersistAggregateStore` wrapping the given managed vector.
    ///
    /// This is an internal constructor used by the `PersistApp` builder.
    pub(super) fn new(managed: ManagedPersistVec<V>) -> Self {
        Self { managed }
    }

    /// Returns the name of the underlying collection (table name).
    pub fn name(&self) -> &str {
        self.managed.name()
    }

    /// Returns current statistics for the underlying collection.
    ///
    /// Includes counts of items, loaded items, and memory usage estimates.
    pub fn stats(&self) -> ManagedPersistVecStats {
        self.managed.stats()
    }

    /// Consumes the aggregate store and returns the underlying `ManagedPersistVec`.
    ///
    /// This is useful if you need to access lower-level APIs on the managed vector directly.
    pub fn into_managed(self) -> ManagedPersistVec<V> {
        self.managed
    }
}
