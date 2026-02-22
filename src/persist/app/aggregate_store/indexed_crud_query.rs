impl<V> PersistAggregateStore<V>
where
    V: PersistIndexedCollection,
{
    /// Returns a slice of all items in the collection.
    ///
    /// This returns a reference to the in-memory cache of items.
    pub fn list(&self) -> &[V::Item] {
        self.managed.list()
    }

    /// Retrieves an item by its persistence ID.
    ///
    /// Returns `None` if the item does not exist or has not been loaded.
    pub fn get(&self, persist_id: &str) -> Option<&V::Item> {
        self.managed.get(persist_id)
    }

    /// Returns a page of items from the collection.
    ///
    /// # Arguments
    /// * `offset` - The number of items to skip.
    /// * `limit` - The maximum number of items to return.
    pub fn list_page(&self, offset: usize, limit: usize) -> Vec<&V::Item> {
        self.managed.list_page(offset, limit)
    }

    /// Returns a list of items that match the given predicate.
    pub fn list_filtered<F>(&self, predicate: F) -> Vec<&V::Item>
    where
        F: Fn(&V::Item) -> bool,
    {
        self.managed.list_filtered(predicate)
    }

    /// Returns a list of items sorted by the given comparison function.
    pub fn list_sorted_by<F>(&self, compare: F) -> Vec<&V::Item>
    where
        F: FnMut(&V::Item, &V::Item) -> Ordering,
    {
        self.managed.list_sorted_by(compare)
    }

    /// Finds the first item that matches the given predicate.
    pub fn find_first<F>(&self, mut predicate: F) -> Option<V::Item>
    where
        V::Item: Clone,
        F: FnMut(&V::Item) -> bool,
    {
        self.list().iter().find(|item| predicate(item)).cloned()
    }

    /// Returns a paginated result set with filtering and sorting applied.
    ///
    /// This is a high-level query method useful for API endpoints.
    ///
    /// # Arguments
    /// * `page` - The page number (1-based).
    /// * `per_page` - The number of items per page.
    /// * `filter` - A predicate to filter items.
    /// * `compare` - A comparison function for sorting.
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
        let page = page.max(1);
        let per_page = per_page.max(1);

        let mut items = self
            .list_filtered(filter)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by(compare);

        let total = u64::try_from(items.len()).unwrap_or(u64::MAX);
        let total_pages = if total == 0 {
            0
        } else {
            total.div_ceil(u64::from(per_page)) as u32
        };

        let offset = usize::try_from(page.saturating_sub(1)).unwrap_or(usize::MAX)
            * usize::try_from(per_page).unwrap_or(usize::MAX);
        let paged_items = items
            .into_iter()
            .skip(offset)
            .take(usize::try_from(per_page).unwrap_or(usize::MAX))
            .collect();

        PersistAggregatePage {
            items: paged_items,
            page,
            per_page,
            total,
            total_pages,
        }
    }

    /// Persists a new item to the store.
    pub async fn create(&mut self, item: V::Item) -> Result<()> {
        self.managed.create(item).await
    }

    /// Persists multiple new items to the store in a single batch.
    pub async fn create_many(&mut self, items: Vec<V::Item>) -> Result<usize> {
        self.managed.create_many(items).await
    }

    /// Updates an existing item using a mutation closure.
    ///
    /// The closure receives a mutable reference to the item.
    /// Changes are automatically detected and persisted.
    pub async fn update<F>(&mut self, persist_id: &str, mutator: F) -> Result<bool>
    where
        F: FnOnce(&mut V::Item) -> Result<()>,
    {
        self.managed.update(persist_id, mutator).await
    }

    /// Updates one entity and preserves user-defined mutator errors.
    pub async fn update_with<F, E>(
        &mut self,
        persist_id: &str,
        mutator: F,
    ) -> Result<std::result::Result<bool, E>>
    where
        F: FnOnce(&mut V::Item) -> std::result::Result<(), E>,
    {
        self.managed.update_with(persist_id, mutator).await
    }

    /// Applies a mutation to multiple items identified by their persistence IDs.
    pub async fn apply_many<F>(&mut self, persist_ids: &[String], mutator: F) -> Result<usize>
    where
        F: Fn(&mut V::Item) -> Result<()>,
    {
        self.managed.apply_many(persist_ids, mutator).await
    }

    /// Applies mutations to many entities and preserves user-defined mutator errors.
    pub async fn apply_many_with<F, E>(
        &mut self,
        persist_ids: &[String],
        mutator: F,
    ) -> Result<std::result::Result<usize, E>>
    where
        F: Fn(&mut V::Item) -> std::result::Result<(), E>,
    {
        self.managed.apply_many_with(persist_ids, mutator).await
    }

    /// Deletes an item by its persistence ID.
    pub async fn delete(&mut self, persist_id: &str) -> Result<bool> {
        self.managed.delete(persist_id).await
    }

    /// Deletes multiple items by their persistence IDs.
    pub async fn delete_many(&mut self, persist_ids: &[String]) -> Result<usize> {
        self.managed.delete_many(persist_ids).await
    }
}
