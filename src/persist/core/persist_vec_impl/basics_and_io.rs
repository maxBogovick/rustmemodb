impl<T: PersistEntityFactory> PersistVec<T> {
    /// Creates a new, empty persistence collection name `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            items: Vec::new(),
        }
    }

    /// Returns the name of the collection.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the number of items in the collection.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if the collection is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns a slice of the items in the collection.
    pub fn items(&self) -> &[T] {
        &self.items
    }

    /// Returns a mutable slice of the items in the collection.
    pub fn items_mut(&mut self) -> &mut [T] {
        &mut self.items
    }

    /// Adds a single item to the collection.
    pub fn add_one(&mut self, item: T) {
        self.items.push(item);
    }

    /// Adds multiple items to the collection.
    pub fn add_many<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.items.extend(items);
    }

    /// Removes an item by its persistence ID, returning it if found.
    pub fn remove_by_persist_id(&mut self, persist_id: &str) -> Option<T> {
        let position = self
            .items
            .iter()
            .position(|item| item.persist_id() == persist_id)?;
        Some(self.items.remove(position))
    }

    /// Captures the current state of all items in the collection.
    pub fn states(&self) -> Vec<PersistState> {
        self.items.iter().map(|item| item.state()).collect()
    }

    /// Returns descriptors for all items in the collection.
    pub fn descriptors(&self) -> Vec<ObjectDescriptor> {
        self.items.iter().map(|item| item.descriptor()).collect()
    }

    /// Returns a frequency map of available dynamic functions across all items.
    pub fn functions_catalog(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for item in &self.items {
            for function in item.available_functions() {
                *counts.entry(function.name).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Ensures that the backing tables for all items exist in the database.
    pub async fn ensure_all_tables(&mut self, session: &PersistSession) -> Result<()> {
        for item in &mut self.items {
            item.ensure_table(session).await?;
        }
        Ok(())
    }

    /// Saves all items in the collection to the database.
    pub async fn save_all(&mut self, session: &PersistSession) -> Result<()> {
        for item in &mut self.items {
            item.save(session).await?;
        }
        Ok(())
    }
}
