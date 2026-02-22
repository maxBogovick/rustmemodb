impl HeteroPersistVec {
    /// Adds a boxed entity to the collection.
    ///
    /// Fails if the entity's type has not been registered.
    pub fn add_boxed(&mut self, item: Box<dyn PersistEntity>) -> Result<()> {
        let type_name = item.type_name().to_string();
        if !self.registrations.contains_key(&type_name) {
            return Err(DbError::ExecutionError(format!(
                "Type '{}' is not registered in hetero persist vec",
                type_name
            )));
        }

        self.items.push(item);
        Ok(())
    }

    /// Adds a single entity to the collection.
    pub fn add_one<T>(&mut self, item: T) -> Result<()>
    where
        T: PersistEntity + 'static,
    {
        self.add_boxed(Box::new(item))
    }

    /// Adds multiple boxed entities to the collection.
    pub fn add_many_boxed<I>(&mut self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = Box<dyn PersistEntity>>,
    {
        for item in items {
            self.add_boxed(item)?;
        }
        Ok(())
    }

    /// Adds multiple entities of the same type to the collection.
    pub fn add_many<T, I>(&mut self, items: I) -> Result<()>
    where
        T: PersistEntity + 'static,
        I: IntoIterator<Item = T>,
    {
        for item in items {
            self.add_one(item)?;
        }
        Ok(())
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
}
