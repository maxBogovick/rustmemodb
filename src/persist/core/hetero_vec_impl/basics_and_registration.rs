impl HeteroPersistVec {
    /// Creates a new, empty heterogeneous persistence collection with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            items: Vec::new(),
            registrations: HashMap::new(),
        }
    }

    /// Returns the name of this collection.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the number of items currently in the collection.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if the collection contains no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns a sorted list of type names registered with this collection.
    pub fn registered_types(&self) -> Vec<String> {
        let mut names = self.registrations.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    /// Registers a new entity type with the collection, using its default migration plan.
    ///
    /// This is required before adding items of this type or restoring them from snapshots.
    pub fn register_type<T>(&mut self)
    where
        T: PersistEntityFactory + 'static,
    {
        self.register_type_with_migration_plan::<T>(T::migration_plan());
    }

    /// Registers a new entity type with a specific migration plan.
    pub fn register_type_with_migration_plan<T>(&mut self, migration_plan: PersistMigrationPlan)
    where
        T: PersistEntityFactory + 'static,
    {
        let type_name = T::entity_type_name().to_string();
        let schema_version = migration_plan.current_version();
        let plan_clone = migration_plan.clone();

        let registration = PersistTypeRegistration {
            default_table_name: Arc::new(T::default_table_name),
            create_table_sql: Arc::new(T::create_table_sql),
            from_state: Arc::new(|state| {
                let item = T::from_state(state)?;
                Ok(Box::new(item) as Box<dyn PersistEntity>)
            }),
            migration_plan: Arc::new(move || plan_clone.clone()),
            schema_version: Arc::new(move || schema_version),
        };
        self.registrations.insert(type_name, registration);
    }
}
