impl std::fmt::Debug for PersistMigrationStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistMigrationStep")
            .field("from_version", &self.from_version)
            .field("to_version", &self.to_version)
            .field("sql_statements", &self.sql_statements)
            .field("has_state_migrator", &self.state_migrator.is_some())
            .finish()
    }
}

impl PersistMigrationStep {
    /// Creates a new migration step between two versions.
    pub fn new(from_version: u32, to_version: u32) -> Self {
        Self {
            from_version,
            to_version,
            sql_statements: Vec::new(),
            state_migrator: None,
        }
    }

    /// Adds a SQL statement to the migration step.
    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql_statements.push(sql.into());
        self
    }

    /// Adds multiple SQL statements to the migration step.
    pub fn with_sql_many<I, S>(mut self, sql_statements: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for sql in sql_statements {
            self.sql_statements.push(sql.into());
        }
        self
    }

    /// Adds a state migration function to the step.
    ///
    /// The function acts on the raw `PersistState` JSON fields.
    pub fn with_state_migrator<F>(mut self, migrator: F) -> Self
    where
        F: Fn(&mut PersistState) -> Result<()> + Send + Sync + 'static,
    {
        self.state_migrator = Some(Arc::new(migrator));
        self
    }
}
