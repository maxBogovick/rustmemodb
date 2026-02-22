impl PersistMigrationPlan {
    /// Creates a new empty migration plan for a target version.
    pub fn new(current_version: u32) -> Self {
        Self {
            current_version,
            steps: Vec::new(),
        }
    }

    /// Returns the target schema version of this plan.
    pub fn current_version(&self) -> u32 {
        self.current_version
    }

    /// Returns the list of registered migration steps.
    pub fn steps(&self) -> &[PersistMigrationStep] {
        &self.steps
    }

    /// Adds a migration step to the plan, validating it immediately.
    pub fn add_step(&mut self, step: PersistMigrationStep) -> Result<()> {
        self.steps.push(step);
        self.validate()
    }

    /// Fluent builder method to add a step.
    pub fn with_step(mut self, step: PersistMigrationStep) -> Result<Self> {
        self.add_step(step)?;
        Ok(self)
    }

    /// Helper to add a SQL-only migration step.
    pub fn add_sql_step(
        &mut self,
        from_version: u32,
        to_version: u32,
        sql: impl Into<String>,
    ) -> Result<()> {
        self.add_step(PersistMigrationStep::new(from_version, to_version).with_sql(sql))
    }

    /// Helper to add a state-migration-only step.
    pub fn add_state_step<F>(
        &mut self,
        from_version: u32,
        to_version: u32,
        migrator: F,
    ) -> Result<()>
    where
        F: Fn(&mut PersistState) -> Result<()> + Send + Sync + 'static,
    {
        self.add_step(PersistMigrationStep::new(from_version, to_version).with_state_migrator(migrator))
    }

    /// Validates the integrity of the migration plan.
    ///
    /// Checks for:
    /// - version validity (>= 1),
    /// - step direction (`from < to`),
    /// - step bounds (`to <= current`),
    /// - duplicate steps.
    pub fn validate(&self) -> Result<()> {
        if self.current_version == 0 {
            return Err(DbError::ExecutionError(
                "Schema version must be >= 1".to_string(),
            ));
        }

        let mut seen_from = HashSet::<u32>::new();
        for step in &self.steps {
            if step.from_version == 0 {
                return Err(DbError::ExecutionError(
                    "Migration 'from_version' must be >= 1".to_string(),
                ));
            }
            if step.to_version <= step.from_version {
                return Err(DbError::ExecutionError(format!(
                    "Migration step {} -> {} is invalid",
                    step.from_version, step.to_version
                )));
            }
            if step.to_version > self.current_version {
                return Err(DbError::ExecutionError(format!(
                    "Migration step {} -> {} exceeds current schema version {}",
                    step.from_version, step.to_version, self.current_version
                )));
            }
            if !seen_from.insert(step.from_version) {
                return Err(DbError::ExecutionError(format!(
                    "Duplicate migration step starting at version {}",
                    step.from_version
                )));
            }
        }

        Ok(())
    }

    /// Resolves ordered migration edges from `from_version` to `current_version`.
    fn resolve_chain(&self, from_version: u32) -> Result<Vec<&PersistMigrationStep>> {
        if from_version > self.current_version {
            return Err(DbError::ExecutionError(format!(
                "Cannot migrate down from schema version {} to {}",
                from_version, self.current_version
            )));
        }

        if from_version == self.current_version {
            return Ok(Vec::new());
        }

        let mut by_from = HashMap::<u32, &PersistMigrationStep>::new();
        for step in &self.steps {
            by_from.insert(step.from_version, step);
        }

        let mut cursor = from_version;
        let mut chain = Vec::new();
        while cursor < self.current_version {
            let step = by_from.get(&cursor).copied().ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Missing migration step starting at version {} for target {}",
                    cursor, self.current_version
                ))
            })?;

            if step.to_version <= cursor || step.to_version > self.current_version {
                return Err(DbError::ExecutionError(format!(
                    "Invalid migration chain edge {} -> {}",
                    step.from_version, step.to_version
                )));
            }

            chain.push(step);
            cursor = step.to_version;
        }

        Ok(chain)
    }
}
