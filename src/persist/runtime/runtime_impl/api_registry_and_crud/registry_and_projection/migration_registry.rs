impl PersistEntityRuntime {
    /// Registers a simple alias for a command, effectively renaming it while keeping the payload same.
    pub fn register_command_alias(
        &mut self,
        entity_type: impl Into<String>,
        from_command: impl Into<String>,
        from_payload_version: u32,
        to_command: impl Into<String>,
    ) -> Result<()> {
        self.register_command_migration(
            entity_type,
            from_command,
            from_payload_version,
            to_command,
            1,
            Arc::new(|payload| Ok(payload.clone())),
        )
    }

    /// Registers a full migration rule that transforms a commande payload from one version to another.
    pub fn register_command_migration(
        &mut self,
        entity_type: impl Into<String>,
        from_command: impl Into<String>,
        from_payload_version: u32,
        to_command: impl Into<String>,
        to_payload_version: u32,
        transform: RuntimeCommandPayloadMigration,
    ) -> Result<()> {
        let entity_type = entity_type.into();
        let from_command = from_command.into();
        let to_command = to_command.into();

        if entity_type.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Command migration entity_type must not be empty".to_string(),
            ));
        }
        if from_command.trim().is_empty() || to_command.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Command migration command names must not be empty".to_string(),
            ));
        }
        if from_payload_version == 0 || to_payload_version == 0 {
            return Err(DbError::ExecutionError(
                "Command migration payload versions must be >= 1".to_string(),
            ));
        }

        let rule = RuntimeCommandMigrationRule {
            descriptor: RuntimeCommandMigrationDescriptor {
                from_command,
                from_payload_version,
                to_command,
                to_payload_version,
            },
            transform,
        };

        let rules = self
            .command_migration_registry
            .entry(entity_type)
            .or_default();
        if let Some(existing) = rules.iter_mut().find(|existing| {
            existing.descriptor.from_command == rule.descriptor.from_command
                && existing.descriptor.from_payload_version == rule.descriptor.from_payload_version
        }) {
            *existing = rule;
        } else {
            rules.push(rule);
        }
        Ok(())
    }

    /// Lists all registered command migrations for a given entity type.
    pub fn list_command_migrations(
        &self,
        entity_type: &str,
    ) -> Vec<RuntimeCommandMigrationDescriptor> {
        self.command_migration_registry
            .get(entity_type)
            .map(|rules| {
                let mut descriptors = rules
                    .iter()
                    .map(|rule| rule.descriptor.clone())
                    .collect::<Vec<_>>();
                descriptors.sort_by(|a, b| {
                    a.from_command
                        .cmp(&b.from_command)
                        .then(a.from_payload_version.cmp(&b.from_payload_version))
                });
                descriptors
            })
            .unwrap_or_default()
    }
}
