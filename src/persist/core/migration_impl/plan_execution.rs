impl PersistMigrationPlan {
    /// Migrates an in-memory `PersistState` to the current version.
    pub fn migrate_state_to_current(&self, state: &mut PersistState) -> Result<()> {
        self.validate()?;

        let from_version = state.metadata.schema_version;
        if from_version == self.current_version {
            return Ok(());
        }

        let chain = self.resolve_chain(from_version)?;
        for step in chain {
            if let Some(migrator) = &step.state_migrator {
                migrator(state)?;
            }
            state.metadata.schema_version = step.to_version;
        }

        Ok(())
    }

    /// Validates and executes SQL migrations on a specific table.
    ///
    /// Replaces `{table}` placeholder in SQL statements with `table_name`.
    pub async fn migrate_table_from(
        &self,
        session: &PersistSession,
        table_name: &str,
        from_version: u32,
    ) -> Result<()> {
        self.validate()?;

        let chain = self.resolve_chain(from_version)?;
        for step in chain {
            for sql in &step.sql_statements {
                let rendered_sql = sql.replace("{table}", table_name);
                session.execute(&rendered_sql).await?;
            }
        }

        session
            .set_table_schema_version(table_name, self.current_version)
            .await?;
        Ok(())
    }

    /// Ensures the table schema is up-to-date with the current plan.
    ///
    /// Reads the current version from the database and runs migrations if needed.
    pub async fn ensure_table_schema_version(
        &self,
        session: &PersistSession,
        table_name: &str,
    ) -> Result<()> {
        self.validate()?;

        if let Some(current_table_version) = session.get_table_schema_version(table_name).await? {
            // Forward-compatible mode: a table can be ahead of the current runtime plan.
            // This allows restoring snapshots migrated by a newer plan while still operating
            // on known columns/fields.
            if current_table_version > self.current_version {
                return Ok(());
            }
            if current_table_version < self.current_version {
                return self
                    .migrate_table_from(session, table_name, current_table_version)
                    .await;
            }
            return Ok(());
        }

        session
            .set_table_schema_version(table_name, self.current_version)
            .await
    }
}
