/// Core trait for entities that can be persisted.
///
/// Implementors must provide metadata access, serialization state, and lifecycle hooks.
#[async_trait]
pub trait PersistEntity: Send + Sync {
    /// Returns the unique type name of the entity.
    fn type_name(&self) -> &'static str;
    /// Returns the database table name where this entity is stored.
    fn table_name(&self) -> &str;
    /// Returns the unique persistence ID of the entity.
    fn persist_id(&self) -> &str;
    /// Returns a reference to the persistence metadata.
    fn metadata(&self) -> &PersistMetadata;
    /// Returns a mutable reference to the persistence metadata.
    fn metadata_mut(&mut self) -> &mut PersistMetadata;
    /// Returns a list of field names that must be unique.
    fn unique_fields(&self) -> Vec<&'static str> {
        Vec::new()
    }
    /// Returns a list of field names that should be indexed.
    fn indexed_fields(&self) -> Vec<&'static str> {
        Vec::new()
    }
    /// Returns a descriptor of the object's interface.
    fn descriptor(&self) -> ObjectDescriptor;
    /// Serializes the entity into `PersistState`.
    fn state(&self) -> PersistState;
    /// Checks if the entity supports a specific dynamic function.
    fn supports_function(&self, function: &str) -> bool;
    /// Lists all available dynamic functions for this entity.
    fn available_functions(&self) -> Vec<FunctionDescriptor>;
    /// Ensures the backing table exists in the session's database.
    async fn ensure_table(&mut self, session: &PersistSession) -> Result<()>;
    /// Saves the entity's state to the database.
    async fn save(&mut self, session: &PersistSession) -> Result<()>;
    /// Deletes the entity from the database.
    async fn delete(&mut self, session: &PersistSession) -> Result<()>;
    /// Invokes a named dynamic function on the entity.
    async fn invoke(
        &mut self,
        function: &str,
        args: Vec<Value>,
        session: &PersistSession,
    ) -> Result<Value>;
}

/// Trait for factories that can create `PersistEntity` instances.
#[async_trait]
pub trait PersistEntityFactory: PersistEntity + Sized {
    /// Returns the static type name.
    fn entity_type_name() -> &'static str;
    /// Returns the default table name for this type.
    fn default_table_name() -> String;
    /// Generates the SQL to create the table.
    fn create_table_sql(table_name: &str) -> String;
    /// Reconstructs the entity from `PersistState`.
    fn from_state(state: &PersistState) -> Result<Self>;

    /// Returns the current schema version.
    fn schema_version() -> u32 {
        default_schema_version()
    }

    /// Returns the migration plan for this entity type.
    fn migration_plan() -> PersistMigrationPlan {
        PersistMigrationPlan::new(Self::schema_version())
    }

    /// Restores the entity into the database (e.g., during snapshot restoration).
    async fn restore_into_db(&mut self, session: &PersistSession) -> Result<()> {
        self.save(session).await
    }
}

/// Extension trait to convert domain models into their persisted counterparts.
pub trait PersistModelExt: Sized {
    type Persisted: PersistEntityFactory + Send + Sync + 'static;

    fn into_persisted(self) -> Self::Persisted;
}

/// Describes a patchable field in a persistent model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistPatchContract {
    pub field: String,
    pub rust_type: String,
    pub optional: bool,
}

/// Describes a field in a command for a persistent model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistCommandFieldContract {
    pub name: String,
    pub rust_type: String,
    pub optional: bool,
}

/// Describes a command available on a persistent model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistCommandContract {
    pub name: String,
    pub fields: Vec<PersistCommandFieldContract>,
    pub mutates_state: bool,
}

/// Trait for persisted models that support drafts, patches, and commands (CQRS/Event Sourcing style).
pub trait PersistCommandModel: PersistEntity + Sized {
    type Draft: Send + Sync + 'static;
    type Patch: Send + Sync + 'static;
    type Command: Send + Sync + 'static;

    /// Creates an entity from a draft.
    fn from_draft(draft: Self::Draft) -> Self;
    /// Tries to create an entity from a draft, allowing for validation errors.
    fn try_from_draft(draft: Self::Draft) -> Result<Self> {
        Ok(Self::from_draft(draft))
    }
    /// Applies a patch to the entity state.
    fn apply_patch_model(&mut self, patch: Self::Patch) -> Result<bool>;
    /// Applies a command to the entity.
    fn apply_command_model(&mut self, command: Self::Command) -> Result<bool>;

    /// Validates a draft payload.
    fn validate_draft_payload(_draft: &Self::Draft) -> Result<()> {
        Ok(())
    }

    /// Validates a patch payload.
    fn validate_patch_payload(_patch: &Self::Patch) -> Result<()> {
        Ok(())
    }

    /// Validates a command payload.
    fn validate_command_payload(_command: &Self::Command) -> Result<()> {
        Ok(())
    }

    /// Returns the contract describing available patches.
    fn patch_contract() -> Vec<PersistPatchContract>;
    /// Returns the contract describing available commands.
    fn command_contract() -> Vec<PersistCommandContract>;
}

/// Trait for commands to report their own name.
pub trait PersistCommandName {
    fn command_name(&self) -> &'static str;
}
