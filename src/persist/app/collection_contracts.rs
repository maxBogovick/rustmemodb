/// Core trait for a persistable collection.
///
/// Defines the minimum interface required for a collection to be managed by `PersistApp`,
/// including creation, length checking, snapshot generation, and persistence lifecycle hooks.
pub trait PersistCollection: Sized + Send + Sync + 'static {
    /// The type of snapshot used for serialization and persistence.
    type Snapshot: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Creates a new, empty collection with the given name.
    ///
    /// # Arguments
    /// * `name` - The logical name of the collection.
    fn new_collection(name: impl Into<String>) -> Self;

    /// Returns the number of items in the collection.
    fn len(&self) -> usize;

    /// Generates a snapshot of the collection's current state.
    ///
    /// # Arguments
    /// * `mode` - The snapshot mode (e.g., structure-only or including data).
    fn snapshot(&self, mode: SnapshotMode) -> Self::Snapshot;

    /// Persists all dirty state in the collection to the underlying storage session.
    ///
    /// # Arguments
    /// * `session` - The persistence session to write to.
    fn save_all<'a>(
        &'a mut self,
        session: &'a PersistSession,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// Restores the collection state from a snapshot.
    ///
    /// # Arguments
    /// * `snapshot` - The snapshot data to restore from.
    /// * `session` - The active persistence session.
    /// * `conflict_policy` - Policy regarding how to handle existing data during restore.
    fn restore_with_policy<'a>(
        &'a mut self,
        snapshot: Self::Snapshot,
        session: &'a PersistSession,
        conflict_policy: RestoreConflictPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// A persistable collection that supports indexed access and item management.
///
/// Extends `PersistCollection` with methods for adding, removing, and accessing items types.
pub trait PersistIndexedCollection: PersistCollection {
    /// The type of item stored in this collection.
    type Item: PersistEntity + Send + Sync + 'static;

    /// Returns a slice of all items in the collection.
    fn items(&self) -> &[Self::Item];

    /// Returns a mutable slice of all items in the collection.
    fn items_mut(&mut self) -> &mut [Self::Item];

    /// Adds a single item to the collection.
    fn add_one(&mut self, item: Self::Item);

    /// Adds multiple items to the collection.
    fn add_many(&mut self, items: Vec<Self::Item>);

    /// Removes an item by its persistence ID.
    ///
    /// # Arguments
    /// * `persist_id` - The unique persistence ID of the item to remove.
    ///
    /// # Returns
    /// * `Option<Self::Item>` - The removed item if found, otherwise `None`.
    fn remove_by_persist_id(&mut self, persist_id: &str) -> Option<Self::Item>;
}

/// Trait defining a model that supports workflow-based commands and related record creation.
pub trait PersistWorkflowCommandModel<C, R>: PersistCommandModel {
    /// Converts a high-level workflow command into the model's native persistence command.
    fn to_persist_command(command: &C) -> Self::Command;

    /// Generates a related record based on the workflow command and the updated state.
    ///
    /// Use this to create side-effects or audits as part of the command transaction.
    fn to_related_record(command: &C, updated: &Self) -> Result<R>;
}

/// Trait for commands that can be applied autonomously with automatic auditing.
///
/// This trait allows commands to define their own audit event types and messages,
/// enabling a "zero-touch" audit log for domain operations.
pub trait PersistAutonomousCommand<M>: Clone + Send + Sync + 'static
where
    M: PersistCommandModel,
    M::Command: PersistCommandName,
{
    /// Converts the autonomous command into the model's native persistence command.
    fn to_persist_command(self) -> M::Command;

    /// Returns the audit event type string for this command.
    ///
    /// Defaults to a snake_case normalization of the command name.
    fn audit_event_type(&self, command: &M::Command) -> String {
        default_audit_event_type(command.command_name())
    }

    /// Returns the audit message for this command.
    ///
    /// Defaults to "system: command '<event_type>' applied".
    fn audit_message(&self, command: &M::Command) -> String {
        format!(
            "system: command '{}' applied",
            self.audit_event_type(command)
        )
    }

    /// Returns the audit event type string for a bulk execution of this command.
    fn bulk_audit_event_type(&self, command: &M::Command) -> String {
        format!("bulk_{}", self.audit_event_type(command))
    }

    /// Returns the audit message for a bulk execution of this command.
    fn bulk_audit_message(&self, command: &M::Command) -> String {
        format!("bulk {}", self.audit_message(command))
    }
}

/// Bridge between a generated persisted entity and its source domain model.
///
/// Implemented by derive-generated persisted wrappers so high-level autonomous
/// APIs can expose source-model ergonomics while keeping persistence internals hidden.
pub trait PersistBackedModel<M>: PersistEntity + PersistCommandModel + Clone + Send + Sync + 'static
where
    M: Send + Sync + 'static,
{
    /// Immutable access to source-model data.
    fn model(&self) -> &M;

    /// Mutable access to source-model data.
    fn model_mut(&mut self) -> &mut M;
}

/// Source-model contract for `#[derive(Autonomous)]`.
///
/// This allows opening and mutating a domain by source model type directly.
pub trait PersistAutonomousModel: Sized + Send + Sync + 'static {
    /// Persisted wrapper generated for this model.
    type Persisted: PersistBackedModel<Self>;

    /// Collection wrapper generated for this model.
    type Collection: PersistIndexedCollection<Item = Self::Persisted>;

    /// Converts source model into persisted wrapper.
    fn into_persisted(self) -> Self::Persisted;

    /// Converts persisted wrapper back into source model.
    fn from_persisted(persisted: Self::Persisted) -> Self;
}

/// Contract for models that expose fully generated REST router surface.
///
/// Implemented by derive macros (`#[expose_rest]`) so application code can
/// mount autonomous model routes without writing API/store boilerplate.
pub trait PersistAutonomousRestModel: PersistAutonomousModel {
    /// Builds a ready-to-mount axum router for the model.
    fn mount_router(handle: PersistAutonomousModelHandle<Self>) -> axum::Router;
}

// Built-in audit record for tracking changes in autonomous aggregates.
//
// Stores the target entity's ID, the type of event, a human-readable message,
// and the version of the entity resulting from the operation.
crate::persist_struct!(
    pub struct PersistAuditRecord {
        aggregate_persist_id: String,
        event_type: String,
        message: String,
        resulting_version: i64,
    }
);
crate::persist_vec!(pub PersistAuditRecordVec, PersistAuditRecord);

// Built-in idempotency receipt for autogenerated REST command endpoints.
//
// One record represents one executed HTTP command identified by a stable scope key:
// `<aggregate_id>:<operation_name>:<idempotency_key>`.
// The stored response payload is replayed on duplicate requests with the same key.
crate::persist_struct!(
    pub struct PersistRestIdempotencyRecord {
        #[persist(unique)]
        scope_key: String,
        aggregate_persist_id: String,
        operation_name: String,
        idempotency_key: String,
        status_code: i64,
        response_body_json: String,
    }
);
crate::persist_vec!(
    pub PersistRestIdempotencyRecordVec,
    PersistRestIdempotencyRecord
);
