/// Invokes a registered deterministic command handler safely, catching panics.
pub(crate) fn invoke_registered_handler(
    handler: &RegisteredDeterministicCommandHandler,
    state: &mut PersistState,
    envelope: &RuntimeCommandEnvelope,
    context: &RuntimeDeterministicContext,
) -> Result<Vec<RuntimeSideEffectSpec>> {
    let apply = || match handler {
        RegisteredDeterministicCommandHandler::Legacy(handler) => {
            handler(state, &envelope.payload_json).map(|_| Vec::new())
        }
        RegisteredDeterministicCommandHandler::Envelope(handler) => handler(state, envelope),
        RegisteredDeterministicCommandHandler::Context(handler) => {
            handler(state, &envelope.payload_json, context)
        }
    };

    catch_unwind(AssertUnwindSafe(apply)).map_err(|_| {
        DbError::ExecutionError(format!(
            "Deterministic handler panicked for '{}::{}'",
            envelope.entity_type, envelope.command_name
        ))
    })?
}

/// Validates that a command envelope contains all necessary metadata.
pub(crate) fn validate_command_envelope(envelope: &RuntimeCommandEnvelope) -> Result<()> {
    if envelope.entity_type.trim().is_empty() {
        return Err(DbError::ExecutionError(
            "Command envelope entity_type must not be empty".to_string(),
        ));
    }
    if envelope.entity_id.trim().is_empty() {
        return Err(DbError::ExecutionError(
            "Command envelope entity_id must not be empty".to_string(),
        ));
    }
    if envelope.command_name.trim().is_empty() {
        return Err(DbError::ExecutionError(
            "Command envelope command_name must not be empty".to_string(),
        ));
    }
    if envelope.payload_version == 0 {
        return Err(DbError::ExecutionError(
            "Command envelope payload_version must be >= 1".to_string(),
        ));
    }
    Ok(())
}

/// Generates a unique key for idempotency checks based on the envelope's idempotency key.
pub(crate) fn build_idempotency_scope_key(envelope: &RuntimeCommandEnvelope) -> Option<String> {
    envelope.idempotency_key.as_ref().map(|key| {
        format!(
            "{}:{}:{}:{}",
            envelope.entity_type, envelope.entity_id, envelope.command_name, key
        )
    })
}

/// Constructs a projection row from the current entity state based on a contract.
pub(crate) fn build_projection_row(
    contract: &RuntimeProjectionContract,
    state: &PersistState,
) -> Result<RuntimeProjectionRow> {
    let fields = state.fields_object()?;
    let mut values = serde_json::Map::with_capacity(contract.fields.len());

    for projection_field in &contract.fields {
        let value = fields
            .get(projection_field.state_field.as_str())
            .cloned()
            .ok_or_else(|| {
                DbError::ExecutionError(format!(
                    "Projection field '{}.{}' is missing in state '{}'",
                    contract.entity_type, projection_field.state_field, state.persist_id
                ))
            })?;

        if !payload_matches_type(&value, &projection_field.payload_type) {
            return Err(DbError::ExecutionError(format!(
                "Projection field '{}.{}' type mismatch: expected {:?}, got {}",
                contract.entity_type,
                projection_field.state_field,
                projection_field.payload_type,
                json_type_name(&value)
            )));
        }

        values.insert(projection_field.column_name.clone(), value);
    }

    Ok(RuntimeProjectionRow {
        entity_id: state.persist_id.clone(),
        values,
        updated_at: state.metadata.updated_at,
    })
}

/// Generates a string key for indexing a generic JSON value.
pub(crate) fn projection_index_key(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

/// Checks if a JSON value matches the expected runtime payload type.
pub(crate) fn payload_matches_type(
    value: &serde_json::Value,
    payload_type: &RuntimePayloadType,
) -> bool {
    match payload_type {
        RuntimePayloadType::Null => value.is_null(),
        RuntimePayloadType::Boolean => value.is_boolean(),
        RuntimePayloadType::Integer => value.as_i64().is_some(),
        RuntimePayloadType::Float => value.as_f64().is_some(),
        RuntimePayloadType::Text => value.as_str().is_some(),
        RuntimePayloadType::Array => value.as_array().is_some(),
        RuntimePayloadType::Object => value.as_object().is_some(),
    }
}

/// Returns a human-readable name for the JSON value's type.
pub(crate) fn json_type_name(value: &serde_json::Value) -> &'static str {
    if value.is_null() {
        return "null";
    }
    if value.is_boolean() {
        return "boolean";
    }
    if value.as_i64().is_some() {
        return "integer";
    }
    if value.as_f64().is_some() {
        return "float";
    }
    if value.is_string() {
        return "string";
    }
    if value.is_array() {
        return "array";
    }
    if value.is_object() {
        return "object";
    }
    "unknown"
}

/// Normalizes the runtime policy, ensuring consistency between durability and replication settings.
pub(crate) fn normalize_runtime_policy(
    mut policy: RuntimeOperationalPolicy,
) -> RuntimeOperationalPolicy {
    match policy.consistency {
        RuntimeConsistencyMode::Strong => {
            policy.durability = RuntimeDurabilityMode::Strict;
            policy.replication.mode = RuntimeReplicationMode::Sync;
        }
        RuntimeConsistencyMode::LocalDurable => {
            policy.durability = RuntimeDurabilityMode::Strict;
        }
        RuntimeConsistencyMode::Eventual => {
            policy.durability = RuntimeDurabilityMode::Eventual {
                sync_interval_ms: 250,
            };
            policy.replication.mode = RuntimeReplicationMode::AsyncBestEffort;
        }
    }
    policy
}

/// Calculates the list of replication targets based on root directories.
pub(crate) fn runtime_replica_targets(
    root_dir: &Path,
    replica_roots: &[PathBuf],
) -> Vec<RuntimePaths> {
    let mut result = Vec::new();
    for replica_root in replica_roots {
        if replica_root == root_dir {
            continue;
        }
        result.push(RuntimePaths {
            root_dir: replica_root.clone(),
            snapshot_file: replica_root.join(RUNTIME_SNAPSHOT_FILE),
            journal_file: replica_root.join(RUNTIME_JOURNAL_FILE),
        });
    }
    result
}
