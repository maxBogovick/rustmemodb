use chrono::{DateTime, Utc};
use rustmemodb::{
    DbError, PersistEntityRuntime, PersistSession, PersistState, RuntimeCommandPayloadSchema,
    RuntimeDurabilityMode, RuntimeLifecyclePolicy, RuntimeLifecycleReport,
    RuntimeOperationalPolicy, RuntimePayloadType,
};
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use tempfile::tempdir;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct CommandEnvelopeV2 {
    envelope_id: Uuid,
    entity_type: String,
    entity_id: String,
    causation_id: Option<Uuid>,
    correlation_id: Option<Uuid>,
    expected_version: Option<u64>,
    command_name: String,
    payload_json: JsonValue,
    payload_version: u32,
    created_at: DateTime<Utc>,
    idempotency_key: Option<String>,
    actor_id: Option<String>,
}

impl CommandEnvelopeV2 {
    fn new(
        entity_type: impl Into<String>,
        entity_id: impl Into<String>,
        command_name: impl Into<String>,
        payload_json: JsonValue,
    ) -> Self {
        Self {
            envelope_id: Uuid::new_v4(),
            entity_type: entity_type.into(),
            entity_id: entity_id.into(),
            causation_id: None,
            correlation_id: None,
            expected_version: None,
            command_name: command_name.into(),
            payload_json,
            payload_version: 1,
            created_at: Utc::now(),
            idempotency_key: None,
            actor_id: None,
        }
    }

    fn with_expected_version(mut self, expected_version: u64) -> Self {
        self.expected_version = Some(expected_version);
        self
    }

    fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    fn with_actor(mut self, actor_id: impl Into<String>) -> Self {
        self.actor_id = Some(actor_id.into());
        self
    }

    fn with_correlation(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    fn with_causation(mut self, causation_id: Uuid) -> Self {
        self.causation_id = Some(causation_id);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SideEffectSpec {
    NotifyEmailChanged { entity_id: String, email: String },
    PublishTodoCompleted { entity_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OutboxStatus {
    Pending,
    Dispatched,
}

#[derive(Debug, Clone)]
struct OutboxRecord {
    outbox_id: String,
    envelope_id: Uuid,
    entity_type: String,
    entity_id: String,
    effect: SideEffectSpec,
    status: OutboxStatus,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct CommandExecutionReceipt {
    envelope_id: Uuid,
    state: PersistState,
    idempotent_replay: bool,
}

struct ReferenceSliceKernel {
    runtime: PersistEntityRuntime,
    projection_session: PersistSession,
    idempotency_results: HashMap<String, CommandExecutionReceipt>,
    outbox: Vec<OutboxRecord>,
}

impl ReferenceSliceKernel {
    async fn open(
        root_dir: impl Into<std::path::PathBuf>,
        policy: RuntimeOperationalPolicy,
    ) -> rustmemodb::Result<Self> {
        let mut runtime = PersistEntityRuntime::open(root_dir, policy).await?;

        runtime.register_deterministic_command_with_schema(
            "User",
            "deposit",
            RuntimeCommandPayloadSchema::object()
                .require_field("amount", RuntimePayloadType::Integer)
                .allow_extra_fields(false),
            std::sync::Arc::new(|state, payload| {
                let amount = payload
                    .get("amount")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| DbError::ExecutionError("Missing amount".to_string()))?;
                if amount <= 0 {
                    return Err(DbError::ExecutionError(
                        "Amount must be positive".to_string(),
                    ));
                }

                let fields = state.fields_object_mut()?;
                let current = fields
                    .get("balance")
                    .and_then(|value| value.as_i64())
                    .unwrap_or_default();
                fields.insert("balance".to_string(), json!(current + amount));
                state.metadata.version = state.metadata.version.saturating_add(1);
                Ok(())
            }),
        );

        runtime.register_deterministic_command_with_schema(
            "User",
            "set_email",
            RuntimeCommandPayloadSchema::object()
                .require_field("email", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            std::sync::Arc::new(|state, payload| {
                let email = payload
                    .get("email")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| DbError::ExecutionError("Missing email".to_string()))?;

                let fields = state.fields_object_mut()?;
                fields.insert("email".to_string(), json!(email));
                state.metadata.version = state.metadata.version.saturating_add(1);
                Ok(())
            }),
        );

        runtime.register_deterministic_command_with_schema(
            "Todo",
            "set_done",
            RuntimeCommandPayloadSchema::object()
                .require_field("done", RuntimePayloadType::Boolean)
                .allow_extra_fields(false),
            std::sync::Arc::new(|state, payload| {
                let done = payload
                    .get("done")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| DbError::ExecutionError("Missing done".to_string()))?;

                let fields = state.fields_object_mut()?;
                fields.insert("done".to_string(), json!(done));
                state.metadata.version = state.metadata.version.saturating_add(1);
                Ok(())
            }),
        );

        runtime.register_deterministic_command_with_schema(
            "Todo",
            "rename",
            RuntimeCommandPayloadSchema::object()
                .require_field("title", RuntimePayloadType::Text)
                .allow_extra_fields(false),
            std::sync::Arc::new(|state, payload| {
                let title = payload
                    .get("title")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| DbError::ExecutionError("Missing title".to_string()))?;

                let fields = state.fields_object_mut()?;
                fields.insert("title".to_string(), json!(title));
                state.metadata.version = state.metadata.version.saturating_add(1);
                Ok(())
            }),
        );

        let projection_session = PersistSession::new(rustmemodb::InMemoryDB::new());
        projection_session
            .execute(
                "CREATE TABLE IF NOT EXISTS user_projection (entity_id TEXT PRIMARY KEY, email TEXT NOT NULL, balance INTEGER NOT NULL, updated_at TIMESTAMP NOT NULL)",
            )
            .await?;
        projection_session
            .execute(
                "CREATE TABLE IF NOT EXISTS todo_projection (entity_id TEXT PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN NOT NULL, updated_at TIMESTAMP NOT NULL)",
            )
            .await?;
        projection_session
            .execute(
                "CREATE TABLE IF NOT EXISTS outbox_projection (outbox_id TEXT PRIMARY KEY, envelope_id TEXT NOT NULL, entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, effect_kind TEXT NOT NULL, effect_payload TEXT NOT NULL, status TEXT NOT NULL, created_at TIMESTAMP NOT NULL)",
            )
            .await?;

        Ok(Self {
            runtime,
            projection_session,
            idempotency_results: HashMap::new(),
            outbox: Vec::new(),
        })
    }

    async fn create_user(&mut self, email: &str, balance: i64) -> rustmemodb::Result<String> {
        let entity_id = self
            .runtime
            .create_entity(
                "User",
                "user_state",
                json!({
                    "email": email,
                    "balance": balance,
                }),
                1,
            )
            .await?;
        let state = self.runtime.get_state("User", &entity_id)?;
        self.sync_projection(&state).await?;
        Ok(entity_id)
    }

    async fn create_todo(&mut self, title: &str) -> rustmemodb::Result<String> {
        let entity_id = self
            .runtime
            .create_entity(
                "Todo",
                "todo_state",
                json!({
                    "title": title,
                    "done": false,
                }),
                1,
            )
            .await?;
        let state = self.runtime.get_state("Todo", &entity_id)?;
        self.sync_projection(&state).await?;
        Ok(entity_id)
    }

    async fn execute_envelope(
        &mut self,
        envelope: CommandEnvelopeV2,
    ) -> rustmemodb::Result<CommandExecutionReceipt> {
        self.validate_envelope(&envelope)?;

        let idempotency_scope_key = self.idempotency_scope_key(&envelope);
        if let Some(scope_key) = &idempotency_scope_key {
            if let Some(existing) = self.idempotency_results.get(scope_key) {
                return Ok(CommandExecutionReceipt {
                    envelope_id: existing.envelope_id,
                    state: existing.state.clone(),
                    idempotent_replay: true,
                });
            }
        }

        let current = self
            .runtime
            .get_state(&envelope.entity_type, &envelope.entity_id)
            .map_err(|_| {
                DbError::ExecutionError(format!(
                    "Entity not found for envelope {}: {}:{}",
                    envelope.envelope_id, envelope.entity_type, envelope.entity_id
                ))
            })?;

        if let Some(expected) = envelope.expected_version {
            let actual = current.metadata.version as u64;
            if expected != actual {
                return Err(DbError::ExecutionError(format!(
                    "Expected version mismatch for {}:{} (expected {}, actual {})",
                    envelope.entity_type, envelope.entity_id, expected, actual
                )));
            }
        }

        let state = self
            .runtime
            .apply_deterministic_command(
                &envelope.entity_type,
                &envelope.entity_id,
                &envelope.command_name,
                envelope.payload_json.clone(),
            )
            .await?;

        self.sync_projection(&state).await?;

        let side_effects = derive_side_effects(&envelope, &state)?;
        for (index, effect) in side_effects.into_iter().enumerate() {
            let outbox_record = OutboxRecord {
                outbox_id: format!("{}:{}", envelope.envelope_id, index),
                envelope_id: envelope.envelope_id,
                entity_type: envelope.entity_type.clone(),
                entity_id: envelope.entity_id.clone(),
                effect: effect.clone(),
                status: OutboxStatus::Pending,
                created_at: envelope.created_at,
            };
            self.persist_outbox_record(&outbox_record).await?;
            self.outbox.push(outbox_record);
        }

        let receipt = CommandExecutionReceipt {
            envelope_id: envelope.envelope_id,
            state,
            idempotent_replay: false,
        };

        if let Some(scope_key) = idempotency_scope_key {
            self.idempotency_results.insert(scope_key, receipt.clone());
        }

        Ok(receipt)
    }

    async fn dispatch_outbox(&mut self) -> rustmemodb::Result<usize> {
        let mut dispatched = 0usize;
        for record in &mut self.outbox {
            if record.status == OutboxStatus::Pending {
                record.status = OutboxStatus::Dispatched;
                self.projection_session
                    .execute(&format!(
                        "UPDATE outbox_projection SET status = 'DISPATCHED' WHERE outbox_id = '{}'",
                        rustmemodb::persist::sql_escape_string(&record.outbox_id)
                    ))
                    .await?;
                dispatched = dispatched.saturating_add(1);
            }
        }
        Ok(dispatched)
    }

    fn pending_outbox_count(&self) -> usize {
        self.outbox
            .iter()
            .filter(|record| record.status == OutboxStatus::Pending)
            .count()
    }

    async fn pending_outbox_count_sql(&self) -> rustmemodb::Result<usize> {
        let result = self
            .projection_session
            .query("SELECT outbox_id FROM outbox_projection WHERE status = 'PENDING'")
            .await?;
        Ok(result.row_count())
    }

    fn outbox_records(&self) -> &[OutboxRecord] {
        &self.outbox
    }

    async fn run_lifecycle(&mut self) -> rustmemodb::Result<RuntimeLifecycleReport> {
        self.runtime.run_lifecycle_maintenance().await
    }

    async fn query_user_balance(&self, entity_id: &str) -> rustmemodb::Result<i64> {
        let result = self
            .projection_session
            .query(&format!(
                "SELECT balance FROM user_projection WHERE entity_id = '{}'",
                rustmemodb::persist::sql_escape_string(entity_id)
            ))
            .await?;

        let balance = result
            .rows()
            .first()
            .and_then(|row| row.first())
            .and_then(|value| match value {
                rustmemodb::Value::Integer(v) => Some(*v),
                _ => None,
            })
            .ok_or_else(|| DbError::ExecutionError("Missing projected balance".to_string()))?;

        Ok(balance)
    }

    async fn query_user_email(&self, entity_id: &str) -> rustmemodb::Result<String> {
        let result = self
            .projection_session
            .query(&format!(
                "SELECT email FROM user_projection WHERE entity_id = '{}'",
                rustmemodb::persist::sql_escape_string(entity_id)
            ))
            .await?;

        let email = result
            .rows()
            .first()
            .and_then(|row| row.first())
            .and_then(|value| match value {
                rustmemodb::Value::Text(v) => Some(v.clone()),
                _ => None,
            })
            .ok_or_else(|| DbError::ExecutionError("Missing projected email".to_string()))?;

        Ok(email)
    }

    fn validate_envelope(&self, envelope: &CommandEnvelopeV2) -> rustmemodb::Result<()> {
        if envelope.entity_type.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Envelope entity_type must not be empty".to_string(),
            ));
        }
        if envelope.entity_id.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Envelope entity_id must not be empty".to_string(),
            ));
        }
        if envelope.command_name.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "Envelope command_name must not be empty".to_string(),
            ));
        }
        if envelope.payload_version != 1 {
            return Err(DbError::ExecutionError(format!(
                "Unsupported payload_version {}",
                envelope.payload_version
            )));
        }
        Ok(())
    }

    fn idempotency_scope_key(&self, envelope: &CommandEnvelopeV2) -> Option<String> {
        envelope.idempotency_key.as_ref().map(|idempotency_key| {
            format!(
                "{}:{}:{}:{}",
                envelope.entity_type, envelope.entity_id, envelope.command_name, idempotency_key
            )
        })
    }

    async fn sync_projection(&self, state: &PersistState) -> rustmemodb::Result<()> {
        let fields = state.fields_object()?;
        match state.type_name.as_str() {
            "User" => {
                let email = fields
                    .get("email")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| DbError::ExecutionError("User.email missing".to_string()))?;
                let balance = fields
                    .get("balance")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| DbError::ExecutionError("User.balance missing".to_string()))?;

                self.upsert_projection_row(
                    "user_projection",
                    &state.persist_id,
                    vec![
                        (
                            "email",
                            format!("'{}'", rustmemodb::persist::sql_escape_string(email)),
                        ),
                        ("balance", balance.to_string()),
                        (
                            "updated_at",
                            format!("'{}'", state.metadata.updated_at.to_rfc3339()),
                        ),
                    ],
                )
                .await?;
            }
            "Todo" => {
                let title = fields
                    .get("title")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| DbError::ExecutionError("Todo.title missing".to_string()))?;
                let done = fields
                    .get("done")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| DbError::ExecutionError("Todo.done missing".to_string()))?;

                self.upsert_projection_row(
                    "todo_projection",
                    &state.persist_id,
                    vec![
                        (
                            "title",
                            format!("'{}'", rustmemodb::persist::sql_escape_string(title)),
                        ),
                        (
                            "done",
                            if done {
                                "TRUE".to_string()
                            } else {
                                "FALSE".to_string()
                            },
                        ),
                        (
                            "updated_at",
                            format!("'{}'", state.metadata.updated_at.to_rfc3339()),
                        ),
                    ],
                )
                .await?;
            }
            other => {
                return Err(DbError::ExecutionError(format!(
                    "Unsupported projection type '{}'",
                    other
                )));
            }
        }

        Ok(())
    }

    async fn upsert_projection_row(
        &self,
        table: &str,
        entity_id: &str,
        assignments: Vec<(&'static str, String)>,
    ) -> rustmemodb::Result<()> {
        let escaped_id = rustmemodb::persist::sql_escape_string(entity_id);

        let exists = self
            .projection_session
            .query(&format!(
                "SELECT entity_id FROM {} WHERE entity_id = '{}'",
                table, escaped_id
            ))
            .await?
            .row_count()
            > 0;

        if exists {
            let set_clause = assignments
                .iter()
                .map(|(column, value)| format!("{} = {}", column, value))
                .collect::<Vec<_>>()
                .join(", ");
            self.projection_session
                .execute(&format!(
                    "UPDATE {} SET {} WHERE entity_id = '{}'",
                    table, set_clause, escaped_id
                ))
                .await?;
        } else {
            let mut columns = vec!["entity_id".to_string()];
            let mut values = vec![format!("'{}'", escaped_id)];
            for (column, value) in assignments {
                columns.push(column.to_string());
                values.push(value);
            }

            self.projection_session
                .execute(&format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    table,
                    columns.join(", "),
                    values.join(", ")
                ))
                .await?;
        }

        Ok(())
    }

    async fn persist_outbox_record(&self, record: &OutboxRecord) -> rustmemodb::Result<()> {
        let (effect_kind, effect_payload) = serialize_effect(&record.effect);
        let status = match record.status {
            OutboxStatus::Pending => "PENDING",
            OutboxStatus::Dispatched => "DISPATCHED",
        };

        self.projection_session
            .execute(&format!(
                "INSERT INTO outbox_projection (outbox_id, envelope_id, entity_type, entity_id, effect_kind, effect_payload, status, created_at) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
                rustmemodb::persist::sql_escape_string(&record.outbox_id),
                rustmemodb::persist::sql_escape_string(&record.envelope_id.to_string()),
                rustmemodb::persist::sql_escape_string(&record.entity_type),
                rustmemodb::persist::sql_escape_string(&record.entity_id),
                rustmemodb::persist::sql_escape_string(effect_kind),
                rustmemodb::persist::sql_escape_string(&effect_payload),
                status,
                rustmemodb::persist::sql_escape_string(&record.created_at.to_rfc3339())
            ))
            .await?;

        Ok(())
    }
}

fn derive_side_effects(
    envelope: &CommandEnvelopeV2,
    state: &PersistState,
) -> rustmemodb::Result<Vec<SideEffectSpec>> {
    match (
        envelope.entity_type.as_str(),
        envelope.command_name.as_str(),
    ) {
        ("User", "set_email") => {
            let email = state
                .fields_object()?
                .get("email")
                .and_then(|value| value.as_str())
                .ok_or_else(|| DbError::ExecutionError("User.email missing".to_string()))?
                .to_string();
            Ok(vec![SideEffectSpec::NotifyEmailChanged {
                entity_id: state.persist_id.clone(),
                email,
            }])
        }
        ("Todo", "set_done") => {
            let done = state
                .fields_object()?
                .get("done")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            if done {
                Ok(vec![SideEffectSpec::PublishTodoCompleted {
                    entity_id: state.persist_id.clone(),
                }])
            } else {
                Ok(Vec::new())
            }
        }
        _ => Ok(Vec::new()),
    }
}

fn serialize_effect(effect: &SideEffectSpec) -> (&'static str, String) {
    match effect {
        SideEffectSpec::NotifyEmailChanged { entity_id, email } => (
            "NotifyEmailChanged",
            json!({
                "entity_id": entity_id,
                "email": email,
            })
            .to_string(),
        ),
        SideEffectSpec::PublishTodoCompleted { entity_id } => (
            "PublishTodoCompleted",
            json!({
                "entity_id": entity_id,
            })
            .to_string(),
        ),
    }
}

#[tokio::test]
async fn eternal_reference_slice_user_todo_contract_is_enforced() {
    let dir = tempdir().unwrap();

    let mut policy = RuntimeOperationalPolicy::default();
    policy.durability = RuntimeDurabilityMode::Strict;
    policy.lifecycle = RuntimeLifecyclePolicy {
        passivate_after_ms: 5,
        gc_after_ms: 5_000,
        max_hot_objects: 100,
        gc_only_if_never_touched: false,
    };

    let mut kernel = ReferenceSliceKernel::open(dir.path(), policy)
        .await
        .unwrap();

    let user_id = kernel.create_user("alice@example.com", 10).await.unwrap();
    let todo_id = kernel.create_todo("Write ADR v2").await.unwrap();

    let correlation = Uuid::new_v4();
    let deposit = CommandEnvelopeV2::new("User", &user_id, "deposit", json!({ "amount": 15 }))
        .with_expected_version(1)
        .with_idempotency_key("user-deposit-1")
        .with_actor("tester")
        .with_correlation(correlation);

    let receipt = kernel.execute_envelope(deposit.clone()).await.unwrap();
    assert!(!receipt.idempotent_replay);
    assert_eq!(receipt.state.metadata.version, 2);
    assert_eq!(kernel.query_user_balance(&user_id).await.unwrap(), 25);

    let replay = kernel.execute_envelope(deposit).await.unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(kernel.query_user_balance(&user_id).await.unwrap(), 25);

    let set_email = CommandEnvelopeV2::new(
        "User",
        &user_id,
        "set_email",
        json!({ "email": "alice+v2@example.com" }),
    )
    .with_expected_version(2)
    .with_idempotency_key("user-email-1")
    .with_actor("tester")
    .with_correlation(correlation)
    .with_causation(receipt.envelope_id);

    let email_receipt = kernel.execute_envelope(set_email).await.unwrap();
    assert_eq!(email_receipt.state.metadata.version, 3);
    assert_eq!(
        kernel.query_user_email(&user_id).await.unwrap(),
        "alice+v2@example.com"
    );

    let set_done = CommandEnvelopeV2::new("Todo", &todo_id, "set_done", json!({ "done": true }))
        .with_expected_version(1)
        .with_idempotency_key("todo-done-1")
        .with_actor("tester")
        .with_correlation(correlation)
        .with_causation(email_receipt.envelope_id);

    let todo_receipt = kernel.execute_envelope(set_done).await.unwrap();
    assert_eq!(todo_receipt.state.metadata.version, 2);

    assert_eq!(kernel.pending_outbox_count(), 2);
    assert_eq!(kernel.pending_outbox_count_sql().await.unwrap(), 2);

    let bad_payload =
        CommandEnvelopeV2::new("User", &user_id, "deposit", json!({ "amount": "oops" }))
            .with_expected_version(3);
    assert!(kernel.execute_envelope(bad_payload).await.is_err());

    let conflict = CommandEnvelopeV2::new("User", &user_id, "deposit", json!({ "amount": 1 }))
        .with_expected_version(1);
    assert!(kernel.execute_envelope(conflict).await.is_err());

    let dispatched = kernel.dispatch_outbox().await.unwrap();
    assert_eq!(dispatched, 2);
    assert_eq!(kernel.pending_outbox_count(), 0);
    assert_eq!(kernel.pending_outbox_count_sql().await.unwrap(), 0);

    for record in kernel.outbox_records() {
        assert_eq!(record.status, OutboxStatus::Dispatched);
        assert_eq!(record.entity_id.is_empty(), false);
        assert_eq!(record.entity_type.is_empty(), false);
        assert_eq!(record.outbox_id.is_empty(), false);
        assert!(record.created_at <= Utc::now());
        assert!(!record.envelope_id.is_nil());
        match &record.effect {
            SideEffectSpec::NotifyEmailChanged { email, .. } => {
                assert_eq!(email, "alice+v2@example.com");
            }
            SideEffectSpec::PublishTodoCompleted { .. } => {}
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(8)).await;
    let passivation_report = kernel.run_lifecycle().await.unwrap();
    assert!(passivation_report.passivated >= 1);

    let resurrecting_deposit =
        CommandEnvelopeV2::new("User", &user_id, "deposit", json!({ "amount": 5 }))
            .with_expected_version(3)
            .with_idempotency_key("user-deposit-2");

    let post_cold = kernel.execute_envelope(resurrecting_deposit).await.unwrap();
    assert_eq!(post_cold.state.metadata.version, 4);

    let lifecycle_report = kernel.run_lifecycle().await.unwrap();
    assert!(lifecycle_report.resurrected >= 1);
    assert_eq!(kernel.query_user_balance(&user_id).await.unwrap(), 30);
}
