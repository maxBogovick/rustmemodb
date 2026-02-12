use rustmemodb::{
    InMemoryDB, PersistEntityRuntime, PersistSession, RuntimeOperationalPolicy, RuntimePayloadType,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[rustmemodb::persistent(schema_version = 2, table = "wallet_dsl")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WalletModel {
    #[sql(index)]
    pub owner: String,
    #[sql]
    pub balance: i64,
}

#[rustmemodb::persistent_impl]
impl WalletModel {
    #[rustmemodb::command]
    pub fn deposit(&mut self, amount: i64) -> rustmemodb::Result<i64> {
        if amount <= 0 {
            return Err(rustmemodb::DbError::ExecutionError(
                "amount must be positive".to_string(),
            ));
        }
        self.balance += amount;
        Ok(self.balance)
    }

    #[rustmemodb::command(name = "rename_owner")]
    pub fn rename_owner(&mut self, owner: String) {
        self.owner = owner;
    }
}

#[test]
fn persistent_impl_generates_command_contract_and_names() {
    let contract = WalletModelPersisted::domain_command_contract();
    assert_eq!(contract.len(), 2);
    assert!(contract.iter().any(|entry| entry.name == "deposit"));
    assert!(contract.iter().any(|entry| entry.name == "rename_owner"));

    let cmd_a = WalletModelPersistentCommand::Deposit { amount: 1 };
    let cmd_b = WalletModelPersistentCommand::RenameOwner {
        owner: "alice".to_string(),
    };

    assert_eq!(cmd_a.name(), "deposit");
    assert_eq!(cmd_b.name(), "rename_owner");

    let payload = cmd_a.payload_json().unwrap();
    assert_eq!(payload.get("amount").and_then(|v| v.as_i64()), Some(1));

    let schema = cmd_a.runtime_payload_schema();
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].name, "amount");
    assert_eq!(schema.fields[0].payload_type, RuntimePayloadType::Integer);
    assert!(!schema.allow_extra_fields);

    let rename_schema =
        WalletModelPersistentCommand::runtime_payload_schema_by_name("rename_owner").unwrap();
    assert_eq!(rename_schema.fields.len(), 1);
    assert_eq!(rename_schema.fields[0].name, "owner");
    assert_eq!(
        rename_schema.fields[0].payload_type,
        RuntimePayloadType::Text
    );

    let envelope = cmd_a.to_runtime_envelope("wallet-1").unwrap();
    assert_eq!(envelope.entity_type, "WalletModel");
    assert_eq!(envelope.entity_id, "wallet-1");
    assert_eq!(envelope.command_name, "deposit");
}

#[tokio::test]
async fn persistent_impl_applies_and_persists_domain_commands() {
    let session = PersistSession::new(InMemoryDB::new());
    let mut persisted = WalletModel {
        owner: "alice".to_string(),
        balance: 10,
    }
    .into_persisted();

    persisted.bind_session(session);
    persisted.save_bound().await.unwrap();

    let deposit_result = persisted
        .apply_domain_command_persisted(WalletModelPersistentCommand::Deposit { amount: 7 })
        .await
        .unwrap();
    assert_eq!(deposit_result.as_i64(), Some(17));

    let rename_result = persisted
        .apply_domain_command_persisted(WalletModelPersistentCommand::RenameOwner {
            owner: "bob".to_string(),
        })
        .await
        .unwrap();
    assert!(rename_result.is_null());

    let state = persisted.state_json();
    assert_eq!(state.get("owner").and_then(|v| v.as_str()), Some("bob"));
    assert_eq!(state.get("balance").and_then(|v| v.as_i64()), Some(17));

    let command = WalletModelPersistentCommand::Deposit { amount: 2 };
    let envelope = persisted
        .domain_command_envelope_with_expected_version(&command)
        .unwrap();
    assert_eq!(envelope.entity_type, "WalletModel");
    assert_eq!(envelope.entity_id, persisted.persist_id());
    assert_eq!(envelope.command_name, "deposit");
    assert_eq!(
        envelope.payload_json.get("amount").and_then(|v| v.as_i64()),
        Some(2)
    );
    assert_eq!(envelope.expected_version, Some(3));
}

#[tokio::test]
async fn persistent_impl_registers_domain_commands_in_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = PersistEntityRuntime::open(dir.path(), RuntimeOperationalPolicy::default())
        .await
        .unwrap();

    WalletModelPersisted::register_domain_commands_in_runtime(&mut runtime);

    let id = runtime
        .create_entity(
            "WalletModel",
            "wallet_runtime",
            json!({
                "owner": "alice",
                "balance": 10
            }),
            1,
        )
        .await
        .unwrap();

    let command = WalletModelPersistentCommand::Deposit { amount: 4 };
    let envelope = command
        .to_runtime_envelope(&id)
        .unwrap()
        .with_expected_version(1)
        .with_idempotency_key("wallet-1");

    let applied = runtime.apply_command_envelope(envelope).await.unwrap();
    assert_eq!(
        applied
            .state
            .fields
            .get("balance")
            .and_then(|value| value.as_i64()),
        Some(14)
    );
    assert_eq!(applied.state.metadata.version, 2);
}

#[tokio::test]
async fn persistent_impl_registers_projection_contract_and_index_helpers() {
    let dir = tempfile::tempdir().unwrap();
    let mut runtime = PersistEntityRuntime::open(dir.path(), RuntimeOperationalPolicy::default())
        .await
        .unwrap();

    WalletModelPersisted::try_register_domain_commands_in_runtime(&mut runtime).unwrap();

    let id = runtime
        .create_entity(
            "WalletModel",
            "wallet_runtime",
            json!({
                "owner": "alice",
                "balance": 10
            }),
            1,
        )
        .await
        .unwrap();

    let by_owner =
        WalletModelPersisted::find_projection_ids_by_owner(&runtime, "alice".to_string()).unwrap();
    assert_eq!(by_owner, vec![id.clone()]);

    let rows =
        WalletModelPersisted::find_projection_rows_by_owner(&runtime, "alice".to_string()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .values
            .get("balance")
            .and_then(|value| value.as_i64()),
        Some(10)
    );

    let rename = WalletModelPersistentCommand::RenameOwner {
        owner: "bob".to_string(),
    }
    .to_runtime_envelope(&id)
    .unwrap()
    .with_expected_version(1);
    runtime.apply_command_envelope(rename).await.unwrap();

    let old =
        WalletModelPersisted::find_projection_ids_by_owner(&runtime, "alice".to_string()).unwrap();
    assert!(old.is_empty());

    let new =
        WalletModelPersisted::find_projection_ids_by_owner(&runtime, "bob".to_string()).unwrap();
    assert_eq!(new, vec![id]);
}
