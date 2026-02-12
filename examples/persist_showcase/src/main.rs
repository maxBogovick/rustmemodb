use anyhow::Result;
use rustmemodb::{
    InMemoryDB, InvokeStatus, PersistEntity, PersistMigrationPlan, PersistMigrationStep,
    PersistModel, PersistSession, RestoreConflictPolicy, SnapshotMode, Value, persist_struct,
    persist_vec,
};
use serde_json::json;

#[derive(Debug, Clone, PersistModel)]
#[persist_model(table = "catalog_products", schema_version = 2)]
struct CatalogProduct {
    sku: String,
    title: String,
    price_cents: i64,
    active: bool,
}

persist_struct!(pub struct PersistedCatalogProduct from_struct = CatalogProduct);

persist_struct! {
    pub struct OpsTicket {
        title: String,
        severity: i64,
        acknowledged: bool,
    }
}

persist_struct! {
    pub struct AuditEvent from_json_schema = r#"{
        "type": "object",
        "properties": {
            "event_name": { "type": "string" },
            "duration_ms": { "type": "integer" },
            "payload": { "type": "object" },
            "source": { "type": "string" }
        },
        "required": ["event_name"]
    }"#
}

persist_vec!(pub OpsTicketVec, OpsTicket);
persist_vec!(hetero pub WorkspaceVec);

#[tokio::main]
async fn main() -> Result<()> {
    println!("\n=== Persist Showcase ===");
    println!(
        "This demo covers: derive/from_struct, auto-persist, schema migrations, dynamic schema, hetero persist_vec."
    );

    auto_persist_showcase().await?;
    typed_migration_showcase().await?;
    dynamic_schema_showcase().await?;
    heterogeneous_showcase().await?;

    println!("\n✅ Showcase completed successfully.");
    Ok(())
}

async fn auto_persist_showcase() -> Result<()> {
    println!("\n--- 1) Auto-persist + derive(from_struct) ---");

    let session = PersistSession::new(InMemoryDB::new());

    let mut product = CatalogProduct {
        sku: "KB-01".to_string(),
        title: "Mechanical Keyboard".to_string(),
        price_cents: 12_900,
        active: true,
    }
    .into_persisted();

    product.bind_session(session.clone());
    product.set_auto_persist(true)?;

    product.set_price_cents_persisted(10_900).await?;
    product
        .mutate_persisted(|p| {
            p.set_title("Mechanical Keyboard Pro".to_string());
            p.set_active(false);
        })
        .await?;

    let sql = format!(
        "SELECT sku, title, price_cents, active, __schema_version FROM {} WHERE __persist_id = '{}'",
        product.table_name(),
        product.persist_id()
    );
    let rows = session.query(&sql).await?;
    println!("Rows after auto-persist:");
    rows.print();

    Ok(())
}

async fn typed_migration_showcase() -> Result<()> {
    println!("\n--- 2) Versioned schema + typed restore migration ---");

    let source_session = PersistSession::new(InMemoryDB::new());
    let mut source_vec = OpsTicketVec::new("ops-source");

    let mut db_degraded = OpsTicket::new("Database degraded".to_string(), 3, false);
    db_degraded.register_function("escalate", |ticket, _args| {
        let next = *ticket.severity() + 1;
        ticket.set_severity(next);
        Ok(Value::Integer(next))
    });

    let api_spike = OpsTicket::new("API latency spike".to_string(), 2, false);

    source_vec.add_one(db_degraded);
    source_vec.add_one(api_spike);
    source_vec.save_all(&source_session).await?;

    let outcomes = source_vec
        .invoke_supported("escalate", vec![], &source_session)
        .await?;
    let invoked = outcomes
        .iter()
        .filter(|o| matches!(o.status, InvokeStatus::Invoked))
        .count();
    let skipped = outcomes
        .iter()
        .filter(|o| matches!(o.status, InvokeStatus::SkippedUnsupported))
        .count();
    println!(
        "Selective invoke results: invoked = {}, skipped = {}",
        invoked, skipped
    );

    let mut snapshot = source_vec.snapshot(SnapshotMode::WithData);
    snapshot.schema_version = 1;
    for state in &mut snapshot.states {
        state.metadata.schema_version = 1;
    }

    let mut migration_plan = PersistMigrationPlan::new(2);
    migration_plan.add_step(
        PersistMigrationStep::new(1, 2)
            .with_sql("ALTER TABLE {table} ADD COLUMN migration_note TEXT")
            .with_state_migrator(|state| {
                let fields = state.fields_object_mut()?;
                let old = fields
                    .get("severity")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                fields.insert("severity".to_string(), json!(old * 10));
                Ok(())
            }),
    )?;

    let restore_session = PersistSession::new(InMemoryDB::new());
    restore_session
        .set_table_schema_version(&snapshot.table_name, 1)
        .await?;

    let mut restored_vec = OpsTicketVec::new("ops-restored");
    restored_vec
        .restore_with_custom_migration_plan(
            snapshot,
            &restore_session,
            RestoreConflictPolicy::FailFast,
            migration_plan,
        )
        .await?;

    println!("Restored tickets after migration:");
    for ticket in restored_vec.items() {
        println!(
            "- {} | severity={} | schema_version={}",
            ticket.title(),
            ticket.severity(),
            ticket.metadata().schema_version
        );
    }

    let table = restored_vec
        .items()
        .first()
        .map(|i| i.table_name().to_string())
        .unwrap_or_default();
    if !table.is_empty() {
        let version = restore_session.get_table_schema_version(&table).await?;
        println!(
            "Registry schema version for table '{}': {:?}",
            table, version
        );
    }

    Ok(())
}

async fn dynamic_schema_showcase() -> Result<()> {
    println!("\n--- 3) Dynamic schema entity (JSON Schema mode) ---");

    let session = PersistSession::new(InMemoryDB::new());
    let mut event = AuditEvent::new()?;

    event.bind_session(session.clone());
    event.set_auto_persist(true)?;

    event
        .set_field_persisted("event_name", Value::Text("user.login".to_string()))
        .await?;
    event
        .set_field_persisted("duration_ms", Value::Integer(47))
        .await?;
    event
        .set_field_persisted(
            "payload",
            Value::Json(json!({"user_id": "u-42", "ip": "127.0.0.1"})),
        )
        .await?;
    event
        .set_field_persisted("source", Value::Text("edge-proxy".to_string()))
        .await?;

    println!("Dynamic entity available functions:");
    for function in event.available_functions() {
        println!("- {}", function.name);
    }

    let sql = format!(
        "SELECT event_name, duration_ms, source, __schema_version FROM {} WHERE __persist_id = '{}'",
        event.table_name(),
        event.persist_id()
    );
    let rows = session.query(&sql).await?;
    rows.print();

    Ok(())
}

async fn heterogeneous_showcase() -> Result<()> {
    println!("\n--- 4) Heterogeneous persist_vec + per-type migration plans ---");

    let source_session = PersistSession::new(InMemoryDB::new());
    let mut source_workspace = WorkspaceVec::new("workspace-source");

    source_workspace.register_type::<PersistedCatalogProduct>();
    source_workspace.register_type::<OpsTicket>();
    source_workspace.register_type::<AuditEvent>();

    let product = CatalogProduct {
        sku: "MOUSE-9".to_string(),
        title: "Pro Mouse".to_string(),
        price_cents: 4_900,
        active: true,
    }
    .into_persisted();

    let mut ticket = OpsTicket::new("Node pool overloaded".to_string(), 4, false);
    ticket.register_function("ack", |t, _args| {
        t.set_acknowledged(true);
        Ok(Value::Boolean(true))
    });

    let mut audit = AuditEvent::new()?;
    audit.set_field("event_name", Value::Text("service.scale".to_string()))?;
    audit.set_field("duration_ms", Value::Integer(12))?;
    audit.set_field(
        "payload",
        Value::Json(json!({"service": "worker", "replicas": 8})),
    )?;

    source_workspace.add_one(product)?;
    source_workspace.add_one(ticket)?;
    source_workspace.add_one(audit)?;
    source_workspace.save_all(&source_session).await?;

    let invoke = source_workspace
        .invoke_supported("ack", vec![], &source_session)
        .await?;
    let invoked = invoke
        .iter()
        .filter(|o| matches!(o.status, InvokeStatus::Invoked))
        .count();
    let skipped = invoke
        .iter()
        .filter(|o| matches!(o.status, InvokeStatus::SkippedUnsupported))
        .count();
    println!(
        "Workspace invoke(ack): invoked = {}, skipped = {}",
        invoked, skipped
    );

    let mut snapshot = source_workspace.snapshot(SnapshotMode::WithData);
    for state in &mut snapshot.states {
        if state.type_name == "CatalogProduct" {
            state.metadata.schema_version = 2;
        }
    }

    let mut product_plan = PersistMigrationPlan::new(3);
    product_plan.add_step(
        PersistMigrationStep::new(2, 3)
            .with_sql("ALTER TABLE {table} ADD COLUMN pricing_tier TEXT")
            .with_state_migrator(|state| {
                let fields = state.fields_object_mut()?;
                let old = fields
                    .get("price_cents")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_default();
                fields.insert("price_cents".to_string(), json!(old + 500));
                Ok(())
            }),
    )?;

    let restore_session = PersistSession::new(InMemoryDB::new());
    if let Some(product_table) = snapshot
        .types
        .iter()
        .find(|t| t.type_name == "CatalogProduct")
        .map(|t| t.table_name.clone())
    {
        restore_session
            .set_table_schema_version(&product_table, 2)
            .await?;
    }

    let mut restored_workspace = WorkspaceVec::new("workspace-restored");
    restored_workspace.register_type_with_migration_plan::<PersistedCatalogProduct>(product_plan);
    restored_workspace.register_type::<OpsTicket>();
    restored_workspace.register_type::<AuditEvent>();
    restored_workspace
        .restore(snapshot, &restore_session)
        .await?;

    let states = restored_workspace.states();
    if let Some(product_state) = states.iter().find(|s| s.type_name == "CatalogProduct") {
        let migrated_price = product_state
            .fields
            .as_object()
            .and_then(|obj| obj.get("price_cents"))
            .and_then(|v| v.as_i64())
            .unwrap_or_default();
        println!(
            "Migrated product state: price_cents = {}, schema_version = {}",
            migrated_price, product_state.metadata.schema_version
        );
    }

    println!("Heterogeneous restored object count: {}", states.len());
    Ok(())
}
