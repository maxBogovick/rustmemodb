use anyhow::{Result, anyhow};
use chrono::Utc;
use rustmemodb::{
    InvokeStatus, PersistApp, PersistMigrationPlan, PersistMigrationStep, PersistModel,
    PersistEntity, RestoreConflictPolicy, SnapshotMode, Value, persist_struct, persist_vec,
};
use serde_json::json;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PersistModel)]
#[persist_model(table = "catalog_products", schema_version = 2)]
pub struct CatalogProduct {
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

persist_vec!(pub CatalogProductVec, PersistedCatalogProduct);
persist_vec!(pub OpsTicketVec, OpsTicket);
persist_vec!(pub AuditEventVec, AuditEvent);
persist_vec!(hetero pub WorkspaceVec);

#[tokio::main]
async fn main() -> Result<()> {
    println!("\n=== Persist Showcase ===");
    println!(
        "This demo covers managed PersistApp flow: derive/from_struct, migrations, dynamic schema, hetero persist_vec."
    );

    auto_persist_showcase().await?;
    typed_migration_showcase().await?;
    dynamic_schema_showcase().await?;
    heterogeneous_showcase().await?;

    println!("\n✅ Showcase completed successfully.");
    Ok(())
}

fn demo_root(name: &str) -> PathBuf {
    let nonce = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis() * 1_000_000);
    std::env::temp_dir().join(format!("persist_showcase_{name}_{nonce}"))
}

async fn cleanup_root(path: &Path) {
    let _ = tokio::fs::remove_dir_all(path).await;
}

async fn auto_persist_showcase() -> Result<()> {
    println!("\n--- 1) Managed auto-persist + derive(from_struct) ---");

    let root = demo_root("auto");
    let result = async {
        let app = PersistApp::open_auto(root.clone()).await?;
        let mut products = app
            .open_vec::<CatalogProductVec>("catalog-products")
            .await?;

        let product_id = products
            .create_from_draft(PersistedCatalogProductDraft::new(
                "KB-01".to_string(),
                "Mechanical Keyboard".to_string(),
                12_900,
                true,
            ))
            .await?;

        products
            .apply_command(
                &product_id,
                PersistedCatalogProductCommand::SetPriceCents(10_900),
            )
            .await?;

        products
            .patch(
                &product_id,
                PersistedCatalogProductPatch {
                    title: Some("Mechanical Keyboard Pro".to_string()),
                    active: Some(false),
                    ..Default::default()
                },
            )
            .await?;

        let stored = products
            .get(&product_id)
            .ok_or_else(|| anyhow!("persisted product not found after mutations"))?;

        println!("Stored product state:");
        println!(
            "- sku={} | title={} | price_cents={} | active={} | schema_version={}",
            stored.sku(),
            stored.title(),
            stored.price_cents(),
            stored.active(),
            stored.metadata().schema_version
        );

        Ok(())
    }
    .await;

    cleanup_root(&root).await;
    result
}

async fn typed_migration_showcase() -> Result<()> {
    println!("\n--- 2) Versioned schema + typed restore migration ---");

    let source_root = demo_root("typed_source");
    let restore_root = demo_root("typed_restore");

    let result = async {
        let source_app = PersistApp::open_auto(source_root.clone()).await?;
        let mut source_vec = source_app.open_vec::<OpsTicketVec>("ops-source").await?;

        let mut db_degraded = OpsTicket::new("Database degraded".to_string(), 3, false);
        db_degraded.register_function("escalate", |ticket, _args| {
            let next = *ticket.severity() + 1;
            ticket.set_severity(next);
            Ok(Value::Integer(next))
        });

        let api_spike = OpsTicket::new("API latency spike".to_string(), 2, false);
        source_vec.create_many(vec![db_degraded, api_spike]).await?;

        let (counts_tx, counts_rx) = tokio::sync::oneshot::channel();
        source_vec
            .mutate_async(move |vec, session| {
                Box::pin(async move {
                    let outcomes = vec.invoke_supported("escalate", vec![], session).await?;
                    let invoked = outcomes
                        .iter()
                        .filter(|o| matches!(o.status, InvokeStatus::Invoked))
                        .count();
                    let skipped = outcomes
                        .iter()
                        .filter(|o| matches!(o.status, InvokeStatus::SkippedUnsupported))
                        .count();
                    let _ = counts_tx.send((invoked, skipped));
                    Ok(())
                })
            })
            .await?;

        let (invoked, skipped) = counts_rx.await.unwrap_or((0, 0));
        println!(
            "Selective invoke results: invoked = {}, skipped = {}",
            invoked, skipped
        );

        let mut snapshot = source_vec.collection().snapshot(SnapshotMode::WithData);
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

        let table_name = snapshot.table_name.clone();
        let restore_app = PersistApp::open_auto(restore_root.clone()).await?;
        let mut restored_vec = restore_app.open_vec::<OpsTicketVec>("ops-restored").await?;

        restored_vec
            .mutate_async(move |vec, session| {
                Box::pin(async move {
                    session.set_table_schema_version(&table_name, 1).await?;
                    vec.restore_with_custom_migration_plan(
                        snapshot,
                        session,
                        RestoreConflictPolicy::FailFast,
                        migration_plan,
                    )
                    .await?;
                    Ok(())
                })
            })
            .await?;

        println!("Restored tickets after migration:");
        for ticket in restored_vec.collection().items() {
            println!(
                "- {} | severity={} | schema_version={}",
                ticket.title(),
                ticket.severity(),
                ticket.metadata().schema_version
            );
        }

        let table = restored_vec
            .collection()
            .items()
            .first()
            .map(|i| i.table_name().to_string())
            .unwrap_or_default();
        if !table.is_empty() {
            let table_for_lookup = table.clone();
            let (version_tx, version_rx) = tokio::sync::oneshot::channel();
            restored_vec
                .mutate_async(move |_vec, session| {
                    Box::pin(async move {
                        let version = session.get_table_schema_version(&table_for_lookup).await?;
                        let _ = version_tx.send(version);
                        Ok(())
                    })
                })
                .await?;

            if let Ok(version) = version_rx.await {
                println!("Registry schema version for table '{}': {:?}", table, version);
            }
        }

        Ok(())
    }
    .await;

    cleanup_root(&source_root).await;
    cleanup_root(&restore_root).await;
    result
}

async fn dynamic_schema_showcase() -> Result<()> {
    println!("\n--- 3) Dynamic schema entity (JSON Schema mode) ---");

    let root = demo_root("dynamic");
    let result = async {
        let app = PersistApp::open_auto(root.clone()).await?;
        let mut events = app.open_vec::<AuditEventVec>("audit-events").await?;

        let mut event = AuditEvent::new()?;
        event.set_field("event_name", Value::Text("user.login".to_string()))?;
        event.set_field("duration_ms", Value::Integer(41))?;
        event.set_field(
            "payload",
            Value::Json(json!({"user_id": "u-42", "ip": "127.0.0.1"})),
        )?;
        event.set_field("source", Value::Text("edge-proxy".to_string()))?;

        let event_id = event.persist_id().to_string();
        events.create(event).await?;
        events
            .update(&event_id, |saved| {
                saved.set_field("duration_ms", Value::Integer(47))?;
                Ok(())
            })
            .await?;

        let saved = events
            .get(&event_id)
            .ok_or_else(|| anyhow!("dynamic event not found after create/update"))?;

        println!("Dynamic entity available functions:");
        for function in saved.available_functions() {
            println!("- {}", function.name);
        }

        let event_name = match saved.get_field("event_name") {
            Some(Value::Text(value)) => value.clone(),
            _ => "<missing>".to_string(),
        };
        let duration_ms = match saved.get_field("duration_ms") {
            Some(Value::Integer(value)) => *value,
            _ => 0,
        };
        let source = match saved.get_field("source") {
            Some(Value::Text(value)) => value.clone(),
            _ => "<missing>".to_string(),
        };

        println!(
            "Stored dynamic state: event_name={} | duration_ms={} | source={} | schema_version={}",
            event_name,
            duration_ms,
            source,
            saved.metadata().schema_version
        );

        Ok(())
    }
    .await;

    cleanup_root(&root).await;
    result
}

async fn heterogeneous_showcase() -> Result<()> {
    println!("\n--- 4) Heterogeneous persist_vec + per-type migration plans ---");

    let source_root = demo_root("hetero_source");
    let restore_root = demo_root("hetero_restore");

    let result = async {
        let source_app = PersistApp::open_auto(source_root.clone()).await?;
        let mut source_workspace = source_app.open_vec::<WorkspaceVec>("workspace-source").await?;

        source_workspace
            .mutate(|workspace| {
                workspace.register_type::<PersistedCatalogProduct>();
                workspace.register_type::<OpsTicket>();
                workspace.register_type::<AuditEvent>();

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

                workspace.add_one(product)?;
                workspace.add_one(ticket)?;
                workspace.add_one(audit)?;
                Ok(())
            })
            .await?;

        let (counts_tx, counts_rx) = tokio::sync::oneshot::channel();
        source_workspace
            .mutate_async(move |workspace, session| {
                Box::pin(async move {
                    let invoke = workspace.invoke_supported("ack", vec![], session).await?;
                    let invoked = invoke
                        .iter()
                        .filter(|o| matches!(o.status, InvokeStatus::Invoked))
                        .count();
                    let skipped = invoke
                        .iter()
                        .filter(|o| matches!(o.status, InvokeStatus::SkippedUnsupported))
                        .count();
                    let _ = counts_tx.send((invoked, skipped));
                    Ok(())
                })
            })
            .await?;

        let (invoked, skipped) = counts_rx.await.unwrap_or((0, 0));
        println!(
            "Workspace invoke(ack): invoked = {}, skipped = {}",
            invoked, skipped
        );

        let mut snapshot = source_workspace.collection().snapshot(SnapshotMode::WithData);
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

        let product_table = snapshot
            .types
            .iter()
            .find(|t| t.type_name == "CatalogProduct")
            .map(|t| t.table_name.clone());

        let restore_app = PersistApp::open_auto(restore_root.clone()).await?;
        let mut restored_workspace = restore_app
            .open_vec::<WorkspaceVec>("workspace-restored")
            .await?;

        restored_workspace
            .mutate_async(move |workspace, session| {
                Box::pin(async move {
                    workspace.register_type_with_migration_plan::<PersistedCatalogProduct>(
                        product_plan,
                    );
                    workspace.register_type::<OpsTicket>();
                    workspace.register_type::<AuditEvent>();

                    if let Some(table) = product_table {
                        session.set_table_schema_version(&table, 2).await?;
                    }

                    workspace.restore(snapshot, session).await?;
                    Ok(())
                })
            })
            .await?;

        let states = restored_workspace.collection().states();
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
    .await;

    cleanup_root(&source_root).await;
    cleanup_root(&restore_root).await;
    result
}
