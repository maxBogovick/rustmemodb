//! Managed persistence demo (PersistApp + persist_struct! + persist_vec!).
//!
//! This example shows the recommended application DX:
//! - no manual `PersistSession::new(...)`
//! - no manual checkpoint or restore calls in request code
//! - automatic recovery after restart via `PersistApp::open_auto(...)`
//!
//! Run with:
//!   cargo run --offline --example persistence_demo

use anyhow::{Result, anyhow};
use chrono::Utc;
use rustmemodb::{PersistApp, PersistModel, persist_struct, persist_vec};

#[derive(Debug, Clone, PersistModel)]
#[persist_model(table = "demo_accounts", schema_version = 1)]
pub struct AccountModel {
    owner: String,
    balance_cents: i64,
    active: bool,
}

persist_struct!(pub struct PersistedAccount from_struct = AccountModel);
persist_vec!(pub AccountVec, PersistedAccount);

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== RustMemDB Managed Persistence Demo ===\n");

    let suffix = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis() * 1_000_000);
    let data_root = std::env::temp_dir().join(format!("rustmemodb_persist_demo_{suffix}"));

    let result = async {
        println!("1) Boot #1: create entities and mutate state");
        let app = PersistApp::open_auto(data_root.clone()).await?;
        let mut accounts = app.open_vec::<AccountVec>("accounts").await?;

        let alice_id = accounts
            .create_from_draft(PersistedAccountDraft::new(
                "Alice".to_string(),
                150_00,
                true,
            ))
            .await?;
        let bob_id = accounts
            .create_from_draft(PersistedAccountDraft::new("Bob".to_string(), 20_00, true))
            .await?;

        accounts
            .apply_command(&alice_id, PersistedAccountCommand::SetBalanceCents(175_00))
            .await?;
        accounts
            .patch(
                &bob_id,
                PersistedAccountPatch {
                    balance_cents: Some(0),
                    active: Some(false),
                    ..Default::default()
                },
            )
            .await?;

        println!("   - created {} accounts", accounts.list().len());
        println!("   - stats: {:?}", accounts.stats());

        drop(accounts);
        drop(app);

        println!("\n2) Boot #2: automatic recovery from snapshot/journal");
        let app_restarted = PersistApp::open_auto(data_root.clone()).await?;
        let mut restored = app_restarted.open_vec::<AccountVec>("accounts").await?;

        println!("   - restored {} accounts", restored.list().len());
        for account in restored.list() {
            println!(
                "   - {}: balance_cents={}, active={}, version={}",
                account.owner(),
                account.balance_cents(),
                account.active(),
                account.metadata().version
            );
        }

        let alice = restored
            .get(&alice_id)
            .ok_or_else(|| anyhow!("Alice account missing after recovery"))?;
        if *alice.balance_cents() != 175_00 {
            return Err(anyhow!(
                "unexpected Alice balance after recovery: {}",
                alice.balance_cents()
            ));
        }

        restored
            .apply_command(&alice_id, PersistedAccountCommand::SetBalanceCents(200_00))
            .await?;
        println!("\n3) Post-recovery write successful (no manual restore/checkpoint code)");

        Ok(())
    }
    .await;

    let _ = tokio::fs::remove_dir_all(&data_root).await;

    result?;
    println!("\n=== Demo completed successfully ===");
    Ok(())
}
