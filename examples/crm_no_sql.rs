//! CRM No-SQL Example
//!
//! This example demonstrates the "Invisible Database" concept.
//! The developer defines structs and business logic.
//! The system handles storage, IDs, relations, and crash recovery.
//!
//! RUN: cargo run --example crm_no_sql

use anyhow::Result;
use rustmemodb::{PersistApp, persist_vec, persistent, persistent_impl};
use serde::{Deserialize, Serialize};

// ==================================================================================
// 1. DATA MODEL (The only thing the developer needs to design)
// ==================================================================================

// A Client in our CRM
#[persistent(schema_version = 1)] // Auto-migration enabled
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientModel {
    pub name: String,
    pub email: String,
    pub is_vip: bool,
    pub total_revenue: i64,
}

// A Deal/Project associated with a client
#[persistent(schema_version = 1)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DealModel {
    pub client_id: String, // "Foreign Key" (just a string field here)
    pub title: String,
    pub amount: i64,
    pub stage: String, // "New", "Won", "Lost"
}

// ==================================================================================
// 2. INVISIBLE DB GLUE (Macros that generate the engine)
// ==================================================================================

#[persistent_impl]
impl ClientModel {
    #[command]
    pub fn add_revenue(&mut self, delta: i64) -> rustmemodb::Result<i64> {
        if delta <= 0 {
            return Err(rustmemodb::DbError::ExecutionError(
                "Revenue delta must be positive".to_string(),
            ));
        }
        self.total_revenue += delta;
        Ok(self.total_revenue)
    }

    #[command]
    pub fn promote_vip(&mut self) {
        self.is_vip = true;
    }
}

#[persistent_impl]
impl DealModel {
    #[command(name = "close_won")]
    pub fn close_as_won(&mut self) {
        self.stage = "Won".to_string();
    }
}

// Generate the Collections (Tables)
persist_vec!(pub ClientVec, ClientModelPersisted);
persist_vec!(pub DealVec, DealModelPersisted);

// ==================================================================================
// 3. THE APPLICATION
// ==================================================================================

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting CRM (Invisible Database Mode)...\n");

    // A. SETUP: Zero configuration. Just pick a folder.
    // The engine handles schemas, files, and recovery automatically.
    let app = PersistApp::open_auto("./tmp_crm_data").await?;

    // Get handles to our collections
    let mut clients = app.open_vec::<ClientVec>("clients").await?;
    let mut deals = app.open_vec::<DealVec>("deals").await?;

    println!("📊 Current Stats:");
    println!("   Clients: {}", clients.list().len());
    println!("   Deals:   {}\n", deals.list().len());

    // B. SEEDING: Add data if empty
    if clients.list().len() == 0 {
        println!("🌱 Seeding new database...");

        // Create a Client using the generated Draft struct
        let client_id = clients
            .create_from_draft(ClientModelPersistedDraft {
                name: "Elon Musk".to_string(),
                email: "elon@mars.com".to_string(),
                is_vip: false,
                total_revenue: 0,
            })
            .await?;

        // Create a Deal for this client
        deals
            .create_from_draft(DealModelPersistedDraft {
                client_id: client_id.clone(),
                title: "Mars Colony Ticket".to_string(),
                amount: 500_000,
                stage: "New".to_string(),
            })
            .await?;

        println!("✅ Created Client and Deal.");
    }

    // C. OPERATION: Modify data naturally
    println!("💼 Processing Deals...");

    // Simulate finding a deal and closing it
    // We iterate over the vector in memory (fast!), but it's backed by disk.
    // We collect IDs first to avoid borrowing issues during mutation
    let deals_to_process: Vec<(String, String, i64, String)> = deals
        .list()
        .iter()
        .filter(|d| *d.stage() == "New")
        .map(|d| {
            (
                d.persist_id().to_string(),
                d.title().to_string(),
                *d.amount(),
                d.client_id().to_string(),
            )
        })
        .collect();

    for (deal_id, title, amount, client_id) in deals_to_process {
        println!("   Closing deal: '{}' for ${}", title, amount);

        // 1. Update the Deal (Transactionally)
        deals
            .update(&deal_id, |deal| {
                let _ = deal.apply_domain_command(DealModelPersistentCommand::CloseAsWon)?;
                Ok(())
            })
            .await?;

        // 2. Update the Client's revenue + VIP promotion
        clients
            .update(&client_id, |client| {
                let _ = client.apply_domain_command(ClientModelPersistentCommand::AddRevenue {
                    delta: amount,
                })?;
                if *client.total_revenue() > 10_000 && !*client.is_vip() {
                    let _ =
                        client.apply_domain_command(ClientModelPersistentCommand::PromoteVip)?;
                    println!("   🌟 Client promoted to VIP!");
                }
                Ok(())
            })
            .await?;
    }

    // D. REPORTING
    println!("\n📈 Final Report:");
    for client in clients.list() {
        println!(
            "   - {} (VIP: {}): Revenue=${}",
            client.name(),
            client.is_vip(),
            client.total_revenue()
        );
    }

    println!("\n✅ Done. Run this again to see data persist!");
    Ok(())
}
