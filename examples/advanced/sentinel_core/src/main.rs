mod config;
mod telemetry;

use anyhow::Result;
use config::{SentinelConfigVec, default_sentinel_config};
use rustmemodb::PersistApp;
use std::time::Duration;
use telemetry::TelemetryManager;
use tokio::time::sleep;
use serde_json::json;
use rand::Rng;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Professional Startup Banner
    println!("{}", "\x1b[36m");
    println!(r#"
   _____            _   _            _ 
  / ____|          | | (_)          | |
 | (___   ___ _ __ | |_ _ _ __   ___| |
  \___ \ / _ \ '_ \| __| | '_ \ / _ \ |
  ____) |  __/ | | | |_| | | | |  __/ |
 |_____/ \___|_| |_|\__|_|_| |_|\___|_|
      INFRASTRUCTURE MONITORING AGENT
    "#);
    println!("\x1b[0m");

    println!("Allocating resources...");

    // 2. Initialize the Engine (Invisible Database)
    let app = PersistApp::open_auto("./sentinel_data").await?;
    println!("✓ Storage Engine Online (WAL + Snapshots enabled)");

    // 3. Load Configuration (Object Persistence)
    let mut config_vec = app.open_vec::<SentinelConfigVec>("system_config").await?;
    
    // Auto-seed if missing
    if config_vec.list().len() == 0 {
        println!("! No configuration found. Seeding default...");
        let draft = default_sentinel_config();
        config_vec.create_from_draft(draft).await?;
        println!("✓ Configuration seeded.");
    }
    
    // Read config (simulating "Business Logic Access")
    // We hold a reference. The persistent vector owns the data.
    let config = config_vec.list().first().expect("Config must exist"); 
    println!("✓ Loaded Config for Host: \x1b[33m{}\x1b[0m", config.hostname());

    // 4. Initialize Telemetry (SQL + JSON Store)
    let mut telemetry = TelemetryManager::new(&app).await?;
    println!("✓ Telemetry Subsystem Online");

    // 5. Simulation Loop
    println!("\n\x1b[32m[SYSTEM RUNNING]\x1b[0m Press Ctrl+C to stop.\n");
    
    let mut rng = rand::thread_rng();
    let mut ticks = 0;

    // Simulate 5 "ticks" of activity then exit for demo purposes
    for _ in 0..5 {
        ticks += 1;
        sleep(Duration::from_millis(500)).await;

        // A. Generate Metric (SQL Write)
        let cpu = rng.gen_range(10.0..95.0);
        let ram = rng.gen_range(20.0..60.0);
        let disk = rng.gen_range(100..5000); // i32 -> i64 auto-conversion
        
        telemetry.record_metric(config.hostname(), cpu, ram, disk).await?;
        print!("."); // Pulse

        // B. Simulate Anomalies (Event Write)
        if cpu > *config.alert_threshold_cpu() {
            println!("\n\x1b[31m[ALERT] High CPU Detected: {:.1}%\x1b[0m", cpu);
            telemetry.record_event(
                "CRITICAL", 
                "system_monitor", 
                json!({"cpu": cpu, "limit": config.alert_threshold_cpu()})
            ).await?;
        } else if ticks % 3 == 0 {
            // Random INFO log
            telemetry.record_event(
                "INFO", 
                "kernel", 
                json!({"msg": "USB device connected"})
            ).await?;
        }

        // C. Real-time Analysis (SQL Aggregate)
        if ticks % 5 == 0 {
            let avg_cpu = telemetry.get_average_cpu().await?;
            let event_count = telemetry.get_events_count();
            let critical_events = telemetry.get_critical_events().len();
            
            println!("\n--- [STATUS REPORT] ---");
            println!("Metrics Stored: {}", telemetry.get_metrics_count());
            println!("Events Logged:  {}", event_count);
            println!("Critical Evts:  {}", critical_events);
            println!("Avg CPU (All):  {:.1}%", avg_cpu);
            println!("-----------------------");
        }
    }

    println!("\n\n\x1b[33mShutting down...\x1b[0m");
    println!("Persistence Checkpoint: OK");
    println!("Exited gracefully.");

    Ok(())
}
