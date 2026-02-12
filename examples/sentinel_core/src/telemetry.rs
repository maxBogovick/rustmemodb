use anyhow::Result;
use rustmemodb::{persist_struct, persist_vec, PersistModel, PersistApp, ManagedPersistVec};
use serde::{Deserialize, Serialize};
use serde_json::json;

// ==================================================================================
// 1. DATA MODELS
// ==================================================================================

// A single point of time-series data
#[derive(Debug, Clone, Serialize, Deserialize, PersistModel)]
#[persist_model(schema_version = 1)]
pub struct MetricPointModel {
    pub ts: String, // ISO8601
    pub host: String,
    pub cpu_usage: f64,
    pub ram_usage: f64,
    pub disk_io: i64,
}

// An unstructured security event log
#[derive(Debug, Clone, Serialize, Deserialize, PersistModel)]
#[persist_model(schema_version = 1)]
pub struct SecurityEventModel {
    pub ts: String,
    pub severity: String, // INFO, WARN, CRITICAL
    pub source: String,
    pub details: String, // JSON payload as Text
}

// ==================================================================================
// 2. GENERATE PERSISTENCE LAYERS
// ==================================================================================

persist_struct!(pub struct MetricPoint from_struct = MetricPointModel);
persist_struct!(pub struct SecurityEvent from_struct = SecurityEventModel);

persist_vec!(pub MetricVec, MetricPoint);
persist_vec!(pub EventVec, SecurityEvent);

// ==================================================================================
// 3. TELEMETRY MANAGER
// ==================================================================================

pub struct TelemetryManager {
    metrics: ManagedPersistVec<MetricVec>,
    events: ManagedPersistVec<EventVec>,
}

impl TelemetryManager {
    pub async fn new(app: &PersistApp) -> Result<Self> {
        Ok(Self {
            metrics: app.open_vec::<MetricVec>("metrics_store").await?,
            events: app.open_vec::<EventVec>("security_events").await?,
        })
    }

    pub async fn record_metric(&mut self, host: &str, cpu: f64, ram: f64, disk: i64) -> Result<()> {
        let draft = MetricPointDraft {
            ts: chrono::Utc::now().to_rfc3339(),
            host: host.to_string(),
            cpu_usage: cpu,
            ram_usage: ram,
            disk_io: disk,
        };
        // Auto-saves to WAL
        self.metrics.create_from_draft(draft).await?;
        Ok(())
    }

    pub async fn record_event(&mut self, severity: &str, source: &str, details: serde_json::Value) -> Result<()> {
        let details_json = serde_json::to_string(&details)?;
        let draft = SecurityEventDraft {
            ts: chrono::Utc::now().to_rfc3339(),
            severity: severity.to_string(),
            source: source.to_string(),
            details: details_json,
        };
        self.events.create_from_draft(draft).await?;
        Ok(())
    }

    pub fn get_metrics_count(&self) -> usize {
        self.metrics.list().len()
    }

    pub fn get_events_count(&self) -> usize {
        self.events.list().len()
    }
    
    // Demonstrate SQL over Managed Vecs (Hybrid Power!)
    // Managed Vecs are backed by real SQL tables. We can query them directly using the session.
    pub async fn get_average_cpu(&self) -> Result<f64> {
        // We iterate in memory for simplicity
        let items = self.metrics.list();
        if items.is_empty() {
            return Ok(0.0);
        }
        
        let sum: f64 = items.iter().map(|m| *m.cpu_usage()).sum();
        Ok(sum / items.len() as f64)
    }

    pub fn get_critical_events(&self) -> Vec<&SecurityEvent> {
        self.events.list_filtered(|e| *e.severity() == "CRITICAL")
    }
}
