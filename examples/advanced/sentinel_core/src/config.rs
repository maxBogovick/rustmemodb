use rustmemodb::{persist_struct, persist_vec, PersistModel};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PersistModel)]
#[persist_model(schema_version = 1)]
pub struct SentinelConfigModel {
    pub hostname: String,
    pub collection_interval_ms: i64,
    pub retention_policy_days: i64,
    pub alert_threshold_cpu: f64,
    pub alert_threshold_ram_percent: f64,
}

// Generate the persistent wrapper
persist_struct!(pub struct SentinelConfig from_struct = SentinelConfigModel);

// Generate the vector collection
persist_vec!(pub SentinelConfigVec, SentinelConfig);

// Helper function to create default config
pub fn default_sentinel_config() -> SentinelConfigDraft {
    SentinelConfigDraft {
        hostname: "localhost-dev".to_string(),
        collection_interval_ms: 2000,
        retention_policy_days: 7,
        alert_threshold_cpu: 85.0,
        alert_threshold_ram_percent: 90.0,
    }
}
