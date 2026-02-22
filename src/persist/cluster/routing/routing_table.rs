/// Defines the topology of the cluster, mapping shards to leaders and followers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeShardRoutingTable {
    pub shard_count: u32,
    pub leaders: HashMap<u32, RuntimeShardLeader>,
    pub default_leader: String,
    #[serde(default)]
    pub followers: HashMap<u32, Vec<String>>,
    #[serde(default)]
    pub write_quorum: HashMap<u32, usize>,
}

// Keep routing-table behavior split by concern to keep cluster logic maintainable.
include!("routing_table/construct_and_validate.rs");
include!("routing_table/mutations.rs");
include!("routing_table/lookups.rs");
