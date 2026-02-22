/// Represents the leadership status of a shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeShardLeader {
    pub node_id: String,
    pub epoch: u64,
}

impl RuntimeShardLeader {
    /// Creates a new shard leader instance.
    pub fn new(node_id: impl Into<String>, epoch: u64) -> Self {
        Self {
            node_id: node_id.into(),
            epoch: epoch.max(1),
        }
    }
}

/// A computed route for an operation on a specific shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeShardRoute {
    pub shard_id: u32,
    pub leader_node_id: String,
    pub leader_epoch: u64,
    pub local_is_leader: bool,
}

/// Represents a change in shard leadership (migration/failover).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeShardMovement {
    pub shard_id: u32,
    pub previous_leader: RuntimeShardLeader,
    pub next_leader: RuntimeShardLeader,
    pub followers: Vec<String>,
}
