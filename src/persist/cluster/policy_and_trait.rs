/// Defines the write consistency policy for the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeClusterWritePolicy {
    /// If true, writes require acknowledgment from a quorum of replicas.
    pub require_quorum: bool,
    /// If true, writes verify that the leader is in the correct epoch (fencing).
    pub enforce_epoch_fencing: bool,
}

impl Default for RuntimeClusterWritePolicy {
    fn default() -> Self {
        Self {
            require_quorum: true,
            enforce_epoch_fencing: true,
        }
    }
}

/// Status of the quorum for a specific operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeClusterQuorumStatus {
    pub shard_id: u32,
    pub required_acks: usize,
    pub acknowledged_nodes: Vec<String>,
    pub failed_nodes: Vec<String>,
}

impl RuntimeClusterQuorumStatus {
    /// Returns true if the number of acknowledged nodes meets the requirement.
    pub fn quorum_met(&self) -> bool {
        self.acknowledged_nodes.len() >= self.required_acks
    }
}

/// The result of applying a command within the cluster.
#[derive(Debug, Clone)]
pub struct RuntimeClusterApplyResult {
    /// The route used for the operation.
    pub route: RuntimeShardRoute,
    /// Whether the command was forwarded to another node (the leader).
    pub forwarded: bool,
    /// The quorum status of the operation.
    pub quorum: RuntimeClusterQuorumStatus,
    /// The result of the local application of the command.
    pub apply_result: RuntimeEnvelopeApplyResult,
}

/// Trait for components that can forward commands to other nodes in the cluster.
#[async_trait]
pub trait RuntimeClusterForwarder: Send + Sync {
    /// Forwards a command to a target node.
    async fn forward_command(
        &self,
        target_node: &str,
        envelope: RuntimeCommandEnvelope,
        route: RuntimeShardRoute,
    ) -> Result<RuntimeEnvelopeApplyResult>;

    /// Probes a replica to check reachability and route validity.
    async fn probe_replica(&self, _target_node: &str, _route: &RuntimeShardRoute) -> Result<()> {
        Ok(())
    }

    /// Replicates a command to a follower node.
    async fn replicate_command(
        &self,
        target_node: &str,
        envelope: RuntimeCommandEnvelope,
        route: RuntimeShardRoute,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        self.forward_command(target_node, envelope, route).await
    }
}
