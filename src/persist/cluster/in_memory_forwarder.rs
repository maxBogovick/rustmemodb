/// Represents a peer node in the in-memory cluster.
#[derive(Clone)]
struct InMemoryRuntimePeer {
    runtime: Arc<Mutex<PersistEntityRuntime>>,
    routing_table: Option<RuntimeShardRoutingTable>,
}

/// An in-memory implementation of `RuntimeClusterForwarder` for testing.
///
/// Simulates network transmission by directly locking and invoking peer runtimes.
#[derive(Clone, Default)]
pub struct InMemoryRuntimeForwarder {
    peers: Arc<Mutex<HashMap<String, InMemoryRuntimePeer>>>,
}

impl InMemoryRuntimeForwarder {
    /// Creates a new, empty forwarder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a peer node with the forwarder, enabling it to receive commands.
    pub async fn register_peer(
        &self,
        node_id: impl Into<String>,
        runtime: Arc<Mutex<PersistEntityRuntime>>,
    ) -> Result<()> {
        self.register_peer_with_routing(node_id, runtime, None)
            .await
    }

    /// Registers a peer node with a specific routing table.
    ///
    /// This allows simulating nodes with different views of the cluster topology (e.g. split brain).
    pub async fn register_peer_with_routing(
        &self,
        node_id: impl Into<String>,
        runtime: Arc<Mutex<PersistEntityRuntime>>,
        routing_table: Option<RuntimeShardRoutingTable>,
    ) -> Result<()> {
        let node_id = node_id.into();
        if node_id.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "node_id must not be empty".to_string(),
            ));
        }
        if let Some(table) = routing_table.as_ref() {
            table.validate()?;
        }
        let mut peers = self.peers.lock().await;
        peers.insert(
            node_id,
            InMemoryRuntimePeer {
                runtime,
                routing_table,
            },
        );
        Ok(())
    }

    /// Updates the routing table for an existing peer.
    pub async fn update_peer_routing_table(
        &self,
        node_id: &str,
        routing_table: Option<RuntimeShardRoutingTable>,
    ) -> Result<()> {
        if let Some(table) = routing_table.as_ref() {
            table.validate()?;
        }
        let mut peers = self.peers.lock().await;
        let peer = peers.get_mut(node_id).ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Cluster forwarder target node '{}' is not registered",
                node_id
            ))
        })?;
        peer.routing_table = routing_table;
        Ok(())
    }

    async fn peer(&self, node_id: &str) -> Result<InMemoryRuntimePeer> {
        let peers = self.peers.lock().await;
        peers.get(node_id).cloned().ok_or_else(|| {
            DbError::ExecutionError(format!(
                "Cluster forwarder target node '{}' is not registered",
                node_id
            ))
        })
    }

    fn validate_peer_route(
        node_id: &str,
        peer: &InMemoryRuntimePeer,
        route: &RuntimeShardRoute,
    ) -> Result<()> {
        let Some(routing_table) = peer.routing_table.as_ref() else {
            return Ok(());
        };

        let peer_leader = routing_table.leader_for_shard(route.shard_id);
        if peer_leader.node_id != route.leader_node_id || peer_leader.epoch != route.leader_epoch {
            return Err(DbError::ExecutionError(format!(
                "Epoch fence rejected by node '{}' for shard {}: expected leader '{}'@{}, peer has '{}'@{}",
                node_id,
                route.shard_id,
                route.leader_node_id,
                route.leader_epoch,
                peer_leader.node_id,
                peer_leader.epoch
            )));
        }
        Ok(())
    }
}

#[async_trait]
#[async_trait]
impl RuntimeClusterForwarder for InMemoryRuntimeForwarder {
    /// Forwards a command envelope to a target node within the in-memory cluster.
    ///
    /// Validates routing epochs before execution.
    async fn forward_command(
        &self,
        target_node: &str,
        envelope: RuntimeCommandEnvelope,
        route: RuntimeShardRoute,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        if target_node != route.leader_node_id {
            return Err(DbError::ExecutionError(format!(
                "Routing mismatch: target node '{}' does not match route leader '{}'",
                target_node, route.leader_node_id
            )));
        }

        let peer = self.peer(target_node).await?;
        Self::validate_peer_route(target_node, &peer, &route)?;

        let mut guard = peer.runtime.lock().await;
        guard.apply_command_envelope(envelope).await
    }

    /// Probes a replica to ensure it receives and validates the route.
    ///
    /// Used for epoch fencing pre-flight checks.
    async fn probe_replica(&self, target_node: &str, route: &RuntimeShardRoute) -> Result<()> {
        let peer = self.peer(target_node).await?;
        Self::validate_peer_route(target_node, &peer, route)
    }

    /// Replicates a successfully applied command to a follower.
    async fn replicate_command(
        &self,
        target_node: &str,
        envelope: RuntimeCommandEnvelope,
        route: RuntimeShardRoute,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        let peer = self.peer(target_node).await?;
        Self::validate_peer_route(target_node, &peer, &route)?;

        let mut guard = peer.runtime.lock().await;
        guard.apply_command_envelope(envelope).await
    }
}
