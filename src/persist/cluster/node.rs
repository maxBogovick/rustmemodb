/// Represents a node in the runtime cluster.
///
/// Orchestrates command routing, forwarding, replication, and quorum consistency.
pub struct RuntimeClusterNode {
    node_id: String,
    routing_table: RuntimeShardRoutingTable,
    forwarder: Arc<dyn RuntimeClusterForwarder>,
    write_policy: RuntimeClusterWritePolicy,
}

impl RuntimeClusterNode {
    /// Creates a new cluster node with default write policy.
    pub fn new(
        node_id: impl Into<String>,
        routing_table: RuntimeShardRoutingTable,
        forwarder: Arc<dyn RuntimeClusterForwarder>,
    ) -> Result<Self> {
        Self::new_with_policy(
            node_id,
            routing_table,
            forwarder,
            RuntimeClusterWritePolicy::default(),
        )
    }

    /// Creates a new cluster node with a specific write policy.
    pub fn new_with_policy(
        node_id: impl Into<String>,
        routing_table: RuntimeShardRoutingTable,
        forwarder: Arc<dyn RuntimeClusterForwarder>,
        write_policy: RuntimeClusterWritePolicy,
    ) -> Result<Self> {
        let node_id = node_id.into();
        if node_id.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "node_id must not be empty".to_string(),
            ));
        }
        routing_table.validate()?;
        Ok(Self {
            node_id,
            routing_table,
            forwarder,
            write_policy,
        })
    }

    /// Returns the unique ID of this node.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Returns a reference to the current routing table.
    pub fn routing_table(&self) -> &RuntimeShardRoutingTable {
        &self.routing_table
    }

    /// Updates the routing table for the node.
    ///
    /// Validates the table before applying.
    pub fn set_routing_table(&mut self, routing_table: RuntimeShardRoutingTable) -> Result<()> {
        routing_table.validate()?;
        self.routing_table = routing_table;
        Ok(())
    }

    /// Returns a reference to the current write policy.
    pub fn write_policy(&self) -> &RuntimeClusterWritePolicy {
        &self.write_policy
    }

    /// Updates the write policy for the node.
    pub fn set_write_policy(&mut self, write_policy: RuntimeClusterWritePolicy) {
        self.write_policy = write_policy;
    }

    /// Calculates the route for a specific entity.
    pub fn route_for(&self, entity_type: &str, entity_id: &str) -> RuntimeShardRoute {
        self.routing_table
            .route_for(entity_type, entity_id, self.node_id.as_str())
    }

    /// Applies a command envelope, handling routing and consistency automatically.
    ///
    /// Returns just the application result, hiding cluster details.
    pub async fn apply_command_envelope(
        &self,
        runtime: &mut PersistEntityRuntime,
        envelope: RuntimeCommandEnvelope,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        let detailed = self
            .apply_command_envelope_with_cluster(runtime, envelope)
            .await?;
        Ok(detailed.apply_result)
    }

    /// Applies a command envelope, returning detailed cluster execution info.
    ///
    /// Handles:
    /// - Routing to the correct shard leader.
    /// - Forwarding if local node is not the leader.
    /// - Replication and quorum checks if local node is the leader.
    pub async fn apply_command_envelope_with_cluster(
        &self,
        runtime: &mut PersistEntityRuntime,
        envelope: RuntimeCommandEnvelope,
    ) -> Result<RuntimeClusterApplyResult> {
        let mut envelope = envelope;
        if envelope.idempotency_key.is_none() {
            envelope.idempotency_key = Some(format!("cluster-envelope:{}", envelope.envelope_id));
        }

        let route = self.route_for(envelope.entity_type.as_str(), envelope.entity_id.as_str());
        if route.local_is_leader {
            self.apply_on_local_leader(runtime, envelope, route).await
        } else {
            if self.write_policy.enforce_epoch_fencing {
                self.forwarder
                    .probe_replica(route.leader_node_id.as_str(), &route)
                    .await?;
            }
            let target_node = route.leader_node_id.clone();
            let apply_result = self
                .forwarder
                .forward_command(target_node.as_str(), envelope, route.clone())
                .await?;
            let quorum = RuntimeClusterQuorumStatus {
                shard_id: route.shard_id,
                required_acks: self.routing_table.write_quorum_for_shard(route.shard_id),
                acknowledged_nodes: vec![target_node],
                failed_nodes: Vec::new(),
            };
            Ok(RuntimeClusterApplyResult {
                route,
                forwarded: true,
                quorum,
                apply_result,
            })
        }
    }

    /// Helper to apply a basic command constructed from arguments.
    pub async fn apply_deterministic_command(
        &self,
        runtime: &mut PersistEntityRuntime,
        entity_type: &str,
        entity_id: &str,
        command: &str,
        payload: serde_json::Value,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        self.apply_command_envelope(
            runtime,
            RuntimeCommandEnvelope::new(entity_type, entity_id, command, payload),
        )
        .await
    }

    /// Internal logic for executing a command when the local node is the leader.
    ///
    /// Performs pre-flight checks, local application, and replication.
    async fn apply_on_local_leader(
        &self,
        runtime: &mut PersistEntityRuntime,
        envelope: RuntimeCommandEnvelope,
        route: RuntimeShardRoute,
    ) -> Result<RuntimeClusterApplyResult> {
        let followers = self.routing_table.followers_for_shard(route.shard_id);
        let required_acks = self.routing_table.write_quorum_for_shard(route.shard_id);

        if (self.write_policy.require_quorum || self.write_policy.enforce_epoch_fencing)
            && !followers.is_empty()
        {
            let mut preflight_ok = vec![self.node_id.clone()];
            let mut preflight_failed = Vec::new();
            for follower in &followers {
                match self
                    .forwarder
                    .probe_replica(follower.as_str(), &route)
                    .await
                {
                    Ok(()) => preflight_ok.push(follower.clone()),
                    Err(err) => preflight_failed.push(format!("{} ({})", follower, err)),
                }
            }

            if self.write_policy.require_quorum && preflight_ok.len() < required_acks {
                return Err(DbError::ExecutionError(format!(
                    "Cluster quorum preflight failed for shard {} at epoch {}: required {}, reachable {}, failures: {}",
                    route.shard_id,
                    route.leader_epoch,
                    required_acks,
                    preflight_ok.len(),
                    preflight_failed.join("; ")
                )));
            }
        }

        let apply_result = runtime.apply_command_envelope(envelope.clone()).await?;

        let mut acknowledged_nodes = vec![self.node_id.clone()];
        let mut failed_nodes = Vec::new();
        for follower in followers {
            match self
                .forwarder
                .replicate_command(follower.as_str(), envelope.clone(), route.clone())
                .await
            {
                Ok(_) => acknowledged_nodes.push(follower),
                Err(err) => failed_nodes.push(format!("{} ({})", follower, err)),
            }
        }

        let quorum = RuntimeClusterQuorumStatus {
            shard_id: route.shard_id,
            required_acks,
            acknowledged_nodes,
            failed_nodes,
        };
        if self.write_policy.require_quorum && !quorum.quorum_met() {
            return Err(DbError::ExecutionError(format!(
                "Cluster quorum not met for shard {} at epoch {}: required {}, acknowledged {}, failed: {}",
                route.shard_id,
                route.leader_epoch,
                quorum.required_acks,
                quorum.acknowledged_nodes.len(),
                quorum.failed_nodes.join("; ")
            )));
        }

        Ok(RuntimeClusterApplyResult {
            route,
            forwarded: false,
            quorum,
            apply_result,
        })
    }
}
