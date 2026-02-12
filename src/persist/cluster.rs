use super::runtime::{PersistEntityRuntime, RuntimeCommandEnvelope, RuntimeEnvelopeApplyResult};
use crate::core::{DbError, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeShardLeader {
    pub node_id: String,
    pub epoch: u64,
}

impl RuntimeShardLeader {
    pub fn new(node_id: impl Into<String>, epoch: u64) -> Self {
        Self {
            node_id: node_id.into(),
            epoch: epoch.max(1),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeShardRoute {
    pub shard_id: u32,
    pub leader_node_id: String,
    pub leader_epoch: u64,
    pub local_is_leader: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeClusterMembership {
    #[serde(default)]
    pub nodes: HashSet<String>,
}

impl RuntimeClusterMembership {
    pub fn new(nodes: Vec<String>) -> Result<Self> {
        let mut membership = Self::default();
        for node in nodes {
            membership.add_node(node)?;
        }
        Ok(membership)
    }

    pub fn add_node(&mut self, node_id: impl Into<String>) -> Result<()> {
        let node_id = node_id.into();
        if node_id.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "membership node_id must not be empty".to_string(),
            ));
        }
        self.nodes.insert(node_id);
        Ok(())
    }

    pub fn remove_node(&mut self, node_id: &str) -> bool {
        self.nodes.remove(node_id)
    }

    pub fn contains(&self, node_id: &str) -> bool {
        self.nodes.contains(node_id)
    }

    pub fn all_nodes(&self) -> Vec<String> {
        let mut nodes = self.nodes.iter().cloned().collect::<Vec<_>>();
        nodes.sort();
        nodes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeShardMovement {
    pub shard_id: u32,
    pub previous_leader: RuntimeShardLeader,
    pub next_leader: RuntimeShardLeader,
    pub followers: Vec<String>,
}

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

impl RuntimeShardRoutingTable {
    pub fn new(shard_count: u32, default_leader: impl Into<String>) -> Result<Self> {
        let default_leader = default_leader.into();
        if shard_count == 0 {
            return Err(DbError::ExecutionError(
                "shard_count must be >= 1".to_string(),
            ));
        }
        if default_leader.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "default_leader must not be empty".to_string(),
            ));
        }

        Ok(Self {
            shard_count,
            leaders: HashMap::new(),
            default_leader,
            followers: HashMap::new(),
            write_quorum: HashMap::new(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.shard_count == 0 {
            return Err(DbError::ExecutionError(
                "shard_count must be >= 1".to_string(),
            ));
        }
        if self.default_leader.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "default_leader must not be empty".to_string(),
            ));
        }
        for (shard, leader) in &self.leaders {
            if *shard >= self.shard_count {
                return Err(DbError::ExecutionError(format!(
                    "Shard {} is out of range for shard_count {}",
                    shard, self.shard_count
                )));
            }
            if leader.node_id.trim().is_empty() {
                return Err(DbError::ExecutionError(format!(
                    "Leader node id for shard {} must not be empty",
                    shard
                )));
            }
            if leader.epoch == 0 {
                return Err(DbError::ExecutionError(format!(
                    "Leader epoch for shard {} must be >= 1",
                    shard
                )));
            }
        }

        for (shard, followers) in &self.followers {
            if *shard >= self.shard_count {
                return Err(DbError::ExecutionError(format!(
                    "Followers for shard {} are out of range for shard_count {}",
                    shard, self.shard_count
                )));
            }

            let mut dedupe = HashSet::new();
            let leader = self.leader_for_shard(*shard);
            for follower in followers {
                if follower.trim().is_empty() {
                    return Err(DbError::ExecutionError(format!(
                        "Follower node id for shard {} must not be empty",
                        shard
                    )));
                }
                if follower == &leader.node_id {
                    return Err(DbError::ExecutionError(format!(
                        "Follower '{}' for shard {} cannot be the current leader",
                        follower, shard
                    )));
                }
                if !dedupe.insert(follower) {
                    return Err(DbError::ExecutionError(format!(
                        "Follower '{}' appears more than once for shard {}",
                        follower, shard
                    )));
                }
            }
        }

        for (shard, quorum) in &self.write_quorum {
            if *shard >= self.shard_count {
                return Err(DbError::ExecutionError(format!(
                    "Quorum override for shard {} is out of range for shard_count {}",
                    shard, self.shard_count
                )));
            }
            let replica_count = self.replica_nodes_for_shard(*shard).len().max(1);
            if *quorum == 0 || *quorum > replica_count {
                return Err(DbError::ExecutionError(format!(
                    "Invalid quorum {} for shard {} with replica count {}",
                    quorum, shard, replica_count
                )));
            }
        }

        Ok(())
    }

    pub fn set_shard_leader(
        &mut self,
        shard_id: u32,
        node_id: impl Into<String>,
        epoch: u64,
    ) -> Result<()> {
        if shard_id >= self.shard_count {
            return Err(DbError::ExecutionError(format!(
                "Shard {} is out of range for shard_count {}",
                shard_id, self.shard_count
            )));
        }

        let node_id = node_id.into();
        if node_id.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "node_id must not be empty".to_string(),
            ));
        }

        self.leaders
            .insert(shard_id, RuntimeShardLeader::new(node_id, epoch));
        Ok(())
    }

    pub fn set_shard_followers(&mut self, shard_id: u32, followers: Vec<String>) -> Result<()> {
        if shard_id >= self.shard_count {
            return Err(DbError::ExecutionError(format!(
                "Shard {} is out of range for shard_count {}",
                shard_id, self.shard_count
            )));
        }

        let leader = self.leader_for_shard(shard_id);
        let mut seen = HashSet::new();
        let mut normalized = Vec::new();
        for follower in followers {
            let follower = follower.trim().to_string();
            if follower.is_empty() {
                return Err(DbError::ExecutionError(
                    "follower node id must not be empty".to_string(),
                ));
            }
            if follower == leader.node_id {
                return Err(DbError::ExecutionError(format!(
                    "Follower '{}' for shard {} cannot be the current leader",
                    follower, shard_id
                )));
            }
            if seen.insert(follower.clone()) {
                normalized.push(follower);
            }
        }

        if normalized.is_empty() {
            self.followers.remove(&shard_id);
        } else {
            self.followers.insert(shard_id, normalized);
        }
        Ok(())
    }

    pub fn set_shard_quorum(&mut self, shard_id: u32, required_acks: usize) -> Result<()> {
        if shard_id >= self.shard_count {
            return Err(DbError::ExecutionError(format!(
                "Shard {} is out of range for shard_count {}",
                shard_id, self.shard_count
            )));
        }
        let replica_count = self.replica_nodes_for_shard(shard_id).len().max(1);
        if required_acks == 0 || required_acks > replica_count {
            return Err(DbError::ExecutionError(format!(
                "Invalid quorum {} for shard {} with replica count {}",
                required_acks, shard_id, replica_count
            )));
        }
        self.write_quorum.insert(shard_id, required_acks);
        Ok(())
    }

    pub fn move_shard_leader(
        &mut self,
        shard_id: u32,
        new_leader: impl Into<String>,
        membership: Option<&RuntimeClusterMembership>,
    ) -> Result<RuntimeShardMovement> {
        if shard_id >= self.shard_count {
            return Err(DbError::ExecutionError(format!(
                "Shard {} is out of range for shard_count {}",
                shard_id, self.shard_count
            )));
        }

        let new_leader = new_leader.into();
        if new_leader.trim().is_empty() {
            return Err(DbError::ExecutionError(
                "new_leader must not be empty".to_string(),
            ));
        }
        if let Some(membership) = membership {
            if !membership.contains(new_leader.as_str()) {
                return Err(DbError::ExecutionError(format!(
                    "Shard move rejected: leader '{}' is not part of cluster membership",
                    new_leader
                )));
            }
        }

        let previous_leader = self.leader_for_shard(shard_id);
        let mut followers = self.followers_for_shard(shard_id);
        followers.retain(|node| node != &new_leader);
        if previous_leader.node_id != new_leader
            && !followers
                .iter()
                .any(|node| node == previous_leader.node_id.as_str())
        {
            followers.push(previous_leader.node_id.clone());
        }

        let mut seen = HashSet::new();
        followers.retain(|node| seen.insert(node.clone()));

        if let Some(quorum) = self.write_quorum.get(&shard_id).copied() {
            let replica_count = 1usize.saturating_add(followers.len());
            if quorum > replica_count {
                return Err(DbError::ExecutionError(format!(
                    "Shard move would violate quorum {} with replica count {} for shard {}",
                    quorum, replica_count, shard_id
                )));
            }
        }

        let next_leader =
            RuntimeShardLeader::new(new_leader, previous_leader.epoch.saturating_add(1));
        self.leaders.insert(shard_id, next_leader.clone());
        if followers.is_empty() {
            self.followers.remove(&shard_id);
        } else {
            self.followers.insert(shard_id, followers.clone());
        }

        Ok(RuntimeShardMovement {
            shard_id,
            previous_leader,
            next_leader,
            followers,
        })
    }

    pub fn followers_for_shard(&self, shard_id: u32) -> Vec<String> {
        self.followers.get(&shard_id).cloned().unwrap_or_default()
    }

    pub fn replica_nodes_for_shard(&self, shard_id: u32) -> Vec<String> {
        let mut nodes = Vec::new();
        let mut seen = HashSet::new();
        let leader = self.leader_for_shard(shard_id);
        if seen.insert(leader.node_id.clone()) {
            nodes.push(leader.node_id);
        }
        for follower in self.followers_for_shard(shard_id) {
            if seen.insert(follower.clone()) {
                nodes.push(follower);
            }
        }
        nodes
    }

    pub fn write_quorum_for_shard(&self, shard_id: u32) -> usize {
        if let Some(override_quorum) = self.write_quorum.get(&shard_id) {
            return *override_quorum;
        }
        let total = self.replica_nodes_for_shard(shard_id).len().max(1);
        (total / 2) + 1
    }

    pub fn shard_for(&self, entity_type: &str, entity_id: &str) -> u32 {
        stable_shard_for(entity_type, entity_id, self.shard_count)
    }

    pub fn leader_for_shard(&self, shard_id: u32) -> RuntimeShardLeader {
        self.leaders
            .get(&shard_id)
            .cloned()
            .unwrap_or_else(|| RuntimeShardLeader::new(self.default_leader.clone(), 1))
    }

    pub fn route_for(
        &self,
        entity_type: &str,
        entity_id: &str,
        local_node_id: &str,
    ) -> RuntimeShardRoute {
        let shard_id = self.shard_for(entity_type, entity_id);
        let leader = self.leader_for_shard(shard_id);

        RuntimeShardRoute {
            shard_id,
            leader_node_id: leader.node_id.clone(),
            leader_epoch: leader.epoch,
            local_is_leader: leader.node_id == local_node_id,
        }
    }
}

pub fn stable_shard_for(entity_type: &str, entity_id: &str, shard_count: u32) -> u32 {
    if shard_count == 0 {
        return 0;
    }
    let mut hash = 14695981039346656037u64;
    for byte in entity_type.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash ^= 0xff;
    hash = hash.wrapping_mul(1099511628211);
    for byte in entity_id.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    (hash % shard_count as u64) as u32
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeClusterWritePolicy {
    pub require_quorum: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeClusterQuorumStatus {
    pub shard_id: u32,
    pub required_acks: usize,
    pub acknowledged_nodes: Vec<String>,
    pub failed_nodes: Vec<String>,
}

impl RuntimeClusterQuorumStatus {
    pub fn quorum_met(&self) -> bool {
        self.acknowledged_nodes.len() >= self.required_acks
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeClusterApplyResult {
    pub route: RuntimeShardRoute,
    pub forwarded: bool,
    pub quorum: RuntimeClusterQuorumStatus,
    pub apply_result: RuntimeEnvelopeApplyResult,
}

#[async_trait]
pub trait RuntimeClusterForwarder: Send + Sync {
    async fn forward_command(
        &self,
        target_node: &str,
        envelope: RuntimeCommandEnvelope,
        route: RuntimeShardRoute,
    ) -> Result<RuntimeEnvelopeApplyResult>;

    async fn probe_replica(&self, _target_node: &str, _route: &RuntimeShardRoute) -> Result<()> {
        Ok(())
    }

    async fn replicate_command(
        &self,
        target_node: &str,
        envelope: RuntimeCommandEnvelope,
        route: RuntimeShardRoute,
    ) -> Result<RuntimeEnvelopeApplyResult> {
        self.forward_command(target_node, envelope, route).await
    }
}

pub struct RuntimeClusterNode {
    node_id: String,
    routing_table: RuntimeShardRoutingTable,
    forwarder: Arc<dyn RuntimeClusterForwarder>,
    write_policy: RuntimeClusterWritePolicy,
}

impl RuntimeClusterNode {
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

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn routing_table(&self) -> &RuntimeShardRoutingTable {
        &self.routing_table
    }

    pub fn set_routing_table(&mut self, routing_table: RuntimeShardRoutingTable) -> Result<()> {
        routing_table.validate()?;
        self.routing_table = routing_table;
        Ok(())
    }

    pub fn write_policy(&self) -> &RuntimeClusterWritePolicy {
        &self.write_policy
    }

    pub fn set_write_policy(&mut self, write_policy: RuntimeClusterWritePolicy) {
        self.write_policy = write_policy;
    }

    pub fn route_for(&self, entity_type: &str, entity_id: &str) -> RuntimeShardRoute {
        self.routing_table
            .route_for(entity_type, entity_id, self.node_id.as_str())
    }

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

#[derive(Clone)]
struct InMemoryRuntimePeer {
    runtime: Arc<Mutex<PersistEntityRuntime>>,
    routing_table: Option<RuntimeShardRoutingTable>,
}

#[derive(Clone, Default)]
pub struct InMemoryRuntimeForwarder {
    peers: Arc<Mutex<HashMap<String, InMemoryRuntimePeer>>>,
}

impl InMemoryRuntimeForwarder {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_peer(
        &self,
        node_id: impl Into<String>,
        runtime: Arc<Mutex<PersistEntityRuntime>>,
    ) -> Result<()> {
        self.register_peer_with_routing(node_id, runtime, None)
            .await
    }

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
impl RuntimeClusterForwarder for InMemoryRuntimeForwarder {
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

    async fn probe_replica(&self, target_node: &str, route: &RuntimeShardRoute) -> Result<()> {
        let peer = self.peer(target_node).await?;
        Self::validate_peer_route(target_node, &peer, route)
    }

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
