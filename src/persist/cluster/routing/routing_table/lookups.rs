impl RuntimeShardRoutingTable {
    /// Returns follower nodes for the shard.
    pub fn followers_for_shard(&self, shard_id: u32) -> Vec<String> {
        self.followers.get(&shard_id).cloned().unwrap_or_default()
    }

    /// Returns all replica nodes (leader first, then followers) for the shard.
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

    /// Returns effective write quorum for the shard.
    ///
    /// Uses configured override when present, otherwise computes majority.
    pub fn write_quorum_for_shard(&self, shard_id: u32) -> usize {
        if let Some(override_quorum) = self.write_quorum.get(&shard_id) {
            return *override_quorum;
        }
        let total = self.replica_nodes_for_shard(shard_id).len().max(1);
        (total / 2) + 1
    }

    /// Computes shard id for `(entity_type, entity_id)`.
    pub fn shard_for(&self, entity_type: &str, entity_id: &str) -> u32 {
        stable_shard_for(entity_type, entity_id, self.shard_count)
    }

    /// Returns the current leader for a shard.
    ///
    /// Falls back to the `default_leader` with epoch `1` when shard has no explicit override.
    pub fn leader_for_shard(&self, shard_id: u32) -> RuntimeShardLeader {
        self.leaders
            .get(&shard_id)
            .cloned()
            .unwrap_or_else(|| RuntimeShardLeader::new(self.default_leader.clone(), 1))
    }

    /// Calculates route for a specific entity from the perspective of a local node.
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
