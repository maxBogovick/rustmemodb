impl RuntimeShardRoutingTable {
    /// Explicitly sets the leader for a shard.
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

    /// Sets the list of followers for a shard.
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

    /// Sets a custom write quorum for a shard.
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

    /// Moves shard leadership to `new_leader` and bumps leader epoch.
    ///
    /// The previous leader is demoted to follower (unless identical to the new leader),
    /// follower list is deduplicated, and configured shard quorum is validated.
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
}
