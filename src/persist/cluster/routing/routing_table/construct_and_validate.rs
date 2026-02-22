impl RuntimeShardRoutingTable {
    /// Creates a new routing table with a specific shard count and default leader.
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

    /// Validates the integrity of the routing table.
    ///
    /// Checks for:
    /// - valid shard counts and indices,
    /// - non-empty node IDs,
    /// - valid epochs,
    /// - distinct leaders and followers,
    /// - satisfiable per-shard quorum overrides.
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
}
