/// Maintains the set of active nodes in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeClusterMembership {
    #[serde(default)]
    pub nodes: HashSet<String>,
}

impl RuntimeClusterMembership {
    /// Creates a new membership set from a list of nodes.
    pub fn new(nodes: Vec<String>) -> Result<Self> {
        let mut membership = Self::default();
        for node in nodes {
            membership.add_node(node)?;
        }
        Ok(membership)
    }

    /// Adds a valid node to the membership.
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

    /// Removes a node from the membership.
    ///
    /// Returns true if the node was present.
    pub fn remove_node(&mut self, node_id: &str) -> bool {
        self.nodes.remove(node_id)
    }

    /// Checks if a node is part of the cluster.
    pub fn contains(&self, node_id: &str) -> bool {
        self.nodes.contains(node_id)
    }

    /// Returns a sorted list of all nodes.
    pub fn all_nodes(&self) -> Vec<String> {
        let mut nodes = self.nodes.iter().cloned().collect::<Vec<_>>();
        nodes.sort();
        nodes
    }
}
