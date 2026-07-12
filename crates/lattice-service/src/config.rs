use std::collections::BTreeSet;
use std::time::Duration;

use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
use lattice_remoting::config::RemotingConfig;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub cluster_id: ClusterId,
    pub node_id: String,
    pub address: NodeAddress,
    pub incarnation: NodeIncarnation,
    pub roles: BTreeSet<String>,
    pub remoting: RemotingConfig,
    pub maximum_actor_protocols: usize,
    pub maximum_watches: usize,
    pub maximum_supervised_tasks: usize,
    pub shutdown_timeout: Duration,
}

impl NodeConfig {
    pub fn validate(&self) -> Result<(), NodeConfigError> {
        if self.node_id.is_empty()
            || self.node_id.len() > 128
            || self.node_id.contains(['/', '\\'])
            || self.node_id.chars().any(char::is_control)
        {
            return Err(NodeConfigError::InvalidNodeId);
        }
        if self.roles.len() > 32
            || self
                .roles
                .iter()
                .any(|role| role.is_empty() || role.len() > 128)
        {
            return Err(NodeConfigError::InvalidRoles);
        }
        if self.maximum_actor_protocols == 0
            || self.maximum_watches == 0
            || self.maximum_supervised_tasks == 0
            || self.shutdown_timeout.is_zero()
        {
            return Err(NodeConfigError::ZeroLimit);
        }
        self.remoting.validate().map_err(NodeConfigError::Remoting)
    }
}

#[derive(Debug, Error)]
pub enum NodeConfigError {
    #[error("node ID is not canonical")]
    InvalidNodeId,
    #[error("node roles exceed their bounds or are invalid")]
    InvalidRoles,
    #[error("node limits and shutdown timeout must be nonzero")]
    ZeroLimit,
    #[error("remoting configuration is invalid")]
    Remoting(#[source] lattice_remoting::config::RemotingConfigError),
}
