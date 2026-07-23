use std::{collections::BTreeSet, time::Duration};

use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
use lattice_remoting::config::{RemotingConfig, RemotingConfigError};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterJoinConfig {
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub retry_multiplier: f64,
    pub retry_jitter: f64,
    pub probe_concurrency: usize,
    pub leadership_refresh_interval: Duration,
    pub join_timeout: Option<Duration>,
    pub discovery_stale_grace: Duration,
    pub leave_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for ClusterJoinConfig {
    fn default() -> Self {
        Self {
            retry_initial: Duration::from_millis(250),
            retry_max: Duration::from_secs(30),
            retry_multiplier: 2.0,
            retry_jitter: 0.2,
            probe_concurrency: 4,
            leadership_refresh_interval: Duration::from_secs(5),
            join_timeout: None,
            discovery_stale_grace: Duration::from_secs(60),
            leave_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(45),
        }
    }
}

impl ClusterJoinConfig {
    pub fn validate(&self) -> Result<(), ClusterJoinConfigError> {
        if self.retry_initial.is_zero()
            || self.retry_max.is_zero()
            || self.probe_concurrency == 0
            || self.leadership_refresh_interval.is_zero()
            || self.discovery_stale_grace.is_zero()
            || self.leave_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
            || self.join_timeout.is_some_and(|timeout| timeout.is_zero())
        {
            return Err(ClusterJoinConfigError::ZeroLimit);
        }
        if self.retry_initial > self.retry_max {
            return Err(ClusterJoinConfigError::RetryRange);
        }
        if !self.retry_multiplier.is_finite() || self.retry_multiplier < 1.0 {
            return Err(ClusterJoinConfigError::RetryMultiplier);
        }
        if !self.retry_jitter.is_finite() || !(0.0..1.0).contains(&self.retry_jitter) {
            return Err(ClusterJoinConfigError::RetryJitter);
        }
        if self.leave_timeout > self.shutdown_timeout {
            return Err(ClusterJoinConfigError::LeaveBudget);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ClusterJoinConfigError {
    #[error("cluster join limits and durations must be nonzero")]
    ZeroLimit,
    #[error("cluster join retry initial delay exceeds retry maximum")]
    RetryRange,
    #[error("cluster join retry multiplier must be finite and at least one")]
    RetryMultiplier,
    #[error("cluster join retry jitter must be finite and in [0, 1)")]
    RetryJitter,
    #[error("cluster leave timeout exceeds the overall shutdown timeout")]
    LeaveBudget,
}

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
    Remoting(#[source] RemotingConfigError),
}

#[cfg(test)]
mod join_tests {
    use std::time::Duration;

    use super::{ClusterJoinConfig, ClusterJoinConfigError};

    #[test]
    fn cluster_join_defaults_are_valid() {
        ClusterJoinConfig::default().validate().unwrap();
    }

    #[test]
    fn cluster_join_rejects_invalid_retry_and_shutdown_bounds() {
        let mut config = ClusterJoinConfig {
            retry_initial: Duration::from_secs(31),
            ..ClusterJoinConfig::default()
        };
        assert_eq!(config.validate(), Err(ClusterJoinConfigError::RetryRange));

        config = ClusterJoinConfig::default();
        config.retry_jitter = 1.0;
        assert_eq!(config.validate(), Err(ClusterJoinConfigError::RetryJitter));

        config = ClusterJoinConfig::default();
        config.leadership_refresh_interval = Duration::ZERO;
        assert_eq!(config.validate(), Err(ClusterJoinConfigError::ZeroLimit));

        config = ClusterJoinConfig::default();
        config.leave_timeout = Duration::from_secs(46);
        assert_eq!(config.validate(), Err(ClusterJoinConfigError::LeaveBudget));
    }
}
