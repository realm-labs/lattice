use std::collections::BTreeSet;
use std::pin::Pin;

use futures_util::Stream;
use lattice_core::actor_ref::NodeAddress;
use lattice_core::coordinator::CoordinatorScope;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiscoveryOrigin {
    Static {
        name: String,
    },
    ConfigStore {
        key: String,
    },
    Dns {
        query: String,
        server_name: String,
        weight: u16,
    },
    KubernetesEndpointSlice {
        namespace: String,
        service: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiscoverySource {
    origins: BTreeSet<DiscoveryOrigin>,
    tls_server_name: Option<String>,
}

impl DiscoverySource {
    pub fn single(origin: DiscoveryOrigin) -> Self {
        let tls_server_name = match &origin {
            DiscoveryOrigin::Dns { server_name, .. } => Some(server_name.clone()),
            DiscoveryOrigin::Static { .. }
            | DiscoveryOrigin::ConfigStore { .. }
            | DiscoveryOrigin::KubernetesEndpointSlice { .. } => None,
        };
        Self {
            origins: BTreeSet::from([origin]),
            tls_server_name,
        }
    }

    /// Declares the certificate hostname a probe must validate against. Any
    /// provider can set it, including implementations outside this crate whose
    /// addresses are resolved IPs.
    pub fn with_tls_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.tls_server_name = Some(server_name.into());
        self
    }

    pub fn tls_server_name(&self) -> Option<&str> {
        self.tls_server_name.as_deref()
    }

    pub fn origins(&self) -> impl ExactSizeIterator<Item = &DiscoveryOrigin> {
        self.origins.iter()
    }

    pub fn merge(&mut self, other: &Self) {
        self.origins.extend(other.origins.iter().cloned());
        if self.tls_server_name.is_none() {
            self.tls_server_name.clone_from(&other.tls_server_name);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.origins.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryTarget {
    pub address: NodeAddress,
    pub expected_node_id: Option<String>,
    pub source: DiscoverySource,
    pub priority: u16,
}

impl DiscoveryTarget {
    /// The certificate hostname this candidate must be validated against. A
    /// resolved IP address never replaces it.
    pub fn tls_server_name(&self) -> Option<&str> {
        self.source.tls_server_name()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorDirectorySnapshot {
    pub scope: CoordinatorScope,
    pub generation: u64,
    pub targets: Vec<DiscoveryTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DiscoveryError {
    #[error("invalid discovery configuration: {message}")]
    InvalidConfiguration { message: String },
    #[error("discovery provider {provider} failed: {message}")]
    Provider {
        provider: &'static str,
        message: String,
    },
    #[error("discovery snapshot is invalid: {message}")]
    InvalidSnapshot { message: String },
}

pub trait CoordinatorDiscovery: Send + Sync {
    fn scope(&self) -> &CoordinatorScope;

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>;
}

pub(crate) fn validate_target(target: &DiscoveryTarget) -> Result<(), DiscoveryError> {
    if target.source.is_empty() {
        return Err(DiscoveryError::InvalidSnapshot {
            message: format!("target {} has no source", target.address),
        });
    }
    if target
        .expected_node_id
        .as_ref()
        .is_some_and(|node_id| node_id.is_empty() || node_id.len() > 128)
    {
        return Err(DiscoveryError::InvalidSnapshot {
            message: format!("target {} has an invalid expected node ID", target.address),
        });
    }
    for origin in target.source.origins() {
        let valid = match origin {
            DiscoveryOrigin::Static { name } => !name.is_empty(),
            DiscoveryOrigin::ConfigStore { key } => !key.is_empty(),
            DiscoveryOrigin::Dns {
                query, server_name, ..
            } => !query.is_empty() && !server_name.is_empty(),
            DiscoveryOrigin::KubernetesEndpointSlice { namespace, service } => {
                !namespace.is_empty() && !service.is_empty()
            }
        };
        if !valid {
            return Err(DiscoveryError::InvalidSnapshot {
                message: format!("target {} has invalid source metadata", target.address),
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_snapshot(
    snapshot: &CoordinatorDirectorySnapshot,
) -> Result<(), DiscoveryError> {
    if snapshot.generation == 0 {
        return Err(DiscoveryError::InvalidSnapshot {
            message: "generation zero is reserved".to_string(),
        });
    }
    let mut addresses = BTreeSet::new();
    for target in &snapshot.targets {
        validate_target(target)?;
        if !addresses.insert(target.address.clone()) {
            return Err(DiscoveryError::InvalidSnapshot {
                message: format!("duplicate target address {}", target.address),
            });
        }
    }
    Ok(())
}
