use std::pin::Pin;

use futures_util::Stream;
use futures_util::stream;
use lattice_core::actor_ref::NodeAddress;
use lattice_core::coordinator::CoordinatorScope;

use crate::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
    DiscoverySource, DiscoveryTarget, validate_snapshot,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticEndpoint {
    pub address: NodeAddress,
    pub expected_node_id: Option<String>,
    pub priority: u16,
}

#[derive(Debug, Clone)]
pub struct StaticDiscovery {
    snapshot: CoordinatorDirectorySnapshot,
}

impl StaticDiscovery {
    pub fn new(
        scope: CoordinatorScope,
        name: impl Into<String>,
        endpoints: Vec<StaticEndpoint>,
    ) -> Result<Self, DiscoveryError> {
        let name = name.into();
        if name.is_empty() || name.len() > 128 {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "static provider name must contain at most 128 bytes".to_string(),
            });
        }
        let targets = endpoints
            .into_iter()
            .map(|endpoint| DiscoveryTarget {
                address: endpoint.address,
                expected_node_id: endpoint.expected_node_id,
                source: DiscoverySource::single(DiscoveryOrigin::Static { name: name.clone() }),
                priority: endpoint.priority,
            })
            .collect();
        let snapshot = CoordinatorDirectorySnapshot {
            scope,
            generation: 1,
            targets,
        };
        validate_snapshot(&snapshot)?;
        Ok(Self { snapshot })
    }
}

impl CoordinatorDiscovery for StaticDiscovery {
    fn scope(&self) -> &CoordinatorScope {
        &self.snapshot.scope
    }

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        Box::pin(stream::once(async { Ok(self.snapshot.clone()) }))
    }
}
