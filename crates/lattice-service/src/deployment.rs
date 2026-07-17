use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use lattice_core::actor_ref::PlacementDomainId;
use lattice_core::coordinator::CoordinatorScope;
use lattice_discovery::static_provider::{StaticDiscovery, StaticEndpoint};
use lattice_placement::control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter};
use lattice_placement::runtime::host::{CoordinatorHost, CoordinatorHostConfig};
use lattice_placement::storage::{
    CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore,
};
use lattice_placement::types::NodeKey;

use crate::builder::{LatticeService, LatticeServiceBuilder};
use crate::config::NodeConfig;
use crate::error::ServiceError;
use crate::lifecycle::NodeLifecycleState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorDeploymentMode {
    EmbeddedCandidate,
    ClientOnly,
    DedicatedCandidate,
}

/// Configuration for a Coordinator candidate managed inside an application
/// process. The candidate has a separate remoting identity from the logic
/// service so control-plane election never depends on its own placement.
#[derive(Debug, Clone)]
pub struct EmbeddedCoordinatorConfig {
    pub node: NodeConfig,
    pub candidates: Vec<StaticEndpoint>,
    pub host: CoordinatorHostConfig,
}

impl EmbeddedCoordinatorConfig {
    pub fn new(node: NodeConfig) -> Self {
        let local = StaticEndpoint {
            address: node.address.clone(),
            expected_node_id: Some(node.node_id.clone()),
            priority: 1,
        };
        Self {
            node,
            candidates: vec![local],
            host: CoordinatorHostConfig::default(),
        }
    }

    pub fn candidates(mut self, candidates: Vec<StaticEndpoint>) -> Self {
        self.candidates = candidates;
        self
    }

    pub fn host_config(mut self, host: CoordinatorHostConfig) -> Self {
        self.host = host;
        self
    }
}

/// Supervises the services required by one deployment mode.
pub struct LatticeApplication {
    mode: CoordinatorDeploymentMode,
    logic: Option<Arc<LatticeService>>,
    coordinator: Option<Arc<LatticeService>>,
}

impl LatticeApplication {
    fn client(logic: LatticeService) -> Self {
        Self {
            mode: CoordinatorDeploymentMode::ClientOnly,
            logic: Some(Arc::new(logic)),
            coordinator: None,
        }
    }

    fn embedded(logic: LatticeService, coordinator: LatticeService) -> Self {
        Self {
            mode: CoordinatorDeploymentMode::EmbeddedCandidate,
            logic: Some(Arc::new(logic)),
            coordinator: Some(Arc::new(coordinator)),
        }
    }

    pub fn mode(&self) -> CoordinatorDeploymentMode {
        self.mode
    }

    pub fn logic(&self) -> Option<&Arc<LatticeService>> {
        self.logic.as_ref()
    }

    pub fn coordinator_service(&self) -> Option<&Arc<LatticeService>> {
        self.coordinator.as_ref()
    }

    pub async fn start(&self) -> Result<(), ServiceError> {
        if let Some(coordinator) = &self.coordinator {
            coordinator.start().await?;
        }
        if let Some(logic) = &self.logic
            && let Err(error) = logic.start().await
        {
            if let Some(coordinator) = &self.coordinator {
                let _ = coordinator.force_shutdown().await;
            }
            return Err(error);
        }
        Ok(())
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<(), ServiceError> {
        tokio::time::timeout(timeout, async {
            if let Some(logic) = &self.logic {
                let mut health = logic.subscribe_health();
                loop {
                    let ready =
                        health.borrow().node == NodeLifecycleState::Ready
                            && health.borrow().domains.values().all(|state| {
                                *state == crate::lifecycle::PlacementDomainState::Ready
                            });
                    if ready {
                        break;
                    }
                    if matches!(
                        health.borrow().node,
                        NodeLifecycleState::Stopping | NodeLifecycleState::Terminated
                    ) {
                        return Err(ServiceError::TaskFailed);
                    }
                    health
                        .changed()
                        .await
                        .map_err(|_| ServiceError::TaskFailed)?;
                }
            }
            if let Some(coordinator) = &self.coordinator {
                let mut health = coordinator.subscribe_health();
                loop {
                    let ready = health.borrow().node == NodeLifecycleState::Ready
                        && !health.borrow().coordinator_scopes.is_empty()
                        && health.borrow().coordinator_scopes.values().all(|state| {
                            matches!(
                                state,
                                crate::lifecycle::CoordinatorScopeState::Active
                                    | crate::lifecycle::CoordinatorScopeState::Standby
                            )
                        });
                    if ready {
                        break;
                    }
                    if matches!(
                        health.borrow().node,
                        NodeLifecycleState::Stopping | NodeLifecycleState::Terminated
                    ) {
                        return Err(ServiceError::TaskFailed);
                    }
                    health
                        .changed()
                        .await
                        .map_err(|_| ServiceError::TaskFailed)?;
                }
            }
            Ok::<(), ServiceError>(())
        })
        .await
        .map_err(|_| ServiceError::ReadinessTimeout)??;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        let mut first_error = None;
        if let Some(logic) = &self.logic
            && let Err(error) = logic.shutdown().await
        {
            first_error = Some(ServiceError::ApplicationShutdown {
                component: "logic",
                source: Box::new(error),
            });
        }
        if let Some(coordinator) = &self.coordinator
            && let Err(error) = coordinator.shutdown().await
            && first_error.is_none()
        {
            first_error = Some(ServiceError::ApplicationShutdown {
                component: "coordinator-candidate",
                source: Box::new(error),
            });
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Terminates every component without requiring placement migration to another deployment.
    ///
    /// This is intended for an intentional whole-deployment stop. Local actors still receive
    /// their normal stop lifecycle; use the lower-level force APIs only for operator recovery.
    pub async fn terminal_shutdown(&self) -> Result<(), ServiceError> {
        let mut first_error = None;
        if let Some(logic) = &self.logic
            && let Err(error) = logic.terminal_shutdown().await
        {
            first_error = Some(ServiceError::ApplicationShutdown {
                component: "logic",
                source: Box::new(error),
            });
        }
        if let Some(coordinator) = &self.coordinator
            && let Err(error) = coordinator.terminal_shutdown().await
            && first_error.is_none()
        {
            first_error = Some(ServiceError::ApplicationShutdown {
                component: "coordinator-candidate",
                source: Box::new(error),
            });
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub async fn dedicated_candidate<S>(
        node: NodeConfig,
        store: Arc<S>,
        domains: BTreeSet<PlacementDomainId>,
        host_config: CoordinatorHostConfig,
    ) -> Result<Self, ServiceError>
    where
        S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
    {
        let coordinator = assemble_coordinator(node, store, domains, host_config).await?;
        Ok(Self {
            mode: CoordinatorDeploymentMode::DedicatedCandidate,
            logic: None,
            coordinator: Some(Arc::new(coordinator)),
        })
    }
}

impl LatticeServiceBuilder {
    pub fn build_client(self) -> Result<LatticeApplication, ServiceError> {
        Ok(LatticeApplication::client(self.build()?))
    }

    pub async fn build_embedded<S>(
        mut self,
        store: Arc<S>,
        config: EmbeddedCoordinatorConfig,
    ) -> Result<LatticeApplication, ServiceError>
    where
        S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
    {
        if self.node_config().cluster_id != config.node.cluster_id
            || self.node_config().address == config.node.address
        {
            return Err(ServiceError::InvalidDeployment);
        }
        let mut candidates = config.candidates;
        if !candidates
            .iter()
            .any(|candidate| candidate.address == config.node.address)
        {
            candidates.push(StaticEndpoint {
                address: config.node.address.clone(),
                expected_node_id: Some(config.node.node_id.clone()),
                priority: 1,
            });
        }
        let candidate_domains = self.hosted_domains();
        let joined_domains = self.placement_domains();
        self = install_static_discovery(self, &joined_domains, &candidates)?;
        let coordinator =
            assemble_coordinator(config.node, store, candidate_domains, config.host).await?;
        let logic = self.build()?;
        Ok(LatticeApplication::embedded(logic, coordinator))
    }
}

fn install_static_discovery(
    mut builder: LatticeServiceBuilder,
    domains: &BTreeSet<PlacementDomainId>,
    candidates: &[StaticEndpoint],
) -> Result<LatticeServiceBuilder, ServiceError> {
    builder = builder.coordinator_discovery(Arc::new(StaticDiscovery::new(
        CoordinatorScope::Membership,
        "application-coordinator-membership",
        candidates.to_vec(),
    )?))?;
    for domain in domains {
        builder = builder.coordinator_discovery(Arc::new(StaticDiscovery::new(
            CoordinatorScope::Placement(domain.clone()),
            format!("application-coordinator-{}", domain.as_str()),
            candidates.to_vec(),
        )?))?;
    }
    Ok(builder)
}

async fn assemble_coordinator<S>(
    node: NodeConfig,
    store: Arc<S>,
    domains: BTreeSet<PlacementDomainId>,
    host_config: CoordinatorHostConfig,
) -> Result<LatticeService, ServiceError>
where
    S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
{
    let address = node.address.clone();
    let node_key = NodeKey {
        node_id: node.node_id.clone(),
        address,
        incarnation: node.incarnation,
    };
    let builder = LatticeService::builder(node)?;
    let host = CoordinatorHost::elect(
        store,
        builder.association_manager(),
        node_key,
        domains,
        host_config,
    )
    .await?;
    let (control, controls) = PlacementControlRouter::bounded(256, DEFAULT_MAX_CONTROL_PAYLOAD)
        .map_err(ServiceError::PlacementControl)?;
    builder
        .coordinator_host(Arc::new(control), host, controls)
        .build()
}

#[cfg(test)]
mod tests {
    use super::{CoordinatorDeploymentMode, EmbeddedCoordinatorConfig, LatticeApplication};
    use crate::builder::LatticeService;
    use crate::config::NodeConfig;
    use crate::lifecycle::NodeLifecycleState;
    use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};
    use lattice_placement::runtime::host::CoordinatorHostConfig;
    use lattice_placement::storage::InMemoryPlacementStore;
    use lattice_remoting::config::RemotingConfig;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    fn unused_address() -> NodeAddress {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        NodeAddress::new("127.0.0.1", port).unwrap()
    }

    fn node(cluster: &ClusterId, node_id: &str) -> NodeConfig {
        NodeConfig {
            cluster_id: cluster.clone(),
            node_id: node_id.to_owned(),
            address: unused_address(),
            incarnation: NodeIncarnation::generate(),
            roles: BTreeSet::new(),
            remoting: RemotingConfig::default(),
            maximum_actor_protocols: 8,
            maximum_watches: 16,
            maximum_supervised_tasks: 32,
            shutdown_timeout: Duration::from_secs(3),
        }
    }

    #[tokio::test]
    async fn client_only_starts_without_a_coordinator_runtime() {
        let cluster = ClusterId::new("client-mode-test").unwrap();
        let application = LatticeService::builder(node(&cluster, "client"))
            .unwrap()
            .build_client()
            .unwrap();

        assert_eq!(application.mode(), CoordinatorDeploymentMode::ClientOnly);
        assert!(application.logic().is_some());
        assert!(application.coordinator_service().is_none());
        application.start().await.unwrap();
        application
            .wait_ready(Duration::from_secs(2))
            .await
            .unwrap();
        application.shutdown().await.unwrap();
        assert_eq!(
            application.logic().unwrap().node_lifecycle_state(),
            NodeLifecycleState::Terminated
        );
    }

    #[tokio::test]
    async fn client_only_supports_terminal_shutdown() {
        let cluster = ClusterId::new("client-terminal-test").unwrap();
        let application = LatticeService::builder(node(&cluster, "client"))
            .unwrap()
            .build_client()
            .unwrap();

        application.start().await.unwrap();
        application
            .wait_ready(Duration::from_secs(2))
            .await
            .unwrap();
        application.terminal_shutdown().await.unwrap();

        assert_eq!(
            application.logic().unwrap().node_lifecycle_state(),
            NodeLifecycleState::Terminated
        );
    }

    #[tokio::test]
    async fn dedicated_candidate_starts_only_the_control_plane() {
        let cluster = ClusterId::new("dedicated-mode-test").unwrap();
        let application = LatticeApplication::dedicated_candidate(
            node(&cluster, "coordinator"),
            Arc::new(InMemoryPlacementStore::new(16, 16).unwrap()),
            BTreeSet::new(),
            CoordinatorHostConfig::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            application.mode(),
            CoordinatorDeploymentMode::DedicatedCandidate
        );
        assert!(application.logic().is_none());
        assert!(application.coordinator_service().is_some());
        application.start().await.unwrap();
        application
            .wait_ready(Duration::from_secs(2))
            .await
            .unwrap();
        let health = application.coordinator_service().unwrap().health_snapshot();
        assert!(!health.coordinator_scopes.is_empty());
        assert!(health.coordinator_scopes.values().all(|state| matches!(
            state,
            crate::lifecycle::CoordinatorScopeState::Active
                | crate::lifecycle::CoordinatorScopeState::Standby
        )));
        application.shutdown().await.unwrap();
        assert_eq!(
            application
                .coordinator_service()
                .unwrap()
                .node_lifecycle_state(),
            NodeLifecycleState::Terminated
        );
    }

    #[tokio::test]
    async fn embedded_candidate_joins_the_managed_control_plane() {
        let cluster = ClusterId::new("embedded-mode-test").unwrap();
        let application = LatticeService::builder(node(&cluster, "logic"))
            .unwrap()
            .build_embedded(
                Arc::new(InMemoryPlacementStore::new(16, 16).unwrap()),
                EmbeddedCoordinatorConfig::new(node(&cluster, "coordinator")),
            )
            .await
            .unwrap();

        assert_eq!(
            application.mode(),
            CoordinatorDeploymentMode::EmbeddedCandidate
        );
        assert!(application.logic().is_some());
        assert!(application.coordinator_service().is_some());
        application.start().await.unwrap();
        application
            .wait_ready(Duration::from_secs(5))
            .await
            .unwrap();
        application.shutdown().await.unwrap();
    }
}
