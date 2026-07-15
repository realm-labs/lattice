use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use lattice_actor::host::ActorHost;
use lattice_actor::host::ProtocolHostRegistry;
use lattice_actor::protocol::{
    ActorProtocol, ActorProtocolBinding, Protocol, SupportsAsk, SupportsTell,
};
use lattice_actor::recipient::{
    ActorSystem, RecipientBackend, RecipientError, RegisteredActorProtocol,
};
use lattice_actor::registry::{ActorLoader, ActorRegistry};
use lattice_actor::traits::{Actor, Message, Request};
use lattice_core::actor_ref::{ActorRef, EntityRef, ProtocolTag, RecipientRef, SingletonRef};
use lattice_discovery::provider::CoordinatorDiscovery;
use lattice_placement::authority::AuthorityEffect;
use lattice_placement::control::{
    PlacementControlDirectory, PlacementControlEvent, PlacementControlRouter,
};
use lattice_placement::coordinator::{
    MemberChange, MemberEvent, MemberHello, PlacementDomainHello, SingletonConfig,
};
use lattice_placement::region::EntityConfig;
use lattice_placement::runtime::CoordinatorHandle;
use lattice_placement::runtime::host::CoordinatorHost;
use lattice_placement::session::LogicCoordinatorConfig;
use lattice_placement::session::LogicCoordinatorHandle;
use lattice_placement::session::LogicPlacementEffect;
use lattice_placement::session::PlacementDomainSession;
use lattice_placement::storage::{
    CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore,
};
use lattice_placement::types::NodeKey;
use lattice_remoting::association::Association;
use lattice_remoting::association::AssociationManager;
use lattice_remoting::bootstrap::BootstrapLeader;
use lattice_remoting::control::ControlDispatch;
use lattice_remoting::control::RejectControlDispatch;
use lattice_remoting::endpoint::EndpointSecurity;
use lattice_remoting::endpoint::RemotingEndpoint;
use lattice_remoting::handshake::NodeIdentity;
use lattice_remoting::messaging::outbound::OutboundMessaging;
use lattice_remoting::protocol::ProtocolDescriptor;
use lattice_remoting::watch::WatchRegistry;
use tokio::sync::{mpsc, watch};

use crate::backend::{
    DomainRouterDirectory, LogicalRouter, ServiceInboundDispatch, ServiceRecipientBackend,
};
use crate::cluster::join::{BootstrapView, JoinController};
use crate::cluster::members::{MemberDirectory, MemberSnapshot};
use crate::cluster::membership_runtime::MembershipJoinRuntime;
use crate::cluster::peers::PeerReconciler;
use crate::cluster::runtime::LogicJoinRuntime;
use crate::cluster::{ClusterRouterError, DomainLogicalRouter, LogicalBufferConfig};
use crate::config::{ClusterJoinConfig, NodeConfig};
use crate::control::ServiceControlDispatch;
use crate::error::ServiceError;
use crate::lifecycle::{
    NodeLifecycle, NodeLifecycleState, PlacementDomainState, ServiceHealthSnapshot,
    ServiceLifecycleEvent,
};
use crate::supervisor::TaskSupervisor;

type ActorSystemInstaller = Box<
    dyn Fn(&ActorSystem) -> Result<(), lattice_actor::recipient::ProtocolRegistrationError>
        + Send
        + Sync,
>;
type DomainEntityInstaller =
    dyn Fn(&mut DomainLogicalRouter) -> Result<(), ClusterRouterError> + Send + Sync;

#[derive(Clone)]
pub(crate) struct LogicalEntityInstaller {
    pub domain: lattice_core::actor_ref::PlacementDomainId,
    pub install: Arc<DomainEntityInstaller>,
}

pub struct LatticeServiceBuilder {
    config: NodeConfig,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    hosts: ProtocolHostRegistry,
    protocols: BTreeMap<u64, lattice_remoting::protocol::ProtocolFingerprint>,
    actor_protocols: BTreeMap<u64, RegisteredActorProtocol>,
    actor_system_installers: Vec<ActorSystemInstaller>,
    entity_configs: Vec<EntityConfig>,
    proxied_entity_configs: Vec<EntityConfig>,
    singleton_configs: Vec<SingletonConfig>,
    proxied_singleton_configs: Vec<SingletonConfig>,
    entity_installers: Vec<LogicalEntityInstaller>,
    logical: Option<Arc<dyn LogicalRouter>>,
    control_dispatch: Arc<dyn ControlDispatch>,
    logic_runtime: Option<LogicRuntimeAssembly>,
    control_scope: Option<lattice_remoting::association::AssociationKey>,
    coordinator_runtime: Option<CoordinatorRuntimeAssembly>,
    endpoint_security: Option<EndpointSecurity>,
    discoveries:
        BTreeMap<lattice_core::coordinator::CoordinatorScope, Arc<dyn CoordinatorDiscovery>>,
    join_config: ClusterJoinConfig,
    member_event_capacity: usize,
    domain_capacity: BTreeMap<lattice_core::actor_ref::PlacementDomainId, u64>,
}

struct LogicRuntimeAssembly {
    domain: lattice_core::actor_ref::PlacementDomainId,
    session: PlacementDomainSession,
    controls: mpsc::Receiver<PlacementControlEvent>,
    effects: mpsc::Receiver<LogicPlacementEffect>,
    handle: LogicCoordinatorHandle,
    router: Arc<dyn LogicalRouter>,
}

struct CoordinatorRuntimeAssembly {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    shutdown: watch::Sender<bool>,
    handles: BTreeMap<lattice_core::actor_ref::PlacementDomainId, CoordinatorHandle>,
    bootstrap_leaders: Vec<BootstrapLeader>,
    directory: watch::Receiver<
        BTreeMap<
            lattice_core::coordinator::CoordinatorScope,
            lattice_placement::coordinator::LeaderRecord,
        >,
    >,
}

impl LatticeServiceBuilder {
    pub fn new(config: NodeConfig) -> Result<Self, ServiceError> {
        config.validate().map_err(ServiceError::Config)?;
        let associations = Arc::new(
            AssociationManager::new(
                config.address.clone(),
                config.incarnation,
                config.remoting.clone(),
            )
            .map_err(ServiceError::Association)?,
        );
        let messaging = Arc::new(
            OutboundMessaging::new(config.remoting.max_pending_asks)
                .map_err(ServiceError::Messaging)?,
        );
        Ok(Self {
            hosts: ProtocolHostRegistry::new(config.maximum_actor_protocols)
                .map_err(ServiceError::Host)?,
            config,
            protocols: BTreeMap::new(),
            actor_protocols: BTreeMap::new(),
            actor_system_installers: Vec::new(),
            entity_configs: Vec::new(),
            proxied_entity_configs: Vec::new(),
            singleton_configs: Vec::new(),
            proxied_singleton_configs: Vec::new(),
            entity_installers: Vec::new(),
            logical: None,
            control_dispatch: Arc::new(RejectControlDispatch),
            logic_runtime: None,
            control_scope: None,
            coordinator_runtime: None,
            endpoint_security: None,
            discoveries: BTreeMap::new(),
            join_config: ClusterJoinConfig::default(),
            member_event_capacity: 256,
            domain_capacity: BTreeMap::new(),
            associations,
            messaging,
        })
    }

    pub fn association_manager(&self) -> Arc<AssociationManager> {
        self.associations.clone()
    }

    pub fn outbound_messaging(&self) -> Arc<OutboundMessaging> {
        self.messaging.clone()
    }

    pub fn register_actor<A: Actor, P: Protocol>(
        mut self,
        registry: Arc<ActorRegistry<A>>,
        protocol: Arc<ActorProtocolBinding<A, P>>,
    ) -> Result<Self, ServiceError> {
        if registry.protocol_id() != Some(protocol.protocol_id()) {
            return Err(ServiceError::ProtocolRegistration(
                lattice_actor::recipient::ProtocolRegistrationError::RegistryProtocolMismatch {
                    registry_protocol_id: registry.protocol_id().map(|id| id.get()),
                    binding_protocol_id: protocol.protocol_id().get(),
                },
            ));
        }
        self.register_protocol_entry(protocol.protocol().clone())?;
        let actor_registry = registry.clone();
        self.actor_system_installers
            .push(Box::new(move |actor_system| {
                actor_registry
                    .install_actor_system(actor_system.clone())
                    .map_err(|_| {
                        lattice_actor::recipient::ProtocolRegistrationError::ActorSystemAlreadyInstalled
                    })
            }));
        self.hosts
            .register(ActorHost::new(registry, protocol))
            .map_err(ServiceError::Host)?;
        Ok(self)
    }

    /// Registers an actor as a placement-managed logical entity.
    ///
    /// The entity declaration is advertised in `PlacementDomainHello`, its protocol is
    /// installed in the service catalogue, and the loader is re-registered
    /// automatically whenever discovery selects a new Coordinator.
    pub fn register_entity<A, L, P>(
        mut self,
        config: EntityConfig,
        registry: Arc<ActorRegistry<A>>,
        protocol: Arc<ActorProtocolBinding<A, P>>,
        loader: L,
    ) -> Result<Self, ServiceError>
    where
        A: Actor,
        L: ActorLoader<A>,
        P: Protocol,
    {
        config.validate().map_err(ServiceError::EntityConfig)?;
        if config.protocol_id != protocol.protocol_id() {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::ProtocolMismatch,
            ));
        }
        if self
            .entity_configs
            .iter()
            .chain(self.proxied_entity_configs.iter())
            .any(|registered| {
                registered.domain == config.domain && registered.entity_type == config.entity_type
            })
        {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::DuplicateEntity {
                    domain: config.domain,
                    entity_type: config.entity_type,
                },
            ));
        }
        self = self.register_actor(registry.clone(), protocol.clone())?;
        self.entity_configs.push(config.clone());
        self.entity_installers.push(LogicalEntityInstaller {
            domain: config.domain.clone(),
            install: Arc::new(move |router| {
                router.register_entity(
                    config.clone(),
                    registry.clone(),
                    protocol.clone(),
                    loader.clone(),
                )
            }),
        });
        Ok(self)
    }

    /// Registers a logical entity protocol for outbound routing without
    /// making this node eligible to own its shards.
    pub fn use_entity<P: Protocol>(mut self, config: EntityConfig) -> Result<Self, ServiceError> {
        config.validate().map_err(ServiceError::EntityConfig)?;
        let protocol = Arc::new(P::build_protocol().map_err(ServiceError::ProtocolBuild)?);
        if config.protocol_id != protocol.protocol_id() {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::ProtocolMismatch,
            ));
        }
        if self
            .entity_configs
            .iter()
            .chain(self.proxied_entity_configs.iter())
            .any(|registered| {
                registered.domain == config.domain && registered.entity_type == config.entity_type
            })
        {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::DuplicateEntity {
                    domain: config.domain,
                    entity_type: config.entity_type,
                },
            ));
        }
        let fingerprint = protocol.fingerprint();
        self = self.register_protocol(protocol)?;
        self.proxied_entity_configs.push(config.clone());
        self.entity_installers.push(LogicalEntityInstaller {
            domain: config.domain.clone(),
            install: Arc::new(move |router| {
                router.register_entity_proxy(config.clone(), fingerprint)
            }),
        });
        Ok(self)
    }

    pub fn register_singleton<A, L, P>(
        mut self,
        config: SingletonConfig,
        registry: Arc<ActorRegistry<A>>,
        protocol: Arc<ActorProtocolBinding<A, P>>,
        loader: L,
    ) -> Result<Self, ServiceError>
    where
        A: Actor,
        L: ActorLoader<A>,
        P: Protocol,
    {
        if !config.validate() || config.protocol_id != protocol.protocol_id() {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::ProtocolMismatch,
            ));
        }
        if self
            .singleton_configs
            .iter()
            .chain(self.proxied_singleton_configs.iter())
            .any(|registered| registered.domain == config.domain && registered.kind == config.kind)
        {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::DuplicateSingleton {
                    domain: config.domain,
                    kind: config.kind,
                },
            ));
        }
        self = self.register_actor(registry.clone(), protocol.clone())?;
        self.singleton_configs.push(config.clone());
        self.entity_installers.push(LogicalEntityInstaller {
            domain: config.domain.clone(),
            install: Arc::new(move |router| {
                router.register_singleton(
                    config.clone(),
                    registry.clone(),
                    protocol.clone(),
                    loader.clone(),
                )
            }),
        });
        Ok(self)
    }

    pub fn use_singleton<P: Protocol>(
        mut self,
        config: SingletonConfig,
    ) -> Result<Self, ServiceError> {
        if !config.validate() {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::ProtocolMismatch,
            ));
        }
        let protocol = Arc::new(P::build_protocol().map_err(ServiceError::ProtocolBuild)?);
        if config.protocol_id != protocol.protocol_id() {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::ProtocolMismatch,
            ));
        }
        if self
            .singleton_configs
            .iter()
            .chain(self.proxied_singleton_configs.iter())
            .any(|registered| registered.domain == config.domain && registered.kind == config.kind)
        {
            return Err(ServiceError::LogicalRouter(
                ClusterRouterError::DuplicateSingleton {
                    domain: config.domain,
                    kind: config.kind,
                },
            ));
        }
        let fingerprint = protocol.fingerprint();
        self = self.register_protocol(protocol)?;
        self.proxied_singleton_configs.push(config.clone());
        self.entity_installers.push(LogicalEntityInstaller {
            domain: config.domain.clone(),
            install: Arc::new(move |router| {
                router.register_singleton_proxy(config.clone(), fingerprint)
            }),
        });
        Ok(self)
    }

    /// Registers a typed actor protocol for outbound use without hosting a
    /// corresponding actor implementation in this process.
    pub fn register_protocol<P: Protocol>(
        mut self,
        protocol: Arc<ActorProtocol<P>>,
    ) -> Result<Self, ServiceError> {
        self.register_protocol_entry(protocol)?;
        Ok(self)
    }

    /// Builds and registers a protocol for outbound use without requiring the
    /// caller to allocate or retain its runtime descriptor.
    pub fn use_protocol<P: Protocol>(self) -> Result<Self, ServiceError> {
        let protocol = Arc::new(P::build_protocol().map_err(ServiceError::ProtocolBuild)?);
        self.register_protocol(protocol)
    }

    fn register_protocol_entry<P: Protocol>(
        &mut self,
        protocol: Arc<ActorProtocol<P>>,
    ) -> Result<(), ServiceError> {
        let protocol_id = protocol.protocol_id().get();
        if let Some(current) = self.protocols.get(&protocol_id) {
            if current == &protocol.fingerprint() {
                return Ok(());
            }
            return Err(ServiceError::ProtocolRegistration(
                lattice_actor::recipient::ProtocolRegistrationError::DuplicateProtocol(
                    protocol.protocol_id(),
                ),
            ));
        }
        self.protocols.insert(protocol_id, protocol.fingerprint());
        self.actor_protocols
            .insert(protocol_id, RegisteredActorProtocol::new(protocol));
        Ok(())
    }

    pub fn logical_router(mut self, router: Arc<dyn LogicalRouter>) -> Self {
        self.logical = Some(router);
        self
    }

    pub fn control_dispatch(mut self, dispatch: Arc<dyn ControlDispatch>) -> Self {
        self.control_dispatch = dispatch;
        self
    }

    pub fn endpoint_security(mut self, security: EndpointSecurity) -> Self {
        self.endpoint_security = Some(security);
        self
    }

    pub fn coordinator_discovery(
        mut self,
        discovery: Arc<dyn CoordinatorDiscovery>,
    ) -> Result<Self, ServiceError> {
        let scope = discovery.scope().clone();
        if self.discoveries.insert(scope, discovery).is_some() {
            return Err(ServiceError::InvalidPlacementDomains);
        }
        Ok(self)
    }

    pub fn join_config(mut self, config: ClusterJoinConfig) -> Self {
        self.join_config = config;
        self
    }

    pub fn member_event_capacity(mut self, capacity: usize) -> Self {
        self.member_event_capacity = capacity;
        self
    }

    pub fn domain_capacity(
        mut self,
        domain: lattice_core::actor_ref::PlacementDomainId,
        capacity_units: u64,
    ) -> Result<Self, ServiceError> {
        if capacity_units == 0
            || self
                .domain_capacity
                .insert(domain, capacity_units)
                .is_some()
        {
            return Err(ServiceError::InvalidCapacity);
        }
        Ok(self)
    }

    pub fn cluster_logic_runtime(
        mut self,
        router: Arc<dyn LogicalRouter>,
        dispatch: Arc<PlacementControlRouter>,
        session: PlacementDomainSession,
        controls: mpsc::Receiver<PlacementControlEvent>,
        effects: mpsc::Receiver<LogicPlacementEffect>,
    ) -> Self {
        let handle = session.control_handle();
        let domain = handle.domain().clone();
        self.control_scope = Some(session.coordinator_key().clone());
        self.logical = Some(router.clone());
        self.control_dispatch = dispatch;
        self.logic_runtime = Some(LogicRuntimeAssembly {
            domain,
            session,
            controls,
            effects,
            handle,
            router,
        });
        self
    }

    pub fn coordinator_host<S>(
        mut self,
        dispatch: Arc<PlacementControlRouter>,
        host: CoordinatorHost<S>,
        controls: mpsc::Receiver<PlacementControlEvent>,
    ) -> Self
    where
        S: CoordinatorLeaseStore + ScopedElectionStore + MembershipStore + PlacementDomainStore,
    {
        let directory = host.subscribe_directory();
        let mut scope_records = Vec::new();
        if let Some(lattice_placement::runtime::host::CoordinatorHostScopeState::Active(record)) =
            host.scope_state(&lattice_core::coordinator::CoordinatorScope::Membership)
        {
            scope_records.push(record.clone());
        }
        scope_records.extend(
            host.active_domain_leaders()
                .map(|(_, record)| record.clone()),
        );
        let handles = host
            .active_domain_leaders()
            .filter_map(|(domain, _)| {
                host.domain_handle(domain)
                    .map(|handle| (domain.clone(), handle))
            })
            .collect::<BTreeMap<_, _>>();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let bootstrap_leaders = scope_records
            .into_iter()
            .map(|record| BootstrapLeader {
                scope: record.scope,
                identity: NodeIdentity {
                    cluster_id: self.config.cluster_id.clone(),
                    node_id: record.node.node_id,
                    address: record.node.address,
                    incarnation: record.node.incarnation,
                },
                term: record.term.get(),
                protocol_generation: record.protocol_generation,
            })
            .collect();
        self.control_dispatch = dispatch;
        self.coordinator_runtime = Some(CoordinatorRuntimeAssembly {
            future: Box::pin(async move {
                if let Err(error) = host.run(controls, shutdown_rx).await {
                    tracing::error!(
                        target: "lattice.cluster.coordinator",
                        %error,
                        "Coordinator leader runtime terminated"
                    );
                }
            }),
            shutdown,
            handles,
            bootstrap_leaders,
            directory,
        });
        self
    }

    pub fn build(mut self) -> Result<LatticeService, ServiceError> {
        self.join_config
            .validate()
            .map_err(ServiceError::JoinConfig)?;
        let members = Arc::new(
            MemberDirectory::new(self.member_event_capacity)
                .map_err(ServiceError::MemberDirectory)?,
        );
        let mut discovered_router = None;
        let mut auto_join = Vec::new();
        let mut auto_membership = None;
        if !self.discoveries.is_empty() {
            if self.logic_runtime.is_some() {
                return Err(ServiceError::ConflictingClusterRuntime);
            }
            let dispatch = Arc::new(
                PlacementControlDirectory::new(
                    self.member_event_capacity,
                    self.config.maximum_actor_protocols,
                    lattice_placement::control::DEFAULT_MAX_CONTROL_PAYLOAD,
                )
                .map_err(ServiceError::PlacementControl)?,
            );
            self.control_dispatch = dispatch.clone();
            let protocols = self
                .protocols
                .iter()
                .map(|(protocol_id, fingerprint)| ProtocolDescriptor {
                    protocol_id: lattice_core::actor_ref::ProtocolId::new(*protocol_id)
                        .expect("registered actor protocols have nonzero IDs"),
                    fingerprint: *fingerprint,
                })
                .collect();
            let domains = self
                .entity_configs
                .iter()
                .chain(self.proxied_entity_configs.iter())
                .map(|config| config.domain.clone())
                .chain(
                    self.singleton_configs
                        .iter()
                        .chain(self.proxied_singleton_configs.iter())
                        .map(|config| config.domain.clone()),
                )
                .collect::<BTreeSet<_>>();
            if !domains.is_empty() {
                let directory = Arc::new(
                    DomainRouterDirectory::new(
                        domains.iter().cloned(),
                        self.config.maximum_actor_protocols,
                    )
                    .map_err(|_| ServiceError::InvalidPlacementDomains)?,
                );
                self.logical = Some(directory.clone());
                discovered_router = Some(directory);
            }
            let node = NodeKey {
                node_id: self.config.node_id.clone(),
                address: self.config.address.clone(),
                incarnation: self.config.incarnation,
            };
            let member_hello = MemberHello {
                node: node.clone(),
                roles: self.config.roles.clone(),
                failure_domains: BTreeMap::new(),
                protocols,
                remoting_capabilities: BTreeSet::new(),
            };
            let membership_scope = lattice_core::coordinator::CoordinatorScope::Membership;
            let membership_discovery = self
                .discoveries
                .remove(&membership_scope)
                .ok_or(ServiceError::InvalidPlacementDomains)?;
            let membership_controls = dispatch
                .register(membership_scope)
                .map_err(ServiceError::PlacementControl)?;
            auto_membership = Some((
                membership_discovery,
                membership_controls,
                member_hello.clone(),
            ));
            for domain in domains {
                let scope = lattice_core::coordinator::CoordinatorScope::Placement(domain.clone());
                let discovery = self
                    .discoveries
                    .remove(&scope)
                    .ok_or(ServiceError::InvalidPlacementDomains)?;
                let capacity = self
                    .domain_capacity
                    .remove(&domain)
                    .ok_or(ServiceError::InvalidCapacity)?;
                let controls = dispatch
                    .register(scope)
                    .map_err(ServiceError::PlacementControl)?;
                let hosted = self
                    .entity_configs
                    .iter()
                    .filter(|config| config.domain == domain)
                    .cloned()
                    .collect::<Vec<_>>();
                let proxied = self
                    .proxied_entity_configs
                    .iter()
                    .filter(|config| config.domain == domain)
                    .cloned()
                    .collect::<Vec<_>>();
                let hosted_singletons = self
                    .singleton_configs
                    .iter()
                    .filter(|config| config.domain == domain)
                    .cloned()
                    .collect::<Vec<_>>();
                let proxied_singletons = self
                    .proxied_singleton_configs
                    .iter()
                    .filter(|config| config.domain == domain)
                    .cloned()
                    .collect::<Vec<_>>();
                auto_join.push((
                    discovery,
                    controls,
                    member_hello.clone(),
                    PlacementDomainHello::new(
                        node.clone(),
                        domain,
                        capacity,
                        hosted
                            .iter()
                            .map(|config| config.entity_type.clone())
                            .collect(),
                        proxied
                            .iter()
                            .map(|config| config.entity_type.clone())
                            .collect(),
                        hosted_singletons
                            .iter()
                            .map(|config| config.kind.clone())
                            .collect(),
                        proxied_singletons
                            .iter()
                            .map(|config| config.kind.clone())
                            .collect(),
                        hosted,
                        hosted_singletons,
                        BTreeMap::new(),
                    ),
                ));
            }
            if !self.discoveries.is_empty() || !self.domain_capacity.is_empty() {
                return Err(ServiceError::InvalidPlacementDomains);
            }
        }
        let associations = self.associations;
        let messaging = self.messaging;
        let hosts = Arc::new(self.hosts);
        let logical = self.logical;
        let supervisor = Arc::new(TaskSupervisor::new(self.config.maximum_supervised_tasks)?);
        let watches = Arc::new(std::sync::Mutex::new(
            WatchRegistry::new(self.config.maximum_watches, self.config.maximum_watches)
                .map_err(ServiceError::Watch)?,
        ));
        let backend: Arc<dyn RecipientBackend> = Arc::new(ServiceRecipientBackend {
            local_cluster: self.config.cluster_id.clone(),
            local_address: self.config.address.clone(),
            local_incarnation: self.config.incarnation,
            hosts: hosts.clone(),
            associations: associations.clone(),
            messaging: messaging.clone(),
            watches: watches.clone(),
            maximum_control_payload: lattice_placement::control::DEFAULT_MAX_CONTROL_PAYLOAD,
            supervisor: supervisor.clone(),
            logical: logical.clone(),
        });
        let actor_system = ActorSystem::new(backend, self.actor_protocols.into_values())
            .map_err(ServiceError::ProtocolRegistration)?;
        for install in self.actor_system_installers {
            install(&actor_system).map_err(ServiceError::ProtocolRegistration)?;
        }
        let inbound = Arc::new(ServiceInboundDispatch {
            hosts: hosts.clone(),
            logical,
        });
        let control_dispatch = Arc::new(
            ServiceControlDispatch::new(
                self.control_dispatch,
                associations.clone(),
                hosts.clone(),
                watches.clone(),
                supervisor.clone(),
                lattice_placement::control::DEFAULT_MAX_CONTROL_PAYLOAD,
                self.control_scope,
            )
            .map_err(ServiceError::Control)?,
        );
        let local_identity = NodeIdentity {
            cluster_id: self.config.cluster_id.clone(),
            node_id: self.config.node_id.clone(),
            address: self.config.address.clone(),
            incarnation: self.config.incarnation,
        };
        let endpoint = Arc::new(
            RemotingEndpoint::new_with_control_and_security(
                local_identity.clone(),
                self.config.remoting.clone(),
                associations.clone(),
                messaging.clone(),
                inbound,
                control_dispatch,
                self.protocols
                    .into_iter()
                    .map(|(protocol_id, fingerprint)| ProtocolDescriptor {
                        protocol_id: lattice_core::actor_ref::ProtocolId::new(protocol_id)
                            .expect("registered actor protocols have nonzero IDs"),
                        fingerprint,
                    })
                    .collect(),
                self.endpoint_security,
            )
            .map_err(ServiceError::Endpoint)?,
        );
        let bootstrap_view = Arc::new(BootstrapView::new(local_identity));
        if let Some(runtime) = self.coordinator_runtime.as_ref() {
            for leader in &runtime.bootstrap_leaders {
                bootstrap_view.install(leader.clone());
            }
        }
        endpoint.install_bootstrap_handler(bootstrap_view.clone());
        let peers = Arc::new(PeerReconciler::new(
            self.config.cluster_id.clone(),
            endpoint.clone(),
            associations.clone(),
            members.clone(),
        ));
        let lifecycle = Arc::new(std::sync::Mutex::new(NodeLifecycle::default()));
        let (lifecycle_events, _) = watch::channel(NodeLifecycleState::Booting);
        let mut initial_logic_handles = BTreeMap::new();
        if let Some(runtime) = self.logic_runtime.as_ref() {
            initial_logic_handles.insert(runtime.domain.clone(), runtime.handle.clone());
        }
        let logic_handles = Arc::new(std::sync::Mutex::new(initial_logic_handles));
        let (drain_ready, _) = watch::channel(BTreeMap::new());
        let mut configured_domains = auto_join
            .iter()
            .map(|(_, _, _, hello)| hello.domain.clone())
            .collect::<BTreeSet<_>>();
        if let Some(runtime) = self.logic_runtime.as_ref() {
            configured_domains.insert(runtime.domain.clone());
        }
        let health = Arc::new(std::sync::Mutex::new(ServiceHealthSnapshot {
            node: NodeLifecycleState::Booting,
            domains: configured_domains
                .iter()
                .cloned()
                .map(|domain| (domain, PlacementDomainState::Joining))
                .collect(),
        }));
        let (health_events, _) = watch::channel(health.lock().expect("health poisoned").clone());
        let membership_ready = Arc::new(AtomicBool::new(false));
        let membership_handle = Arc::new(std::sync::Mutex::new(None));
        let join_runtimes = auto_join
            .into_iter()
            .map(|(discovery, controls, _member_hello, domain_hello)| {
                let controller = JoinController::new(
                    discovery,
                    endpoint.clone(),
                    associations.clone(),
                    self.join_config.clone(),
                )
                .map_err(ServiceError::Join)?;
                Ok(LogicJoinRuntime {
                    controller: Arc::new(controller),
                    domain_hello,
                    associations: associations.clone(),
                    controls: Some(controls),
                    config: LogicCoordinatorConfig::default(),
                    effect_capacity: self.member_event_capacity,
                    router: discovered_router
                        .clone()
                        .expect("discovery always installs a logical router"),
                    entity_installers: self.entity_installers.clone(),
                    messaging: messaging.clone(),
                    buffer_config: LogicalBufferConfig::default(),
                    maximum_registrations: self.config.maximum_actor_protocols,
                    peers: peers.clone(),
                    watches: watches.clone(),
                    lifecycle: lifecycle.clone(),
                    lifecycle_events: lifecycle_events.clone(),
                    health: health.clone(),
                    health_events: health_events.clone(),
                    logic_handles: logic_handles.clone(),
                    drain_ready: drain_ready.clone(),
                    bootstrap_view: bootstrap_view.clone(),
                    membership_ready: membership_ready.clone(),
                })
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;
        let membership_join_runtime = auto_membership
            .map(|(discovery, controls, hello)| {
                let controller = JoinController::new(
                    discovery,
                    endpoint.clone(),
                    associations.clone(),
                    self.join_config.clone(),
                )
                .map_err(ServiceError::Join)?;
                Ok(MembershipJoinRuntime {
                    controller: Arc::new(controller),
                    hello,
                    associations: associations.clone(),
                    controls: Some(controls),
                    config: LogicCoordinatorConfig::default(),
                    effect_capacity: self.member_event_capacity,
                    peers: peers.clone(),
                    watches: watches.clone(),
                    lifecycle: lifecycle.clone(),
                    lifecycle_events: lifecycle_events.clone(),
                    health: health.clone(),
                    health_events: health_events.clone(),
                    bootstrap_view: bootstrap_view.clone(),
                    ready: membership_ready,
                    handle: membership_handle.clone(),
                })
            })
            .transpose()?;
        Ok(LatticeService {
            cluster_id: self.config.cluster_id.clone(),
            actor_system,
            associations,
            messaging,
            endpoint,
            supervisor,
            logic_runtime: std::sync::Mutex::new(self.logic_runtime),
            join_runtimes: std::sync::Mutex::new(join_runtimes),
            membership_join_runtime: std::sync::Mutex::new(membership_join_runtime),
            membership_handle,
            logic_shutdown: std::sync::Mutex::new(None),
            join_shutdown: std::sync::Mutex::new(None),
            logic_handles,
            watches,
            coordinator_runtime: std::sync::Mutex::new(self.coordinator_runtime),
            coordinator_shutdown: std::sync::Mutex::new(None),
            coordinator_handles: std::sync::Mutex::new(BTreeMap::new()),
            lifecycle,
            lifecycle_events,
            health,
            health_events,
            members,
            peers,
            bootstrap_view,
            drain_ready,
            configured_domains,
            drain_operation: std::sync::Mutex::new(None),
            join_config: self.join_config,
        })
    }
}

include!("builder/service.rs");
