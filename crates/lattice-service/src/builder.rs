use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use lattice_actor::host::ActorHost;
use lattice_actor::host::ProtocolHostRegistry;
use lattice_actor::protocol::ActorProtocol;
use lattice_actor::recipient::{
    ActorSystem, RecipientBackend, RecipientError, RegisteredActorProtocol,
};
use lattice_actor::traits::{Actor, Message, Request};
use lattice_core::actor_ref::{ActorRef, EntityRef, RecipientRef, SingletonRef};
use lattice_discovery::provider::ClusterDiscovery;
use lattice_placement::authority::AuthorityEffect;
use lattice_placement::control::{PlacementControlEvent, PlacementControlRouter};
use lattice_placement::coordinator::{MemberChange, MemberEvent, NodeHello};
use lattice_placement::runtime::CoordinatorHandle;
use lattice_placement::runtime::CoordinatorLeader;
use lattice_placement::session::LogicCoordinatorConfig;
use lattice_placement::session::LogicCoordinatorHandle;
use lattice_placement::session::LogicCoordinatorSession;
use lattice_placement::session::LogicPlacementEffect;
use lattice_placement::storage::CoordinatorStore;
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

use crate::backend::{LogicalRouter, ServiceInboundDispatch, ServiceRecipientBackend};
use crate::cluster::join::{BootstrapView, JoinController};
use crate::cluster::members::{MemberDirectory, MemberSnapshot};
use crate::cluster::peers::PeerReconciler;
use crate::cluster::runtime::LogicJoinRuntime;
use crate::config::{ClusterJoinConfig, NodeConfig};
use crate::control::ServiceControlDispatch;
use crate::error::ServiceError;
use crate::lifecycle::{ServiceLifecycle, ServiceLifecycleEvent, ServiceLifecycleState};
use crate::supervisor::TaskSupervisor;

type ActorSystemInstaller = Box<
    dyn Fn(&ActorSystem) -> Result<(), lattice_actor::recipient::ProtocolRegistrationError>
        + Send
        + Sync,
>;

pub struct LatticeServiceBuilder {
    config: NodeConfig,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    hosts: ProtocolHostRegistry,
    protocols: BTreeMap<u64, lattice_remoting::protocol::ProtocolFingerprint>,
    actor_protocols: BTreeMap<u64, RegisteredActorProtocol>,
    actor_system_installers: Vec<ActorSystemInstaller>,
    logical: Option<Arc<dyn LogicalRouter>>,
    control_dispatch: Arc<dyn ControlDispatch>,
    logic_runtime: Option<LogicRuntimeAssembly>,
    control_scope: Option<lattice_remoting::association::AssociationKey>,
    coordinator_runtime: Option<CoordinatorRuntimeAssembly>,
    endpoint_security: Option<EndpointSecurity>,
    discovery: Option<Arc<dyn ClusterDiscovery>>,
    join_config: ClusterJoinConfig,
    member_event_capacity: usize,
}

struct LogicRuntimeAssembly {
    session: LogicCoordinatorSession,
    controls: mpsc::Receiver<PlacementControlEvent>,
    effects: mpsc::Receiver<LogicPlacementEffect>,
    handle: LogicCoordinatorHandle,
    router: Arc<dyn LogicalRouter>,
}

struct CoordinatorRuntimeAssembly {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    shutdown: watch::Sender<bool>,
    handle: CoordinatorHandle,
    bootstrap_leader: BootstrapLeader,
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
            logical: None,
            control_dispatch: Arc::new(RejectControlDispatch),
            logic_runtime: None,
            control_scope: None,
            coordinator_runtime: None,
            endpoint_security: None,
            discovery: None,
            join_config: ClusterJoinConfig::default(),
            member_event_capacity: 256,
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

    pub fn register_actor<A: Actor>(
        mut self,
        registry: Arc<lattice_actor::registry::ActorRegistry<A>>,
        protocol: Arc<ActorProtocol<A>>,
    ) -> Result<Self, ServiceError> {
        self.register_protocol_entry(protocol.clone())?;
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

    /// Registers a typed actor protocol for outbound use without hosting that
    /// actor type in this process.
    pub fn register_protocol<A: Actor>(
        mut self,
        protocol: Arc<ActorProtocol<A>>,
    ) -> Result<Self, ServiceError> {
        self.register_protocol_entry(protocol)?;
        Ok(self)
    }

    fn register_protocol_entry<A: Actor>(
        &mut self,
        protocol: Arc<ActorProtocol<A>>,
    ) -> Result<(), ServiceError> {
        let protocol_id = protocol.protocol_id().get();
        if self.actor_protocols.contains_key(&protocol_id) {
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

    pub fn cluster_discovery(mut self, discovery: Arc<dyn ClusterDiscovery>) -> Self {
        self.discovery = Some(discovery);
        self
    }

    pub fn join_config(mut self, config: ClusterJoinConfig) -> Self {
        self.join_config = config;
        self
    }

    pub fn member_event_capacity(mut self, capacity: usize) -> Self {
        self.member_event_capacity = capacity;
        self
    }

    pub fn cluster_logic_runtime(
        mut self,
        router: Arc<dyn LogicalRouter>,
        dispatch: Arc<PlacementControlRouter>,
        session: LogicCoordinatorSession,
        controls: mpsc::Receiver<PlacementControlEvent>,
        effects: mpsc::Receiver<LogicPlacementEffect>,
    ) -> Self {
        let handle = session.control_handle();
        self.control_scope = Some(session.coordinator_key().clone());
        self.logical = Some(router.clone());
        self.control_dispatch = dispatch;
        self.logic_runtime = Some(LogicRuntimeAssembly {
            session,
            controls,
            effects,
            handle,
            router,
        });
        self
    }

    pub fn cluster_coordinator_runtime<S: CoordinatorStore>(
        mut self,
        dispatch: Arc<PlacementControlRouter>,
        leader: CoordinatorLeader<S>,
        controls: mpsc::Receiver<PlacementControlEvent>,
    ) -> Self {
        let handle = leader.handle();
        let record = leader.leader().clone();
        let (shutdown, shutdown_rx) = watch::channel(false);
        self.control_dispatch = dispatch;
        self.coordinator_runtime = Some(CoordinatorRuntimeAssembly {
            future: Box::pin(async move {
                let _ = leader.run(controls, shutdown_rx).await;
            }),
            shutdown,
            handle,
            bootstrap_leader: BootstrapLeader {
                identity: NodeIdentity {
                    cluster_id: self.config.cluster_id.clone(),
                    node_id: record.node.node_id,
                    address: record.node.address,
                    incarnation: record.node.incarnation,
                },
                term: record.term.get(),
                protocol_generation: record.protocol_generation,
            },
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
        let auto_join = if let Some(discovery) = self.discovery.take() {
            if self.logic_runtime.is_some() {
                return Err(ServiceError::ConflictingClusterRuntime);
            }
            let (dispatch, controls) = PlacementControlRouter::bounded(
                self.member_event_capacity,
                lattice_placement::control::DEFAULT_MAX_CONTROL_PAYLOAD,
            )
            .map_err(ServiceError::PlacementControl)?;
            self.control_dispatch = Arc::new(dispatch);
            let protocols = self
                .protocols
                .iter()
                .map(|(protocol_id, fingerprint)| ProtocolDescriptor {
                    protocol_id: lattice_core::actor_ref::ProtocolId::new(*protocol_id)
                        .expect("registered actor protocols have nonzero IDs"),
                    fingerprint: *fingerprint,
                })
                .collect();
            Some((
                discovery,
                controls,
                NodeHello {
                    node: NodeKey {
                        node_id: self.config.node_id.clone(),
                        address: self.config.address.clone(),
                        incarnation: self.config.incarnation,
                    },
                    roles: self.config.roles.clone(),
                    capacity_units: 1,
                    hosted_entity_types: Default::default(),
                    proxied_entity_types: Default::default(),
                    singleton_eligibility: Default::default(),
                    used_singletons: Default::default(),
                    protocols,
                    entity_configs: Vec::new(),
                    singleton_configs: Vec::new(),
                },
            ))
        } else {
            None
        };
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
            bootstrap_view.install(runtime.bootstrap_leader.clone());
        }
        endpoint.install_bootstrap_handler(bootstrap_view.clone());
        let peers = Arc::new(PeerReconciler::new(
            self.config.cluster_id.clone(),
            endpoint.clone(),
            associations.clone(),
            members.clone(),
        ));
        let lifecycle = Arc::new(std::sync::Mutex::new(ServiceLifecycle::default()));
        let (lifecycle_events, _) = watch::channel(ServiceLifecycleState::Booting);
        let logic_handle = Arc::new(std::sync::Mutex::new(None));
        let (drain_ready, _) = watch::channel(None);
        let join_runtime = auto_join
            .map(|(discovery, controls, hello)| {
                let controller = JoinController::new(
                    discovery,
                    endpoint.clone(),
                    associations.clone(),
                    self.join_config.clone(),
                )
                .map_err(ServiceError::Join)?;
                Ok(LogicJoinRuntime {
                    controller: Arc::new(controller),
                    hello,
                    associations: associations.clone(),
                    controls: Some(controls),
                    config: LogicCoordinatorConfig::default(),
                    effect_capacity: self.member_event_capacity,
                    router: None,
                    members: members.clone(),
                    peers: peers.clone(),
                    watches: watches.clone(),
                    lifecycle: lifecycle.clone(),
                    lifecycle_events: lifecycle_events.clone(),
                    logic_handle: logic_handle.clone(),
                    drain_ready: drain_ready.clone(),
                    bootstrap_view: bootstrap_view.clone(),
                })
            })
            .transpose()?;
        Ok(LatticeService {
            actor_system,
            associations,
            messaging,
            endpoint,
            supervisor,
            logic_runtime: std::sync::Mutex::new(self.logic_runtime),
            join_runtime: std::sync::Mutex::new(join_runtime),
            logic_shutdown: std::sync::Mutex::new(None),
            join_shutdown: std::sync::Mutex::new(None),
            logic_handle,
            watches,
            coordinator_runtime: std::sync::Mutex::new(self.coordinator_runtime),
            coordinator_shutdown: std::sync::Mutex::new(None),
            coordinator_handle: std::sync::Mutex::new(None),
            lifecycle,
            lifecycle_events,
            members,
            peers,
            drain_ready,
            drain_operation: std::sync::Mutex::new(None),
            join_config: self.join_config,
        })
    }
}

pub struct LatticeService {
    actor_system: ActorSystem,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    endpoint: Arc<RemotingEndpoint>,
    supervisor: Arc<TaskSupervisor>,
    logic_runtime: std::sync::Mutex<Option<LogicRuntimeAssembly>>,
    join_runtime: std::sync::Mutex<Option<LogicJoinRuntime>>,
    logic_shutdown: std::sync::Mutex<Option<watch::Sender<bool>>>,
    join_shutdown: std::sync::Mutex<Option<watch::Sender<bool>>>,
    logic_handle: Arc<std::sync::Mutex<Option<LogicCoordinatorHandle>>>,
    watches: Arc<std::sync::Mutex<WatchRegistry>>,
    coordinator_runtime: std::sync::Mutex<Option<CoordinatorRuntimeAssembly>>,
    coordinator_shutdown: std::sync::Mutex<Option<watch::Sender<bool>>>,
    coordinator_handle: std::sync::Mutex<Option<CoordinatorHandle>>,
    lifecycle: Arc<std::sync::Mutex<ServiceLifecycle>>,
    lifecycle_events: watch::Sender<ServiceLifecycleState>,
    members: Arc<MemberDirectory>,
    peers: Arc<PeerReconciler>,
    drain_ready: watch::Sender<Option<String>>,
    drain_operation: std::sync::Mutex<Option<String>>,
    join_config: ClusterJoinConfig,
}

impl LatticeService {
    pub fn builder(config: NodeConfig) -> Result<LatticeServiceBuilder, ServiceError> {
        LatticeServiceBuilder::new(config)
    }

    pub fn actor_system(&self) -> &ActorSystem {
        &self.actor_system
    }

    pub async fn tell<A, M>(
        &self,
        target: impl Into<RecipientRef<A>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        A: Actor,
        M: Message,
    {
        self.actor_system.tell(target, message).await
    }

    pub async fn ask<A, R>(
        &self,
        target: impl Into<RecipientRef<A>>,
        request: R,
        deadline: std::time::Instant,
    ) -> Result<R::Response, RecipientError>
    where
        A: Actor,
        R: Request,
    {
        self.actor_system.ask(target, request, deadline).await
    }

    pub async fn watch<A>(
        &self,
        target: &ActorRef<A>,
    ) -> Result<lattice_remoting::watch::WatchId, RecipientError> {
        self.actor_system.watch(target).await
    }

    pub async fn watch_entity_current<A>(
        &self,
        target: &EntityRef<A>,
    ) -> Result<lattice_remoting::watch::WatchId, RecipientError> {
        self.actor_system.watch_entity_current(target).await
    }

    pub async fn watch_singleton_current<A>(
        &self,
        target: &SingletonRef<A>,
    ) -> Result<lattice_remoting::watch::WatchId, RecipientError> {
        self.actor_system.watch_singleton_current(target).await
    }

    pub async fn unwatch(
        &self,
        watch_id: lattice_remoting::watch::WatchId,
    ) -> Result<(), RecipientError> {
        self.actor_system.unwatch(watch_id).await
    }

    pub fn associations(&self) -> &AssociationManager {
        &self.associations
    }

    pub fn messaging(&self) -> &OutboundMessaging {
        &self.messaging
    }

    pub fn supervisor(&self) -> &TaskSupervisor {
        &self.supervisor
    }

    pub fn watch_status(
        &self,
        watch_id: lattice_remoting::watch::WatchId,
    ) -> lattice_remoting::watch::WatchStatus {
        self.watches
            .lock()
            .expect("watch registry poisoned")
            .status(watch_id)
    }

    pub fn coordinator(&self) -> Option<CoordinatorHandle> {
        self.coordinator_handle
            .lock()
            .expect("service Coordinator handle poisoned")
            .clone()
    }

    pub fn lifecycle_state(&self) -> ServiceLifecycleState {
        self.lifecycle
            .lock()
            .expect("service lifecycle poisoned")
            .state()
    }

    pub fn subscribe_lifecycle(&self) -> watch::Receiver<ServiceLifecycleState> {
        self.lifecycle_events.subscribe()
    }

    pub fn member_snapshot(&self) -> MemberSnapshot {
        self.members.snapshot()
    }

    pub fn subscribe_members(&self) -> tokio::sync::broadcast::Receiver<MemberEvent> {
        self.members.subscribe()
    }

    pub async fn connect_member(&self, node: &NodeKey) -> Result<Arc<Association>, ServiceError> {
        match self.peers.connect(node).await {
            Ok(association) => Ok(association),
            Err(crate::cluster::peers::PeerError::Endpoint(error)) => {
                Err(ServiceError::Endpoint(error))
            }
            Err(crate::cluster::peers::PeerError::NotAuthoritativeUp)
            | Err(crate::cluster::peers::PeerError::Directory(_)) => {
                Err(ServiceError::CoordinatorUnavailable)
            }
        }
    }

    fn transition(&self, event: ServiceLifecycleEvent) -> Result<(), ServiceError> {
        let mut lifecycle = self.lifecycle.lock().expect("service lifecycle poisoned");
        let previous = lifecycle.state();
        lifecycle
            .transition(event)
            .map_err(ServiceError::Lifecycle)?;
        let next = lifecycle.state();
        tracing::info!(
            target: "lattice.cluster.lifecycle",
            ?event,
            ?previous,
            ?next,
            "member lifecycle transition"
        );
        self.lifecycle_events.send_replace(next);
        Ok(())
    }

    pub async fn start(&self) -> Result<(), ServiceError> {
        if let Err(error) = self.endpoint.bind().await {
            let _ = self
                .lifecycle
                .lock()
                .expect("service lifecycle poisoned")
                .transition(ServiceLifecycleEvent::StartupFailed);
            self.lifecycle_events
                .send_replace(ServiceLifecycleState::Terminated);
            return Err(ServiceError::Endpoint(error));
        }
        self.transition(ServiceLifecycleEvent::RemotingReady)?;
        if let Some(runtime) = self
            .coordinator_runtime
            .lock()
            .expect("service Coordinator runtime poisoned")
            .take()
        {
            *self
                .coordinator_shutdown
                .lock()
                .expect("service Coordinator shutdown poisoned") = Some(runtime.shutdown);
            *self
                .coordinator_handle
                .lock()
                .expect("service Coordinator handle poisoned") = Some(runtime.handle);
            self.supervisor.spawn(runtime.future)?;
        }
        let join_runtime = self
            .join_runtime
            .lock()
            .expect("service join runtime poisoned")
            .take();
        let has_join_runtime = join_runtime.is_some();
        if let Some(runtime) = join_runtime {
            let (shutdown, shutdown_rx) = watch::channel(false);
            *self
                .join_shutdown
                .lock()
                .expect("service join shutdown poisoned") = Some(shutdown);
            self.supervisor.spawn(runtime.run(shutdown_rx))?;
        }
        let runtime = self
            .logic_runtime
            .lock()
            .expect("service logic runtime poisoned")
            .take();
        let has_logic_runtime = runtime.is_some();
        if let Some(runtime) = runtime {
            let (shutdown, shutdown_rx) = watch::channel(false);
            let mut readiness_shutdown = shutdown_rx.clone();
            *self
                .logic_shutdown
                .lock()
                .expect("service logic shutdown poisoned") = Some(shutdown);
            let LogicRuntimeAssembly {
                session,
                controls,
                mut effects,
                handle,
                router,
            } = runtime;
            let readiness_handle = handle.clone();
            let lifecycle = self.lifecycle.clone();
            let lifecycle_events = self.lifecycle_events.clone();
            self.supervisor.spawn(async move {
                let changed = readiness_handle.change_notifier();
                loop {
                    if readiness_handle.ready() {
                        let mut lifecycle = lifecycle.lock().expect("service lifecycle poisoned");
                        if lifecycle.state() == ServiceLifecycleState::Joining {
                            let _ = lifecycle.transition(ServiceLifecycleEvent::SnapshotInstalled);
                            lifecycle_events.send_replace(lifecycle.state());
                        }
                        break;
                    }
                    tokio::select! {
                        _ = changed.notified() => {}
                        result = readiness_shutdown.changed() => {
                            if result.is_err() || *readiness_shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
            })?;
            self.supervisor.spawn(async move {
                let _ = session.run(controls, shutdown_rx).await;
            })?;
            let watches = self.watches.clone();
            let members = self.members.clone();
            let peers = self.peers.clone();
            let drain_ready = self.drain_ready.clone();
            self.supervisor.spawn(async move {
                while let Some(effect) = effects.recv().await {
                    let (slot, effect) = match effect {
                        LogicPlacementEffect::MemberEvent(event) => {
                            if let MemberEvent {
                                revision,
                                change: MemberChange::Removed { node, reason },
                            } = event.as_ref()
                            {
                                tracing::info!(
                                    target: "lattice.cluster.members",
                                    node_id = %node.node_id,
                                    incarnation = node.incarnation.get(),
                                    revision = revision.get(),
                                    ?reason,
                                    "authoritative member removed"
                                );
                                watches
                                    .lock()
                                    .expect("watch registry poisoned")
                                    .node_down(node.incarnation);
                            } else if let MemberEvent {
                                revision,
                                change: MemberChange::Upsert(record),
                            } = event.as_ref()
                            {
                                tracing::info!(
                                    target: "lattice.cluster.members",
                                    node_id = %record.node.node_id,
                                    incarnation = record.node.incarnation.get(),
                                    revision = revision.get(),
                                    status = ?record.status,
                                    "authoritative member upserted"
                                );
                            }
                            let _ = peers.apply(*event);
                            continue;
                        }
                        LogicPlacementEffect::MemberSnapshot {
                            revision,
                            members: snapshot,
                        } => {
                            let _ = members.install_snapshot(revision, snapshot);
                            continue;
                        }
                        LogicPlacementEffect::DrainReady {
                            operation_id,
                            incarnation: _,
                        } => {
                            if handle.complete_member_drain(operation_id.clone()).is_ok() {
                                drain_ready.send_replace(Some(operation_id));
                            }
                            continue;
                        }
                        LogicPlacementEffect::Authority { slot, effect } => (slot, effect),
                    };
                    let result = match effect {
                        AuthorityEffect::DrainSlot => {
                            let succeeded = router.drain_slot(slot.clone()).await.unwrap_or(false);
                            handle.complete_drain(slot, succeeded).await
                        }
                        AuthorityEffect::PublishReady => handle.publish_ready(&slot),
                        AuthorityEffect::PublishDrained => handle.publish_drained(&slot),
                        AuthorityEffect::PublishStopFailed => handle.publish_stop_failed(&slot),
                        AuthorityEffect::StopSlot => {
                            router.stop_fenced_slot(slot).await.map_err(|_| {
                                lattice_placement::session::LogicSessionError::ControlClosed
                            })
                        }
                        AuthorityEffect::FenceAdmission
                        | AuthorityEffect::OpenAdmission
                        | AuthorityEffect::StartSlot
                        | AuthorityEffect::StateLossPossible => Ok(()),
                    };
                    if result.is_err() {
                        break;
                    }
                }
            })?;
        }
        if !has_logic_runtime && !has_join_runtime {
            self.transition(ServiceLifecycleEvent::SnapshotInstalled)?;
        }
        Ok(())
    }

    pub async fn connect_peer(&self, peer: NodeIdentity) -> Result<Arc<Association>, ServiceError> {
        self.endpoint
            .connect_peer(peer)
            .await
            .map_err(ServiceError::Endpoint)
    }

    pub async fn leave(&self, deadline: tokio::time::Instant) -> Result<(), ServiceError> {
        match self.lifecycle_state() {
            ServiceLifecycleState::Terminated => return Ok(()),
            ServiceLifecycleState::Booting | ServiceLifecycleState::Joining => {
                return self.force_shutdown().await;
            }
            ServiceLifecycleState::Stopping => return self.stop_components().await,
            ServiceLifecycleState::Ready | ServiceLifecycleState::Degraded => {
                self.transition(ServiceLifecycleEvent::BeginDrain)?;
            }
            ServiceLifecycleState::Draining => {}
        }
        let operation_id = {
            let mut operation = self
                .drain_operation
                .lock()
                .expect("service drain operation poisoned");
            operation
                .get_or_insert_with(|| format!("leave-{}", uuid::Uuid::new_v4()))
                .clone()
        };
        let handle = self
            .logic_handle
            .lock()
            .expect("logic handle poisoned")
            .clone()
            .ok_or(ServiceError::CoordinatorUnavailable)?;
        handle
            .begin_drain(operation_id.clone())
            .map_err(|_| ServiceError::CoordinatorUnavailable)?;
        let mut ready = self.drain_ready.subscribe();
        loop {
            if ready.borrow().as_ref() == Some(&operation_id) {
                self.transition(ServiceLifecycleEvent::DrainComplete)?;
                return self.stop_components().await;
            }
            tokio::time::timeout_at(deadline, ready.changed())
                .await
                .map_err(|_| ServiceError::LeaveTimeout)?
                .map_err(|_| ServiceError::CoordinatorUnavailable)?;
        }
    }

    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        let deadline = tokio::time::Instant::now() + self.join_config.leave_timeout;
        if self
            .join_shutdown
            .lock()
            .expect("service join shutdown poisoned")
            .is_some()
            && self.leave(deadline).await.is_ok()
        {
            return Ok(());
        }
        self.force_shutdown().await
    }

    pub async fn force_shutdown(&self) -> Result<(), ServiceError> {
        let state = self.lifecycle_state();
        if state == ServiceLifecycleState::Terminated {
            return Ok(());
        }
        if state != ServiceLifecycleState::Stopping {
            tracing::warn!(
                target: "lattice.cluster.lifecycle",
                ?state,
                "forced shutdown fences local cluster authority"
            );
            self.transition(ServiceLifecycleEvent::ForceStop)?;
        }
        self.stop_components().await
    }

    async fn stop_components(&self) -> Result<(), ServiceError> {
        if let Some(shutdown) = self
            .join_shutdown
            .lock()
            .expect("service join shutdown poisoned")
            .take()
        {
            let _ = shutdown.send(true);
        }
        if let Some(shutdown) = self
            .logic_shutdown
            .lock()
            .expect("service logic shutdown poisoned")
            .take()
        {
            let _ = shutdown.send(true);
        }
        if let Some(shutdown) = self
            .coordinator_shutdown
            .lock()
            .expect("service Coordinator shutdown poisoned")
            .take()
        {
            let _ = shutdown.send(true);
        }
        self.endpoint
            .shutdown()
            .await
            .map_err(ServiceError::Endpoint)?;
        self.supervisor
            .shutdown(self.join_config.shutdown_timeout)
            .await?;
        if self.lifecycle_state() == ServiceLifecycleState::Stopping {
            self.transition(ServiceLifecycleEvent::ShutdownComplete)?;
        }
        Ok(())
    }
}
