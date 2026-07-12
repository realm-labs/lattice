use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use lattice_actor::host::ActorHost;
use lattice_actor::host::ProtocolHostRegistry;
use lattice_actor::protocol::ActorProtocol;
use lattice_actor::recipient::BoundRecipient;
use lattice_actor::recipient::RecipientBackend;
use lattice_actor::traits::Actor;
use lattice_core::actor_ref::RecipientRef;
use lattice_placement::authority::AuthorityEffect;
use lattice_placement::control::{PlacementControlEvent, PlacementControlRouter};
use lattice_placement::runtime::CoordinatorHandle;
use lattice_placement::runtime::CoordinatorLeader;
use lattice_placement::session::LogicCoordinatorHandle;
use lattice_placement::session::LogicCoordinatorSession;
use lattice_placement::session::LogicPlacementEffect;
use lattice_placement::storage::CoordinatorStore;
use lattice_remoting::association::Association;
use lattice_remoting::association::AssociationManager;
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
use crate::config::NodeConfig;
use crate::control::ServiceControlDispatch;
use crate::error::ServiceError;
use crate::lifecycle::{ServiceLifecycle, ServiceLifecycleEvent, ServiceLifecycleState};
use crate::supervisor::TaskSupervisor;

pub struct LatticeServiceBuilder {
    config: NodeConfig,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    hosts: ProtocolHostRegistry,
    protocols: BTreeMap<u64, lattice_remoting::protocol::ProtocolFingerprint>,
    logical: Option<Arc<dyn LogicalRouter>>,
    control_dispatch: Arc<dyn ControlDispatch>,
    logic_runtime: Option<LogicRuntimeAssembly>,
    control_scope: Option<lattice_remoting::association::AssociationKey>,
    coordinator_runtime: Option<CoordinatorRuntimeAssembly>,
    endpoint_security: Option<EndpointSecurity>,
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
            logical: None,
            control_dispatch: Arc::new(RejectControlDispatch),
            logic_runtime: None,
            control_scope: None,
            coordinator_runtime: None,
            endpoint_security: None,
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
        self.protocols
            .insert(protocol.protocol_id().get(), protocol.fingerprint());
        self.hosts
            .register(ActorHost::new(registry, protocol))
            .map_err(ServiceError::Host)?;
        Ok(self)
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
        let (shutdown, shutdown_rx) = watch::channel(false);
        self.control_dispatch = dispatch;
        self.coordinator_runtime = Some(CoordinatorRuntimeAssembly {
            future: Box::pin(async move {
                let _ = leader.run(controls, shutdown_rx).await;
            }),
            shutdown,
            handle,
        });
        self
    }

    pub fn build(self) -> Result<LatticeService, ServiceError> {
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
        let endpoint = Arc::new(
            RemotingEndpoint::new_with_control_and_security(
                NodeIdentity {
                    cluster_id: self.config.cluster_id.clone(),
                    node_id: self.config.node_id.clone(),
                    address: self.config.address.clone(),
                    incarnation: self.config.incarnation,
                },
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
        Ok(LatticeService {
            config: self.config,
            backend,
            associations,
            messaging,
            endpoint,
            supervisor,
            logic_runtime: std::sync::Mutex::new(self.logic_runtime),
            logic_shutdown: std::sync::Mutex::new(None),
            watches,
            coordinator_runtime: std::sync::Mutex::new(self.coordinator_runtime),
            coordinator_shutdown: std::sync::Mutex::new(None),
            coordinator_handle: std::sync::Mutex::new(None),
            lifecycle: Arc::new(std::sync::Mutex::new(ServiceLifecycle::default())),
        })
    }
}

pub struct LatticeService {
    config: NodeConfig,
    backend: Arc<dyn RecipientBackend>,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    endpoint: Arc<RemotingEndpoint>,
    supervisor: Arc<TaskSupervisor>,
    logic_runtime: std::sync::Mutex<Option<LogicRuntimeAssembly>>,
    logic_shutdown: std::sync::Mutex<Option<watch::Sender<bool>>>,
    watches: Arc<std::sync::Mutex<WatchRegistry>>,
    coordinator_runtime: std::sync::Mutex<Option<CoordinatorRuntimeAssembly>>,
    coordinator_shutdown: std::sync::Mutex<Option<watch::Sender<bool>>>,
    coordinator_handle: std::sync::Mutex<Option<CoordinatorHandle>>,
    lifecycle: Arc<std::sync::Mutex<ServiceLifecycle>>,
}

impl LatticeService {
    pub fn builder(config: NodeConfig) -> Result<LatticeServiceBuilder, ServiceError> {
        LatticeServiceBuilder::new(config)
    }

    pub fn recipient<A: Actor>(
        &self,
        target: RecipientRef<A>,
        protocol: Arc<ActorProtocol<A>>,
    ) -> Result<BoundRecipient<A>, lattice_actor::recipient::RecipientError> {
        BoundRecipient::new(target, protocol, self.backend.clone())
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

    pub async fn start(&self) -> Result<(), ServiceError> {
        if let Err(error) = self.endpoint.bind().await {
            let _ = self
                .lifecycle
                .lock()
                .expect("service lifecycle poisoned")
                .transition(ServiceLifecycleEvent::StartupFailed);
            return Err(ServiceError::Endpoint(error));
        }
        self.lifecycle
            .lock()
            .expect("service lifecycle poisoned")
            .transition(ServiceLifecycleEvent::RemotingReady)
            .map_err(ServiceError::Lifecycle)?;
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
            self.supervisor.spawn(async move {
                let changed = readiness_handle.change_notifier();
                loop {
                    if readiness_handle.ready() {
                        let mut lifecycle = lifecycle.lock().expect("service lifecycle poisoned");
                        if lifecycle.state() == ServiceLifecycleState::Joining {
                            let _ = lifecycle.transition(ServiceLifecycleEvent::SnapshotInstalled);
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
            self.supervisor.spawn(async move {
                while let Some(effect) = effects.recv().await {
                    let (slot, effect) = match effect {
                        LogicPlacementEffect::NodeDown(incarnation) => {
                            watches
                                .lock()
                                .expect("watch registry poisoned")
                                .node_down(incarnation);
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
        if !has_logic_runtime {
            self.lifecycle
                .lock()
                .expect("service lifecycle poisoned")
                .transition(ServiceLifecycleEvent::SnapshotInstalled)
                .map_err(ServiceError::Lifecycle)?;
        }
        Ok(())
    }

    pub async fn connect_peer(&self, peer: NodeIdentity) -> Result<Arc<Association>, ServiceError> {
        self.endpoint
            .connect_peer(peer)
            .await
            .map_err(ServiceError::Endpoint)
    }

    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        {
            let mut lifecycle = self.lifecycle.lock().expect("service lifecycle poisoned");
            if lifecycle.state() != ServiceLifecycleState::Terminated
                && lifecycle.state() != ServiceLifecycleState::Stopping
            {
                lifecycle
                    .transition(ServiceLifecycleEvent::ForceStop)
                    .map_err(ServiceError::Lifecycle)?;
            }
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
            .shutdown(self.config.shutdown_timeout)
            .await?;
        self.lifecycle
            .lock()
            .expect("service lifecycle poisoned")
            .transition(ServiceLifecycleEvent::ShutdownComplete)
            .map_err(ServiceError::Lifecycle)?;
        Ok(())
    }
}
