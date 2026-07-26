use std::{sync::atomic::Ordering, time::Duration};

use lattice_actor::{
    registry::ActorCellDiagnostics, traits::ActorLifecycleState, watch::LocalActorRef,
};
use lattice_core::{
    actor_ref::ClusterId,
    release::{ClusterReleaseState, ReleaseError, ReleaseManifest},
};
use lattice_placement::{membership_session::MembershipCoordinatorHandle, types::PlacementSlotKey};
use lattice_remoting::watch::{WatchId, WatchStatus};
use tokio::{
    sync::broadcast::{Receiver, error::RecvError},
    time::Instant,
};

use crate::{
    cluster::api::Cluster,
    cluster::peers::PeerError,
    cluster::runtime::LogicEffectApplier,
    ingress::ExternalIngress,
    lifecycle::{
        CoordinatorScopeState, LifecycleInterventionReport, NodeAdmissionSnapshot,
        ServiceLifecycleMetricsSnapshot,
    },
};

use super::{
    ActorRef, ActorSystem, Arc, Association, AssociationManager, AtomicBool, BTreeMap, BTreeSet,
    BootstrapLeader, BootstrapView, ClusterJoinConfig, CoordinatorHandle,
    CoordinatorHostScopeState, CoordinatorRuntimeAssembly, EntityRef, LatticeServiceBuilder,
    LogicCoordinatorHandle, LogicJoinRuntime, LogicRuntimeAssembly, MemberDirectory, MemberEvent,
    MemberSnapshot, MembershipJoinRuntime, Message, Mutex, NodeConfig, NodeIdentity, NodeKey,
    NodeLifecycleState, OutboundMessaging, PeerReconciler, PlacementDomainId, PlacementDomainState,
    ProductionLifecycleDriver, ProtocolHostRegistry, ProtocolTag, RecipientError, RecipientRef,
    RemotingEndpoint, Request, ServiceError, ServiceHealthSnapshot, ServiceLifecycleEvent,
    SingletonRef, SupportsAsk, SupportsTell, TaskSupervisor, WatchRegistry, watch,
};

pub struct LatticeService {
    pub(super) cluster_id: ClusterId,
    pub(super) release: ReleaseManifest,
    pub(super) actor_system: ActorSystem,
    pub(super) hosts: Arc<ProtocolHostRegistry>,
    pub(super) associations: Arc<AssociationManager>,
    pub(super) messaging: Arc<OutboundMessaging>,
    pub(super) endpoint: Arc<RemotingEndpoint>,
    pub(super) supervisor: Arc<TaskSupervisor>,
    pub(super) logic_runtime: Mutex<Option<LogicRuntimeAssembly>>,
    pub(super) join_runtimes: Mutex<Vec<LogicJoinRuntime>>,
    pub(super) membership_join_runtime: Mutex<Option<MembershipJoinRuntime>>,
    pub(super) membership_handle: Arc<Mutex<Option<MembershipCoordinatorHandle>>>,
    pub(super) logic_shutdown: Mutex<Option<watch::Sender<bool>>>,
    pub(super) join_shutdown: Mutex<Option<watch::Sender<bool>>>,
    pub(super) logic_handles: Arc<Mutex<BTreeMap<PlacementDomainId, LogicCoordinatorHandle>>>,
    pub(super) watches: Arc<Mutex<WatchRegistry>>,
    pub(super) coordinator_runtime: Mutex<Option<CoordinatorRuntimeAssembly>>,
    pub(super) coordinator_shutdown: Mutex<Option<watch::Sender<bool>>>,
    pub(super) coordinator_handles: Mutex<BTreeMap<PlacementDomainId, CoordinatorHandle>>,
    pub(super) lifecycle_driver: ProductionLifecycleDriver,
    pub(super) lifecycle_events: watch::Sender<NodeLifecycleState>,
    pub(super) health: Arc<Mutex<ServiceHealthSnapshot>>,
    pub(super) health_events: watch::Sender<ServiceHealthSnapshot>,
    pub(super) members: Arc<MemberDirectory>,
    pub(super) peers: Arc<PeerReconciler>,
    pub(super) bootstrap_view: Arc<BootstrapView>,
    pub(super) drain_ready: watch::Sender<BTreeMap<PlacementDomainId, String>>,
    pub(super) drain_blockers:
        watch::Sender<BTreeMap<PlacementDomainId, BTreeSet<PlacementSlotKey>>>,
    pub(super) configured_domains: BTreeSet<PlacementDomainId>,
    pub(super) drain_operation: Mutex<Option<String>>,
    pub(super) join_config: ClusterJoinConfig,
    pub(super) force_actor_shutdown: AtomicBool,
    pub(super) start_requested: AtomicBool,
}

impl LatticeService {
    pub fn builder(config: NodeConfig) -> Result<LatticeServiceBuilder, ServiceError> {
        LatticeServiceBuilder::new(config)
    }

    /// Cluster-internal messaging. Actors, and process code acting on the cluster's behalf, send
    /// through this handle; it is governed by exact-activation and placement-claim admission only.
    pub fn actor_system(&self) -> &ActorSystem {
        &self.actor_system
    }

    /// The edge handle for traffic originating outside the cluster.
    ///
    /// Gateways and other process edges should send through this rather than
    /// [`Self::actor_system`] so that a node which lost its membership session sheds external load
    /// while it keeps serving the cluster-internal traffic it can still prove is safe.
    pub fn external_ingress(&self) -> ExternalIngress {
        ExternalIngress::new(
            self.lifecycle_driver.admission_gate(),
            self.actor_system.clone(),
        )
    }

    /// Which classes of traffic this node currently admits.
    ///
    /// A node recovering a membership session reports external admission closed while logical and
    /// exact admission stay open; that pair is what distinguishes it from a node that is booting
    /// or draining, both of which admit nothing.
    pub fn admission_snapshot(&self) -> NodeAdmissionSnapshot {
        self.lifecycle_driver.admission_gate().snapshot()
    }

    /// True while the node is re-joining after losing a membership session it had already used.
    pub fn recovering_membership(&self) -> bool {
        self.lifecycle_driver.recovering_membership()
    }

    pub fn release_manifest(&self) -> &ReleaseManifest {
        &self.release
    }

    pub fn cluster_release_state(&self) -> Result<ClusterReleaseState, ReleaseError> {
        let snapshot = self.members.snapshot();
        ClusterReleaseState::from_manifests(
            snapshot
                .members
                .iter()
                .filter(|member| member.hello.rollout_participant)
                .map(|member| &member.hello.release),
        )
    }

    pub fn retained_actor_cells(&self) -> Vec<ActorCellDiagnostics> {
        self.hosts
            .live_cells()
            .into_iter()
            .filter(|cell| {
                matches!(
                    cell.lifecycle,
                    ActorLifecycleState::StopFailed | ActorLifecycleState::Quarantined
                )
            })
            .collect()
    }

    pub async fn retry_actor_stop(&self, local_ref: LocalActorRef) -> Result<(), ServiceError> {
        self.hosts
            .retry_stop(local_ref)
            .await
            .map_err(ServiceError::ActorLifecycleAdmin)
    }

    pub async fn force_stop_actor(
        &self,
        local_ref: LocalActorRef,
        reason: &str,
        ticket: &str,
    ) -> Result<(), ServiceError> {
        self.hosts
            .force_stop(local_ref, reason, ticket)
            .await
            .map_err(ServiceError::ActorLifecycleAdmin)
    }

    /// Cluster-internal send, equivalent to [`Self::actor_system`].
    ///
    /// Traffic arriving from outside the cluster should go through [`Self::external_ingress`]
    /// instead, so that a node without a membership session sheds it.
    pub async fn tell<P, M>(
        &self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.actor_system.tell(target, message).await
    }

    /// Cluster-internal request, equivalent to [`Self::actor_system`].
    ///
    /// Traffic arriving from outside the cluster should go through [`Self::external_ingress`]
    /// instead, so that a node without a membership session sheds it.
    pub async fn ask<P, R>(
        &self,
        target: impl Into<RecipientRef<P>>,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response, RecipientError>
    where
        P: SupportsAsk<R>,
        R: Request,
    {
        self.actor_system.ask(target, request, timeout).await
    }

    pub async fn watch<P: ProtocolTag>(
        &self,
        target: &ActorRef<P>,
    ) -> Result<WatchId, RecipientError> {
        self.actor_system.watch(target).await
    }

    pub async fn watch_entity_current<P: ProtocolTag>(
        &self,
        target: &EntityRef<P>,
    ) -> Result<WatchId, RecipientError> {
        self.actor_system.watch_entity_current(target).await
    }

    pub async fn watch_singleton_current<P: ProtocolTag>(
        &self,
        target: &SingletonRef<P>,
    ) -> Result<WatchId, RecipientError> {
        self.actor_system.watch_singleton_current(target).await
    }

    pub async fn unwatch(&self, watch_id: WatchId) -> Result<(), RecipientError> {
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

    pub fn watch_status(&self, watch_id: WatchId) -> WatchStatus {
        self.watches
            .lock()
            .expect("watch registry poisoned")
            .status(watch_id)
    }

    pub fn coordinator(&self, domain: &PlacementDomainId) -> Option<CoordinatorHandle> {
        self.coordinator_handles
            .lock()
            .expect("service Coordinator handles poisoned")
            .get(domain)
            .cloned()
    }

    pub fn node_lifecycle_state(&self) -> NodeLifecycleState {
        self.lifecycle_driver.state()
    }

    /// Lifecycle metrics for this service instance only.
    pub fn lifecycle_metrics(&self) -> ServiceLifecycleMetricsSnapshot {
        self.lifecycle_driver.metrics()
    }

    pub fn subscribe_node_lifecycle(&self) -> watch::Receiver<NodeLifecycleState> {
        self.lifecycle_events.subscribe()
    }

    pub fn health_snapshot(&self) -> ServiceHealthSnapshot {
        self.health.lock().expect("service health poisoned").clone()
    }

    pub fn subscribe_health(&self) -> watch::Receiver<ServiceHealthSnapshot> {
        self.health_events.subscribe()
    }

    pub fn member_snapshot(&self) -> MemberSnapshot {
        self.members.snapshot()
    }

    pub fn subscribe_members(&self) -> Receiver<MemberEvent> {
        self.members.subscribe()
    }

    /// Closes readiness and external actor admission without stopping runtimes.
    ///
    /// Call [`Self::leave`] afterwards to migrate placement ownership and stop
    /// the service. Repeated calls are idempotent once draining has started.
    pub fn cordon(&self) -> Result<(), ServiceError> {
        match self.node_lifecycle_state() {
            NodeLifecycleState::JoiningMembership | NodeLifecycleState::Ready => {
                self.transition(ServiceLifecycleEvent::BeginDrain)?;
                Ok(())
            }
            NodeLifecycleState::Draining => Ok(()),
            NodeLifecycleState::Booting
            | NodeLifecycleState::Stopping
            | NodeLifecycleState::Terminated => Err(ServiceError::Lifecycle(
                crate::lifecycle::ServiceLifecycleError {
                    state: self.node_lifecycle_state(),
                    event: ServiceLifecycleEvent::BeginDrain,
                },
            )),
        }
    }

    /// Returns the user-facing handle for observing this node's cluster.
    pub fn cluster(&self) -> Cluster {
        let identity = self.endpoint.local_identity();
        Cluster::new(
            self.cluster_id.clone(),
            NodeKey {
                node_id: identity.node_id.clone(),
                address: identity.address.clone(),
                incarnation: identity.incarnation,
            },
            self.health.clone(),
            self.health_events.clone(),
            self.members.clone(),
        )
    }

    pub async fn connect_member(&self, node: &NodeKey) -> Result<Arc<Association>, ServiceError> {
        match self.peers.connect(node).await {
            Ok(association) => Ok(association),
            Err(PeerError::Endpoint(error)) => Err(ServiceError::Endpoint(error)),
            Err(PeerError::NotAuthoritativeUp) | Err(PeerError::Directory(_)) => {
                Err(ServiceError::CoordinatorUnavailable)
            }
        }
    }

    fn transition(&self, event: ServiceLifecycleEvent) -> Result<(), ServiceError> {
        self.lifecycle_driver
            .transition(event)
            .map_err(ServiceError::Lifecycle)?;
        Ok(())
    }

    /// Binds remoting and starts every configured runtime exactly once.
    ///
    /// Starting is only legal while the node is still `Booting`. A repeated call is rejected
    /// without touching any component, and a failure part way through starting rolls the node
    /// back through `StartupFailed` so it never reports readiness with stopped components.
    pub async fn start(&self) -> Result<(), ServiceError> {
        let state = self.node_lifecycle_state();
        if state != NodeLifecycleState::Booting || self.start_requested.swap(true, Ordering::AcqRel)
        {
            return Err(ServiceError::Lifecycle(
                crate::lifecycle::ServiceLifecycleError {
                    state,
                    event: ServiceLifecycleEvent::RemotingReady,
                },
            ));
        }
        if let Err(error) = self.start_components().await {
            let rollback = match self.node_lifecycle_state() {
                NodeLifecycleState::Booting | NodeLifecycleState::JoiningMembership => {
                    ServiceLifecycleEvent::StartupFailed
                }
                _ => ServiceLifecycleEvent::ForceStop,
            };
            tracing::error!(
                target: "lattice.cluster.lifecycle",
                %error,
                "service startup failed; stopping partially started components"
            );
            let _ = self.transition(rollback);
            let _ = self.stop_components().await;
            return Err(error);
        }
        Ok(())
    }

    async fn start_components(&self) -> Result<(), ServiceError> {
        self.endpoint.bind().await.map_err(ServiceError::Endpoint)?;
        self.transition(ServiceLifecycleEvent::RemotingReady)?;
        if let Some(runtime) = self
            .coordinator_runtime
            .lock()
            .expect("service Coordinator runtime poisoned")
            .take()
        {
            let mut directory = runtime.directory;
            let mut scope_states = runtime.scope_states;
            let bootstrap_view = self.bootstrap_view.clone();
            let cluster_id = self.cluster_id.clone();
            self.supervisor.spawn(async move {
                loop {
                    let leaders = directory
                        .borrow_and_update()
                        .values()
                        .cloned()
                        .map(|record| BootstrapLeader {
                            scope: record.scope,
                            identity: NodeIdentity {
                                cluster_id: cluster_id.clone(),
                                node_id: record.node.node_id,
                                address: record.node.address,
                                incarnation: record.node.incarnation,
                            },
                            term: record.term.get(),
                            protocol_generation: record.protocol_generation,
                        })
                        .collect();
                    bootstrap_view.replace(leaders);
                    if directory.changed().await.is_err() {
                        break;
                    }
                }
            })?;
            let health = self.health.clone();
            let health_events = self.health_events.clone();
            self.supervisor.spawn(async move {
                loop {
                    let scopes = scope_states
                        .borrow_and_update()
                        .iter()
                        .map(|(scope, state)| {
                            let state = match state {
                                CoordinatorHostScopeState::Active(_) => {
                                    CoordinatorScopeState::Active
                                }
                                CoordinatorHostScopeState::Standby => {
                                    CoordinatorScopeState::Standby
                                }
                                CoordinatorHostScopeState::Failed => CoordinatorScopeState::Failed,
                            };
                            (scope.clone(), state)
                        })
                        .collect();
                    let snapshot = {
                        let mut health = health.lock().expect("service health poisoned");
                        health.coordinator_scopes = scopes;
                        health.clone()
                    };
                    health_events.send_replace(snapshot);
                    if scope_states.changed().await.is_err() {
                        break;
                    }
                }
            })?;
            self.lifecycle_driver
                .register_runtime_shutdown(runtime.shutdown.clone());
            *self
                .coordinator_shutdown
                .lock()
                .expect("service Coordinator shutdown poisoned") = Some(runtime.shutdown);
            *self
                .coordinator_handles
                .lock()
                .expect("service Coordinator handles poisoned") = runtime.handles;
            let lifecycle_driver = self.lifecycle_driver.clone();
            let endpoint = self.endpoint.clone();
            self.supervisor.spawn(async move {
                runtime.future.await;
                let _ = endpoint.shutdown().await;
                let _ = lifecycle_driver.transition(ServiceLifecycleEvent::RuntimeTerminated);
            })?;
        }
        let join_runtimes = std::mem::take(
            &mut *self
                .join_runtimes
                .lock()
                .expect("service join runtimes poisoned"),
        );
        let membership_join_runtime = self
            .membership_join_runtime
            .lock()
            .expect("service membership join runtime poisoned")
            .take();
        let has_join_runtime = !join_runtimes.is_empty() || membership_join_runtime.is_some();
        if has_join_runtime {
            let (shutdown, shutdown_rx) = watch::channel(false);
            self.lifecycle_driver
                .register_runtime_shutdown(shutdown.clone());
            *self
                .join_shutdown
                .lock()
                .expect("service join shutdown poisoned") = Some(shutdown);
            if let Some(runtime) = membership_join_runtime {
                self.supervisor.spawn(runtime.run(shutdown_rx.clone()))?;
            }
            for runtime in join_runtimes {
                self.supervisor.spawn(runtime.run(shutdown_rx.clone()))?;
            }
        }
        let runtime = self
            .logic_runtime
            .lock()
            .expect("service logic runtime poisoned")
            .take();
        let has_logic_runtime = runtime.is_some();
        if let Some(runtime) = runtime {
            let (shutdown, shutdown_rx) = watch::channel(false);
            self.lifecycle_driver
                .register_runtime_shutdown(shutdown.clone());
            let mut readiness_shutdown = shutdown_rx.clone();
            *self
                .logic_shutdown
                .lock()
                .expect("service logic shutdown poisoned") = Some(shutdown);
            let LogicRuntimeAssembly {
                domain,
                session,
                controls,
                mut effects,
                handle,
                router,
            } = runtime;
            let readiness_handle = handle.clone();
            let lifecycle_driver = self.lifecycle_driver.clone();
            self.supervisor.spawn(async move {
                let changed = readiness_handle.change_notifier();
                loop {
                    if readiness_handle.ready() {
                        if lifecycle_driver.state() == NodeLifecycleState::JoiningMembership {
                            let _ = lifecycle_driver
                                .transition(ServiceLifecycleEvent::SnapshotInstalled);
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
            let applier = LogicEffectApplier {
                domain: domain.clone(),
                incarnation: self.endpoint.local_identity().incarnation,
                router,
                peers: self.peers.clone(),
                watches: self.watches.clone(),
                drain_ready: self.drain_ready.clone(),
                drain_blockers: self.drain_blockers.clone(),
                supervisor: self.supervisor.clone(),
            };
            let lifecycle_driver = self.lifecycle_driver.clone();
            self.supervisor.spawn(async move {
                while let Some(effect) = effects.recv().await {
                    if applier.apply(effect, &handle).await.is_err() {
                        tracing::warn!(
                            target: "lattice.cluster.logic",
                            domain = %domain.as_str(),
                            "logic placement effect failed; reconciliation required"
                        );
                        lifecycle_driver
                            .set_domain_state(domain.clone(), PlacementDomainState::Degraded);
                        let _ = lifecycle_driver.transition(ServiceLifecycleEvent::CoordinatorLost);
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

    pub async fn leave(&self, deadline: Instant) -> Result<(), ServiceError> {
        match self.node_lifecycle_state() {
            NodeLifecycleState::Terminated => return Ok(()),
            NodeLifecycleState::Booting => {
                self.transition(ServiceLifecycleEvent::StartupFailed)?;
                return self.stop_components().await;
            }
            NodeLifecycleState::JoiningMembership => {
                self.transition(ServiceLifecycleEvent::BeginDrain)?;
                self.drain_blockers.send_replace(BTreeMap::new());
                self.lifecycle_driver.record_blocked_drain_slots(0);
            }
            NodeLifecycleState::Ready => {
                self.transition(ServiceLifecycleEvent::BeginDrain)?;
                self.drain_blockers.send_replace(BTreeMap::new());
                self.lifecycle_driver.record_blocked_drain_slots(0);
            }
            NodeLifecycleState::Draining => {}
            NodeLifecycleState::Stopping => return self.stop_components().await,
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
        let handles = self
            .logic_handles
            .lock()
            .expect("logic handles poisoned")
            .clone();
        if handles.len() != self.configured_domains.len() {
            return Err(ServiceError::CoordinatorUnavailable);
        }
        for handle in handles.values() {
            handle
                .begin_drain(operation_id.clone())
                .map_err(|_| ServiceError::CoordinatorUnavailable)?;
        }
        let mut ready = self.drain_ready.subscribe();
        loop {
            if self.configured_domains.iter().all(|domain| {
                ready
                    .borrow()
                    .get(domain)
                    .is_some_and(|completed| completed == &operation_id)
            }) {
                let membership = self
                    .membership_handle
                    .lock()
                    .expect("membership handle poisoned")
                    .clone();
                if let Some(membership) = membership {
                    membership
                        .complete_drain(operation_id.clone())
                        .await
                        .map_err(|_| ServiceError::CoordinatorUnavailable)?;
                    let local_incarnation = self.endpoint.local_identity().incarnation;
                    self.members.fence_incarnation(local_incarnation);
                    let mut membership_events = self.members.subscribe();
                    while self
                        .members
                        .snapshot()
                        .members
                        .iter()
                        .any(|member| member.node.incarnation == local_incarnation)
                    {
                        match tokio::time::timeout_at(deadline, membership_events.recv()).await {
                            Ok(Ok(_)) | Ok(Err(RecvError::Lagged(_))) => {}
                            Ok(Err(RecvError::Closed)) => {
                                return Err(ServiceError::CoordinatorUnavailable);
                            }
                            Err(_) => return Err(self.drain_timeout_error()),
                        }
                    }
                } else if !self.configured_domains.is_empty() {
                    return Err(ServiceError::CoordinatorUnavailable);
                }
                self.transition(ServiceLifecycleEvent::DrainComplete)?;
                return self.stop_components().await;
            }
            let retry_at =
                (Instant::now() + self.join_config.leadership_refresh_interval).min(deadline);
            match tokio::time::timeout_at(retry_at, ready.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(ServiceError::CoordinatorUnavailable),
                Err(_) => {
                    if Instant::now() >= deadline {
                        return Err(self.drain_timeout_error());
                    }
                    self.redrive_drain(&operation_id);
                }
            }
        }
    }

    /// A drain the Coordinator refused, or one whose command was lost with the session that carried
    /// it, is indistinguishable from a slow one until the leave deadline expires. Waiting it out
    /// turns a graceful leave into the crash path it exists to avoid, so the command is re-driven
    /// on whichever sessions are live while the deadline lasts. It is idempotent on its operation
    /// ID, and a domain that already reported ready is left alone.
    fn redrive_drain(&self, operation_id: &str) {
        let handles = self
            .logic_handles
            .lock()
            .expect("logic handles poisoned")
            .clone();
        let completed = self.drain_ready.borrow().clone();
        for (domain, handle) in &handles {
            if completed
                .get(domain)
                .is_some_and(|current| current == operation_id)
            {
                continue;
            }
            if let Err(error) = handle.begin_drain(operation_id.to_owned()) {
                tracing::warn!(
                    target: "lattice.cluster.lifecycle",
                    domain = %domain.as_str(),
                    %error,
                    "graceful leave could not re-send its drain request"
                );
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        let deadline = Instant::now() + self.join_config.leave_timeout;
        self.leave(deadline).await
    }

    /// Stops this service as part of an intentional whole-deployment termination.
    ///
    /// Unlike [`Self::shutdown`], terminal shutdown does not require hosted placement slots to
    /// migrate to another member. It fences cluster authority first, then drains local actors and
    /// stops the remaining runtimes. Actor stop failures are still reported and are never forced.
    pub async fn terminal_shutdown(&self) -> Result<(), ServiceError> {
        let mut state = self.node_lifecycle_state();
        if state == NodeLifecycleState::Terminated {
            return Ok(());
        }
        if matches!(
            state,
            NodeLifecycleState::JoiningMembership | NodeLifecycleState::Ready
        ) {
            self.transition(ServiceLifecycleEvent::BeginDrain)?;
            state = NodeLifecycleState::Draining;
        }
        let membership = {
            self.membership_handle
                .lock()
                .expect("membership handle poisoned")
                .clone()
        };
        if state == NodeLifecycleState::Draining
            && let Some(membership) = membership
        {
            let operation_id = format!("terminal-leave-{}", uuid::Uuid::new_v4());
            match tokio::time::timeout(
                self.join_config.shutdown_timeout,
                membership.complete_drain(operation_id),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(
                    target: "lattice.cluster.lifecycle",
                    %error,
                    "terminal shutdown could not release membership immediately"
                ),
                Err(_) => tracing::warn!(
                    target: "lattice.cluster.lifecycle",
                    "terminal shutdown membership release exceeded its deadline"
                ),
            }
        }
        if state != NodeLifecycleState::Stopping {
            tracing::info!(
                target: "lattice.cluster.lifecycle",
                ?state,
                "terminal shutdown fences local cluster authority"
            );
            self.transition(ServiceLifecycleEvent::ForceStop)?;
        }
        self.stop_components().await
    }

    pub async fn force_shutdown(&self) -> Result<(), ServiceError> {
        self.force_actor_shutdown.store(true, Ordering::Release);
        let state = self.node_lifecycle_state();
        if state == NodeLifecycleState::Terminated {
            return Ok(());
        }
        if state == NodeLifecycleState::Stopping {
            return self.stop_components().await;
        }
        tracing::warn!(
            target: "lattice.cluster.lifecycle",
            ?state,
            "forced shutdown fences local cluster authority"
        );
        self.transition(ServiceLifecycleEvent::ForceStop)?;
        self.stop_components().await
    }

    async fn stop_components(&self) -> Result<(), ServiceError> {
        let force = self.force_actor_shutdown.load(Ordering::Acquire);
        let remaining_actor_cells = if force {
            let ticket = format!("force-shutdown-{}", uuid::Uuid::new_v4());
            self.hosts
                .force_shutdown_all("service force shutdown", &ticket)
                .await
        } else {
            self.hosts.drain_all().await
        };
        if !remaining_actor_cells.is_empty() {
            return Err(ServiceError::InterventionRequired(
                LifecycleInterventionReport {
                    blocked_slots: BTreeMap::new(),
                    retained_actor_cells: remaining_actor_cells
                        .into_iter()
                        .map(|cell| format!("{cell:?}"))
                        .collect(),
                },
            ));
        }
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
        if self.node_lifecycle_state() == NodeLifecycleState::Stopping {
            for domain in &self.configured_domains {
                self.lifecycle_driver
                    .set_domain_state(domain.clone(), PlacementDomainState::Terminated);
            }
            self.transition(ServiceLifecycleEvent::ShutdownComplete)?;
        }
        Ok(())
    }

    fn drain_timeout_error(&self) -> ServiceError {
        let blocked_slots = self
            .drain_blockers
            .borrow()
            .iter()
            .filter(|(_, slots)| !slots.is_empty())
            .map(|(domain, slots)| (domain.clone(), slots.iter().cloned().collect()))
            .collect();
        let report = LifecycleInterventionReport {
            blocked_slots,
            retained_actor_cells: Vec::new(),
        };
        if report.blocked_slots.is_empty() {
            ServiceError::LeaveTimeout
        } else {
            self.lifecycle_driver
                .record_blocked_drain_slots(report.blocked_slots.values().map(Vec::len).sum());
            ServiceError::InterventionRequired(report)
        }
    }
}
