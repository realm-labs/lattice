pub struct LatticeService {
    cluster_id: lattice_core::actor_ref::ClusterId,
    actor_system: ActorSystem,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    endpoint: Arc<RemotingEndpoint>,
    supervisor: Arc<TaskSupervisor>,
    logic_runtime: std::sync::Mutex<Option<LogicRuntimeAssembly>>,
    join_runtimes: std::sync::Mutex<Vec<LogicJoinRuntime>>,
    membership_join_runtime: std::sync::Mutex<Option<MembershipJoinRuntime>>,
    membership_handle: Arc<
        std::sync::Mutex<
            Option<lattice_placement::membership_session::MembershipCoordinatorHandle>,
        >,
    >,
    logic_shutdown: std::sync::Mutex<Option<watch::Sender<bool>>>,
    join_shutdown: std::sync::Mutex<Option<watch::Sender<bool>>>,
    logic_handles: Arc<
        std::sync::Mutex<
            BTreeMap<lattice_core::actor_ref::PlacementDomainId, LogicCoordinatorHandle>,
        >,
    >,
    watches: Arc<std::sync::Mutex<WatchRegistry>>,
    coordinator_runtime: std::sync::Mutex<Option<CoordinatorRuntimeAssembly>>,
    coordinator_shutdown: std::sync::Mutex<Option<watch::Sender<bool>>>,
    coordinator_handles:
        std::sync::Mutex<BTreeMap<lattice_core::actor_ref::PlacementDomainId, CoordinatorHandle>>,
    lifecycle: Arc<std::sync::Mutex<NodeLifecycle>>,
    lifecycle_events: watch::Sender<NodeLifecycleState>,
    health: Arc<std::sync::Mutex<ServiceHealthSnapshot>>,
    health_events: watch::Sender<ServiceHealthSnapshot>,
    members: Arc<MemberDirectory>,
    peers: Arc<PeerReconciler>,
    bootstrap_view: Arc<BootstrapView>,
    drain_ready: watch::Sender<BTreeMap<lattice_core::actor_ref::PlacementDomainId, String>>,
    configured_domains: BTreeSet<lattice_core::actor_ref::PlacementDomainId>,
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

    pub async fn ask<P, R>(
        &self,
        target: impl Into<RecipientRef<P>>,
        request: R,
        timeout: std::time::Duration,
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
    ) -> Result<lattice_remoting::watch::WatchId, RecipientError> {
        self.actor_system.watch(target).await
    }

    pub async fn watch_entity_current<P: ProtocolTag>(
        &self,
        target: &EntityRef<P>,
    ) -> Result<lattice_remoting::watch::WatchId, RecipientError> {
        self.actor_system.watch_entity_current(target).await
    }

    pub async fn watch_singleton_current<P: ProtocolTag>(
        &self,
        target: &SingletonRef<P>,
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

    pub fn coordinator(
        &self,
        domain: &lattice_core::actor_ref::PlacementDomainId,
    ) -> Option<CoordinatorHandle> {
        self.coordinator_handles
            .lock()
            .expect("service Coordinator handles poisoned")
            .get(domain)
            .cloned()
    }

    pub fn node_lifecycle_state(&self) -> NodeLifecycleState {
        self.lifecycle
            .lock()
            .expect("service lifecycle poisoned")
            .state()
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
        {
            let mut health = self.health.lock().expect("service health poisoned");
            health.node = next;
            self.health_events.send_replace(health.clone());
        }
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
            let _ = self.transition(ServiceLifecycleEvent::StartupFailed);
            return Err(ServiceError::Endpoint(error));
        }
        self.transition(ServiceLifecycleEvent::RemotingReady)?;
        if let Some(runtime) = self
            .coordinator_runtime
            .lock()
            .expect("service Coordinator runtime poisoned")
            .take()
        {
            let mut directory = runtime.directory;
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
            *self
                .coordinator_shutdown
                .lock()
                .expect("service Coordinator shutdown poisoned") = Some(runtime.shutdown);
            *self
                .coordinator_handles
                .lock()
                .expect("service Coordinator handles poisoned") = runtime.handles;
            let lifecycle = self.lifecycle.clone();
            let lifecycle_events = self.lifecycle_events.clone();
            let endpoint = self.endpoint.clone();
            self.supervisor.spawn(async move {
                runtime.future.await;
                let _ = endpoint.shutdown().await;
                let mut lifecycle = lifecycle.lock().expect("service lifecycle poisoned");
                if lifecycle
                    .transition(ServiceLifecycleEvent::RuntimeTerminated)
                    .is_ok()
                {
                    lifecycle_events.send_replace(lifecycle.state());
                }
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
            let lifecycle = self.lifecycle.clone();
            let lifecycle_events = self.lifecycle_events.clone();
            self.supervisor.spawn(async move {
                let changed = readiness_handle.change_notifier();
                loop {
                    if readiness_handle.ready() {
                        let mut lifecycle = lifecycle.lock().expect("service lifecycle poisoned");
                        if lifecycle.state() == NodeLifecycleState::JoiningMembership {
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
            let peers = self.peers.clone();
            let drain_ready = self.drain_ready.clone();
            self.supervisor.spawn(async move {
                while let Some(effect) = effects.recv().await {
                    let (slot, effect) = match effect {
                        LogicPlacementEffect::MemberEvent(event) => {
                            if let MemberEvent {
                                version,
                                change: MemberChange::Removed { node, reason },
                            } = event.as_ref()
                            {
                                tracing::info!(
                                    target: "lattice.cluster.members",
                                    node_id = %node.node_id,
                                    incarnation = node.incarnation.get(),
                                    term = version.term.get(),
                                    revision = version.revision.get(),
                                    ?reason,
                                    "authoritative member removed"
                                );
                                watches
                                    .lock()
                                    .expect("watch registry poisoned")
                                    .node_down(node.incarnation);
                            } else if let MemberEvent {
                                version,
                                change: MemberChange::Upsert(record),
                            } = event.as_ref()
                            {
                                tracing::info!(
                                    target: "lattice.cluster.members",
                                    node_id = %record.node.node_id,
                                    incarnation = record.node.incarnation.get(),
                                    term = version.term.get(),
                                    revision = version.revision.get(),
                                    status = ?record.status,
                                    "authoritative member upserted"
                                );
                            }
                            let _ = peers.apply(*event).await;
                            continue;
                        }
                        LogicPlacementEffect::MemberSnapshot {
                            version,
                            members: snapshot,
                        } => {
                            let _ = peers.install_snapshot(version, snapshot).await;
                            continue;
                        }
                        LogicPlacementEffect::DrainReady {
                            operation_id,
                            incarnation: _,
                        } => {
                            if handle
                                .complete_member_drain(operation_id.clone())
                                .await
                                .is_ok()
                            {
                                let mut completed = drain_ready.borrow().clone();
                                completed.insert(domain.clone(), operation_id);
                                drain_ready.send_replace(completed);
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
        match self.node_lifecycle_state() {
            NodeLifecycleState::Terminated => return Ok(()),
            NodeLifecycleState::Booting | NodeLifecycleState::JoiningMembership => {
                return self.force_shutdown().await;
            }
            NodeLifecycleState::Ready => {
                self.transition(ServiceLifecycleEvent::BeginDrain)?;
            }
            NodeLifecycleState::Draining => {}
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
                    .clone()
                    .ok_or(ServiceError::CoordinatorUnavailable)?;
                membership
                    .complete_drain(operation_id.clone())
                    .await
                    .map_err(|_| ServiceError::CoordinatorUnavailable)?;
                let local_incarnation = self.endpoint.local_identity().incarnation;
                let mut membership_events = self.members.subscribe();
                while self
                    .members
                    .snapshot()
                    .members
                    .iter()
                    .any(|member| member.node.incarnation == local_incarnation)
                {
                    match tokio::time::timeout_at(deadline, membership_events.recv()).await {
                        Ok(Ok(_)) | Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
                        Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                            return Err(ServiceError::CoordinatorUnavailable);
                        }
                        Err(_) => return Err(ServiceError::LeaveTimeout),
                    }
                }
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
        let state = self.node_lifecycle_state();
        if state == NodeLifecycleState::Terminated {
            return Ok(());
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
        if self.node_lifecycle_state() == NodeLifecycleState::Draining {
            self.transition(ServiceLifecycleEvent::ShutdownComplete)?;
        }
        Ok(())
    }
}
