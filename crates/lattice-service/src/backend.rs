use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use lattice_actor::host::ProtocolHostRegistry;
use lattice_actor::recipient::RecipientBackend;
use lattice_core::actor_ref::{ActorRef, EntityRef, PlacementDomainId, RecipientRef, SingletonRef};
use lattice_placement::types::PlacementSlotKey;
use lattice_remoting::association::AssociationId;
use lattice_remoting::association::AssociationManager;
use lattice_remoting::messaging::error::AskError;
use lattice_remoting::messaging::error::RemoteFailureCode;
use lattice_remoting::messaging::error::RemoteMessageError;
use lattice_remoting::messaging::error::TellError;
use lattice_remoting::messaging::inbound::InboundDispatch;
use lattice_remoting::messaging::outbound::OutboundMessaging;
use lattice_remoting::messaging::target::ExactActorTarget;
use lattice_remoting::messaging::target::LogicalEntityTarget;
use lattice_remoting::messaging::target::LogicalSingletonTarget;
use lattice_remoting::messaging::target::SenderIdentity;
use lattice_remoting::protocol::ProtocolFingerprint;
use lattice_remoting::watch::TerminatedReason;
use lattice_remoting::watch::WatchCommand;
use lattice_remoting::watch::WatchError;
use lattice_remoting::watch::WatchId;
use lattice_remoting::watch::WatchRegistry;
use lattice_remoting::watch::encode_watch_command;

#[async_trait]
pub trait LogicalRouter: Send + Sync + 'static {
    async fn tell_entity(
        &self,
        sender: Option<ActorRef>,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;

    async fn ask_entity(
        &self,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;

    async fn tell_singleton(
        &self,
        sender: Option<ActorRef>,
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;

    async fn ask_singleton(
        &self,
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;

    async fn resolve_entity_current(
        &self,
        target: EntityRef,
    ) -> Result<Option<ActorRef>, WatchError>;

    async fn resolve_singleton_current(
        &self,
        target: SingletonRef,
    ) -> Result<Option<ActorRef>, WatchError>;

    async fn drain_slot(&self, _slot: PlacementSlotKey) -> Result<bool, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn stop_fenced_slot(&self, _slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn receive_entity_tell(
        &self,
        _sender: Option<ActorRef>,
        _target: LogicalEntityTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn receive_entity_ask(
        &self,
        _target: LogicalEntityTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn receive_singleton_tell(
        &self,
        _sender: Option<ActorRef>,
        _target: LogicalSingletonTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn receive_singleton_ask(
        &self,
        _target: LogicalSingletonTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }
}

/// Stable logical routing facade used by services that discover their
/// Coordinator at runtime.  Recipient backends keep this facade for their
/// whole lifetime while the active cluster router is replaced whenever the
/// authoritative Coordinator changes.
pub(crate) struct SwitchableDomainRouter {
    current: RwLock<Option<Arc<dyn LogicalRouter>>>,
}

impl SwitchableDomainRouter {
    pub(crate) fn new() -> Self {
        Self {
            current: RwLock::new(None),
        }
    }

    pub(crate) fn install(&self, router: Arc<dyn LogicalRouter>) {
        *self.current.write().expect("logical router poisoned") = Some(router);
    }

    pub(crate) fn clear(&self) {
        *self.current.write().expect("logical router poisoned") = None;
    }

    fn current(&self) -> Result<Arc<dyn LogicalRouter>, RemoteMessageError> {
        self.current
            .read()
            .expect("logical router poisoned")
            .clone()
            .ok_or(RemoteMessageError::ShardUnavailable)
    }
}

#[async_trait]
impl LogicalRouter for SwitchableDomainRouter {
    async fn tell_entity(
        &self,
        sender: Option<ActorRef>,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.current()?
            .tell_entity(sender, target, fingerprint, message_id, payload)
            .await
    }

    async fn ask_entity(
        &self,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        self.current()
            .map_err(AskError::Protocol)?
            .ask_entity(target, fingerprint, message_id, payload, deadline)
            .await
    }

    async fn tell_singleton(
        &self,
        sender: Option<ActorRef>,
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.current()?
            .tell_singleton(sender, target, fingerprint, message_id, payload)
            .await
    }

    async fn ask_singleton(
        &self,
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        self.current()
            .map_err(AskError::Protocol)?
            .ask_singleton(target, fingerprint, message_id, payload, deadline)
            .await
    }

    async fn resolve_entity_current(
        &self,
        target: EntityRef,
    ) -> Result<Option<ActorRef>, WatchError> {
        self.current()
            .map_err(|_| WatchError::Unavailable)?
            .resolve_entity_current(target)
            .await
    }

    async fn resolve_singleton_current(
        &self,
        target: SingletonRef,
    ) -> Result<Option<ActorRef>, WatchError> {
        self.current()
            .map_err(|_| WatchError::Unavailable)?
            .resolve_singleton_current(target)
            .await
    }

    async fn drain_slot(&self, slot: PlacementSlotKey) -> Result<bool, RemoteMessageError> {
        self.current()?.drain_slot(slot).await
    }

    async fn stop_fenced_slot(&self, slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        self.current()?.stop_fenced_slot(slot).await
    }

    async fn receive_entity_tell(
        &self,
        sender: Option<ActorRef>,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.current()?
            .receive_entity_tell(sender, target, message_id, payload)
            .await
    }

    async fn receive_entity_ask(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        self.current()?
            .receive_entity_ask(target, message_id, payload, deadline)
            .await
    }

    async fn receive_singleton_tell(
        &self,
        sender: Option<ActorRef>,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.current()?
            .receive_singleton_tell(sender, target, message_id, payload)
            .await
    }

    async fn receive_singleton_ask(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        self.current()?
            .receive_singleton_ask(target, message_id, payload, deadline)
            .await
    }
}

/// Bounded logical router directory with one independently switchable entry per domain.
pub struct DomainRouterDirectory {
    routers: BTreeMap<PlacementDomainId, Arc<SwitchableDomainRouter>>,
}

impl DomainRouterDirectory {
    pub(crate) fn new(
        domains: impl IntoIterator<Item = PlacementDomainId>,
        maximum_domains: usize,
    ) -> Result<Self, RemoteMessageError> {
        if maximum_domains == 0 {
            return Err(RemoteMessageError::Unauthorized);
        }
        let routers = domains
            .into_iter()
            .map(|domain| (domain, Arc::new(SwitchableDomainRouter::new())))
            .collect::<BTreeMap<_, _>>();
        if routers.is_empty() || routers.len() > maximum_domains {
            return Err(RemoteMessageError::Unauthorized);
        }
        Ok(Self { routers })
    }

    pub(crate) fn install(
        &self,
        domain: &PlacementDomainId,
        router: Arc<dyn LogicalRouter>,
    ) -> Result<(), RemoteMessageError> {
        self.routers
            .get(domain)
            .ok_or(RemoteMessageError::ShardUnavailable)?
            .install(router);
        Ok(())
    }

    pub(crate) fn clear(&self, domain: &PlacementDomainId) {
        if let Some(router) = self.routers.get(domain) {
            router.clear();
        }
    }

    fn router(
        &self,
        domain: &PlacementDomainId,
    ) -> Result<Arc<SwitchableDomainRouter>, RemoteMessageError> {
        self.routers
            .get(domain)
            .cloned()
            .ok_or(RemoteMessageError::ShardUnavailable)
    }
}

#[async_trait]
impl LogicalRouter for DomainRouterDirectory {
    async fn tell_entity(
        &self,
        sender: Option<ActorRef>,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.router(target.domain())?
            .tell_entity(sender, target, fingerprint, message_id, payload)
            .await
    }

    async fn ask_entity(
        &self,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        self.router(target.domain())
            .map_err(AskError::Protocol)?
            .ask_entity(target, fingerprint, message_id, payload, deadline)
            .await
    }

    async fn tell_singleton(
        &self,
        sender: Option<ActorRef>,
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.router(target.domain())?
            .tell_singleton(sender, target, fingerprint, message_id, payload)
            .await
    }

    async fn ask_singleton(
        &self,
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        self.router(target.domain())
            .map_err(AskError::Protocol)?
            .ask_singleton(target, fingerprint, message_id, payload, deadline)
            .await
    }

    async fn resolve_entity_current(
        &self,
        target: EntityRef,
    ) -> Result<Option<ActorRef>, WatchError> {
        self.router(target.domain())
            .map_err(|_| WatchError::Unavailable)?
            .resolve_entity_current(target)
            .await
    }

    async fn resolve_singleton_current(
        &self,
        target: SingletonRef,
    ) -> Result<Option<ActorRef>, WatchError> {
        self.router(target.domain())
            .map_err(|_| WatchError::Unavailable)?
            .resolve_singleton_current(target)
            .await
    }

    async fn drain_slot(&self, slot: PlacementSlotKey) -> Result<bool, RemoteMessageError> {
        self.router(slot.domain())?.drain_slot(slot).await
    }

    async fn stop_fenced_slot(&self, slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        self.router(slot.domain())?.stop_fenced_slot(slot).await
    }

    async fn receive_entity_tell(
        &self,
        sender: Option<ActorRef>,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.router(target.reference.domain())?
            .receive_entity_tell(sender, target, message_id, payload)
            .await
    }

    async fn receive_entity_ask(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        self.router(target.reference.domain())?
            .receive_entity_ask(target, message_id, payload, deadline)
            .await
    }

    async fn receive_singleton_tell(
        &self,
        sender: Option<ActorRef>,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.router(target.reference.domain())?
            .receive_singleton_tell(sender, target, message_id, payload)
            .await
    }

    async fn receive_singleton_ask(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        self.router(target.reference.domain())?
            .receive_singleton_ask(target, message_id, payload, deadline)
            .await
    }
}

pub(crate) struct ServiceInboundDispatch {
    pub hosts: Arc<ProtocolHostRegistry>,
    pub logical: Option<Arc<dyn LogicalRouter>>,
    pub admission: crate::lifecycle::NodeAdmissionGate,
}

#[async_trait]
impl InboundDispatch for ServiceInboundDispatch {
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if !self.admission.is_open() {
            return Err(RemoteMessageError::Unauthorized);
        }
        self.hosts.tell(sender, target, message_id, payload).await
    }

    async fn ask(
        &self,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if !self.admission.is_open() {
            return Err(RemoteMessageError::Unauthorized);
        }
        self.hosts.ask(target, message_id, payload, deadline).await
    }

    async fn tell_entity(
        &self,
        sender: Option<ActorRef>,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if !self.admission.is_open() {
            return Err(RemoteMessageError::Unauthorized);
        }
        self.logical
            .as_ref()
            .ok_or(RemoteMessageError::Unauthorized)?
            .receive_entity_tell(sender, target, message_id, payload)
            .await
    }

    async fn ask_entity(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if !self.admission.is_open() {
            return Err(RemoteMessageError::Unauthorized);
        }
        self.logical
            .as_ref()
            .ok_or(RemoteMessageError::Unauthorized)?
            .receive_entity_ask(target, message_id, payload, deadline)
            .await
    }

    async fn tell_singleton(
        &self,
        sender: Option<ActorRef>,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if !self.admission.is_open() {
            return Err(RemoteMessageError::Unauthorized);
        }
        self.logical
            .as_ref()
            .ok_or(RemoteMessageError::Unauthorized)?
            .receive_singleton_tell(sender, target, message_id, payload)
            .await
    }

    async fn ask_singleton(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if !self.admission.is_open() {
            return Err(RemoteMessageError::Unauthorized);
        }
        self.logical
            .as_ref()
            .ok_or(RemoteMessageError::Unauthorized)?
            .receive_singleton_ask(target, message_id, payload, deadline)
            .await
    }
}

pub(crate) struct ServiceRecipientBackend {
    pub local_cluster: lattice_core::actor_ref::ClusterId,
    pub local_address: lattice_core::actor_ref::NodeAddress,
    pub local_incarnation: lattice_core::actor_ref::NodeIncarnation,
    pub hosts: Arc<ProtocolHostRegistry>,
    pub associations: Arc<AssociationManager>,
    pub messaging: Arc<OutboundMessaging>,
    pub watches: Arc<Mutex<WatchRegistry>>,
    pub maximum_control_payload: usize,
    pub supervisor: Arc<crate::supervisor::TaskSupervisor>,
    pub logical: Option<Arc<dyn LogicalRouter>>,
    pub admission: crate::lifecycle::NodeAdmissionGate,
}

impl ServiceRecipientBackend {
    fn is_local(&self, reference: &ActorRef) -> bool {
        reference.cluster_id() == &self.local_cluster
            && reference.node_address() == &self.local_address
            && reference.node_incarnation() == self.local_incarnation
    }

    fn association(
        &self,
        reference: &ActorRef,
    ) -> Result<
        Arc<lattice_remoting::association::Association>,
        lattice_remoting::association::AssociationError,
    > {
        self.associations.get_or_create(
            reference.cluster_id().clone(),
            reference.node_address().clone(),
            reference.node_incarnation(),
        )
    }
}

#[async_trait]
impl RecipientBackend for ServiceRecipientBackend {
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: RecipientRef,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), TellError> {
        if !self.admission.is_open() {
            return Err(TellError::Remote(RemoteMessageError::Unauthorized));
        }
        match target {
            RecipientRef::Actor(reference) if self.is_local(&reference) => self
                .hosts
                .tell(sender, (&reference).into(), message_id, payload)
                .await
                .map_err(TellError::Remote),
            RecipientRef::Actor(reference) => {
                let association = self
                    .association(&reference)
                    .map_err(TellError::Association)?;
                let sender = sender
                    .as_ref()
                    .map(SenderIdentity::from)
                    .unwrap_or_else(|| SenderIdentity::Process(self.local_incarnation.get()));
                self.messaging
                    .tell(
                        &association,
                        &sender,
                        &reference,
                        protocol_fingerprint,
                        message_id,
                        payload,
                    )
                    .map(|_| ())
            }
            RecipientRef::Entity(reference) => self
                .logical
                .as_ref()
                .ok_or(TellError::Remote(RemoteMessageError::Unauthorized))?
                .tell_entity(sender, reference, protocol_fingerprint, message_id, payload)
                .await
                .map_err(TellError::Remote),
            RecipientRef::Singleton(reference) => self
                .logical
                .as_ref()
                .ok_or(TellError::Remote(RemoteMessageError::Unauthorized))?
                .tell_singleton(sender, reference, protocol_fingerprint, message_id, payload)
                .await
                .map_err(TellError::Remote),
        }
    }

    async fn ask(
        &self,
        target: RecipientRef,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        if !self.admission.is_open() {
            return Err(AskError::Protocol(RemoteMessageError::Unauthorized));
        }
        match target {
            RecipientRef::Actor(reference) if self.is_local(&reference) => self
                .hosts
                .ask((&reference).into(), message_id, payload, deadline)
                .await
                .map_err(map_remote_ask),
            RecipientRef::Actor(reference) => {
                let association = self.association(&reference).map_err(AskError::from)?;
                self.messaging
                    .ask(
                        &association,
                        &SenderIdentity::Process(self.local_incarnation.get()),
                        &reference,
                        protocol_fingerprint,
                        message_id,
                        payload,
                        deadline,
                    )
                    .await
            }
            RecipientRef::Entity(reference) => {
                self.logical
                    .as_ref()
                    .ok_or(AskError::Protocol(RemoteMessageError::Unauthorized))?
                    .ask_entity(
                        reference,
                        protocol_fingerprint,
                        message_id,
                        payload,
                        deadline,
                    )
                    .await
            }
            RecipientRef::Singleton(reference) => {
                self.logical
                    .as_ref()
                    .ok_or(AskError::Protocol(RemoteMessageError::Unauthorized))?
                    .ask_singleton(
                        reference,
                        protocol_fingerprint,
                        message_id,
                        payload,
                        deadline,
                    )
                    .await
            }
        }
    }

    async fn watch_actor(&self, target: ActorRef) -> Result<WatchId, WatchError> {
        if self.is_local(&target) {
            let association_id = AssociationId::new(self.local_incarnation.get())
                .ok_or(WatchError::InvalidCommand)?;
            let (watch_id, command) = self
                .watches
                .lock()
                .expect("watch registry poisoned")
                .watch(association_id, &target)?;
            let WatchCommand::Watch { target, .. } = command else {
                return Err(WatchError::InvalidCommand);
            };
            let terminated = self.hosts.subscribe_terminated(&target);
            let response = self
                .watches
                .lock()
                .expect("watch registry poisoned")
                .receive_watch(association_id, watch_id, target.clone(), |candidate| {
                    self.hosts.is_current(candidate)
                })?;
            return match response {
                WatchCommand::WatchAck { watch_id, target } => {
                    self.watches
                        .lock()
                        .expect("watch registry poisoned")
                        .receive_ack(watch_id, &target);
                    if let Some(mut terminated) = terminated {
                        let watches = self.watches.clone();
                        self.supervisor
                            .spawn(async move {
                                let Ok(event) = terminated.recv().await else {
                                    return;
                                };
                                let reason = match event.reason {
                                    lattice_actor::watch::TerminatedReason::Stopped => {
                                        TerminatedReason::Stopped
                                    }
                                    lattice_actor::watch::TerminatedReason::Passivated => {
                                        TerminatedReason::Passivated
                                    }
                                    lattice_actor::watch::TerminatedReason::Migrated => {
                                        TerminatedReason::Handoff
                                    }
                                    lattice_actor::watch::TerminatedReason::Fenced => {
                                        TerminatedReason::ClaimLost
                                    }
                                    lattice_actor::watch::TerminatedReason::NodeDown => {
                                        TerminatedReason::NodeDown
                                    }
                                };
                                let commands = watches
                                    .lock()
                                    .expect("watch registry poisoned")
                                    .target_terminated(&target, reason);
                                for (_, command) in commands {
                                    if let WatchCommand::Terminated {
                                        watch_id, target, ..
                                    } = command
                                    {
                                        watches
                                            .lock()
                                            .expect("watch registry poisoned")
                                            .receive_terminated(watch_id, &target);
                                    }
                                }
                            })
                            .map_err(|_| WatchError::TargetCapacity)?;
                    }
                    Ok(watch_id)
                }
                WatchCommand::Terminated {
                    watch_id, target, ..
                } => {
                    self.watches
                        .lock()
                        .expect("watch registry poisoned")
                        .receive_terminated(watch_id, &target);
                    Ok(watch_id)
                }
                WatchCommand::Watch { .. } | WatchCommand::Unwatch { .. } => {
                    Err(WatchError::InvalidCommand)
                }
            };
        }
        let association = self
            .association(&target)
            .map_err(|_| WatchError::InvalidCommand)?;
        let (watch_id, command) = self
            .watches
            .lock()
            .expect("watch registry poisoned")
            .watch(association.id(), &target)?;
        let payload = encode_watch_command(&command, self.maximum_control_payload)?;
        association
            .admit_control_command(payload)
            .map_err(|_| WatchError::InvalidCommand)?;
        Ok(watch_id)
    }

    async fn watch_entity_current(&self, target: EntityRef) -> Result<WatchId, WatchError> {
        let current = self
            .logical
            .as_ref()
            .ok_or(WatchError::NotActive)?
            .resolve_entity_current(target)
            .await?
            .ok_or(WatchError::NotActive)?;
        self.watch_actor(current).await
    }

    async fn watch_singleton_current(&self, target: SingletonRef) -> Result<WatchId, WatchError> {
        let current = self
            .logical
            .as_ref()
            .ok_or(WatchError::Unavailable)?
            .resolve_singleton_current(target)
            .await?
            .ok_or(WatchError::Unavailable)?;
        self.watch_actor(current).await
    }

    async fn unwatch(&self, watch_id: WatchId) -> Result<(), WatchError> {
        let (association_id, command) = self
            .watches
            .lock()
            .expect("watch registry poisoned")
            .unwatch(watch_id)
            .ok_or(WatchError::InvalidCommand)?;
        let association = self
            .associations
            .get_by_id(association_id)
            .ok_or(WatchError::InvalidCommand)?;
        association
            .admit_control_command(encode_watch_command(
                &command,
                self.maximum_control_payload,
            )?)
            .map(|_| ())
            .map_err(|_| WatchError::InvalidCommand)
    }
}

fn map_remote_ask(error: RemoteMessageError) -> AskError {
    let code = match error {
        RemoteMessageError::StaleActivation => RemoteFailureCode::StaleActivation,
        RemoteMessageError::StaleAuthority => RemoteFailureCode::StaleActivation,
        RemoteMessageError::UnknownMessage | RemoteMessageError::UnsupportedProtocol => {
            RemoteFailureCode::UnknownMessage
        }
        RemoteMessageError::ProtocolFingerprintMismatch => RemoteFailureCode::ProtocolMismatch,
        RemoteMessageError::MailboxRejected => RemoteFailureCode::MailboxFull,
        RemoteMessageError::BufferFull => RemoteFailureCode::MailboxFull,
        RemoteMessageError::InvalidPayload => RemoteFailureCode::DecodeFailed,
        RemoteMessageError::DeadlineExceeded => RemoteFailureCode::DeadlineExceeded,
        RemoteMessageError::Unauthorized => RemoteFailureCode::Unauthorized,
        RemoteMessageError::ShardUnavailable
        | RemoteMessageError::HandlerFailed
        | RemoteMessageError::ZeroPendingLimit => RemoteFailureCode::HandlerFailed,
    };
    AskError::Remote(code)
}
