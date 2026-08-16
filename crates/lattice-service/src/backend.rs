use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_actor::{
    host::ProtocolHostRegistry,
    recipient::{ImmediateRecipientTellDispatch, RecipientBackend, RecipientTell},
};
use lattice_core::{
    actor_ref::{
        ActorRef, ClusterId, EntityRef, NodeAddress, NodeIncarnation, PlacementDomainId,
        RecipientRef, SingletonRef,
    },
    watch::WatchId,
};
use lattice_placement::types::PlacementSlotKey;
use lattice_remoting::{
    association::{Association, AssociationError, AssociationId, AssociationManager},
    messaging::{
        error::{AskError, RemoteFailureCode, RemoteMessageError, TellError},
        inbound::{ImmediateTellDispatch, InboundDispatch},
        outbound::{OutboundMessage, OutboundMessaging},
        target::{ExactActorTarget, InboundTell, LogicalEntityTarget, LogicalSingletonTarget},
    },
    protocol::ProtocolFingerprint,
    watch::{RegisteredWatch, WatchCommand, WatchError, WatchRegistry, encode_watch_command},
};

use crate::{
    exact_tell_routes::{ExactTellMessage, ExactTellRouteCache, RejectedExactTell},
    lifecycle::{AdmissionScope, NodeAdmissionGate},
    supervisor::TaskSupervisor,
};

#[async_trait]
pub trait LogicalRouter: Send + Sync + 'static {
    async fn tell_entity(
        &self,
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

    async fn wait_slot_drained(&self, _slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn receive_entity_tell(
        &self,
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
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.current()?
            .tell_entity(target, fingerprint, message_id, payload)
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
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.current()?
            .tell_singleton(target, fingerprint, message_id, payload)
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

    async fn wait_slot_drained(&self, slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        self.current()?.wait_slot_drained(slot).await
    }

    async fn receive_entity_tell(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.current()?
            .receive_entity_tell(target, message_id, payload)
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
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.current()?
            .receive_singleton_tell(target, message_id, payload)
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
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.router(target.domain())?
            .tell_entity(target, fingerprint, message_id, payload)
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
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.router(target.domain())?
            .tell_singleton(target, fingerprint, message_id, payload)
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

    async fn wait_slot_drained(&self, slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        self.router(slot.domain())?.wait_slot_drained(slot).await
    }

    async fn receive_entity_tell(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.router(target.reference.domain())?
            .receive_entity_tell(target, message_id, payload)
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
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.router(target.reference.domain())?
            .receive_singleton_tell(target, message_id, payload)
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
    pub admission: NodeAdmissionGate,
}

impl ServiceInboundDispatch {
    /// Peer traffic never carries the external scope: the remoting handshake already proved the
    /// peer is a member of this cluster, so what arrives here is only ever exact or logical.
    fn admitted<E: FromClosedAdmission>(&self, scope: AdmissionScope) -> Result<(), E> {
        if self.admission.is_open(scope) {
            Ok(())
        } else {
            Err(E::closed_admission())
        }
    }

    fn logical(&self) -> Result<&Arc<dyn LogicalRouter>, RemoteMessageError> {
        self.logical
            .as_ref()
            .ok_or(RemoteMessageError::Unauthorized)
    }
}

#[async_trait]
impl InboundDispatch for ServiceInboundDispatch {
    fn try_tell_immediate(&self, tell: InboundTell) -> ImmediateTellDispatch {
        if !self.admission.is_open(AdmissionScope::Exact) {
            return ImmediateTellDispatch::Complete(Err(RemoteMessageError::Unauthorized));
        }
        self.hosts.try_tell_immediate(tell)
    }

    async fn tell(
        &self,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.admitted::<RemoteMessageError>(AdmissionScope::Exact)?;
        self.hosts.tell_wait(target, message_id, payload).await
    }

    async fn ask(
        &self,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        self.admitted::<RemoteMessageError>(AdmissionScope::Exact)?;
        self.hosts.ask(target, message_id, payload, deadline).await
    }

    async fn tell_entity(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.admitted::<RemoteMessageError>(AdmissionScope::Logical)?;
        self.logical()?
            .receive_entity_tell(target, message_id, payload)
            .await
    }

    async fn ask_entity(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        self.admitted::<RemoteMessageError>(AdmissionScope::Logical)?;
        self.logical()?
            .receive_entity_ask(target, message_id, payload, deadline)
            .await
    }

    async fn tell_singleton(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.admitted::<RemoteMessageError>(AdmissionScope::Logical)?;
        self.logical()?
            .receive_singleton_tell(target, message_id, payload)
            .await
    }

    async fn ask_singleton(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        self.admitted::<RemoteMessageError>(AdmissionScope::Logical)?;
        self.logical()?
            .receive_singleton_ask(target, message_id, payload, deadline)
            .await
    }
}

pub(crate) struct ServiceRecipientBackend {
    pub local_cluster: ClusterId,
    pub local_address: NodeAddress,
    pub local_incarnation: NodeIncarnation,
    pub hosts: Arc<ProtocolHostRegistry>,
    pub associations: Arc<AssociationManager>,
    pub messaging: Arc<OutboundMessaging>,
    pub exact_tell_routes: ExactTellRouteCache,
    pub watches: Arc<Mutex<WatchRegistry>>,
    pub maximum_control_payload: usize,
    pub supervisor: Arc<TaskSupervisor>,
    pub logical: Option<Arc<dyn LogicalRouter>>,
    pub admission: NodeAdmissionGate,
}

/// Maps a closed node admission gate onto each dispatch surface's error type.
trait FromClosedAdmission {
    fn closed_admission() -> Self;
}

impl FromClosedAdmission for RemoteMessageError {
    fn closed_admission() -> Self {
        RemoteMessageError::Unauthorized
    }
}

impl FromClosedAdmission for TellError {
    fn closed_admission() -> Self {
        TellError::Remote(RemoteMessageError::Unauthorized)
    }
}

impl FromClosedAdmission for AskError {
    fn closed_admission() -> Self {
        AskError::Protocol(RemoteMessageError::Unauthorized)
    }
}

/// The admission scope a recipient target belongs to.
///
/// This is a property of the destination, not of the caller: an `ActorRef` names one activation
/// and is fenced by its own incarnation, while an `EntityRef`/`SingletonRef` names a logical
/// destination whose validity comes from a placement claim. The scope is the same whether the
/// message is being admitted from a peer or originated locally.
fn recipient_scope(target: &RecipientRef) -> AdmissionScope {
    match target {
        RecipientRef::Actor(_) => AdmissionScope::Exact,
        RecipientRef::Entity(_) | RecipientRef::Singleton(_) => AdmissionScope::Logical,
    }
}

impl ServiceRecipientBackend {
    /// Egress admission. A node that is draining or stopping must stop originating traffic as
    /// well as stop accepting it, which is why the same scoped gate governs both directions.
    fn admitted<E: FromClosedAdmission>(&self, scope: AdmissionScope) -> Result<(), E> {
        if self.admission.is_open(scope) {
            Ok(())
        } else {
            Err(E::closed_admission())
        }
    }

    fn is_local(&self, reference: &ActorRef) -> bool {
        reference.cluster_id() == &self.local_cluster
            && reference.node_address() == &self.local_address
            && reference.node_incarnation() == self.local_incarnation
    }

    fn association(&self, reference: &ActorRef) -> Result<Arc<Association>, AssociationError> {
        self.associations.get_or_create(
            reference.cluster_id().clone(),
            reference.node_address().clone(),
            reference.node_incarnation(),
        )
    }

    fn try_tell_remote_actor(
        &self,
        reference: ActorRef,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), Box<RejectedExactTell>> {
        debug_assert!(!self.is_local(&reference));
        self.exact_tell_routes.tell(
            &self.messaging,
            reference,
            ExactTellMessage {
                fingerprint: protocol_fingerprint,
                message_id,
                payload,
            },
            |target| self.association(target).map_err(TellError::Association),
        )
    }

    async fn tell_remote_actor(
        &self,
        reference: ActorRef,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), TellError> {
        self.exact_tell_routes
            .tell_wait(
                &self.messaging,
                reference,
                ExactTellMessage {
                    fingerprint: protocol_fingerprint,
                    message_id,
                    payload,
                },
                |target| self.association(target).map_err(TellError::Association),
            )
            .await
    }
}

#[async_trait]
impl RecipientBackend for ServiceRecipientBackend {
    fn try_tell_immediate(&self, tell: RecipientTell) -> ImmediateRecipientTellDispatch {
        if !self.admission.is_open(recipient_scope(&tell.target)) {
            return ImmediateRecipientTellDispatch::Complete(Err(TellError::Remote(
                RemoteMessageError::Unauthorized,
            )));
        }
        let RecipientTell {
            target,
            protocol_fingerprint,
            message_id,
            payload,
        } = tell;
        match target {
            RecipientRef::Actor(reference) if self.is_local(&reference) => {
                ImmediateRecipientTellDispatch::Complete(
                    self.hosts
                        .try_tell((&reference).into(), message_id, payload)
                        .map_err(TellError::Remote),
                )
            }
            RecipientRef::Actor(reference) => match self.try_tell_remote_actor(
                reference,
                protocol_fingerprint,
                message_id,
                payload,
            ) {
                Ok(()) => ImmediateRecipientTellDispatch::Complete(Ok(())),
                Err(rejected) if is_temporary_backpressure(&rejected.error) => {
                    ImmediateRecipientTellDispatch::Deferred(RecipientTell {
                        target: RecipientRef::Actor(rejected.target),
                        protocol_fingerprint: rejected.fingerprint,
                        message_id: rejected.message_id,
                        payload: rejected.payload,
                    })
                }
                Err(rejected) => ImmediateRecipientTellDispatch::Complete(Err(rejected.error)),
            },
            target => ImmediateRecipientTellDispatch::Deferred(RecipientTell {
                target,
                protocol_fingerprint,
                message_id,
                payload,
            }),
        }
    }

    async fn tell(
        &self,
        target: RecipientRef,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), TellError> {
        self.admitted::<TellError>(recipient_scope(&target))?;
        match target {
            RecipientRef::Actor(reference) if self.is_local(&reference) => self
                .hosts
                .try_tell((&reference).into(), message_id, payload)
                .map_err(TellError::Remote),
            RecipientRef::Actor(reference) => {
                self.tell_remote_actor(reference, protocol_fingerprint, message_id, payload)
                    .await
            }
            RecipientRef::Entity(reference) => self
                .logical
                .as_ref()
                .ok_or(TellError::Remote(RemoteMessageError::Unauthorized))?
                .tell_entity(reference, protocol_fingerprint, message_id, payload)
                .await
                .map_err(TellError::Remote),
            RecipientRef::Singleton(reference) => self
                .logical
                .as_ref()
                .ok_or(TellError::Remote(RemoteMessageError::Unauthorized))?
                .tell_singleton(reference, protocol_fingerprint, message_id, payload)
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
        self.admitted::<AskError>(recipient_scope(&target))?;
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
                        &reference,
                        OutboundMessage::new(protocol_fingerprint, message_id, payload),
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

    async fn watch_actor(&self, target: ActorRef) -> Result<RegisteredWatch, WatchError> {
        if self.is_local(&target) {
            let association_id = AssociationId::new(self.local_incarnation.get())
                .ok_or(WatchError::InvalidCommand)?;
            let (registered, command) = self
                .watches
                .lock()
                .expect("watch registry poisoned")
                .watch(association_id, &target)?;
            let watch_id = registered.id();
            let WatchCommand::Watch { target, .. } = command else {
                return Err(WatchError::InvalidCommand);
            };
            let terminated = self.hosts.subscribe_terminated(&target);
            let response = match self
                .watches
                .lock()
                .expect("watch registry poisoned")
                .receive_watch(association_id, watch_id, target.clone(), |candidate| {
                    self.hosts.is_current(candidate)
                }) {
                Ok(response) => response,
                Err(error) => {
                    self.watches
                        .lock()
                        .expect("watch registry poisoned")
                        .begin_unwatch(watch_id);
                    self.watches
                        .lock()
                        .expect("watch registry poisoned")
                        .complete_unwatch(watch_id);
                    return Err(error);
                }
            };
            return match response {
                WatchCommand::WatchAck { watch_id, target } => {
                    self.watches
                        .lock()
                        .expect("watch registry poisoned")
                        .receive_ack(watch_id, &target);
                    if let Some(mut terminated) = terminated {
                        let watches = self.watches.clone();
                        let associations = self.associations.clone();
                        let maximum_payload = self.maximum_control_payload;
                        let task = match self.supervisor.spawn_abortable(async move {
                            let Ok(event) = terminated.recv().await else {
                                return;
                            };
                            let commands = watches
                                .lock()
                                .expect("watch registry poisoned")
                                .target_terminated(&target, event.reason);
                            for (association_id, command) in commands {
                                if let WatchCommand::Terminated {
                                    watch_id,
                                    target,
                                    reason,
                                } = &command
                                {
                                    let local = watches
                                        .lock()
                                        .expect("watch registry poisoned")
                                        .contains_desired(*watch_id);
                                    if local {
                                        watches
                                            .lock()
                                            .expect("watch registry poisoned")
                                            .receive_terminated(*watch_id, target, *reason);
                                    } else if let Some(association) =
                                        associations.get_by_id(association_id)
                                        && let Ok(payload) =
                                            encode_watch_command(&command, maximum_payload)
                                    {
                                        let _ = association
                                            .admit_control_command_in_wait_configured(
                                                lattice_remoting::control::ControlStreamId::WATCH,
                                                payload,
                                            )
                                            .await;
                                    }
                                }
                            }
                        }) {
                            Ok(task) => task,
                            Err(_) => {
                                self.watches
                                    .lock()
                                    .expect("watch registry poisoned")
                                    .begin_unwatch(watch_id);
                                self.watches
                                    .lock()
                                    .expect("watch registry poisoned")
                                    .complete_unwatch(watch_id);
                                self.watches
                                    .lock()
                                    .expect("watch registry poisoned")
                                    .receive_unwatch(association_id, watch_id);
                                return Err(WatchError::TargetCapacity);
                            }
                        };
                        self.watches
                            .lock()
                            .expect("watch registry poisoned")
                            .attach_target_task(association_id, watch_id, task);
                    }
                    Ok(registered)
                }
                WatchCommand::Terminated {
                    watch_id,
                    target,
                    reason,
                } => {
                    self.watches
                        .lock()
                        .expect("watch registry poisoned")
                        .receive_terminated(watch_id, &target, reason);
                    Ok(registered)
                }
                WatchCommand::Watch { .. } | WatchCommand::Unwatch { .. } => {
                    Err(WatchError::InvalidCommand)
                }
            };
        }
        let association = self
            .association(&target)
            .map_err(|_| WatchError::InvalidCommand)?;
        let (registered, command) = self
            .watches
            .lock()
            .expect("watch registry poisoned")
            .watch(association.id(), &target)?;
        let watch_id = registered.id();
        let payload = match encode_watch_command(&command, self.maximum_control_payload) {
            Ok(payload) => payload,
            Err(error) => {
                self.watches
                    .lock()
                    .expect("watch registry poisoned")
                    .begin_unwatch(watch_id);
                self.watches
                    .lock()
                    .expect("watch registry poisoned")
                    .complete_unwatch(watch_id);
                return Err(error);
            }
        };
        if association
            .admit_control_command_in_wait_configured(
                lattice_remoting::control::ControlStreamId::WATCH,
                payload,
            )
            .await
            .is_err()
        {
            self.watches
                .lock()
                .expect("watch registry poisoned")
                .begin_unwatch(watch_id);
            self.watches
                .lock()
                .expect("watch registry poisoned")
                .complete_unwatch(watch_id);
            return Err(WatchError::InvalidCommand);
        }
        Ok(registered)
    }

    async fn watch_entity_current(&self, target: EntityRef) -> Result<RegisteredWatch, WatchError> {
        let current = self
            .logical
            .as_ref()
            .ok_or(WatchError::NotActive)?
            .resolve_entity_current(target)
            .await?
            .ok_or(WatchError::NotActive)?;
        self.watch_actor(current).await
    }

    async fn watch_singleton_current(
        &self,
        target: SingletonRef,
    ) -> Result<RegisteredWatch, WatchError> {
        let current = self
            .logical
            .as_ref()
            .ok_or(WatchError::Unavailable)?
            .resolve_singleton_current(target)
            .await?
            .ok_or(WatchError::Unavailable)?;
        self.watch_actor(current).await
    }

    fn unwatch(&self, watch_id: WatchId) -> Result<(), WatchError> {
        let (association_id, target, command) = self
            .watches
            .lock()
            .expect("watch registry poisoned")
            .begin_unwatch(watch_id)
            .ok_or(WatchError::InvalidCommand)?;
        if target.cluster_id == self.local_cluster
            && target.node_address == self.local_address
            && target.node_incarnation == self.local_incarnation
        {
            self.watches
                .lock()
                .expect("watch registry poisoned")
                .receive_unwatch(association_id, watch_id);
            self.watches
                .lock()
                .expect("watch registry poisoned")
                .complete_unwatch(watch_id);
            return Ok(());
        }
        let association = self
            .associations
            .get_by_id(association_id)
            .ok_or(WatchError::InvalidCommand)?;
        association
            .admit_control_command_in(
                lattice_remoting::control::ControlStreamId::WATCH,
                encode_watch_command(&command, self.maximum_control_payload)?,
            )
            .map_err(|_| WatchError::InvalidCommand)?;
        self.watches
            .lock()
            .expect("watch registry poisoned")
            .complete_unwatch(watch_id);
        Ok(())
    }
}

fn is_temporary_backpressure(error: &TellError) -> bool {
    matches!(
        error,
        TellError::Association(
            AssociationError::QueueFull
                | AssociationError::ByteBudgetExceeded
                | AssociationError::NodeByteBudgetExceeded
        )
    )
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
        RemoteMessageError::ActorPanicked => RemoteFailureCode::ActorPanicked,
        RemoteMessageError::ShardUnavailable
        | RemoteMessageError::HandlerFailed
        | RemoteMessageError::ZeroPendingLimit => RemoteFailureCode::HandlerFailed,
    };
    AskError::Remote(code)
}
