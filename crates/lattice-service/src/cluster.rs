use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_actor::protocol::{ActorProtocol, DispatchError, DispatchMode, DispatchReply};
use lattice_actor::registry::{ActorLoader, ActorRegistry};
use lattice_actor::traits::{Actor, ActorLifecycleState, PassivationReason, StopReason};
use lattice_actor::{error::ActorCallError, handle::ActorHandle};
use lattice_core::actor_ref::{
    ActorRef, ConfigFingerprint, EntityRef, EntityType, ProtocolId, SingletonKind, SingletonRef,
};
use lattice_core::id::ActorId;
use lattice_placement::{
    EntityConfig, LogicPlacementState, NodeKey, PlacementSlot, PlacementSlotKey, PlacementSlotState,
};
use lattice_remoting::protocol::ProtocolFingerprint;
use lattice_remoting::{
    AskError, AssociationKey, AssociationManager, AssociationState, LogicalEntityTarget,
    LogicalSingletonTarget, OutboundMessaging, RemoteMessageError, SenderIdentity, WatchError,
};

use crate::backend::LogicalRouter;

static NEXT_LOGICAL_RESOLUTION: AtomicU64 = AtomicU64::new(1);
const LOGICAL_RESOLVE_MESSAGE_ID: u64 = u64::MAX;

#[derive(Debug, Clone)]
pub struct LogicalBufferConfig {
    pub maximum_messages_per_slot: usize,
    pub maximum_messages: usize,
    pub maximum_bytes: usize,
    pub maximum_residence: Duration,
    pub maximum_control_payload: usize,
}

impl Default for LogicalBufferConfig {
    fn default() -> Self {
        Self {
            maximum_messages_per_slot: 1_024,
            maximum_messages: 10_000,
            maximum_bytes: 64 * 1024 * 1024,
            maximum_residence: Duration::from_secs(30),
            maximum_control_payload: lattice_placement::control::DEFAULT_MAX_CONTROL_PAYLOAD,
        }
    }
}

impl LogicalBufferConfig {
    fn validate(&self) -> Result<(), ClusterRouterError> {
        if self.maximum_messages_per_slot == 0
            || self.maximum_messages == 0
            || self.maximum_messages_per_slot > self.maximum_messages
            || self.maximum_bytes == 0
            || self.maximum_residence.is_zero()
            || self.maximum_control_payload == 0
        {
            return Err(ClusterRouterError::InvalidBufferConfig);
        }
        Ok(())
    }
}

#[derive(Default)]
struct RouteBufferState {
    per_slot: BTreeMap<PlacementSlotKey, usize>,
    resolving: BTreeSet<PlacementSlotKey>,
    messages: usize,
    bytes: usize,
}

struct RouteBuffer {
    config: LogicalBufferConfig,
    state: Mutex<RouteBufferState>,
}

impl RouteBuffer {
    fn new(config: LogicalBufferConfig) -> Self {
        Self {
            config,
            state: Mutex::new(RouteBufferState::default()),
        }
    }

    fn admit(
        &self,
        slot: PlacementSlotKey,
        bytes: usize,
        requested_deadline: Option<Instant>,
    ) -> Result<(RouteBufferAdmission<'_>, Instant, bool), RemoteMessageError> {
        let now = Instant::now();
        let residence_deadline = now + self.config.maximum_residence;
        let deadline = requested_deadline
            .map(|deadline| deadline.min(residence_deadline))
            .unwrap_or(residence_deadline);
        if deadline <= now {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        let mut state = self.state.lock().expect("logical route buffer poisoned");
        let slot_messages = state.per_slot.get(&slot).copied().unwrap_or(0);
        if slot_messages == self.config.maximum_messages_per_slot
            || state.messages == self.config.maximum_messages
            || state.bytes.saturating_add(bytes) > self.config.maximum_bytes
        {
            return Err(RemoteMessageError::BufferFull);
        }
        state.messages += 1;
        state.bytes += bytes;
        *state.per_slot.entry(slot.clone()).or_default() += 1;
        let start_resolution = state.resolving.insert(slot.clone());
        Ok((
            RouteBufferAdmission {
                buffer: self,
                slot,
                bytes,
            },
            deadline,
            start_resolution,
        ))
    }

    fn resolved(&self, slot: &PlacementSlotKey) {
        self.state
            .lock()
            .expect("logical route buffer poisoned")
            .resolving
            .remove(slot);
    }
}

struct RouteBufferAdmission<'a> {
    buffer: &'a RouteBuffer,
    slot: PlacementSlotKey,
    bytes: usize,
}

impl Drop for RouteBufferAdmission<'_> {
    fn drop(&mut self) {
        let mut state = self
            .buffer
            .state
            .lock()
            .expect("logical route buffer poisoned");
        state.messages = state.messages.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(self.bytes);
        if let Some(count) = state.per_slot.get_mut(&self.slot) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.per_slot.remove(&self.slot);
                state.resolving.remove(&self.slot);
            }
        }
    }
}

pub struct ClusterLogicalRouter {
    local_node: NodeKey,
    state: Arc<Mutex<LogicPlacementState>>,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    coordinator: AssociationKey,
    buffer_config: LogicalBufferConfig,
    entities: BTreeMap<EntityType, Arc<dyn EntityRoute>>,
    singletons: BTreeMap<SingletonKind, Arc<dyn SingletonRoute>>,
    maximum_registrations: usize,
}

impl ClusterLogicalRouter {
    pub fn new(
        local_node: NodeKey,
        state: Arc<Mutex<LogicPlacementState>>,
        associations: Arc<AssociationManager>,
        messaging: Arc<OutboundMessaging>,
        coordinator: AssociationKey,
        buffer_config: LogicalBufferConfig,
        maximum_registrations: usize,
    ) -> Result<Self, ClusterRouterError> {
        local_node
            .validate()
            .map_err(|_| ClusterRouterError::InvalidNode)?;
        if maximum_registrations == 0 {
            return Err(ClusterRouterError::ZeroLimit);
        }
        buffer_config.validate()?;
        if coordinator.local_incarnation != local_node.incarnation
            || coordinator.remote_address == local_node.address
        {
            return Err(ClusterRouterError::InvalidCoordinator);
        }
        Ok(Self {
            local_node,
            state,
            associations,
            messaging,
            coordinator,
            buffer_config,
            entities: BTreeMap::new(),
            singletons: BTreeMap::new(),
            maximum_registrations,
        })
    }

    pub fn register_entity<A, L>(
        &mut self,
        config: EntityConfig,
        registry: Arc<ActorRegistry<A>>,
        protocol: Arc<ActorProtocol<A>>,
        loader: L,
    ) -> Result<(), ClusterRouterError>
    where
        A: Actor,
        L: ActorLoader<A>,
    {
        if self.entities.len() + self.singletons.len() == self.maximum_registrations {
            return Err(ClusterRouterError::Capacity);
        }
        if protocol.protocol_id() != config.protocol_id {
            return Err(ClusterRouterError::ProtocolMismatch);
        }
        let entity_type = config.entity_type.clone();
        if self
            .entities
            .insert(
                entity_type.clone(),
                Arc::new(EntityRouteHost {
                    local_node: self.local_node.clone(),
                    state: self.state.clone(),
                    associations: self.associations.clone(),
                    messaging: self.messaging.clone(),
                    coordinator: self.coordinator.clone(),
                    buffer: RouteBuffer::new(self.buffer_config.clone()),
                    config,
                    registry,
                    protocol,
                    loader,
                }),
            )
            .is_some()
        {
            return Err(ClusterRouterError::DuplicateEntity(entity_type));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_singleton<A, L>(
        &mut self,
        kind: SingletonKind,
        config_fingerprint: ConfigFingerprint,
        protocol_id: ProtocolId,
        registry: Arc<ActorRegistry<A>>,
        protocol: Arc<ActorProtocol<A>>,
        loader: L,
    ) -> Result<(), ClusterRouterError>
    where
        A: Actor,
        L: ActorLoader<A>,
    {
        if self.entities.len() + self.singletons.len() == self.maximum_registrations {
            return Err(ClusterRouterError::Capacity);
        }
        if protocol.protocol_id() != protocol_id {
            return Err(ClusterRouterError::ProtocolMismatch);
        }
        if self
            .singletons
            .insert(
                kind.clone(),
                Arc::new(SingletonRouteHost {
                    local_node: self.local_node.clone(),
                    state: self.state.clone(),
                    associations: self.associations.clone(),
                    messaging: self.messaging.clone(),
                    coordinator: self.coordinator.clone(),
                    buffer: RouteBuffer::new(self.buffer_config.clone()),
                    kind: kind.clone(),
                    config_fingerprint,
                    protocol_id,
                    registry,
                    protocol,
                    loader,
                }),
            )
            .is_some()
        {
            return Err(ClusterRouterError::DuplicateSingleton(kind));
        }
        Ok(())
    }
}

#[async_trait]
impl LogicalRouter for ClusterLogicalRouter {
    async fn tell_entity(
        &self,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.entities
            .get(target.entity_type())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .tell(target, fingerprint, message_id, payload)
            .await
    }

    async fn ask_entity(
        &self,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        self.entities
            .get(target.entity_type())
            .ok_or(AskError::Protocol(RemoteMessageError::UnsupportedProtocol))?
            .ask(target, fingerprint, message_id, payload, deadline)
            .await
    }

    async fn tell_singleton(
        &self,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.singletons
            .get(target.singleton_kind())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .tell(target, fingerprint, message_id, payload)
            .await
    }

    async fn ask_singleton(
        &self,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        self.singletons
            .get(target.singleton_kind())
            .ok_or(AskError::Protocol(RemoteMessageError::UnsupportedProtocol))?
            .ask(target, fingerprint, message_id, payload, deadline)
            .await
    }

    async fn resolve_entity_current(
        &self,
        target: EntityRef<()>,
    ) -> Result<Option<ActorRef<()>>, WatchError> {
        self.entities
            .get(target.entity_type())
            .ok_or(WatchError::NotActive)?
            .resolve_current(target)
            .await
    }

    async fn resolve_singleton_current(
        &self,
        target: SingletonRef<()>,
    ) -> Result<Option<ActorRef<()>>, WatchError> {
        self.singletons
            .get(target.singleton_kind())
            .ok_or(WatchError::Unavailable)?
            .resolve_current(target)
            .await
    }

    async fn drain_slot(&self, slot: PlacementSlotKey) -> Result<bool, RemoteMessageError> {
        match slot {
            PlacementSlotKey::Shard {
                entity_type,
                shard_id,
            } => {
                self.entities
                    .get(&entity_type)
                    .ok_or(RemoteMessageError::UnsupportedProtocol)?
                    .drain(shard_id)
                    .await
            }
            PlacementSlotKey::Singleton(kind) => {
                self.singletons
                    .get(&kind)
                    .ok_or(RemoteMessageError::UnsupportedProtocol)?
                    .drain()
                    .await
            }
        }
    }

    async fn stop_fenced_slot(&self, slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        self.drain_slot(slot).await.map(|_| ())
    }

    async fn receive_entity_tell(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.entities
            .get(target.reference.entity_type())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .receive_tell(target, message_id, payload)
            .await
    }

    async fn receive_entity_ask(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        let route = self
            .entities
            .get(target.reference.entity_type())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?;
        if message_id == LOGICAL_RESOLVE_MESSAGE_ID {
            return route.receive_resolve(target).await;
        }
        route
            .receive_ask(target, message_id, payload, deadline)
            .await
    }

    async fn receive_singleton_tell(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.singletons
            .get(target.reference.singleton_kind())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .receive_tell(target, message_id, payload)
            .await
    }

    async fn receive_singleton_ask(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        let route = self
            .singletons
            .get(target.reference.singleton_kind())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?;
        if message_id == LOGICAL_RESOLVE_MESSAGE_ID {
            return route.receive_resolve(target).await;
        }
        route
            .receive_ask(target, message_id, payload, deadline)
            .await
    }
}

#[async_trait]
trait EntityRoute: Send + Sync {
    async fn tell(
        &self,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;
    async fn ask(
        &self,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;
    async fn receive_tell(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;
    async fn receive_ask(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError>;
    async fn resolve_current(
        &self,
        target: EntityRef<()>,
    ) -> Result<Option<ActorRef<()>>, WatchError>;
    async fn receive_resolve(
        &self,
        target: LogicalEntityTarget,
    ) -> Result<Bytes, RemoteMessageError>;
    async fn drain(&self, shard_id: lattice_placement::ShardId)
    -> Result<bool, RemoteMessageError>;
}

struct EntityRouteHost<A: Actor, L: ActorLoader<A>> {
    local_node: NodeKey,
    state: Arc<Mutex<LogicPlacementState>>,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    coordinator: AssociationKey,
    buffer: RouteBuffer,
    config: EntityConfig,
    registry: Arc<ActorRegistry<A>>,
    protocol: Arc<ActorProtocol<A>>,
    loader: L,
}

impl<A: Actor, L: ActorLoader<A>> EntityRouteHost<A, L> {
    fn slot_key(&self, target: &EntityRef<()>) -> Result<PlacementSlotKey, RemoteMessageError> {
        if target.protocol_id() != self.config.protocol_id
            || target.config_fingerprint() != self.config.fingerprint()
        {
            return Err(RemoteMessageError::ProtocolFingerprintMismatch);
        }
        Ok(PlacementSlotKey::Shard {
            entity_type: self.config.entity_type.clone(),
            shard_id: self.config.shard_for(target.entity_id()),
        })
    }

    fn route_slot(
        &self,
        target: &EntityRef<()>,
    ) -> Result<(PlacementSlotKey, PlacementSlot), RemoteMessageError> {
        let key = self.slot_key(target)?;
        let slot = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .slot(&key)
            .cloned()
            .ok_or(RemoteMessageError::StaleAuthority)?;
        Ok((key, slot))
    }

    fn running_slot(
        &self,
        target: &EntityRef<()>,
    ) -> Result<(PlacementSlotKey, PlacementSlot), RemoteMessageError> {
        let (key, slot) = self.route_slot(target)?;
        if slot.state != PlacementSlotState::Running || slot.owner.is_none() {
            return Err(RemoteMessageError::ShardUnavailable);
        }
        Ok((key, slot))
    }

    fn request_resolution(&self, key: &PlacementSlotKey) -> Result<(), RemoteMessageError> {
        let PlacementSlotKey::Shard {
            entity_type,
            shard_id,
        } = key
        else {
            return Err(RemoteMessageError::InvalidPayload);
        };
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(RemoteMessageError::ShardUnavailable)?;
        if association.state() == AssociationState::Closed {
            return Err(RemoteMessageError::ShardUnavailable);
        }
        let sequence = NEXT_LOGICAL_RESOLUTION.fetch_add(1, Ordering::Relaxed);
        let request_id = (self.local_node.incarnation.get() << 64) ^ u128::from(sequence);
        let payload = lattice_placement::control::encode_control_command(
            &lattice_placement::control::PlacementControlCommand::ResolveShard {
                request_id,
                entity_type: entity_type.clone(),
                shard_id: *shard_id,
            },
            self.buffer.config.maximum_control_payload,
        )
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
        association
            .admit_control_command(payload)
            .map(|_| ())
            .map_err(|_| RemoteMessageError::ShardUnavailable)
    }

    async fn await_running_slot(
        &self,
        target: &EntityRef<()>,
        payload_bytes: usize,
        requested_deadline: Option<Instant>,
    ) -> Result<(PlacementSlotKey, PlacementSlot), RemoteMessageError> {
        match self.running_slot(target) {
            Ok(slot) => return Ok(slot),
            Err(RemoteMessageError::ProtocolFingerprintMismatch) => {
                return Err(RemoteMessageError::ProtocolFingerprintMismatch);
            }
            Err(_) => {}
        }
        let key = self.slot_key(target)?;
        let (_admission, deadline, start_resolution) =
            self.buffer
                .admit(key.clone(), payload_bytes, requested_deadline)?;
        if start_resolution {
            self.request_resolution(&key)?;
        }
        let changed = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .change_notifier();
        loop {
            let notified = changed.notified();
            if let Ok(slot) = self.running_slot(target) {
                self.buffer.resolved(&key);
                return Ok(slot);
            }
            if tokio::time::timeout_at(deadline.into(), notified)
                .await
                .is_err()
            {
                return Err(
                    if requested_deadline.is_some_and(|value| value <= Instant::now()) {
                        RemoteMessageError::DeadlineExceeded
                    } else {
                        RemoteMessageError::ShardUnavailable
                    },
                );
            }
        }
    }

    fn validate_local(
        &self,
        target: &LogicalEntityTarget,
    ) -> Result<PlacementSlotKey, RemoteMessageError> {
        let (key, slot) = self.route_slot(&target.reference)?;
        let state = self.state.lock().expect("logic placement state poisoned");
        if target.owner_address != self.local_node.address
            || target.owner_incarnation != self.local_node.incarnation
            || target.assignment_generation != slot.assignment_generation.get()
            || slot.owner.as_ref() != Some(&self.local_node)
            || slot.state != PlacementSlotState::Running
            || !state.admission_open(&key)
        {
            return Err(RemoteMessageError::StaleAuthority);
        }
        Ok(key)
    }

    async fn activate(
        &self,
        target: &LogicalEntityTarget,
    ) -> Result<ActorHandle<A>, RemoteMessageError> {
        self.validate_local(target)?;
        self.registry
            .get_or_load(
                ActorId::Bytes(target.reference.entity_id().as_bytes().to_vec()),
                self.loader.clone(),
            )
            .await
            .map_err(|_| RemoteMessageError::HandlerFailed)
    }
}

#[async_trait]
impl<A, L> EntityRoute for EntityRouteHost<A, L>
where
    A: Actor,
    L: ActorLoader<A>,
{
    async fn tell(
        &self,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if fingerprint != self.protocol.fingerprint() {
            return Err(RemoteMessageError::ProtocolFingerprintMismatch);
        }
        let (_, slot) = self
            .await_running_slot(&target, payload.len(), None)
            .await?;
        let owner = slot.owner.ok_or(RemoteMessageError::StaleAuthority)?;
        let logical = LogicalEntityTarget {
            reference: target,
            owner_address: owner.address.clone(),
            owner_incarnation: owner.incarnation,
            assignment_generation: slot.assignment_generation.get(),
        };
        if owner == self.local_node {
            return self.receive_tell(logical, message_id, payload).await;
        }
        let association = self
            .associations
            .get_exact(
                logical.reference.cluster_id(),
                &owner.address,
                owner.incarnation,
            )
            .ok_or(RemoteMessageError::StaleAuthority)?;
        self.messaging
            .tell_entity(
                &association,
                &SenderIdentity::Process(self.local_node.incarnation.get()),
                &logical.reference,
                owner.address,
                owner.incarnation,
                logical.assignment_generation,
                fingerprint,
                message_id,
                payload,
            )
            .map(|_| ())
            .map_err(map_tell)
    }

    async fn ask(
        &self,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        if fingerprint != self.protocol.fingerprint() {
            return Err(AskError::Protocol(
                RemoteMessageError::ProtocolFingerprintMismatch,
            ));
        }
        let (_, slot) = self
            .await_running_slot(&target, payload.len(), Some(deadline))
            .await
            .map_err(AskError::Protocol)?;
        let owner = slot
            .owner
            .ok_or(AskError::Protocol(RemoteMessageError::StaleAuthority))?;
        let logical = LogicalEntityTarget {
            reference: target,
            owner_address: owner.address.clone(),
            owner_incarnation: owner.incarnation,
            assignment_generation: slot.assignment_generation.get(),
        };
        if owner == self.local_node {
            return self
                .receive_ask(logical, message_id, payload, deadline)
                .await
                .map_err(map_ask);
        }
        let association = self
            .associations
            .get_exact(
                logical.reference.cluster_id(),
                &owner.address,
                owner.incarnation,
            )
            .ok_or(AskError::Protocol(RemoteMessageError::StaleAuthority))?;
        self.messaging
            .ask_entity(
                &association,
                &SenderIdentity::Process(self.local_node.incarnation.get()),
                &logical.reference,
                owner.address,
                owner.incarnation,
                logical.assignment_generation,
                fingerprint,
                message_id,
                payload,
                deadline,
            )
            .await
    }

    async fn receive_tell(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        let handle = self.activate(&target).await?;
        match self
            .protocol
            .dispatch(handle, message_id, DispatchMode::Tell, payload, None)
            .await
            .map_err(map_dispatch)?
        {
            DispatchReply::TellAccepted => Ok(()),
            DispatchReply::Ask(_) => Err(RemoteMessageError::InvalidPayload),
        }
    }

    async fn receive_ask(
        &self,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if Instant::now() >= deadline {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        let handle = self.activate(&target).await?;
        match self
            .protocol
            .dispatch(
                handle,
                message_id,
                DispatchMode::Ask,
                payload,
                Some(deadline),
            )
            .await
            .map_err(map_dispatch)?
        {
            DispatchReply::Ask(reply) => Ok(reply),
            DispatchReply::TellAccepted => Err(RemoteMessageError::InvalidPayload),
        }
    }

    async fn resolve_current(
        &self,
        target: EntityRef<()>,
    ) -> Result<Option<ActorRef<()>>, WatchError> {
        let (key, slot) = self
            .route_slot(&target)
            .map_err(|_| WatchError::NotActive)?;
        let owner = slot.owner.clone().ok_or(WatchError::NotActive)?;
        if slot.state != PlacementSlotState::Running {
            return Ok(None);
        }
        if owner != self.local_node {
            let expected_cluster = target.cluster_id().clone();
            let expected_address = owner.address.clone();
            let expected_incarnation = owner.incarnation;
            let association = self
                .associations
                .get_exact(target.cluster_id(), &owner.address, owner.incarnation)
                .ok_or(WatchError::NotActive)?;
            let logical = LogicalEntityTarget {
                reference: target,
                owner_address: owner.address,
                owner_incarnation: owner.incarnation,
                assignment_generation: slot.assignment_generation.get(),
            };
            let result = self
                .messaging
                .ask_entity(
                    &association,
                    &SenderIdentity::Process(self.local_node.incarnation.get()),
                    &logical.reference,
                    logical.owner_address,
                    logical.owner_incarnation,
                    logical.assignment_generation,
                    self.protocol.fingerprint(),
                    LOGICAL_RESOLVE_MESSAGE_ID,
                    Bytes::new(),
                    Instant::now() + self.buffer.config.maximum_residence,
                )
                .await;
            return match result {
                Ok(bytes) => decode_resolved_actor(
                    &bytes,
                    &expected_cluster,
                    &expected_address,
                    expected_incarnation,
                    self.config.protocol_id,
                )
                .map(Some),
                Err(AskError::Remote(lattice_remoting::RemoteFailureCode::StaleActivation)) => {
                    Ok(None)
                }
                Err(_) => Err(WatchError::NotActive),
            };
        }
        if slot.owner.as_ref() != Some(&self.local_node)
            || !self
                .state
                .lock()
                .expect("logic placement state poisoned")
                .admission_open(&key)
        {
            return Ok(None);
        }
        Ok(self
            .registry
            .get_running(&ActorId::Bytes(target.entity_id().as_bytes().to_vec()))
            .and_then(|handle| handle.actor_ref().map(ActorRef::erase)))
    }

    async fn receive_resolve(
        &self,
        target: LogicalEntityTarget,
    ) -> Result<Bytes, RemoteMessageError> {
        self.validate_local(&target)?;
        let actor = self
            .registry
            .get_running(&ActorId::Bytes(
                target.reference.entity_id().as_bytes().to_vec(),
            ))
            .and_then(|handle| handle.actor_ref().map(ActorRef::erase))
            .ok_or(RemoteMessageError::StaleActivation)?;
        serde_json::to_vec(&actor)
            .map(Bytes::from)
            .map_err(|_| RemoteMessageError::HandlerFailed)
    }

    async fn drain(
        &self,
        shard_id: lattice_placement::ShardId,
    ) -> Result<bool, RemoteMessageError> {
        let actor_ids = self
            .registry
            .running_actor_ids()
            .into_iter()
            .filter(|actor_id| match actor_id {
                ActorId::Bytes(bytes) => lattice_core::actor_ref::EntityId::new(bytes.clone())
                    .is_ok_and(|entity_id| self.config.shard_for(&entity_id) == shard_id),
                ActorId::Str(_) | ActorId::U64(_) | ActorId::I64(_) => false,
            })
            .collect::<Vec<_>>();
        drain_actor_ids(
            &self.registry,
            actor_ids,
            self.buffer.config.maximum_residence,
        )
        .await
    }
}

#[async_trait]
trait SingletonRoute: Send + Sync {
    async fn tell(
        &self,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;
    async fn ask(
        &self,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;
    async fn receive_tell(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;
    async fn receive_ask(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError>;
    async fn resolve_current(
        &self,
        target: SingletonRef<()>,
    ) -> Result<Option<ActorRef<()>>, WatchError>;
    async fn receive_resolve(
        &self,
        target: LogicalSingletonTarget,
    ) -> Result<Bytes, RemoteMessageError>;
    async fn drain(&self) -> Result<bool, RemoteMessageError>;
}

struct SingletonRouteHost<A: Actor, L: ActorLoader<A>> {
    local_node: NodeKey,
    state: Arc<Mutex<LogicPlacementState>>,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    coordinator: AssociationKey,
    buffer: RouteBuffer,
    kind: SingletonKind,
    config_fingerprint: ConfigFingerprint,
    protocol_id: ProtocolId,
    registry: Arc<ActorRegistry<A>>,
    protocol: Arc<ActorProtocol<A>>,
    loader: L,
}

impl<A: Actor, L: ActorLoader<A>> SingletonRouteHost<A, L> {
    fn slot(
        &self,
        target: &SingletonRef<()>,
    ) -> Result<lattice_placement::PlacementSlot, RemoteMessageError> {
        if target.protocol_id() != self.protocol_id
            || target.config_fingerprint() != self.config_fingerprint
        {
            return Err(RemoteMessageError::ProtocolFingerprintMismatch);
        }
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .slot(&PlacementSlotKey::Singleton(self.kind.clone()))
            .cloned()
            .ok_or(RemoteMessageError::StaleAuthority)
    }

    fn running_slot(&self, target: &SingletonRef<()>) -> Result<PlacementSlot, RemoteMessageError> {
        let slot = self.slot(target)?;
        if slot.state != PlacementSlotState::Running || slot.owner.is_none() {
            return Err(RemoteMessageError::ShardUnavailable);
        }
        Ok(slot)
    }

    fn request_resolution(&self) -> Result<(), RemoteMessageError> {
        let association = self
            .associations
            .get(&self.coordinator)
            .ok_or(RemoteMessageError::ShardUnavailable)?;
        if association.state() == AssociationState::Closed {
            return Err(RemoteMessageError::ShardUnavailable);
        }
        let sequence = NEXT_LOGICAL_RESOLUTION.fetch_add(1, Ordering::Relaxed);
        let request_id = (self.local_node.incarnation.get() << 64) ^ u128::from(sequence);
        let payload = lattice_placement::control::encode_control_command(
            &lattice_placement::control::PlacementControlCommand::ResolveSingleton {
                request_id,
                kind: self.kind.clone(),
            },
            self.buffer.config.maximum_control_payload,
        )
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
        association
            .admit_control_command(payload)
            .map(|_| ())
            .map_err(|_| RemoteMessageError::ShardUnavailable)
    }

    async fn await_running_slot(
        &self,
        target: &SingletonRef<()>,
        payload_bytes: usize,
        requested_deadline: Option<Instant>,
    ) -> Result<PlacementSlot, RemoteMessageError> {
        match self.running_slot(target) {
            Ok(slot) => return Ok(slot),
            Err(RemoteMessageError::ProtocolFingerprintMismatch) => {
                return Err(RemoteMessageError::ProtocolFingerprintMismatch);
            }
            Err(_) => {}
        }
        let key = PlacementSlotKey::Singleton(self.kind.clone());
        let (_admission, deadline, start_resolution) =
            self.buffer
                .admit(key.clone(), payload_bytes, requested_deadline)?;
        if start_resolution {
            self.request_resolution()?;
        }
        let changed = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .change_notifier();
        loop {
            let notified = changed.notified();
            if let Ok(slot) = self.running_slot(target) {
                self.buffer.resolved(&key);
                return Ok(slot);
            }
            if tokio::time::timeout_at(deadline.into(), notified)
                .await
                .is_err()
            {
                return Err(
                    if requested_deadline.is_some_and(|value| value <= Instant::now()) {
                        RemoteMessageError::DeadlineExceeded
                    } else {
                        RemoteMessageError::ShardUnavailable
                    },
                );
            }
        }
    }

    fn validate_local(
        &self,
        target: &LogicalSingletonTarget,
    ) -> Result<PlacementSlotKey, RemoteMessageError> {
        let key = PlacementSlotKey::Singleton(self.kind.clone());
        let slot = self.slot(&target.reference)?;
        let state = self.state.lock().expect("logic placement state poisoned");
        if target.owner_address != self.local_node.address
            || target.owner_incarnation != self.local_node.incarnation
            || target.assignment_generation != slot.assignment_generation.get()
            || slot.owner.as_ref() != Some(&self.local_node)
            || slot.state != PlacementSlotState::Running
            || !state.admission_open(&key)
        {
            return Err(RemoteMessageError::StaleAuthority);
        }
        Ok(key)
    }

    async fn activate(
        &self,
        target: &LogicalSingletonTarget,
    ) -> Result<ActorHandle<A>, RemoteMessageError> {
        self.validate_local(target)?;
        self.registry
            .get_or_load(
                ActorId::Str(self.kind.as_str().to_owned()),
                self.loader.clone(),
            )
            .await
            .map_err(|_| RemoteMessageError::HandlerFailed)
    }
}

#[async_trait]
impl<A: Actor, L: ActorLoader<A>> SingletonRoute for SingletonRouteHost<A, L> {
    async fn tell(
        &self,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if fingerprint != self.protocol.fingerprint() {
            return Err(RemoteMessageError::ProtocolFingerprintMismatch);
        }
        let slot = self
            .await_running_slot(&target, payload.len(), None)
            .await?;
        let owner = slot.owner.ok_or(RemoteMessageError::StaleAuthority)?;
        let logical = LogicalSingletonTarget {
            reference: target,
            owner_address: owner.address.clone(),
            owner_incarnation: owner.incarnation,
            assignment_generation: slot.assignment_generation.get(),
        };
        if owner == self.local_node {
            return self.receive_tell(logical, message_id, payload).await;
        }
        let association = self
            .associations
            .get_exact(
                logical.reference.cluster_id(),
                &owner.address,
                owner.incarnation,
            )
            .ok_or(RemoteMessageError::StaleAuthority)?;
        self.messaging
            .tell_singleton(
                &association,
                &SenderIdentity::Process(self.local_node.incarnation.get()),
                &logical.reference,
                owner.address,
                owner.incarnation,
                logical.assignment_generation,
                fingerprint,
                message_id,
                payload,
            )
            .map(|_| ())
            .map_err(map_tell)
    }

    async fn ask(
        &self,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        if fingerprint != self.protocol.fingerprint() {
            return Err(AskError::Protocol(
                RemoteMessageError::ProtocolFingerprintMismatch,
            ));
        }
        let slot = self
            .await_running_slot(&target, payload.len(), Some(deadline))
            .await
            .map_err(AskError::Protocol)?;
        let owner = slot
            .owner
            .ok_or(AskError::Protocol(RemoteMessageError::StaleAuthority))?;
        let logical = LogicalSingletonTarget {
            reference: target,
            owner_address: owner.address.clone(),
            owner_incarnation: owner.incarnation,
            assignment_generation: slot.assignment_generation.get(),
        };
        if owner == self.local_node {
            return self
                .receive_ask(logical, message_id, payload, deadline)
                .await
                .map_err(map_ask);
        }
        let association = self
            .associations
            .get_exact(
                logical.reference.cluster_id(),
                &owner.address,
                owner.incarnation,
            )
            .ok_or(AskError::Protocol(RemoteMessageError::StaleAuthority))?;
        self.messaging
            .ask_singleton(
                &association,
                &SenderIdentity::Process(self.local_node.incarnation.get()),
                &logical.reference,
                owner.address,
                owner.incarnation,
                logical.assignment_generation,
                fingerprint,
                message_id,
                payload,
                deadline,
            )
            .await
    }

    async fn receive_tell(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        let handle = self.activate(&target).await?;
        match self
            .protocol
            .dispatch(handle, message_id, DispatchMode::Tell, payload, None)
            .await
            .map_err(map_dispatch)?
        {
            DispatchReply::TellAccepted => Ok(()),
            DispatchReply::Ask(_) => Err(RemoteMessageError::InvalidPayload),
        }
    }

    async fn receive_ask(
        &self,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if Instant::now() >= deadline {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        let handle = self.activate(&target).await?;
        match self
            .protocol
            .dispatch(
                handle,
                message_id,
                DispatchMode::Ask,
                payload,
                Some(deadline),
            )
            .await
            .map_err(map_dispatch)?
        {
            DispatchReply::Ask(reply) => Ok(reply),
            DispatchReply::TellAccepted => Err(RemoteMessageError::InvalidPayload),
        }
    }

    async fn resolve_current(
        &self,
        target: SingletonRef<()>,
    ) -> Result<Option<ActorRef<()>>, WatchError> {
        let key = PlacementSlotKey::Singleton(self.kind.clone());
        let slot = self.slot(&target).map_err(|_| WatchError::Unavailable)?;
        let owner = slot.owner.clone().ok_or(WatchError::Unavailable)?;
        if slot.state != PlacementSlotState::Running {
            return Ok(None);
        }
        if owner != self.local_node {
            let expected_cluster = target.cluster_id().clone();
            let expected_address = owner.address.clone();
            let expected_incarnation = owner.incarnation;
            let association = self
                .associations
                .get_exact(target.cluster_id(), &owner.address, owner.incarnation)
                .ok_or(WatchError::Unavailable)?;
            let logical = LogicalSingletonTarget {
                reference: target,
                owner_address: owner.address,
                owner_incarnation: owner.incarnation,
                assignment_generation: slot.assignment_generation.get(),
            };
            let result = self
                .messaging
                .ask_singleton(
                    &association,
                    &SenderIdentity::Process(self.local_node.incarnation.get()),
                    &logical.reference,
                    logical.owner_address,
                    logical.owner_incarnation,
                    logical.assignment_generation,
                    self.protocol.fingerprint(),
                    LOGICAL_RESOLVE_MESSAGE_ID,
                    Bytes::new(),
                    Instant::now() + self.buffer.config.maximum_residence,
                )
                .await;
            return match result {
                Ok(bytes) => decode_resolved_actor(
                    &bytes,
                    &expected_cluster,
                    &expected_address,
                    expected_incarnation,
                    self.protocol_id,
                )
                .map(Some),
                Err(AskError::Remote(lattice_remoting::RemoteFailureCode::StaleActivation)) => {
                    Ok(None)
                }
                Err(_) => Err(WatchError::Unavailable),
            };
        }
        if slot.owner.as_ref() != Some(&self.local_node)
            || !self
                .state
                .lock()
                .expect("logic placement state poisoned")
                .admission_open(&key)
        {
            return Ok(None);
        }
        Ok(self
            .registry
            .get_running(&ActorId::Str(self.kind.as_str().to_owned()))
            .and_then(|handle| handle.actor_ref().map(ActorRef::erase)))
    }

    async fn receive_resolve(
        &self,
        target: LogicalSingletonTarget,
    ) -> Result<Bytes, RemoteMessageError> {
        self.validate_local(&target)?;
        let actor = self
            .registry
            .get_running(&ActorId::Str(self.kind.as_str().to_owned()))
            .and_then(|handle| handle.actor_ref().map(ActorRef::erase))
            .ok_or(RemoteMessageError::StaleActivation)?;
        serde_json::to_vec(&actor)
            .map(Bytes::from)
            .map_err(|_| RemoteMessageError::HandlerFailed)
    }

    async fn drain(&self) -> Result<bool, RemoteMessageError> {
        drain_actor_ids(
            &self.registry,
            [ActorId::Str(self.kind.as_str().to_owned())],
            self.buffer.config.maximum_residence,
        )
        .await
    }
}

async fn drain_actor_ids<A, I>(
    registry: &ActorRegistry<A>,
    actor_ids: I,
    timeout: Duration,
) -> Result<bool, RemoteMessageError>
where
    A: Actor,
    I: IntoIterator<Item = ActorId>,
{
    for actor_id in actor_ids {
        let Some(handle) = registry.remove(&actor_id).await else {
            continue;
        };
        let mut lifecycle = handle.subscribe_lifecycle();
        handle
            .stop(StopReason::Passivated(PassivationReason::Drain))
            .await
            .map_err(|_| RemoteMessageError::HandlerFailed)?;
        let stopped = tokio::time::timeout(timeout, async {
            loop {
                match *lifecycle.borrow() {
                    ActorLifecycleState::Stopped => return true,
                    ActorLifecycleState::StopFailed => return false,
                    _ => {}
                }
                if lifecycle.changed().await.is_err() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false);
        if !stopped {
            return Ok(false);
        }
    }
    Ok(true)
}

fn map_dispatch(error: DispatchError) -> RemoteMessageError {
    match error {
        DispatchError::UnknownMessage(_) | DispatchError::UnregisteredType => {
            RemoteMessageError::UnknownMessage
        }
        DispatchError::Decode(_)
        | DispatchError::ModeMismatch
        | DispatchError::ReplyTypeMismatch
        | DispatchError::PayloadTooLarge { .. }
        | DispatchError::Encode(_) => RemoteMessageError::InvalidPayload,
        DispatchError::MissingDeadline | DispatchError::Actor(ActorCallError::DeadlineExceeded) => {
            RemoteMessageError::DeadlineExceeded
        }
        DispatchError::MailboxRejected => RemoteMessageError::MailboxRejected,
        DispatchError::Actor(_) => RemoteMessageError::HandlerFailed,
    }
}

fn decode_resolved_actor(
    payload: &[u8],
    cluster: &lattice_core::actor_ref::ClusterId,
    address: &lattice_core::actor_ref::NodeAddress,
    incarnation: lattice_core::actor_ref::NodeIncarnation,
    protocol_id: ProtocolId,
) -> Result<ActorRef<()>, WatchError> {
    let actor: ActorRef<()> =
        serde_json::from_slice(payload).map_err(|_| WatchError::InvalidCommand)?;
    if actor.cluster_id() != cluster
        || actor.node_address() != address
        || actor.node_incarnation() != incarnation
        || actor.protocol_id() != protocol_id
    {
        return Err(WatchError::InvalidCommand);
    }
    Ok(actor)
}

fn map_tell(error: lattice_remoting::TellError) -> RemoteMessageError {
    match error {
        lattice_remoting::TellError::Protocol(error)
        | lattice_remoting::TellError::Remote(error) => error,
        lattice_remoting::TellError::Association(_) => RemoteMessageError::HandlerFailed,
    }
}

fn map_ask(error: RemoteMessageError) -> AskError {
    AskError::Protocol(error)
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterRouterError {
    #[error("cluster logical router node identity is invalid")]
    InvalidNode,
    #[error("cluster logical router limit must be nonzero")]
    ZeroLimit,
    #[error("cluster logical router buffer configuration is invalid")]
    InvalidBufferConfig,
    #[error("cluster logical router Coordinator identity is invalid")]
    InvalidCoordinator,
    #[error("cluster logical router registration capacity reached")]
    Capacity,
    #[error("cluster logical router protocol does not match its config")]
    ProtocolMismatch,
    #[error("entity type is already registered: {0:?}")]
    DuplicateEntity(EntityType),
    #[error("singleton kind is already registered: {0:?}")]
    DuplicateSingleton(SingletonKind),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::BytesMut;
    use lattice_actor::ProtocolHostRegistry;
    use lattice_actor::actor_protocol;
    use lattice_actor::context::ActorContext;
    use lattice_actor::error::ActorError;
    use lattice_actor::protocol::{DecodeError, EncodeError, WireCodec, WireSchema};
    use lattice_actor::registry::{ActorCreateContext, ActorRefConfig, ActorRegistryConfig};
    use lattice_actor::traits::{Handler, Message};
    use lattice_core::actor_kind;
    use lattice_core::actor_ref::{ClusterId, EntityId, NodeAddress, NodeIncarnation, ProtocolId};
    use lattice_placement::control::{
        DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, PlacementControlRouter,
        encode_control_command,
    };
    use lattice_placement::coordinator::{SnapshotLimits, SnapshotRecord, build_snapshot};
    use lattice_placement::{
        AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, LogicCoordinatorConfig,
        LogicCoordinatorSession, PlacementSlot, Revision,
    };
    use lattice_remoting::{
        AssociationKey, CommandId, ControlDispatch, LaneAttachment, LaneKind, NodeIdentity,
        ProtocolDescriptor, RemotingConfig, RemotingEndpoint,
    };
    use tokio::sync::watch;

    use crate::backend::ServiceInboundDispatch;

    const TEST_PROTOCOL_ID: u64 = 77;

    #[derive(Clone)]
    struct GetValue(u64);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Value(u64);

    impl Message for GetValue {
        type Reply = Value;
    }

    impl WireSchema for GetValue {
        const SCHEMA_ID: u64 = 1;
        const SCHEMA_VERSION: u32 = 1;
    }

    impl WireSchema for Value {
        const SCHEMA_ID: u64 = 2;
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Copy)]
    struct GetCodec;

    impl WireCodec<GetValue> for GetCodec {
        const CODEC_ID: u64 = 1;
        const CODEC_VERSION: u32 = 1;

        fn encode(&self, value: &GetValue, output: &mut BytesMut) -> Result<(), EncodeError> {
            output.extend_from_slice(&value.0.to_be_bytes());
            Ok(())
        }

        fn decode(&self, input: &[u8]) -> Result<GetValue, DecodeError> {
            Ok(GetValue(u64::from_be_bytes(input.try_into().map_err(
                |_| DecodeError::new("GetValue requires eight bytes"),
            )?)))
        }
    }

    #[derive(Clone, Copy)]
    struct ValueCodec;

    impl WireCodec<Value> for ValueCodec {
        const CODEC_ID: u64 = 1;
        const CODEC_VERSION: u32 = 1;

        fn encode(&self, value: &Value, output: &mut BytesMut) -> Result<(), EncodeError> {
            output.extend_from_slice(&value.0.to_be_bytes());
            Ok(())
        }

        fn decode(&self, input: &[u8]) -> Result<Value, DecodeError> {
            Ok(Value(u64::from_be_bytes(input.try_into().map_err(
                |_| DecodeError::new("Value requires eight bytes"),
            )?)))
        }
    }

    struct EntityActor {
        value: u64,
    }

    #[async_trait]
    impl Actor for EntityActor {
        type Error = ActorError;
    }

    #[async_trait]
    impl Handler<GetValue> for EntityActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            message: GetValue,
        ) -> Result<Value, ActorError> {
            Ok(Value(self.value + message.0))
        }
    }

    actor_protocol! {
        EntityProtocol for EntityActor {
            protocol_id: TEST_PROTOCOL_ID;
            name: "cluster-router-test/v1";
            ask 1 => GetValue {
                request_codec: GetCodec,
                reply_codec: ValueCodec,
            }
        }
    }

    #[derive(Clone)]
    struct CountingLoader(Arc<AtomicUsize>);

    #[async_trait]
    impl ActorLoader<EntityActor> for CountingLoader {
        async fn load(&self, _ctx: ActorCreateContext) -> Result<EntityActor, ActorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(EntityActor { value: 40 })
        }
    }

    fn attach_coordinator(
        associations: &AssociationManager,
        cluster_id: &ClusterId,
        local_incarnation: NodeIncarnation,
        coordinator_address: NodeAddress,
        coordinator_incarnation: NodeIncarnation,
    ) -> AssociationKey {
        let association = associations
            .get_or_create(
                cluster_id.clone(),
                coordinator_address.clone(),
                coordinator_incarnation,
            )
            .unwrap();
        let key = AssociationKey {
            cluster_id: cluster_id.clone(),
            local_incarnation,
            remote_address: coordinator_address,
            remote_incarnation: coordinator_incarnation,
        };
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key.clone(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        key
    }

    async fn unused_address() -> NodeAddress {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        NodeAddress::new("127.0.0.1", port).unwrap()
    }

    async fn stage_logic_runtime(
        hello: lattice_placement::NodeHello,
        coordinator: AssociationKey,
        associations: Arc<AssociationManager>,
        slots: Vec<PlacementSlot>,
    ) -> (
        Arc<Mutex<LogicPlacementState>>,
        Arc<PlacementControlRouter>,
        watch::Sender<bool>,
        tokio::task::JoinHandle<Result<(), lattice_placement::LogicSessionError>>,
    ) {
        let (control, controls) =
            PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
        let control = Arc::new(control);
        let (logic, _effects) = LogicCoordinatorSession::new(
            hello.clone(),
            coordinator.clone(),
            associations,
            LogicCoordinatorConfig::default(),
            64,
        )
        .unwrap();
        for slot in &slots {
            if slot.owner.as_ref() == Some(&hello.node) {
                logic
                    .register_authority(slot.key.clone(), Duration::from_millis(10))
                    .unwrap();
            }
        }
        let state = logic.state();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(logic.run(controls, shutdown_rx));
        let revision = slots.iter().map(|slot| slot.revision).max().unwrap();
        let records = slots
            .iter()
            .map(|slot| {
                let key = match &slot.key {
                    PlacementSlotKey::Shard {
                        entity_type,
                        shard_id,
                    } => format!("shard/{}/{}", entity_type.as_str(), shard_id.get()),
                    PlacementSlotKey::Singleton(kind) => {
                        format!("singleton/{}", kind.as_str())
                    }
                };
                SnapshotRecord {
                    key,
                    value: serde_json::to_vec(slot).unwrap().into(),
                }
            })
            .collect();
        let limits = SnapshotLimits::default();
        let (begin, chunks, end) = build_snapshot(revision, records, &limits).unwrap();
        let mut commands = vec![PlacementControlCommand::SnapshotBegin(begin)];
        commands.extend(
            chunks
                .into_iter()
                .map(PlacementControlCommand::SnapshotChunk),
        );
        commands.push(PlacementControlCommand::SnapshotEnd(end));
        for slot in slots {
            if slot.owner.as_ref() == Some(&hello.node) {
                commands.push(PlacementControlCommand::ClaimGranted(ClaimGrant {
                    slot: slot.key,
                    owner: hello.node.clone(),
                    coordinator_term: slot.coordinator_term,
                    assignment_generation: slot.assignment_generation,
                    grant_sequence: GrantSequence::new(1).unwrap(),
                    ttl: Duration::from_secs(5),
                }));
            }
        }
        for command in commands {
            control
                .apply(
                    coordinator.clone(),
                    CommandId::generate(),
                    encode_control_command(&command, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap(),
                )
                .await
                .unwrap();
        }
        (state, control, shutdown, task)
    }

    #[tokio::test]
    async fn stale_generation_never_reaches_entity_loader() {
        let cluster_id = ClusterId::new("router-test").unwrap();
        let local_incarnation = NodeIncarnation::new(1).unwrap();
        let coordinator_incarnation = NodeIncarnation::new(2).unwrap();
        let local_address = NodeAddress::new("127.0.0.1", 25570).unwrap();
        let coordinator_address = NodeAddress::new("127.0.0.1", 25571).unwrap();
        let local_node = NodeKey {
            node_id: "logic".to_owned(),
            address: local_address.clone(),
            incarnation: local_incarnation,
        };
        let remoting = RemotingConfig::default();
        let associations = Arc::new(
            AssociationManager::new(local_address.clone(), local_incarnation, remoting.clone())
                .unwrap(),
        );
        let association = associations
            .get_or_create(
                cluster_id.clone(),
                coordinator_address.clone(),
                coordinator_incarnation,
            )
            .unwrap();
        let association_key = AssociationKey {
            cluster_id: cluster_id.clone(),
            local_incarnation,
            remote_address: coordinator_address,
            remote_incarnation: coordinator_incarnation,
        };
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: association_key.clone(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        let entity_config = EntityConfig::new(
            EntityType::new("entity").unwrap(),
            ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
            16,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .unwrap();
        let entity_id = EntityId::new(b"player-42".to_vec()).unwrap();
        let slot_key = PlacementSlotKey::Shard {
            entity_type: entity_config.entity_type.clone(),
            shard_id: entity_config.shard_for(&entity_id),
        };
        let hello = lattice_placement::NodeHello {
            node: local_node.clone(),
            roles: BTreeSet::new(),
            capacity_units: 1,
            hosted_entity_types: [entity_config.entity_type.clone()].into_iter().collect(),
            proxied_entity_types: BTreeSet::new(),
            singleton_eligibility: BTreeSet::new(),
            used_singletons: BTreeSet::new(),
            protocols: Vec::new(),
            entity_configs: Vec::new(),
            singleton_configs: Vec::new(),
        };
        let (control_router, controls) =
            PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
        let control_router = Arc::new(control_router);
        let (logic, _effects) = LogicCoordinatorSession::new(
            hello,
            association_key.clone(),
            associations.clone(),
            LogicCoordinatorConfig::default(),
            32,
        )
        .unwrap();
        let state = logic.state();
        logic
            .register_authority(slot_key.clone(), Duration::from_secs(2))
            .unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let logic_task = tokio::spawn(logic.run(controls, shutdown_rx));
        let slot = PlacementSlot {
            key: slot_key.clone(),
            config_fingerprint: entity_config.fingerprint(),
            owner: Some(local_node.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(2).unwrap(),
            coordinator_term: CoordinatorTerm::new(1).unwrap(),
            revision: Revision::new(1).unwrap(),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        };
        let limits = SnapshotLimits::default();
        let (begin, chunks, end) = build_snapshot(
            Revision::new(1).unwrap(),
            vec![SnapshotRecord {
                key: "shard/entity/0".to_owned(),
                value: Bytes::from(serde_json::to_vec(&slot).unwrap()),
            }],
            &limits,
        )
        .unwrap();
        let commands = std::iter::once(PlacementControlCommand::SnapshotBegin(begin))
            .chain(
                chunks
                    .into_iter()
                    .map(PlacementControlCommand::SnapshotChunk),
            )
            .chain(std::iter::once(PlacementControlCommand::SnapshotEnd(end)))
            .chain(std::iter::once(PlacementControlCommand::ClaimGranted(
                ClaimGrant {
                    slot: slot_key.clone(),
                    owner: local_node.clone(),
                    coordinator_term: CoordinatorTerm::new(1).unwrap(),
                    assignment_generation: AssignmentGeneration::new(2).unwrap(),
                    grant_sequence: GrantSequence::new(1).unwrap(),
                    ttl: Duration::from_secs(15),
                },
            )));
        for command in commands {
            control_router
                .apply(
                    association_key.clone(),
                    CommandId::generate(),
                    encode_control_command(&command, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap(),
                )
                .await
                .unwrap();
        }
        let protocol = Arc::new(EntityProtocol::build().unwrap());
        let registry = Arc::new(ActorRegistry::new(
            actor_kind!("Entity"),
            ActorRegistryConfig {
                actor_ref: Some(ActorRefConfig {
                    cluster_id: cluster_id.clone(),
                    node_address: local_address.clone(),
                    node_incarnation: local_incarnation,
                    protocol_id: ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
                }),
                ..ActorRegistryConfig::default()
            },
        ));
        let loads = Arc::new(AtomicUsize::new(0));
        let mut router = ClusterLogicalRouter::new(
            local_node.clone(),
            state,
            associations,
            Arc::new(OutboundMessaging::new(8).unwrap()),
            association_key,
            LogicalBufferConfig::default(),
            8,
        )
        .unwrap();
        router
            .register_entity(
                entity_config.clone(),
                registry,
                protocol.clone(),
                CountingLoader(loads.clone()),
            )
            .unwrap();
        let reference = entity_config.entity_ref::<()>(cluster_id, entity_id);
        let (_, request) = protocol
            .encode_request(DispatchMode::Ask, &GetValue(2))
            .unwrap();
        let stale = router
            .receive_entity_ask(
                LogicalEntityTarget {
                    reference: reference.clone(),
                    owner_address: local_address.clone(),
                    owner_incarnation: local_incarnation,
                    assignment_generation: 1,
                },
                1,
                request.clone(),
                Instant::now() + Duration::from_secs(1),
            )
            .await;
        assert_eq!(stale.unwrap_err(), RemoteMessageError::StaleAuthority);
        assert_eq!(loads.load(Ordering::SeqCst), 0);
        let reply = router
            .receive_entity_ask(
                LogicalEntityTarget {
                    reference,
                    owner_address: local_address,
                    owner_incarnation: local_incarnation,
                    assignment_generation: 2,
                },
                1,
                request,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(
            protocol.decode_reply::<GetValue>(1, &reply).unwrap(),
            Value(42)
        );
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        shutdown_tx.send(true).unwrap();
        logic_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn remote_entity_ask_reaches_only_claimed_owner() {
        let cluster_id = ClusterId::new("remote-entity-test").unwrap();
        let source_address = unused_address().await;
        let owner_address = unused_address().await;
        let coordinator_address = unused_address().await;
        let source_incarnation = NodeIncarnation::new(11).unwrap();
        let owner_incarnation = NodeIncarnation::new(12).unwrap();
        let coordinator_incarnation = NodeIncarnation::new(13).unwrap();
        let source_node = NodeKey {
            node_id: "source".to_owned(),
            address: source_address.clone(),
            incarnation: source_incarnation,
        };
        let owner_node = NodeKey {
            node_id: "owner".to_owned(),
            address: owner_address.clone(),
            incarnation: owner_incarnation,
        };
        let remoting = RemotingConfig {
            heartbeat_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(2),
            ..RemotingConfig::default()
        };
        let source_associations = Arc::new(
            AssociationManager::new(source_address.clone(), source_incarnation, remoting.clone())
                .unwrap(),
        );
        let owner_associations = Arc::new(
            AssociationManager::new(owner_address.clone(), owner_incarnation, remoting.clone())
                .unwrap(),
        );
        let source_coordinator = attach_coordinator(
            &source_associations,
            &cluster_id,
            source_incarnation,
            coordinator_address.clone(),
            coordinator_incarnation,
        );
        let owner_coordinator = attach_coordinator(
            &owner_associations,
            &cluster_id,
            owner_incarnation,
            coordinator_address,
            coordinator_incarnation,
        );
        let entity_config = EntityConfig::new(
            EntityType::new("remote-entity").unwrap(),
            ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
            16,
            "weighted-least-load",
            1,
            Vec::new(),
        )
        .unwrap();
        let entity_id = EntityId::new(b"account-42".to_vec()).unwrap();
        let entity_slot = PlacementSlot {
            key: PlacementSlotKey::Shard {
                entity_type: entity_config.entity_type.clone(),
                shard_id: entity_config.shard_for(&entity_id),
            },
            config_fingerprint: entity_config.fingerprint(),
            owner: Some(owner_node.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(7).unwrap(),
            coordinator_term: CoordinatorTerm::new(3).unwrap(),
            revision: Revision::new(9).unwrap(),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        };
        let singleton_kind = SingletonKind::new("remote-singleton").unwrap();
        let singleton_fingerprint = ConfigFingerprint::new([5; 32]);
        let singleton_slot = PlacementSlot {
            key: PlacementSlotKey::Singleton(singleton_kind.clone()),
            config_fingerprint: singleton_fingerprint,
            owner: Some(owner_node.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(4).unwrap(),
            coordinator_term: CoordinatorTerm::new(3).unwrap(),
            revision: Revision::new(9).unwrap(),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        };
        let hello = |node: NodeKey| lattice_placement::NodeHello {
            node,
            roles: BTreeSet::new(),
            capacity_units: 1,
            hosted_entity_types: [entity_config.entity_type.clone()].into_iter().collect(),
            proxied_entity_types: BTreeSet::new(),
            singleton_eligibility: [singleton_kind.clone()].into_iter().collect(),
            used_singletons: [singleton_kind.clone()].into_iter().collect(),
            protocols: Vec::new(),
            entity_configs: Vec::new(),
            singleton_configs: Vec::new(),
        };
        let (source_state, source_control, source_shutdown, source_logic) = stage_logic_runtime(
            hello(source_node.clone()),
            source_coordinator.clone(),
            source_associations.clone(),
            vec![entity_slot.clone(), singleton_slot.clone()],
        )
        .await;
        let (owner_state, owner_control, owner_shutdown, owner_logic) = stage_logic_runtime(
            hello(owner_node.clone()),
            owner_coordinator.clone(),
            owner_associations.clone(),
            vec![entity_slot, singleton_slot],
        )
        .await;
        let protocol = Arc::new(EntityProtocol::build().unwrap());
        let source_loads = Arc::new(AtomicUsize::new(0));
        let owner_loads = Arc::new(AtomicUsize::new(0));
        let registry = |address: NodeAddress, incarnation: NodeIncarnation| {
            Arc::new(ActorRegistry::new(
                actor_kind!("RemoteEntity"),
                ActorRegistryConfig {
                    actor_ref: Some(ActorRefConfig {
                        cluster_id: cluster_id.clone(),
                        node_address: address,
                        node_incarnation: incarnation,
                        protocol_id: ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
                    }),
                    ..ActorRegistryConfig::default()
                },
            ))
        };
        let source_messaging = Arc::new(OutboundMessaging::new(32).unwrap());
        let owner_messaging = Arc::new(OutboundMessaging::new(32).unwrap());
        let source_registry = registry(source_address.clone(), source_incarnation);
        let owner_registry = registry(owner_address.clone(), owner_incarnation);
        let mut source_router = ClusterLogicalRouter::new(
            source_node.clone(),
            source_state,
            source_associations.clone(),
            source_messaging.clone(),
            source_coordinator,
            LogicalBufferConfig::default(),
            8,
        )
        .unwrap();
        source_router
            .register_entity(
                entity_config.clone(),
                source_registry.clone(),
                protocol.clone(),
                CountingLoader(source_loads.clone()),
            )
            .unwrap();
        source_router
            .register_singleton(
                singleton_kind.clone(),
                singleton_fingerprint,
                ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
                source_registry,
                protocol.clone(),
                CountingLoader(source_loads.clone()),
            )
            .unwrap();
        let mut owner_router = ClusterLogicalRouter::new(
            owner_node.clone(),
            owner_state,
            owner_associations.clone(),
            owner_messaging.clone(),
            owner_coordinator,
            LogicalBufferConfig::default(),
            8,
        )
        .unwrap();
        owner_router
            .register_entity(
                entity_config.clone(),
                owner_registry.clone(),
                protocol.clone(),
                CountingLoader(owner_loads.clone()),
            )
            .unwrap();
        owner_router
            .register_singleton(
                singleton_kind.clone(),
                singleton_fingerprint,
                ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
                owner_registry,
                protocol.clone(),
                CountingLoader(owner_loads.clone()),
            )
            .unwrap();
        let source_router: Arc<dyn LogicalRouter> = Arc::new(source_router);
        let owner_router: Arc<dyn LogicalRouter> = Arc::new(owner_router);
        let descriptor = ProtocolDescriptor {
            protocol_id: ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
            fingerprint: protocol.fingerprint(),
        };
        let endpoint = |identity: NodeIdentity,
                        associations: Arc<AssociationManager>,
                        messaging: Arc<OutboundMessaging>,
                        logical: Arc<dyn LogicalRouter>,
                        control: Arc<PlacementControlRouter>| {
            Arc::new(
                RemotingEndpoint::new_with_control(
                    identity,
                    remoting.clone(),
                    associations,
                    messaging,
                    Arc::new(ServiceInboundDispatch {
                        hosts: Arc::new(ProtocolHostRegistry::new(1).unwrap()),
                        logical: Some(logical),
                    }),
                    control,
                    vec![descriptor.clone()],
                )
                .unwrap(),
            )
        };
        let source_identity = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: source_node.node_id.clone(),
            address: source_address,
            incarnation: source_incarnation,
        };
        let owner_identity = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: owner_node.node_id.clone(),
            address: owner_address,
            incarnation: owner_incarnation,
        };
        let source_endpoint = endpoint(
            source_identity,
            source_associations,
            source_messaging,
            source_router.clone(),
            source_control,
        );
        let owner_endpoint = endpoint(
            owner_identity.clone(),
            owner_associations,
            owner_messaging,
            owner_router,
            owner_control,
        );
        owner_endpoint.bind().await.unwrap();
        source_endpoint.connect_peer(owner_identity).await.unwrap();
        let reference = entity_config.entity_ref::<()>(cluster_id.clone(), entity_id);
        assert!(
            source_router
                .resolve_entity_current(reference.clone())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(owner_loads.load(Ordering::SeqCst), 0);
        let (_, request) = protocol
            .encode_request(DispatchMode::Ask, &GetValue(2))
            .unwrap();
        let reply = source_router
            .ask_entity(
                reference.clone(),
                protocol.fingerprint(),
                1,
                request,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(
            protocol.decode_reply::<GetValue>(1, &reply).unwrap(),
            Value(42)
        );
        let current = source_router
            .resolve_entity_current(reference)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.node_address(), &owner_node.address);
        assert_eq!(current.node_incarnation(), owner_node.incarnation);
        let singleton = SingletonRef::new(
            cluster_id,
            singleton_kind,
            ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
            singleton_fingerprint,
        );
        assert!(
            source_router
                .resolve_singleton_current(singleton.clone())
                .await
                .unwrap()
                .is_none()
        );
        let (_, request) = protocol
            .encode_request(DispatchMode::Ask, &GetValue(3))
            .unwrap();
        let reply = source_router
            .ask_singleton(
                singleton.clone(),
                protocol.fingerprint(),
                1,
                request,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(
            protocol.decode_reply::<GetValue>(1, &reply).unwrap(),
            Value(43)
        );
        let current = source_router
            .resolve_singleton_current(singleton)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.node_address(), &owner_node.address);
        assert_eq!(current.node_incarnation(), owner_node.incarnation);
        assert_eq!(source_loads.load(Ordering::SeqCst), 0);
        assert_eq!(owner_loads.load(Ordering::SeqCst), 2);
        source_endpoint.shutdown().await.unwrap();
        owner_endpoint.shutdown().await.unwrap();
        source_shutdown.send(true).unwrap();
        owner_shutdown.send(true).unwrap();
        source_logic.await.unwrap().unwrap();
        owner_logic.await.unwrap().unwrap();
    }
}
