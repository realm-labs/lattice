use super::entity::EntityRouteHost;
use super::singleton::SingletonRouteHost;
use super::{
    Actor, ActorLoader, ActorProtocol, ActorRef, ActorRegistry, Arc, AskError, AssociationKey,
    AssociationManager, BTreeMap, Bytes, ClusterLogicalRouter, ClusterRouterError,
    ConfigFingerprint, EntityConfig, EntityRef, Instant, LOGICAL_RESOLVE_MESSAGE_ID,
    LogicPlacementState, LogicalBufferConfig, LogicalEntityTarget, LogicalRouter,
    LogicalSingletonTarget, Mutex, NodeKey, OutboundMessaging, PlacementSlotKey,
    ProtocolFingerprint, ProtocolId, RemoteMessageError, RouteBuffer, SingletonKind, SingletonRef,
    WatchError, async_trait,
};

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
        sender: Option<ActorRef<()>>,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.entities
            .get(target.entity_type())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .tell(sender, target, fingerprint, message_id, payload)
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
        sender: Option<ActorRef<()>>,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.singletons
            .get(target.singleton_kind())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .tell(sender, target, fingerprint, message_id, payload)
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
        sender: Option<ActorRef<()>>,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.entities
            .get(target.reference.entity_type())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .receive_tell(sender, target, message_id, payload)
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
        sender: Option<ActorRef<()>>,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.singletons
            .get(target.reference.singleton_kind())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .receive_tell(sender, target, message_id, payload)
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
