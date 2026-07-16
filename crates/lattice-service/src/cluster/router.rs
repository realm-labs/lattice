use super::entity::EntityRouteHost;
use super::proxy::EntityProxyRoute;
use super::singleton::SingletonRouteHost;
use super::singleton_proxy::SingletonProxyRoute;
use super::{
    Actor, ActorLoader, ActorProtocolBinding, ActorRef, ActorRegistry, Arc, AskError,
    AssociationKey, AssociationManager, BTreeMap, Bytes, ClusterRouterError, DomainLogicalRouter,
    EntityConfig, EntityRef, Instant, LOGICAL_RESOLVE_MESSAGE_ID, LogicPlacementState,
    LogicalBufferConfig, LogicalEntityTarget, LogicalRouter, LogicalSingletonTarget, Mutex,
    NodeKey, OutboundMessaging, PlacementSlotKey, Protocol, ProtocolFingerprint,
    RemoteMessageError, RouteBuffer, SingletonConfig, SingletonRef, WatchError, async_trait,
};

impl DomainLogicalRouter {
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
            peers: None,
            messaging,
            coordinator,
            buffer_config,
            entities: BTreeMap::new(),
            singletons: BTreeMap::new(),
            maximum_registrations,
        })
    }

    pub(crate) fn with_peer_reconciler(mut self, peers: Arc<super::peers::PeerReconciler>) -> Self {
        self.peers = Some(peers);
        self
    }

    pub fn register_entity<A, L, P>(
        &mut self,
        config: EntityConfig,
        registry: Arc<ActorRegistry<A>>,
        protocol: Arc<ActorProtocolBinding<A, P>>,
        loader: L,
    ) -> Result<(), ClusterRouterError>
    where
        A: Actor,
        L: ActorLoader<A>,
        P: Protocol,
    {
        if self.entities.len() + self.singletons.len() == self.maximum_registrations {
            return Err(ClusterRouterError::Capacity);
        }
        if protocol.protocol_id() != config.protocol_id {
            return Err(ClusterRouterError::ProtocolMismatch);
        }
        let domain = config.domain.clone();
        let entity_type = config.entity_type.clone();
        let key = (domain.clone(), entity_type.clone());
        if self
            .entities
            .insert(
                key,
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
            return Err(ClusterRouterError::DuplicateEntity {
                domain,
                entity_type,
            });
        }
        Ok(())
    }

    pub fn register_entity_proxy(
        &mut self,
        config: EntityConfig,
        fingerprint: ProtocolFingerprint,
    ) -> Result<(), ClusterRouterError> {
        if self.entities.len() + self.singletons.len() == self.maximum_registrations {
            return Err(ClusterRouterError::Capacity);
        }
        let domain = config.domain.clone();
        let entity_type = config.entity_type.clone();
        let key = (domain.clone(), entity_type.clone());
        if self
            .entities
            .insert(
                key,
                Arc::new(EntityProxyRoute {
                    local_node: self.local_node.clone(),
                    state: self.state.clone(),
                    associations: self.associations.clone(),
                    peers: self.peers.clone(),
                    messaging: self.messaging.clone(),
                    coordinator: self.coordinator.clone(),
                    buffer: RouteBuffer::new(self.buffer_config.clone()),
                    config,
                    fingerprint,
                }),
            )
            .is_some()
        {
            return Err(ClusterRouterError::DuplicateEntity {
                domain,
                entity_type,
            });
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_singleton<A, L, P>(
        &mut self,
        config: SingletonConfig,
        registry: Arc<ActorRegistry<A>>,
        protocol: Arc<ActorProtocolBinding<A, P>>,
        loader: L,
    ) -> Result<(), ClusterRouterError>
    where
        A: Actor,
        L: ActorLoader<A>,
        P: Protocol,
    {
        if self.entities.len() + self.singletons.len() == self.maximum_registrations {
            return Err(ClusterRouterError::Capacity);
        }
        if protocol.protocol_id() != config.protocol_id || !config.validate() {
            return Err(ClusterRouterError::ProtocolMismatch);
        }
        let domain = config.domain.clone();
        let kind = config.kind.clone();
        let config_fingerprint = config.fingerprint();
        let protocol_id = config.protocol_id;
        let key = (domain.clone(), kind.clone());
        if self
            .singletons
            .insert(
                key,
                Arc::new(SingletonRouteHost {
                    local_node: self.local_node.clone(),
                    state: self.state.clone(),
                    associations: self.associations.clone(),
                    messaging: self.messaging.clone(),
                    coordinator: self.coordinator.clone(),
                    buffer: RouteBuffer::new(self.buffer_config.clone()),
                    domain: config.domain,
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
            return Err(ClusterRouterError::DuplicateSingleton { domain, kind });
        }
        Ok(())
    }

    pub fn register_singleton_proxy(
        &mut self,
        config: SingletonConfig,
        fingerprint: ProtocolFingerprint,
    ) -> Result<(), ClusterRouterError> {
        if self.entities.len() + self.singletons.len() == self.maximum_registrations {
            return Err(ClusterRouterError::Capacity);
        }
        if !config.validate() {
            return Err(ClusterRouterError::ProtocolMismatch);
        }
        let domain = config.domain.clone();
        let kind = config.kind.clone();
        if self
            .singletons
            .insert(
                (domain.clone(), kind.clone()),
                Arc::new(SingletonProxyRoute {
                    local_node: self.local_node.clone(),
                    state: self.state.clone(),
                    associations: self.associations.clone(),
                    peers: self.peers.clone(),
                    messaging: self.messaging.clone(),
                    coordinator: self.coordinator.clone(),
                    buffer: RouteBuffer::new(self.buffer_config.clone()),
                    config,
                    fingerprint,
                }),
            )
            .is_some()
        {
            return Err(ClusterRouterError::DuplicateSingleton { domain, kind });
        }
        Ok(())
    }
}

#[async_trait]
impl LogicalRouter for DomainLogicalRouter {
    async fn tell_entity(
        &self,
        sender: Option<ActorRef>,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.entities
            .get(&(target.domain().clone(), target.entity_type().clone()))
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .tell(sender, target, fingerprint, message_id, payload)
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
        self.entities
            .get(&(target.domain().clone(), target.entity_type().clone()))
            .ok_or(AskError::Protocol(RemoteMessageError::UnsupportedProtocol))?
            .ask(target, fingerprint, message_id, payload, deadline)
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
        self.singletons
            .get(&(target.domain().clone(), target.singleton_kind().clone()))
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .tell(sender, target, fingerprint, message_id, payload)
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
        self.singletons
            .get(&(target.domain().clone(), target.singleton_kind().clone()))
            .ok_or(AskError::Protocol(RemoteMessageError::UnsupportedProtocol))?
            .ask(target, fingerprint, message_id, payload, deadline)
            .await
    }

    async fn resolve_entity_current(
        &self,
        target: EntityRef,
    ) -> Result<Option<ActorRef>, WatchError> {
        self.entities
            .get(&(target.domain().clone(), target.entity_type().clone()))
            .ok_or(WatchError::NotActive)?
            .resolve_current(target)
            .await
    }

    async fn resolve_singleton_current(
        &self,
        target: SingletonRef,
    ) -> Result<Option<ActorRef>, WatchError> {
        self.singletons
            .get(&(target.domain().clone(), target.singleton_kind().clone()))
            .ok_or(WatchError::Unavailable)?
            .resolve_current(target)
            .await
    }

    async fn drain_slot(&self, slot: PlacementSlotKey) -> Result<bool, RemoteMessageError> {
        match slot {
            PlacementSlotKey::Shard {
                domain,
                entity_type,
                shard_id,
            } => {
                self.entities
                    .get(&(domain, entity_type))
                    .ok_or(RemoteMessageError::UnsupportedProtocol)?
                    .drain(shard_id)
                    .await
            }
            PlacementSlotKey::Singleton { domain, kind } => {
                self.singletons
                    .get(&(domain, kind))
                    .ok_or(RemoteMessageError::UnsupportedProtocol)?
                    .drain()
                    .await
            }
        }
    }

    async fn stop_fenced_slot(&self, slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        match slot {
            PlacementSlotKey::Shard {
                domain,
                entity_type,
                shard_id,
            } => {
                self.entities
                    .get(&(domain, entity_type))
                    .ok_or(RemoteMessageError::UnsupportedProtocol)?
                    .fence(shard_id)
                    .await
            }
            PlacementSlotKey::Singleton { domain, kind } => {
                self.singletons
                    .get(&(domain, kind))
                    .ok_or(RemoteMessageError::UnsupportedProtocol)?
                    .fence()
                    .await
            }
        }
    }

    async fn receive_entity_tell(
        &self,
        sender: Option<ActorRef>,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.entities
            .get(&(
                target.reference.domain().clone(),
                target.reference.entity_type().clone(),
            ))
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
            .get(&(
                target.reference.domain().clone(),
                target.reference.entity_type().clone(),
            ))
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
        sender: Option<ActorRef>,
        target: LogicalSingletonTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.singletons
            .get(&(
                target.reference.domain().clone(),
                target.reference.singleton_kind().clone(),
            ))
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
            .get(&(
                target.reference.domain().clone(),
                target.reference.singleton_kind().clone(),
            ))
            .ok_or(RemoteMessageError::UnsupportedProtocol)?;
        if message_id == LOGICAL_RESOLVE_MESSAGE_ID {
            return route.receive_resolve(target).await;
        }
        route
            .receive_ask(target, message_id, payload, deadline)
            .await
    }
}
