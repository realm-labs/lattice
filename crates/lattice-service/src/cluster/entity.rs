use lattice_actor::registry::ActorQuarantineError;
use lattice_core::{actor_ref::EntityId, coordinator::CoordinatorScope};
use lattice_placement::{control::PlacementControlCommand, types::ShardId};
use lattice_remoting::messaging::error::RemoteFailureCode;

use super::{
    Actor, ActorHandle, ActorId, ActorLoader, ActorProtocolBinding, ActorRef, ActorRegistry, Arc,
    AskError, AssociationKey, AssociationManager, AssociationState, Bytes, DispatchMode,
    DispatchReply, EntityConfig, EntityRef, Instant, LOGICAL_RESOLVE_MESSAGE_ID,
    LogicPlacementState, LogicalEntityTarget, Mutex, NodeKey, OutboundMessage, OutboundMessaging,
    PlacementSlot, PlacementSlotKey, PlacementSlotState, Protocol, ProtocolFingerprint,
    RemoteMessageError, RouteBuffer, SenderIdentity, ShardMapperBinding, WatchError, async_trait,
    decode_resolved_actor, drain_actor_ids, map_ask, map_dispatch, map_tell,
    next_logical_resolution,
};

#[async_trait]
pub(super) trait EntityRoute: Send + Sync {
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;
    async fn ask(
        &self,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;
    async fn receive_tell(
        &self,
        sender: Option<ActorRef>,
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
    async fn resolve_current(&self, target: EntityRef) -> Result<Option<ActorRef>, WatchError>;
    async fn receive_resolve(
        &self,
        target: LogicalEntityTarget,
    ) -> Result<Bytes, RemoteMessageError>;
    async fn drain(&self, shard_id: ShardId) -> Result<bool, RemoteMessageError>;
    async fn wait_drained(&self, _shard_id: ShardId) -> Result<(), RemoteMessageError> {
        Ok(())
    }
    async fn fence(&self, _shard_id: ShardId) -> Result<(), RemoteMessageError> {
        Ok(())
    }
}

pub(super) struct EntityRouteHost<A: Actor, L: ActorLoader<A>, P: Protocol> {
    pub(super) local_node: NodeKey,
    pub(super) state: Arc<Mutex<LogicPlacementState>>,
    pub(super) associations: Arc<AssociationManager>,
    pub(super) messaging: Arc<OutboundMessaging>,
    pub(super) coordinator: AssociationKey,
    pub(super) buffer: RouteBuffer,
    pub(super) config: EntityConfig,
    pub(super) mapper: ShardMapperBinding,
    pub(super) registry: Arc<ActorRegistry<A>>,
    pub(super) protocol: Arc<ActorProtocolBinding<A, P>>,
    pub(super) loader: L,
}

impl<A: Actor, L: ActorLoader<A>, P: Protocol> EntityRouteHost<A, L, P> {
    fn slot_key(&self, target: &EntityRef) -> Result<PlacementSlotKey, RemoteMessageError> {
        if target.protocol_id() != self.config.protocol_id
            || target.domain() != &self.config.domain
            || target.config_fingerprint() != self.config.fingerprint()
        {
            return Err(RemoteMessageError::ProtocolFingerprintMismatch);
        }
        Ok(PlacementSlotKey::Shard {
            domain: self.config.domain.clone(),
            entity_type: self.config.entity_type.clone(),
            shard_id: self
                .mapper
                .shard_for(target.entity_id())
                .map_err(|_| RemoteMessageError::InvalidPayload)?,
        })
    }

    fn route_slot(
        &self,
        target: &EntityRef,
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
        target: &EntityRef,
    ) -> Result<(PlacementSlotKey, PlacementSlot), RemoteMessageError> {
        let (key, slot) = self.route_slot(target)?;
        if slot.state != PlacementSlotState::Running || slot.owner.is_none() {
            return Err(RemoteMessageError::ShardUnavailable);
        }
        Ok((key, slot))
    }

    fn request_resolution(
        &self,
        key: &PlacementSlotKey,
        request_id: u128,
    ) -> Result<(), RemoteMessageError> {
        let PlacementSlotKey::Shard {
            domain,
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
        let coordinator_term = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .coordinator_term()
            .ok_or(RemoteMessageError::ShardUnavailable)?;
        let payload = lattice_placement::control::encode_control_command_for_term(
            &CoordinatorScope::Placement(domain.clone()),
            coordinator_term,
            &PlacementControlCommand::ResolveShard {
                request_id,
                domain: domain.clone(),
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
        target: &EntityRef,
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
        let candidate_request_id = next_logical_resolution(self.local_node.incarnation);
        let (_admission, deadline, resolution) = self.buffer.admit(
            key.clone(),
            payload_bytes,
            requested_deadline,
            candidate_request_id,
        )?;
        if resolution.start {
            self.request_resolution(&key, resolution.request_id)?;
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
            if self
                .state
                .lock()
                .expect("logic placement state poisoned")
                .resolution_failure(&key, resolution.request_id)
                .is_some()
            {
                self.buffer.resolution_failed(&key, resolution.request_id);
                return Err(RemoteMessageError::ShardUnavailable);
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
impl<A, L, P> EntityRoute for EntityRouteHost<A, L, P>
where
    A: Actor,
    L: ActorLoader<A>,
    P: Protocol,
{
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: EntityRef,
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
            return self
                .receive_tell(sender, logical, message_id, payload)
                .await;
        }
        let association = self
            .associations
            .get_exact(
                logical.reference.cluster_id(),
                &owner.address,
                owner.incarnation,
            )
            .ok_or(RemoteMessageError::StaleAuthority)?;
        let sender = sender
            .as_ref()
            .map(SenderIdentity::from)
            .unwrap_or_else(|| SenderIdentity::Process(self.local_node.incarnation.get()));
        self.messaging
            .tell_entity(
                &association,
                &sender,
                logical,
                OutboundMessage::new(fingerprint, message_id, payload),
            )
            .map(|_| ())
            .map_err(map_tell)
    }

    async fn ask(
        &self,
        target: EntityRef,
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
                logical,
                OutboundMessage::new(fingerprint, message_id, payload),
                deadline,
            )
            .await
    }

    async fn receive_tell(
        &self,
        sender: Option<ActorRef>,
        target: LogicalEntityTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if sender
            .as_ref()
            .is_some_and(|sender| sender.cluster_id() != target.reference.cluster_id())
        {
            return Err(RemoteMessageError::Unauthorized);
        }
        let handle = self.activate(&target).await?;
        match self
            .protocol
            .dispatch_with_sender(
                handle,
                message_id,
                DispatchMode::Tell,
                payload,
                None,
                sender,
            )
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

    async fn resolve_current(&self, target: EntityRef) -> Result<Option<ActorRef>, WatchError> {
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
                    logical,
                    OutboundMessage::new(
                        self.protocol.fingerprint(),
                        LOGICAL_RESOLVE_MESSAGE_ID,
                        Bytes::new(),
                    ),
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
                Err(AskError::Remote(RemoteFailureCode::StaleActivation)) => Ok(None),
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

    async fn drain(&self, shard_id: ShardId) -> Result<bool, RemoteMessageError> {
        let actor_ids = self
            .registry
            .active_actor_ids()
            .into_iter()
            .filter(|actor_id| match actor_id {
                ActorId::Bytes(bytes) => EntityId::new(bytes.clone()).is_ok_and(|entity_id| {
                    self.mapper
                        .shard_for(&entity_id)
                        .is_ok_and(|mapped| mapped == shard_id)
                }),
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

    async fn wait_drained(&self, shard_id: ShardId) -> Result<(), RemoteMessageError> {
        let actor_ids = self
            .registry
            .active_actor_ids()
            .into_iter()
            .filter(|actor_id| match actor_id {
                ActorId::Bytes(bytes) => EntityId::new(bytes.clone()).is_ok_and(|entity_id| {
                    self.mapper
                        .shard_for(&entity_id)
                        .is_ok_and(|mapped| mapped == shard_id)
                }),
                ActorId::Str(_) | ActorId::U64(_) | ActorId::I64(_) => false,
            })
            .collect::<Vec<_>>();
        self.registry.wait_actor_ids_terminal(actor_ids).await;
        let key = PlacementSlotKey::Shard {
            domain: self.config.domain.clone(),
            entity_type: self.config.entity_type.clone(),
            shard_id,
        };
        let still_owned = self
            .state
            .lock()
            .expect("logic placement state poisoned")
            .slot(&key)
            .is_some_and(|slot| {
                slot.owner.as_ref() == Some(&self.local_node)
                    && matches!(
                        slot.state,
                        PlacementSlotState::Stopping | PlacementSlotState::StopFailed
                    )
            });
        if still_owned {
            Ok(())
        } else {
            Err(RemoteMessageError::StaleAuthority)
        }
    }

    async fn fence(&self, shard_id: ShardId) -> Result<(), RemoteMessageError> {
        let actor_ids = self
            .registry
            .active_actor_ids()
            .into_iter()
            .filter(|actor_id| match actor_id {
                ActorId::Bytes(bytes) => EntityId::new(bytes.clone()).is_ok_and(|entity_id| {
                    self.mapper
                        .shard_for(&entity_id)
                        .is_ok_and(|mapped| mapped == shard_id)
                }),
                ActorId::Str(_) | ActorId::U64(_) | ActorId::I64(_) => false,
            })
            .collect::<Vec<_>>();
        for actor_id in actor_ids {
            match self.registry.fence_after_authority_loss(&actor_id).await {
                Ok(()) | Err(ActorQuarantineError::NotRetained) => {}
                Err(_) => return Err(RemoteMessageError::HandlerFailed),
            }
        }
        Ok(())
    }
}
