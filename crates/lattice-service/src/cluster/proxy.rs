use std::sync::Arc;

use lattice_core::coordinator::CoordinatorScope;
use lattice_placement::{control::PlacementControlCommand, types::ShardId};
use lattice_remoting::association::Association;

use super::{
    ActorRef, AskError, AssociationKey, AssociationManager, AssociationState, Bytes, EntityConfig,
    EntityRef, Instant, LOGICAL_RESOLVE_MESSAGE_ID, LogicPlacementState, LogicalEntityTarget,
    Mutex, NEXT_LOGICAL_RESOLUTION, NodeKey, Ordering, OutboundMessage, OutboundMessaging,
    PlacementSlot, PlacementSlotKey, PlacementSlotState, ProtocolFingerprint, RemoteMessageError,
    RouteBuffer, SenderIdentity, ShardMapperBinding, WatchError, async_trait,
    decode_resolved_actor, entity::EntityRoute, map_tell, peers::PeerReconciler,
};

pub(super) struct EntityProxyRoute {
    pub(super) local_node: NodeKey,
    pub(super) state: Arc<Mutex<LogicPlacementState>>,
    pub(super) associations: Arc<AssociationManager>,
    pub(super) peers: Option<Arc<PeerReconciler>>,
    pub(super) messaging: Arc<OutboundMessaging>,
    pub(super) coordinator: AssociationKey,
    pub(super) buffer: RouteBuffer,
    pub(super) config: EntityConfig,
    pub(super) mapper: ShardMapperBinding,
    pub(super) fingerprint: ProtocolFingerprint,
}

impl EntityProxyRoute {
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

    fn running_slot(
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
        if slot.state != PlacementSlotState::Running || slot.owner.is_none() {
            return Err(RemoteMessageError::ShardUnavailable);
        }
        Ok((key, slot))
    }

    fn request_resolution(&self, key: &PlacementSlotKey) -> Result<(), RemoteMessageError> {
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
        let sequence = NEXT_LOGICAL_RESOLUTION.fetch_add(1, Ordering::Relaxed);
        let request_id = (self.local_node.incarnation.get() << 64) ^ u128::from(sequence);
        let payload = lattice_placement::control::encode_control_command(
            &CoordinatorScope::Placement(domain.clone()),
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
        if let Ok(slot) = self.running_slot(target) {
            return Ok(slot);
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
                return Err(RemoteMessageError::ShardUnavailable);
            }
        }
    }

    async fn remote_association(
        &self,
        target: &EntityRef,
        owner: &NodeKey,
    ) -> Result<Arc<Association>, RemoteMessageError> {
        if owner == &self.local_node {
            return Err(RemoteMessageError::StaleAuthority);
        }
        if let Some(association) =
            self.associations
                .get_exact(target.cluster_id(), &owner.address, owner.incarnation)
            && association.state() == AssociationState::Active
        {
            return Ok(association);
        }
        let Some(peers) = &self.peers else {
            return Err(RemoteMessageError::StaleAuthority);
        };
        peers.connect(owner).await.map_err(|error| {
            tracing::warn!(
                target: "lattice.cluster.logical",
                %error,
                owner = %owner.node_id,
                "logical route could not establish the owner association"
            );
            RemoteMessageError::StaleAuthority
        })
    }
}

#[async_trait]
impl EntityRoute for EntityProxyRoute {
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: EntityRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if fingerprint != self.fingerprint {
            return Err(RemoteMessageError::ProtocolFingerprintMismatch);
        }
        let (_, slot) = self
            .await_running_slot(&target, payload.len(), None)
            .await?;
        let owner = slot.owner.ok_or(RemoteMessageError::StaleAuthority)?;
        let association = self.remote_association(&target, &owner).await?;
        let sender = sender
            .as_ref()
            .map(SenderIdentity::from)
            .unwrap_or_else(|| SenderIdentity::Process(self.local_node.incarnation.get()));
        self.messaging
            .tell_entity(
                &association,
                &sender,
                LogicalEntityTarget {
                    reference: target,
                    owner_address: owner.address,
                    owner_incarnation: owner.incarnation,
                    assignment_generation: slot.assignment_generation.get(),
                },
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
        if fingerprint != self.fingerprint {
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
        let association = self
            .remote_association(&target, &owner)
            .await
            .map_err(AskError::Protocol)?;
        self.messaging
            .ask_entity(
                &association,
                &SenderIdentity::Process(self.local_node.incarnation.get()),
                LogicalEntityTarget {
                    reference: target,
                    owner_address: owner.address,
                    owner_incarnation: owner.incarnation,
                    assignment_generation: slot.assignment_generation.get(),
                },
                OutboundMessage::new(fingerprint, message_id, payload),
                deadline,
            )
            .await
    }

    async fn receive_tell(
        &self,
        _sender: Option<ActorRef>,
        _target: LogicalEntityTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn receive_ask(
        &self,
        _target: LogicalEntityTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn resolve_current(&self, target: EntityRef) -> Result<Option<ActorRef>, WatchError> {
        let (_, slot) = self
            .running_slot(&target)
            .map_err(|_| WatchError::NotActive)?;
        let owner = slot.owner.ok_or(WatchError::NotActive)?;
        let association = self
            .remote_association(&target, &owner)
            .await
            .map_err(|_| WatchError::NotActive)?;
        let expected_cluster = target.cluster_id().clone();
        let expected_address = owner.address.clone();
        let expected_incarnation = owner.incarnation;
        let result = self
            .messaging
            .ask_entity(
                &association,
                &SenderIdentity::Process(self.local_node.incarnation.get()),
                LogicalEntityTarget {
                    reference: target,
                    owner_address: owner.address,
                    owner_incarnation: owner.incarnation,
                    assignment_generation: slot.assignment_generation.get(),
                },
                OutboundMessage::new(self.fingerprint, LOGICAL_RESOLVE_MESSAGE_ID, Bytes::new()),
                Instant::now() + self.buffer.config.maximum_residence,
            )
            .await
            .map_err(|_| WatchError::NotActive)?;
        decode_resolved_actor(
            &result,
            &expected_cluster,
            &expected_address,
            expected_incarnation,
            self.config.protocol_id,
        )
        .map(Some)
    }

    async fn receive_resolve(
        &self,
        _target: LogicalEntityTarget,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn drain(&self, _shard_id: ShardId) -> Result<bool, RemoteMessageError> {
        Ok(true)
    }
}
