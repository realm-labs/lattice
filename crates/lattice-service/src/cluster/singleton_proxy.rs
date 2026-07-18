use super::singleton::SingletonRoute;
use super::{
    ActorRef, AskError, AssociationKey, AssociationManager, AssociationState, Bytes, Instant,
    LOGICAL_RESOLVE_MESSAGE_ID, LogicPlacementState, LogicalSingletonTarget, Mutex,
    NEXT_LOGICAL_RESOLUTION, NodeKey, Ordering, OutboundMessage, OutboundMessaging, PlacementSlot,
    PlacementSlotKey, PlacementSlotState, ProtocolFingerprint, RemoteMessageError, RouteBuffer,
    SenderIdentity, SingletonConfig, SingletonRef, WatchError, async_trait, decode_resolved_actor,
    map_tell,
};

pub(super) struct SingletonProxyRoute {
    pub local_node: NodeKey,
    pub state: std::sync::Arc<Mutex<LogicPlacementState>>,
    pub associations: std::sync::Arc<AssociationManager>,
    pub peers: Option<std::sync::Arc<super::peers::PeerReconciler>>,
    pub messaging: std::sync::Arc<OutboundMessaging>,
    pub coordinator: AssociationKey,
    pub buffer: RouteBuffer,
    pub config: SingletonConfig,
    pub fingerprint: ProtocolFingerprint,
}

impl SingletonProxyRoute {
    fn slot(&self, target: &SingletonRef) -> Result<PlacementSlot, RemoteMessageError> {
        if target.protocol_id() != self.config.protocol_id
            || target.domain() != &self.config.domain
            || target.config_fingerprint() != self.config.fingerprint()
        {
            return Err(RemoteMessageError::ProtocolFingerprintMismatch);
        }
        self.state
            .lock()
            .expect("logic placement state poisoned")
            .slot(&PlacementSlotKey::Singleton {
                domain: self.config.domain.clone(),
                kind: self.config.kind.clone(),
            })
            .cloned()
            .ok_or(RemoteMessageError::StaleAuthority)
    }

    fn running_slot(&self, target: &SingletonRef) -> Result<PlacementSlot, RemoteMessageError> {
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
            &lattice_core::coordinator::CoordinatorScope::Placement(self.config.domain.clone()),
            &lattice_placement::control::PlacementControlCommand::ResolveSingleton {
                request_id,
                domain: self.config.domain.clone(),
                kind: self.config.kind.clone(),
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
        target: &SingletonRef,
        payload_bytes: usize,
        requested_deadline: Option<Instant>,
    ) -> Result<PlacementSlot, RemoteMessageError> {
        if let Ok(slot) = self.running_slot(target) {
            return Ok(slot);
        }
        let key = PlacementSlotKey::Singleton {
            domain: self.config.domain.clone(),
            kind: self.config.kind.clone(),
        };
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
                return Err(RemoteMessageError::ShardUnavailable);
            }
        }
    }

    async fn remote_association(
        &self,
        target: &SingletonRef,
        owner: &NodeKey,
    ) -> Result<std::sync::Arc<lattice_remoting::association::Association>, RemoteMessageError>
    {
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
        self.peers
            .as_ref()
            .ok_or(RemoteMessageError::StaleAuthority)?
            .connect(owner)
            .await
            .map_err(|_| RemoteMessageError::StaleAuthority)
    }
}

#[async_trait]
impl SingletonRoute for SingletonProxyRoute {
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: SingletonRef,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if fingerprint != self.fingerprint {
            return Err(RemoteMessageError::ProtocolFingerprintMismatch);
        }
        let slot = self
            .await_running_slot(&target, payload.len(), None)
            .await?;
        let owner = slot.owner.ok_or(RemoteMessageError::StaleAuthority)?;
        let association = self.remote_association(&target, &owner).await?;
        let sender = sender
            .as_ref()
            .map(SenderIdentity::from)
            .unwrap_or_else(|| SenderIdentity::Process(self.local_node.incarnation.get()));
        self.messaging
            .tell_singleton(
                &association,
                &sender,
                LogicalSingletonTarget {
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
        target: SingletonRef,
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
        let slot = self
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
            .ask_singleton(
                &association,
                &SenderIdentity::Process(self.local_node.incarnation.get()),
                LogicalSingletonTarget {
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
        _target: LogicalSingletonTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn receive_ask(
        &self,
        _target: LogicalSingletonTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn resolve_current(&self, target: SingletonRef) -> Result<Option<ActorRef>, WatchError> {
        let slot = self.slot(&target).map_err(|_| WatchError::Unavailable)?;
        if slot.state != PlacementSlotState::Running {
            return Ok(None);
        }
        let owner = slot.owner.ok_or(WatchError::Unavailable)?;
        let association = self
            .remote_association(&target, &owner)
            .await
            .map_err(|_| WatchError::Unavailable)?;
        let expected_cluster = target.cluster_id().clone();
        let result = self
            .messaging
            .ask_singleton(
                &association,
                &SenderIdentity::Process(self.local_node.incarnation.get()),
                LogicalSingletonTarget {
                    reference: target,
                    owner_address: owner.address.clone(),
                    owner_incarnation: owner.incarnation,
                    assignment_generation: slot.assignment_generation.get(),
                },
                OutboundMessage::new(self.fingerprint, LOGICAL_RESOLVE_MESSAGE_ID, Bytes::new()),
                Instant::now() + self.buffer.config.maximum_residence,
            )
            .await;
        match result {
            Ok(bytes) => decode_resolved_actor(
                &bytes,
                &expected_cluster,
                &owner.address,
                owner.incarnation,
                self.config.protocol_id,
            )
            .map(Some),
            Err(AskError::Remote(
                lattice_remoting::messaging::error::RemoteFailureCode::StaleActivation,
            )) => Ok(None),
            Err(_) => Err(WatchError::Unavailable),
        }
    }

    async fn receive_resolve(
        &self,
        _target: LogicalSingletonTarget,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn drain(&self) -> Result<bool, RemoteMessageError> {
        Ok(true)
    }
}
