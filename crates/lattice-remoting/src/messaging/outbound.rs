use super::codec::{
    AskWire, EntityAskWire, EntityTellWire, SingletonAskWire, SingletonTellWire, TellWire,
    ask_correlation, entity_target_to_wire, set_logical_ask_correlation, singleton_target_to_wire,
    target_to_wire,
};
use super::error::{AskError, RemoteMessageError, TellError};
use super::target::{
    CorrelationId, ExactActorTarget, LogicalEntityTarget, LogicalSingletonTarget, SenderIdentity,
};
use super::{
    ActorRef, Arc, Association, AssociationId, AtomicU64, Bytes, CatalogueDecision, Duration,
    EntityRef, Frame, FrameKind, HashMap, Instant, Mutex, NodeAddress, NodeIncarnation, Ordering,
    ProtocolFingerprint, ProtocolId, SingletonRef, oneshot,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Commitment {
    Queued,
    SocketWriteStarted,
}

struct PendingAsk {
    association_id: AssociationId,
    commitment: Commitment,
    deadline: Instant,
    completion: oneshot::Sender<Result<Bytes, AskError>>,
}

struct PendingState {
    entries: Mutex<HashMap<CorrelationId, PendingAsk>>,
    maximum: usize,
}

pub struct OutboundMessaging {
    boot_id: u128,
    next_correlation: AtomicU64,
    pending: Arc<PendingState>,
}

impl OutboundMessaging {
    pub fn new(maximum_pending_asks: usize) -> Result<Self, RemoteMessageError> {
        if maximum_pending_asks == 0 {
            return Err(RemoteMessageError::ZeroPendingLimit);
        }
        Ok(Self {
            boot_id: uuid::Uuid::new_v4().as_u128(),
            next_correlation: AtomicU64::new(1),
            pending: Arc::new(PendingState {
                entries: Mutex::new(HashMap::new()),
                maximum: maximum_pending_asks,
            }),
        })
    }

    pub fn tell<A>(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: &ActorRef<A>,
        expected_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<usize, TellError> {
        check_protocol(association, target.protocol_id(), expected_fingerprint)
            .map_err(TellError::Protocol)?;
        let target = ExactActorTarget::from(target);
        let sender_bytes = sender.stable_bytes();
        let target_bytes = target.stable_bytes();
        let wire = TellWire {
            sender: sender_bytes.clone(),
            target: Some(target_to_wire(&target)),
            message_id,
            payload: payload.to_vec(),
            sender_actor: sender
                .actor_ref()
                .map(|reference| target_to_wire(&ExactActorTarget::from(reference))),
        };
        association
            .try_admit_bulk(
                &sender_bytes,
                &target_bytes,
                Frame::encode_message(FrameKind::Tell, &wire),
            )
            .map_err(TellError::Association)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn tell_entity<A>(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: &EntityRef<A>,
        owner_address: NodeAddress,
        owner_incarnation: NodeIncarnation,
        assignment_generation: u64,
        expected_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<usize, TellError> {
        check_protocol(association, target.protocol_id(), expected_fingerprint)
            .map_err(TellError::Protocol)?;
        let target = LogicalEntityTarget {
            reference: target.erase(),
            owner_address,
            owner_incarnation,
            assignment_generation,
        };
        let sender_bytes = sender.stable_bytes();
        let recipient_bytes = entity_logical_bytes(&target);
        association
            .try_admit_bulk(
                &sender_bytes,
                &recipient_bytes,
                Frame::encode_message(
                    FrameKind::EntityTell,
                    &EntityTellWire {
                        sender: sender_bytes.clone(),
                        target: Some(entity_target_to_wire(&target)),
                        message_id,
                        payload: payload.to_vec(),
                        sender_actor: sender
                            .actor_ref()
                            .map(|reference| target_to_wire(&ExactActorTarget::from(reference))),
                    },
                ),
            )
            .map_err(TellError::Association)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn tell_singleton<A>(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: &SingletonRef<A>,
        owner_address: NodeAddress,
        owner_incarnation: NodeIncarnation,
        assignment_generation: u64,
        expected_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<usize, TellError> {
        check_protocol(association, target.protocol_id(), expected_fingerprint)
            .map_err(TellError::Protocol)?;
        let target = LogicalSingletonTarget {
            reference: target.erase(),
            owner_address,
            owner_incarnation,
            assignment_generation,
        };
        let sender_bytes = sender.stable_bytes();
        let recipient_bytes = singleton_logical_bytes(&target);
        association
            .try_admit_bulk(
                &sender_bytes,
                &recipient_bytes,
                Frame::encode_message(
                    FrameKind::SingletonTell,
                    &SingletonTellWire {
                        sender: sender_bytes.clone(),
                        target: Some(singleton_target_to_wire(&target)),
                        message_id,
                        payload: payload.to_vec(),
                        sender_actor: sender
                            .actor_ref()
                            .map(|reference| target_to_wire(&ExactActorTarget::from(reference))),
                    },
                ),
            )
            .map_err(TellError::Association)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn ask<A>(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: &ActorRef<A>,
        expected_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(AskError::DeadlineExceeded)?;
        check_protocol(association, target.protocol_id(), expected_fingerprint)
            .map_err(AskError::Protocol)?;
        let correlation = self.next_correlation()?;
        let (completion, receiver) = oneshot::channel();
        {
            let mut entries = self.pending.entries.lock().expect("pending asks poisoned");
            if entries.len() == self.pending.maximum {
                return Err(AskError::PendingLimit);
            }
            entries.insert(
                correlation,
                PendingAsk {
                    association_id: association.id(),
                    commitment: Commitment::Queued,
                    deadline,
                    completion,
                },
            );
        }
        let mut guard = PendingGuard {
            id: correlation,
            pending: self.pending.clone(),
            armed: true,
        };
        let wire = AskWire {
            sender: sender.stable_bytes(),
            target: Some(target_to_wire(&ExactActorTarget::from(target))),
            correlation_id: correlation.to_bytes().to_vec(),
            timeout_nanos: duration_nanos(remaining),
            message_id,
            payload: payload.to_vec(),
        };
        association
            .try_admit_interactive(Frame::encode_message(FrameKind::Ask, &wire))
            .map_err(AskError::from)?;
        let timeout = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(timeout);
        let result = tokio::select! {
            result = receiver => result.unwrap_or(Err(AskError::AssociationLostBeforeWrite)),
            () = &mut timeout => Err(AskError::DeadlineExceeded),
        };
        guard.disarm_and_remove();
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn ask_entity<A>(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: &EntityRef<A>,
        owner_address: NodeAddress,
        owner_incarnation: NodeIncarnation,
        assignment_generation: u64,
        expected_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(AskError::DeadlineExceeded)?;
        check_protocol(association, target.protocol_id(), expected_fingerprint)
            .map_err(AskError::Protocol)?;
        let logical = LogicalEntityTarget {
            reference: target.erase(),
            owner_address,
            owner_incarnation,
            assignment_generation,
        };
        self.enqueue_logical_ask(
            association,
            deadline,
            Frame::encode_message(
                FrameKind::EntityAsk,
                &EntityAskWire {
                    sender: sender.stable_bytes(),
                    target: Some(entity_target_to_wire(&logical)),
                    correlation_id: Vec::new(),
                    timeout_nanos: duration_nanos(remaining),
                    message_id,
                    payload: payload.to_vec(),
                },
            ),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn ask_singleton<A>(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: &SingletonRef<A>,
        owner_address: NodeAddress,
        owner_incarnation: NodeIncarnation,
        assignment_generation: u64,
        expected_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(AskError::DeadlineExceeded)?;
        check_protocol(association, target.protocol_id(), expected_fingerprint)
            .map_err(AskError::Protocol)?;
        let logical = LogicalSingletonTarget {
            reference: target.erase(),
            owner_address,
            owner_incarnation,
            assignment_generation,
        };
        self.enqueue_logical_ask(
            association,
            deadline,
            Frame::encode_message(
                FrameKind::SingletonAsk,
                &SingletonAskWire {
                    sender: sender.stable_bytes(),
                    target: Some(singleton_target_to_wire(&logical)),
                    correlation_id: Vec::new(),
                    timeout_nanos: duration_nanos(remaining),
                    message_id,
                    payload: payload.to_vec(),
                },
            ),
        )
        .await
    }

    async fn enqueue_logical_ask(
        &self,
        association: &Association,
        deadline: Instant,
        mut frame: Frame,
    ) -> Result<Bytes, AskError> {
        let correlation = self.next_correlation()?;
        set_logical_ask_correlation(&mut frame, correlation)?;
        let (completion, receiver) = oneshot::channel();
        {
            let mut entries = self.pending.entries.lock().expect("pending asks poisoned");
            if entries.len() == self.pending.maximum {
                return Err(AskError::PendingLimit);
            }
            entries.insert(
                correlation,
                PendingAsk {
                    association_id: association.id(),
                    commitment: Commitment::Queued,
                    deadline,
                    completion,
                },
            );
        }
        let mut guard = PendingGuard {
            id: correlation,
            pending: self.pending.clone(),
            armed: true,
        };
        association
            .try_admit_interactive(frame)
            .map_err(AskError::from)?;
        let timeout = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(timeout);
        let result = tokio::select! {
            result = receiver => result.unwrap_or(Err(AskError::AssociationLostBeforeWrite)),
            () = &mut timeout => Err(AskError::DeadlineExceeded),
        };
        guard.disarm_and_remove();
        result
    }

    pub fn mark_socket_write_started(&self, correlation: CorrelationId) -> bool {
        let mut entries = self.pending.entries.lock().expect("pending asks poisoned");
        let Some(pending) = entries.get_mut(&correlation) else {
            return false;
        };
        pending.commitment = Commitment::SocketWriteStarted;
        true
    }

    pub fn prepare_ask_for_socket_write(&self, frame: &mut Frame) -> bool {
        if !matches!(
            frame.kind,
            FrameKind::Ask | FrameKind::EntityAsk | FrameKind::SingletonAsk
        ) {
            return true;
        }
        let Some(correlation) = ask_correlation(frame) else {
            return false;
        };
        let deadline = {
            let entries = self.pending.entries.lock().expect("pending asks poisoned");
            let Some(pending) = entries.get(&correlation) else {
                return false;
            };
            pending.deadline
        };
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            self.complete_failure(correlation, AskError::DeadlineExceeded);
            return false;
        };
        if remaining.is_zero() {
            self.complete_failure(correlation, AskError::DeadlineExceeded);
            return false;
        }
        rewrite_timeout_budget(frame, duration_nanos(remaining))
    }

    pub fn complete_reply(&self, correlation: CorrelationId, payload: Bytes) -> bool {
        self.complete(correlation, Ok(payload))
    }

    pub fn complete_failure(&self, correlation: CorrelationId, error: AskError) -> bool {
        self.complete(correlation, Err(error))
    }

    pub fn fail_association(&self, association_id: AssociationId) -> usize {
        let mut entries = self.pending.entries.lock().expect("pending asks poisoned");
        let ids = entries
            .iter()
            .filter_map(|(id, pending)| (pending.association_id == association_id).then_some(*id))
            .collect::<Vec<_>>();
        let count = ids.len();
        for id in ids {
            if let Some(pending) = entries.remove(&id) {
                let error = match pending.commitment {
                    Commitment::Queued => AskError::AssociationLostBeforeWrite,
                    Commitment::SocketWriteStarted => AskError::UnknownResult,
                };
                let _ = pending.completion.send(Err(error));
            }
        }
        count
    }

    pub fn pending_count(&self) -> usize {
        self.pending
            .entries
            .lock()
            .expect("pending asks poisoned")
            .len()
    }

    pub fn pending_correlations(&self) -> Vec<CorrelationId> {
        self.pending
            .entries
            .lock()
            .expect("pending asks poisoned")
            .keys()
            .copied()
            .collect()
    }

    fn complete(&self, correlation: CorrelationId, result: Result<Bytes, AskError>) -> bool {
        let pending = self
            .pending
            .entries
            .lock()
            .expect("pending asks poisoned")
            .remove(&correlation);
        pending.is_some_and(|pending| pending.completion.send(result).is_ok())
    }

    fn next_correlation(&self) -> Result<CorrelationId, AskError> {
        let sequence = self.next_correlation.fetch_add(1, Ordering::Relaxed);
        CorrelationId::new(self.boot_id, sequence).ok_or(AskError::CorrelationExhausted)
    }
}

struct PendingGuard {
    id: CorrelationId,
    pending: Arc<PendingState>,
    armed: bool,
}

impl PendingGuard {
    fn disarm_and_remove(&mut self) {
        self.pending
            .entries
            .lock()
            .expect("pending asks poisoned")
            .remove(&self.id);
        self.armed = false;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pending
                .entries
                .lock()
                .expect("pending asks poisoned")
                .remove(&self.id);
        }
    }
}

fn check_protocol(
    association: &Association,
    protocol_id: ProtocolId,
    expected: ProtocolFingerprint,
) -> Result<(), RemoteMessageError> {
    match association.protocol_decision(protocol_id, expected) {
        CatalogueDecision::Enabled => Ok(()),
        CatalogueDecision::Unsupported => Err(RemoteMessageError::UnsupportedProtocol),
        CatalogueDecision::FingerprintMismatch { .. } => {
            Err(RemoteMessageError::ProtocolFingerprintMismatch)
        }
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn rewrite_timeout_budget(frame: &mut Frame, timeout_nanos: u64) -> bool {
    match frame.kind {
        FrameKind::Ask => frame
            .decode_message::<AskWire>()
            .ok()
            .is_some_and(|mut wire| {
                wire.timeout_nanos = timeout_nanos;
                *frame = Frame::encode_message(FrameKind::Ask, &wire);
                true
            }),
        FrameKind::EntityAsk => {
            frame
                .decode_message::<EntityAskWire>()
                .ok()
                .is_some_and(|mut wire| {
                    wire.timeout_nanos = timeout_nanos;
                    *frame = Frame::encode_message(FrameKind::EntityAsk, &wire);
                    true
                })
        }
        FrameKind::SingletonAsk => {
            frame
                .decode_message::<SingletonAskWire>()
                .ok()
                .is_some_and(|mut wire| {
                    wire.timeout_nanos = timeout_nanos;
                    *frame = Frame::encode_message(FrameKind::SingletonAsk, &wire);
                    true
                })
        }
        _ => false,
    }
}

fn entity_logical_bytes(target: &LogicalEntityTarget) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(target.reference.entity_type().as_str().as_bytes());
    bytes.extend_from_slice(target.reference.entity_id().as_bytes());
    bytes.extend_from_slice(&target.owner_incarnation.get().to_be_bytes());
    bytes.extend_from_slice(&target.assignment_generation.to_be_bytes());
    bytes
}

fn singleton_logical_bytes(target: &LogicalSingletonTarget) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(target.reference.singleton_kind().as_str().as_bytes());
    bytes.extend_from_slice(&target.owner_incarnation.get().to_be_bytes());
    bytes.extend_from_slice(&target.assignment_generation.to_be_bytes());
    bytes
}
