use tokio::time::Instant as TokioInstant;

use super::{
    ActorRef, Arc, Association, AssociationId, AtomicU64, Bytes, CatalogueDecision, Duration,
    Frame, FrameKind, HashMap, Instant, Mutex, Ordering, ProtocolFingerprint, ProtocolId,
    ProtocolTag,
    codec::{AskWire, EntityAskWire, SingletonAskWire, ask_correlation},
    encode::{
        PreparedExactTarget, ask_frame, entity_ask_frame, entity_tell_frame, prepared_tell_frame,
        singleton_ask_frame, singleton_tell_frame, tell_frame,
    },
    error::{AskError, RemoteMessageError, TellError},
    oneshot,
    target::{
        CorrelationId, LogicalEntityTarget, LogicalSingletonTarget, SenderIdentity,
        update_actor_route_hash,
    },
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

/// A stable exact-actor bulk-tell route bound to one Association generation.
///
/// Protocol compatibility, target encoding, sender encoding, and bulk stripe
/// selection are completed during preparation. If the bound Association is
/// replaced or closes, admission fails and callers must prepare a new route.
#[derive(Debug, Clone)]
pub struct PreparedExactTellRoute {
    association: Arc<Association>,
    stripe: usize,
    target: PreparedExactTarget,
    sender_actor: Option<PreparedExactTarget>,
}

impl PreparedExactTellRoute {
    pub fn association_id(&self) -> AssociationId {
        self.association.id()
    }

    pub fn stripe(&self) -> usize {
        self.stripe
    }

    pub fn tell(&self, message_id: u64, payload: Bytes) -> Result<usize, TellError> {
        self.association
            .try_admit_prepared_bulk(
                self.stripe,
                prepared_tell_frame(
                    &self.target,
                    self.sender_actor.as_ref(),
                    message_id,
                    payload,
                ),
            )
            .map_err(TellError::Association)?;
        Ok(self.stripe)
    }
}

/// Encoded protocol message data shared by exact and logical outbound routes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    expected_fingerprint: ProtocolFingerprint,
    message_id: u64,
    payload: Bytes,
}

impl OutboundMessage {
    pub fn new(expected_fingerprint: ProtocolFingerprint, message_id: u64, payload: Bytes) -> Self {
        Self {
            expected_fingerprint,
            message_id,
            payload,
        }
    }
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

    pub fn tell<A: ProtocolTag>(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: &ActorRef<A>,
        message: OutboundMessage,
    ) -> Result<usize, TellError> {
        check_protocol(
            association,
            target.protocol_id(),
            message.expected_fingerprint,
        )
        .map_err(TellError::Protocol)?;
        let frame = tell_frame(
            target,
            sender.actor_ref(),
            message.message_id,
            message.payload,
        );
        association
            .try_admit_bulk(
                |hasher| {
                    sender.update_route_hash(hasher);
                    update_actor_route_hash(hasher, target);
                },
                frame,
            )
            .map_err(TellError::Association)
    }

    /// Prepares a stable exact-actor tell route for a hot send loop.
    ///
    /// Preparation validates the immutable peer protocol catalogue and caches
    /// the encoded target, optional actor sender, and selected bulk stripe.
    /// The returned route is bound to `association`; callers prepare another
    /// route after that Association closes or is replaced.
    pub fn prepare_exact_tell_route<A: ProtocolTag>(
        &self,
        association: Arc<Association>,
        sender: &SenderIdentity,
        target: &ActorRef<A>,
        expected_fingerprint: ProtocolFingerprint,
    ) -> Result<PreparedExactTellRoute, TellError> {
        check_protocol(&association, target.protocol_id(), expected_fingerprint)
            .map_err(TellError::Protocol)?;
        let stripe = association
            .bulk_stripe(|hasher| {
                sender.update_route_hash(hasher);
                update_actor_route_hash(hasher, target);
            })
            .map_err(TellError::Association)?;
        Ok(PreparedExactTellRoute {
            association,
            stripe,
            target: PreparedExactTarget::new(target),
            sender_actor: sender.actor_ref().map(PreparedExactTarget::new),
        })
    }

    pub fn tell_entity(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: LogicalEntityTarget,
        message: OutboundMessage,
    ) -> Result<usize, TellError> {
        check_protocol(
            association,
            target.reference.protocol_id(),
            message.expected_fingerprint,
        )
        .map_err(TellError::Protocol)?;
        association
            .try_admit_bulk(
                |hasher| {
                    sender.update_route_hash(hasher);
                    target.update_route_hash(hasher);
                },
                entity_tell_frame(
                    &target,
                    sender.actor_ref(),
                    message.message_id,
                    message.payload,
                ),
            )
            .map_err(TellError::Association)
    }

    pub fn tell_singleton(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: LogicalSingletonTarget,
        message: OutboundMessage,
    ) -> Result<usize, TellError> {
        check_protocol(
            association,
            target.reference.protocol_id(),
            message.expected_fingerprint,
        )
        .map_err(TellError::Protocol)?;
        association
            .try_admit_bulk(
                |hasher| {
                    sender.update_route_hash(hasher);
                    target.update_route_hash(hasher);
                },
                singleton_tell_frame(
                    &target,
                    sender.actor_ref(),
                    message.message_id,
                    message.payload,
                ),
            )
            .map_err(TellError::Association)
    }

    pub async fn ask<A: ProtocolTag>(
        &self,
        association: &Association,
        _sender: &SenderIdentity,
        target: &ActorRef<A>,
        message: OutboundMessage,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(AskError::DeadlineExceeded)?;
        check_protocol(
            association,
            target.protocol_id(),
            message.expected_fingerprint,
        )
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
        association
            .try_admit_interactive(ask_frame(
                target,
                correlation,
                duration_nanos(remaining),
                message.message_id,
                message.payload,
            ))
            .map_err(AskError::from)?;
        let timeout = tokio::time::sleep_until(TokioInstant::from_std(deadline));
        tokio::pin!(timeout);
        let result = tokio::select! {
            result = receiver => result.unwrap_or(Err(AskError::AssociationLostBeforeWrite)),
            () = &mut timeout => Err(AskError::DeadlineExceeded),
        };
        guard.disarm_and_remove();
        result
    }

    pub async fn ask_entity(
        &self,
        association: &Association,
        _sender: &SenderIdentity,
        target: LogicalEntityTarget,
        message: OutboundMessage,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(AskError::DeadlineExceeded)?;
        check_protocol(
            association,
            target.reference.protocol_id(),
            message.expected_fingerprint,
        )
        .map_err(AskError::Protocol)?;
        self.enqueue_logical_ask(association, deadline, |correlation| {
            entity_ask_frame(
                &target,
                correlation,
                duration_nanos(remaining),
                message.message_id,
                message.payload,
            )
        })
        .await
    }

    pub async fn ask_singleton(
        &self,
        association: &Association,
        _sender: &SenderIdentity,
        target: LogicalSingletonTarget,
        message: OutboundMessage,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(AskError::DeadlineExceeded)?;
        check_protocol(
            association,
            target.reference.protocol_id(),
            message.expected_fingerprint,
        )
        .map_err(AskError::Protocol)?;
        self.enqueue_logical_ask(association, deadline, |correlation| {
            singleton_ask_frame(
                &target,
                correlation,
                duration_nanos(remaining),
                message.message_id,
                message.payload,
            )
        })
        .await
    }

    async fn enqueue_logical_ask<F>(
        &self,
        association: &Association,
        deadline: Instant,
        encode_frame: F,
    ) -> Result<Bytes, AskError>
    where
        F: FnOnce(CorrelationId) -> Frame,
    {
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
        let frame = encode_frame(correlation);
        association
            .try_admit_interactive(frame)
            .map_err(AskError::from)?;
        let timeout = tokio::time::sleep_until(TokioInstant::from_std(deadline));
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

    pub(crate) fn has_pending_for_association(&self, association_id: AssociationId) -> bool {
        self.pending
            .entries
            .lock()
            .expect("pending asks poisoned")
            .values()
            .any(|pending| pending.association_id == association_id)
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
