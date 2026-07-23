use std::{cmp::Ordering as CmpOrdering, collections::BinaryHeap};

use tokio::{sync::Notify, time::Instant as TokioInstant};

use super::{
    ActorRef, Arc, Association, AssociationId, AtomicU64, Bytes, CatalogueDecision, Duration,
    Frame, FrameKind, HashMap, Instant, Mutex, Ordering, ProtocolFingerprint, ProtocolId,
    ProtocolTag,
    codec::{AskWire, EntityAskWire, SingletonAskWire},
    encode::{
        PreparedExactTellEnvelope, ask_frame, entity_ask_frame, entity_tell_frame,
        entity_tell_frame_len, prepared_tell_frame, prepared_tell_frame_len, singleton_ask_frame,
        singleton_tell_frame, singleton_tell_frame_len, tell_frame, tell_frame_len,
    },
    error::{AskError, RemoteMessageError, TellError},
    oneshot,
    target::{
        CorrelationId, LogicalEntityTarget, LogicalSingletonTarget, SenderIdentity,
        update_actor_route_hash,
    },
};

const DEADLINE_DRIVER_IDLE_TIMEOUT: Duration = Duration::from_secs(1);
const MAXIMUM_STALE_DEADLINES: usize = 1024;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeadlineEntry {
    deadline: Instant,
    correlation: CorrelationId,
}

impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other.deadline.cmp(&self.deadline).then_with(|| {
            other
                .correlation
                .sequence()
                .cmp(&self.correlation.sequence())
        })
    }
}

impl PartialOrd for DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

struct PendingEntries {
    asks: HashMap<CorrelationId, PendingAsk>,
    deadlines: BinaryHeap<DeadlineEntry>,
    deadline_driver_running: bool,
}

pub(crate) enum PreparedOutboundFrame {
    Other,
    Ask(CorrelationId),
}

struct PendingState {
    entries: Mutex<PendingEntries>,
    deadline_changed: Notify,
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
    envelope: PreparedExactTellEnvelope,
    dictionary_id: Option<u64>,
    registered_epoch: Arc<AtomicU64>,
}

impl PreparedExactTellRoute {
    pub fn association_id(&self) -> AssociationId {
        self.association.id()
    }

    pub fn stripe(&self) -> usize {
        self.stripe
    }

    pub fn tell(&self, message_id: u64, payload: Bytes) -> Result<usize, TellError> {
        self.try_tell_retained(message_id, payload)
            .map_err(|(error, _)| error)
    }

    #[doc(hidden)]
    pub fn try_tell_retained(
        &self,
        message_id: u64,
        payload: Bytes,
    ) -> Result<usize, (TellError, Bytes)> {
        let epoch = self.association.bulk_lane_epoch(self.stripe);
        let compact =
            self.dictionary_id.is_some() && self.registered_epoch.load(Ordering::Acquire) == epoch;
        let frame_len = prepared_tell_frame_len(&self.envelope, message_id, payload.len(), compact);
        let admission = match self
            .association
            .try_reserve_prepared_bulk(self.stripe, frame_len)
        {
            Ok(admission) => admission,
            Err(error) => return Err((TellError::Association(error), payload)),
        };
        admission.send(prepared_tell_frame(
            &self.envelope,
            message_id,
            payload,
            compact,
        ));
        if self.dictionary_id.is_some() && !compact {
            self.registered_epoch.store(epoch, Ordering::Release);
        }
        Ok(self.stripe)
    }

    /// Sends one message, waiting for bounded queue or byte-budget capacity.
    ///
    /// This preserves the same bounded admission limits as [`Self::tell`] but
    /// parks the calling task instead of requiring a retry loop when capacity
    /// is temporarily exhausted. Permanent Association errors are returned
    /// immediately.
    pub async fn tell_wait(&self, message_id: u64, payload: Bytes) -> Result<usize, TellError> {
        let epoch = self.association.bulk_lane_epoch(self.stripe);
        let compact =
            self.dictionary_id.is_some() && self.registered_epoch.load(Ordering::Acquire) == epoch;
        let frame_len = prepared_tell_frame_len(&self.envelope, message_id, payload.len(), compact);
        let admission = self
            .association
            .reserve_prepared_bulk(self.stripe, frame_len)
            .await
            .map_err(TellError::Association)?;
        admission.send(prepared_tell_frame(
            &self.envelope,
            message_id,
            payload,
            compact,
        ));
        if self.dictionary_id.is_some() && !compact {
            self.registered_epoch.store(epoch, Ordering::Release);
        }
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
                entries: Mutex::new(PendingEntries {
                    asks: HashMap::new(),
                    deadlines: BinaryHeap::new(),
                    deadline_driver_running: false,
                }),
                deadline_changed: Notify::new(),
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
        self.try_tell_retained(association, sender, target, message)
            .map_err(|(error, _)| error)
    }

    #[doc(hidden)]
    pub fn try_tell_retained<A: ProtocolTag>(
        &self,
        association: &Association,
        sender: &SenderIdentity,
        target: &ActorRef<A>,
        message: OutboundMessage,
    ) -> Result<usize, (TellError, Bytes)> {
        if let Err(error) = check_protocol(
            association,
            target.protocol_id(),
            message.expected_fingerprint,
        ) {
            return Err((TellError::Protocol(error), message.payload));
        }
        let frame_len = tell_frame_len(
            target,
            sender.actor_ref(),
            message.message_id,
            message.payload.len(),
        );
        let (stripe, admission) = match association.try_reserve_bulk(
            |hasher| {
                sender.update_route_hash(hasher);
                update_actor_route_hash(hasher, target);
            },
            frame_len,
        ) {
            Ok(admission) => admission,
            Err(error) => return Err((TellError::Association(error), message.payload)),
        };
        admission.send(tell_frame(
            target,
            sender.actor_ref(),
            message.message_id,
            message.payload,
        ));
        Ok(stripe)
    }

    #[doc(hidden)]
    pub async fn tell_wait<A: ProtocolTag>(
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
        let frame_len = tell_frame_len(
            target,
            sender.actor_ref(),
            message.message_id,
            message.payload.len(),
        );
        let stripe = association
            .bulk_stripe(|hasher| {
                sender.update_route_hash(hasher);
                update_actor_route_hash(hasher, target);
            })
            .map_err(TellError::Association)?;
        let admission = association
            .reserve_prepared_bulk(stripe, frame_len)
            .await
            .map_err(TellError::Association)?;
        admission.send(tell_frame(
            target,
            sender.actor_ref(),
            message.message_id,
            message.payload,
        ));
        Ok(stripe)
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
        let dictionary_id = association.allocate_exact_target_dictionary_id(stripe);
        Ok(PreparedExactTellRoute {
            association,
            stripe,
            envelope: PreparedExactTellEnvelope::new(target, sender.actor_ref(), dictionary_id),
            dictionary_id,
            registered_epoch: Arc::new(AtomicU64::new(0)),
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
        let frame_len = entity_tell_frame_len(
            &target,
            sender.actor_ref(),
            message.message_id,
            message.payload.len(),
        );
        let (stripe, admission) = association
            .try_reserve_bulk(
                |hasher| {
                    sender.update_route_hash(hasher);
                    target.update_route_hash(hasher);
                },
                frame_len,
            )
            .map_err(TellError::Association)?;
        admission.send(entity_tell_frame(
            &target,
            sender.actor_ref(),
            message.message_id,
            message.payload,
        ));
        Ok(stripe)
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
        let frame_len = singleton_tell_frame_len(
            &target,
            sender.actor_ref(),
            message.message_id,
            message.payload.len(),
        );
        let (stripe, admission) = association
            .try_reserve_bulk(
                |hasher| {
                    sender.update_route_hash(hasher);
                    target.update_route_hash(hasher);
                },
                frame_len,
            )
            .map_err(TellError::Association)?;
        admission.send(singleton_tell_frame(
            &target,
            sender.actor_ref(),
            message.message_id,
            message.payload,
        ));
        Ok(stripe)
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
        self.pending.insert(
            correlation,
            PendingAsk {
                association_id: association.id(),
                commitment: Commitment::Queued,
                deadline,
                completion,
            },
        )?;
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
        let result = receiver
            .await
            .unwrap_or(Err(AskError::AssociationLostBeforeWrite));
        guard.disarm();
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
        self.pending.insert(
            correlation,
            PendingAsk {
                association_id: association.id(),
                commitment: Commitment::Queued,
                deadline,
                completion,
            },
        )?;
        let mut guard = PendingGuard {
            id: correlation,
            pending: self.pending.clone(),
            armed: true,
        };
        let frame = encode_frame(correlation);
        association
            .try_admit_interactive(frame)
            .map_err(AskError::from)?;
        let result = receiver
            .await
            .unwrap_or(Err(AskError::AssociationLostBeforeWrite));
        guard.disarm();
        result
    }

    pub fn mark_socket_write_started(&self, correlation: CorrelationId) -> bool {
        let mut entries = self.pending.entries.lock().expect("pending asks poisoned");
        let Some(pending) = entries.asks.get_mut(&correlation) else {
            return false;
        };
        pending.commitment = Commitment::SocketWriteStarted;
        true
    }

    pub fn prepare_ask_for_socket_write(&self, frame: &mut Frame) -> bool {
        self.prepare_outbound_for_socket_write(frame).is_some()
    }

    pub(crate) fn prepare_outbound_for_socket_write(
        &self,
        frame: &mut Frame,
    ) -> Option<PreparedOutboundFrame> {
        if !matches!(
            frame.kind,
            FrameKind::Ask | FrameKind::EntityAsk | FrameKind::SingletonAsk
        ) {
            return Some(PreparedOutboundFrame::Other);
        }
        let mut ask = DecodedAsk::decode(frame)?;
        let correlation = ask.correlation()?;
        let deadline = {
            let entries = self.pending.entries.lock().expect("pending asks poisoned");
            let pending = entries.asks.get(&correlation)?;
            pending.deadline
        };
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            self.complete_failure(correlation, AskError::DeadlineExceeded);
            return None;
        };
        if remaining.is_zero() {
            self.complete_failure(correlation, AskError::DeadlineExceeded);
            return None;
        }
        ask.set_timeout(duration_nanos(remaining));
        *frame = ask.into_frame();
        Some(PreparedOutboundFrame::Ask(correlation))
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
            .asks
            .iter()
            .filter_map(|(id, pending)| (pending.association_id == association_id).then_some(*id))
            .collect::<Vec<_>>();
        let count = ids.len();
        let failed = ids
            .into_iter()
            .filter_map(|id| entries.asks.remove(&id))
            .collect::<Vec<_>>();
        let became_empty = !failed.is_empty() && entries.asks.is_empty();
        compact_stale_deadlines(&mut entries);
        drop(entries);
        if became_empty {
            self.pending.deadline_changed.notify_one();
        }
        for pending in failed {
            let error = match pending.commitment {
                Commitment::Queued => AskError::AssociationLostBeforeWrite,
                Commitment::SocketWriteStarted => AskError::UnknownResult,
            };
            let _ = pending.completion.send(Err(error));
        }
        count
    }

    pub fn pending_count(&self) -> usize {
        self.pending
            .entries
            .lock()
            .expect("pending asks poisoned")
            .asks
            .len()
    }

    pub(crate) fn has_pending_for_association(&self, association_id: AssociationId) -> bool {
        self.pending
            .entries
            .lock()
            .expect("pending asks poisoned")
            .asks
            .values()
            .any(|pending| pending.association_id == association_id)
    }

    pub fn pending_correlations(&self) -> Vec<CorrelationId> {
        self.pending
            .entries
            .lock()
            .expect("pending asks poisoned")
            .asks
            .keys()
            .copied()
            .collect()
    }

    fn complete(&self, correlation: CorrelationId, result: Result<Bytes, AskError>) -> bool {
        let pending = self.pending.remove(correlation);
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
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pending.remove(self.id);
        }
    }
}

impl PendingState {
    fn insert(
        self: &Arc<Self>,
        correlation: CorrelationId,
        pending: PendingAsk,
    ) -> Result<(), AskError> {
        let deadline = pending.deadline;
        let (start_driver, wake_driver) = {
            let mut entries = self.entries.lock().expect("pending asks poisoned");
            if entries.asks.len() == self.maximum {
                return Err(AskError::PendingLimit);
            }
            let was_empty = entries.asks.is_empty();
            let earlier_deadline = entries
                .deadlines
                .peek()
                .is_some_and(|entry| deadline < entry.deadline);
            entries.asks.insert(correlation, pending);
            entries.deadlines.push(DeadlineEntry {
                deadline,
                correlation,
            });
            let start_driver = !entries.deadline_driver_running;
            entries.deadline_driver_running = true;
            (start_driver, was_empty || earlier_deadline)
        };
        if start_driver {
            tokio::spawn(run_deadline_driver(self.clone()));
        } else if wake_driver {
            self.deadline_changed.notify_one();
        }
        Ok(())
    }

    fn remove(&self, correlation: CorrelationId) -> Option<PendingAsk> {
        let (pending, became_empty) = {
            let mut entries = self.entries.lock().expect("pending asks poisoned");
            let pending = entries.asks.remove(&correlation);
            let became_empty = pending.is_some() && entries.asks.is_empty();
            if pending.is_some() {
                compact_stale_deadlines(&mut entries);
            }
            (pending, became_empty)
        };
        if became_empty {
            self.deadline_changed.notify_one();
        }
        pending
    }
}

fn compact_stale_deadlines(entries: &mut PendingEntries) {
    if entries.asks.is_empty() {
        entries.deadlines.clear();
        return;
    }
    if entries.deadlines.len() <= entries.asks.len().saturating_add(MAXIMUM_STALE_DEADLINES) {
        return;
    }
    let PendingEntries {
        asks, deadlines, ..
    } = entries;
    deadlines.retain(|entry| {
        asks.get(&entry.correlation)
            .is_some_and(|ask| ask.deadline == entry.deadline)
    });
}

async fn run_deadline_driver(pending: Arc<PendingState>) {
    loop {
        let changed = pending.deadline_changed.notified();
        let next_deadline = {
            let mut entries = pending.entries.lock().expect("pending asks poisoned");
            while entries.deadlines.peek().is_some_and(|entry| {
                !entries
                    .asks
                    .get(&entry.correlation)
                    .is_some_and(|ask| ask.deadline == entry.deadline)
            }) {
                entries.deadlines.pop();
            }
            entries.deadlines.peek().map(|entry| entry.deadline)
        };
        let Some(next_deadline) = next_deadline else {
            tokio::select! {
                () = changed => continue,
                () = tokio::time::sleep(DEADLINE_DRIVER_IDLE_TIMEOUT) => {}
            }
            let mut entries = pending.entries.lock().expect("pending asks poisoned");
            if entries.asks.is_empty() {
                entries.deadline_driver_running = false;
                return;
            }
            continue;
        };
        tokio::select! {
            () = changed => continue,
            () = tokio::time::sleep_until(TokioInstant::from_std(next_deadline)) => {}
        }

        let expired = {
            let now = Instant::now();
            let mut entries = pending.entries.lock().expect("pending asks poisoned");
            let mut expired = Vec::new();
            while entries
                .deadlines
                .peek()
                .is_some_and(|entry| entry.deadline <= now)
            {
                let entry = entries.deadlines.pop().expect("deadline was present");
                if entries
                    .asks
                    .get(&entry.correlation)
                    .is_some_and(|ask| ask.deadline == entry.deadline)
                    && let Some(ask) = entries.asks.remove(&entry.correlation)
                {
                    expired.push(ask);
                }
            }
            expired
        };
        for ask in expired {
            let _ = ask.completion.send(Err(AskError::DeadlineExceeded));
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

enum DecodedAsk {
    Exact(AskWire),
    Entity(EntityAskWire),
    Singleton(SingletonAskWire),
}

impl DecodedAsk {
    fn decode(frame: &Frame) -> Option<Self> {
        match frame.kind {
            FrameKind::Ask => frame.decode_message().ok().map(Self::Exact),
            FrameKind::EntityAsk => frame.decode_message().ok().map(Self::Entity),
            FrameKind::SingletonAsk => frame.decode_message().ok().map(Self::Singleton),
            _ => None,
        }
    }

    fn correlation(&self) -> Option<CorrelationId> {
        let bytes = match self {
            Self::Exact(wire) => &wire.correlation_id,
            Self::Entity(wire) => &wire.correlation_id,
            Self::Singleton(wire) => &wire.correlation_id,
        };
        CorrelationId::from_bytes(bytes)
    }

    fn set_timeout(&mut self, timeout_nanos: u64) {
        match self {
            Self::Exact(wire) => wire.timeout_nanos = timeout_nanos,
            Self::Entity(wire) => wire.timeout_nanos = timeout_nanos,
            Self::Singleton(wire) => wire.timeout_nanos = timeout_nanos,
        }
    }

    fn into_frame(self) -> Frame {
        match self {
            Self::Exact(wire) => Frame::encode_message(FrameKind::Ask, &wire),
            Self::Entity(wire) => Frame::encode_message(FrameKind::EntityAsk, &wire),
            Self::Singleton(wire) => Frame::encode_message(FrameKind::SingletonAsk, &wire),
        }
    }
}
