// This module keeps inbound Direct Link router state and its white-box tests
// together while Phase 8 is still assembling stable service/runtime seams.
// Split the tests once inbound backpressure and lifecycle delivery have public
// integration fixtures that do not weaken coverage of private routing behavior.
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lattice_actor::error::ActorTellError;
use lattice_actor::handle::ActorHandle;
use lattice_actor::traits::{Actor, Handler};
use lattice_core::actor_ref::ActorRef;
use lattice_core::direct_link::errors::{LinkError, LinkSendError};
use lattice_core::direct_link::ids::{DirectLinkMessageId, LinkId, LinkSequence};
use lattice_core::direct_link::messages::{
    LinkBackpressure, LinkClosed, LinkDirectionClosed, LinkMessageContext, LinkMessageFlags,
    LinkOpened,
};
use lattice_core::direct_link::options::{LinkCloseReason, LinkDirection};
use lattice_core::direct_link::runtime::{
    DirectLinkSender, DirectLinkSession, OutboundDirectLinkMessage,
};
use lattice_core::direct_link::stream::{DirectLinkMetadata, DirectLinkStreamDescriptor};
use lattice_core::id::ActorId;
use lattice_core::kind::ActorKind;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::backpressure::{BackpressureOutcome, BackpressureQueue, BackpressureSnapshot};
use crate::delivery::{DirectLinkDeliveryError, DirectLinkDispatch};
use crate::protocol::{DirectLinkFrame, DirectLinkFrameKind};
use crate::session::{
    CloseAllTransition, CloseTransition, DirectLinkPeerIdentity, DirectLinkSessionManager,
    ManagedLinkSnapshot, MessageFrameError, OpenLinkReject, OpenLinkRejectReason,
    SessionManagerError,
};
use crate::stream::DirectLinkActorBinding;

#[derive(Debug, Error)]
pub enum InboundDeliveryError {
    #[error("direct-link frame kind is not a message")]
    NotMessageFrame,
    #[error("direct-link message frame is missing a message id")]
    MissingMessageId,
    #[error("direct-link actor kind is not bound: {actor_kind:?}")]
    UnboundActorKind { actor_kind: ActorKind },
    #[error("direct-link target actor is unavailable")]
    ActorUnavailable,
    #[error("direct-link open event is unavailable")]
    LinkOpenUnavailable,
    #[error("direct-link inbound backpressure queue is full")]
    BackpressureFull,
    #[error("direct-link inbound backpressure closed the link")]
    BackpressureExceeded,
    #[error("direct-link handshake failed: {0}")]
    Handshake(String),
    #[error(transparent)]
    Frame(#[from] MessageFrameError),
    #[error(transparent)]
    Session(#[from] SessionManagerError),
    #[error(transparent)]
    Delivery(#[from] DirectLinkDeliveryError),
}

pub struct DirectLinkInboundRouter {
    session_manager: Arc<DirectLinkSessionManager>,
    bindings: HashMap<ActorKind, Box<dyn ErasedInboundBinding>>,
    backpressure: Mutex<HashMap<(LinkId, LinkDirection), BackpressureQueue<DirectLinkMessageId>>>,
    outbound_senders: Mutex<HashMap<LinkId, Arc<dyn DirectLinkSender>>>,
}

impl fmt::Debug for DirectLinkInboundRouter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectLinkInboundRouter")
            .field("binding_count", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

impl DirectLinkInboundRouter {
    pub fn builder(
        session_manager: Arc<DirectLinkSessionManager>,
    ) -> DirectLinkInboundRouterBuilder {
        DirectLinkInboundRouterBuilder {
            session_manager,
            bindings: HashMap::new(),
            backpressure: HashMap::new(),
            outbound_senders: HashMap::new(),
        }
    }

    pub fn register_outbound_sender(&self, link_id: LinkId, sender: Arc<dyn DirectLinkSender>) {
        self.outbound_senders
            .lock()
            .expect("direct-link outbound senders poisoned")
            .insert(link_id, sender);
    }

    pub fn unregister_outbound_sender(&self, link_id: &LinkId) {
        self.outbound_senders
            .lock()
            .expect("direct-link outbound senders poisoned")
            .remove(link_id);
    }

    pub fn outbound_session(
        &self,
        link_id: LinkId,
        stream: DirectLinkStreamDescriptor,
    ) -> Result<DirectLinkSession, LinkError> {
        let sender = self
            .outbound_senders
            .lock()
            .expect("direct-link outbound senders poisoned")
            .get(&link_id)
            .cloned()
            .ok_or(LinkError::Unavailable)?;
        self.session_manager.outbound_session(
            link_id,
            LinkDirection::TargetToSource,
            stream,
            sender,
        )
    }

    pub fn deliver_frame(&self, frame: DirectLinkFrame) -> Result<(), InboundDeliveryError> {
        if frame.kind != DirectLinkFrameKind::Message {
            return Err(InboundDeliveryError::NotMessageFrame);
        }
        let direction = frame.direction();
        let message_id = frame
            .message_id
            .ok_or(InboundDeliveryError::MissingMessageId)?;
        self.session_manager.reserve_message_frame(
            &frame.link_id,
            direction,
            message_id,
            frame.sequence,
        )?;
        let snapshot = self
            .session_manager
            .link_snapshot(&frame.link_id)
            .ok_or(MessageFrameError::UnknownLink)?;
        let actor_ref = actor_for_direction(&snapshot, direction).clone();
        let binding = self.bindings.get(&actor_ref.actor_kind).ok_or_else(|| {
            InboundDeliveryError::UnboundActorKind {
                actor_kind: actor_ref.actor_kind.clone(),
            }
        })?;
        let action = self.apply_inbound_backpressure(&snapshot, direction, message_id)?;
        if action == InboundBackpressureAction::Drop {
            self.session_manager.complete_message_frame(
                &frame.link_id,
                direction,
                message_id,
                frame.sequence,
            )?;
            return Ok(());
        }
        let context = LinkMessageContext {
            link_id: frame.link_id.clone(),
            source: snapshot.source,
            target: snapshot.target,
            sequence: frame.sequence.0,
            received_at: std::time::Instant::now(),
            flags: LinkMessageFlags::from_bits(frame.flags.bits()),
        };
        match binding.deliver(
            &actor_ref,
            message_id,
            &frame.payload,
            &frame.header,
            context,
        ) {
            Ok(()) => {
                self.session_manager.complete_message_frame(
                    &frame.link_id,
                    direction,
                    message_id,
                    frame.sequence,
                )?;
                self.complete_inbound_backpressure(&frame.link_id, direction);
                Ok(())
            }
            Err(error) if is_mailbox_full(&error) => {
                self.complete_inbound_backpressure(&frame.link_id, direction);
                self.emit_inbound_backpressure(&actor_ref, &frame.link_id, direction)?;
                Err(error)
            }
            Err(error) => {
                self.complete_inbound_backpressure(&frame.link_id, direction);
                Err(error)
            }
        }
    }

    pub fn process_frame(&self, frame: DirectLinkFrame) -> Result<(), InboundDeliveryError> {
        self.process_frame_at(frame, Instant::now())
    }

    pub fn process_frame_at(
        &self,
        frame: DirectLinkFrame,
        now: Instant,
    ) -> Result<(), InboundDeliveryError> {
        match frame.kind {
            DirectLinkFrameKind::Message => {
                let link_id = frame.link_id.clone();
                match self.deliver_frame(frame) {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        if let Some(reason) = protocol_error_close_reason(&error) {
                            let _ =
                                self.close_all(&link_id, LinkCloseReason::ProtocolError(reason));
                        }
                        Err(error)
                    }
                }
            }
            DirectLinkFrameKind::Heartbeat | DirectLinkFrameKind::HeartbeatAck => self
                .session_manager
                .record_heartbeat_at(&frame.link_id, now)
                .map_err(Into::into),
            DirectLinkFrameKind::CloseDirection => {
                let direction = frame.direction();
                let reason = frame.decode_close_reason();
                self.close_direction(&frame.link_id, direction, reason)
            }
            DirectLinkFrameKind::Close => {
                let reason = frame.decode_close_reason();
                self.close_all(&frame.link_id, reason)
            }
            DirectLinkFrameKind::ProtocolError => {
                let reason = String::from_utf8(frame.payload)
                    .unwrap_or_else(|_| "remote protocol error".to_string());
                self.close_all(&frame.link_id, LinkCloseReason::ProtocolError(reason))
            }
            _ => Err(InboundDeliveryError::NotMessageFrame),
        }
    }

    pub fn process_open_link_frame(
        &self,
        frame: DirectLinkFrame,
        peer_identity: Option<DirectLinkPeerIdentity>,
    ) -> Result<DirectLinkFrame, InboundDeliveryError> {
        let envelope = frame
            .decode_open_link_envelope()
            .map_err(|error| InboundDeliveryError::Handshake(error.to_string()))?;
        let request = envelope.request;
        let peer_identity = peer_identity.or(envelope.peer_identity);
        match self
            .session_manager
            .open_link_from_peer(request, peer_identity)
        {
            Ok(ack) => {
                if let Err(error) = self.deliver_link_opened_to_target(&ack.link_id) {
                    let _ = self.session_manager.close_all(
                        &ack.link_id,
                        LinkCloseReason::ProtocolError(format!(
                            "open-link delivery failed: {error}"
                        )),
                    );
                    return DirectLinkFrame::open_link_reject(&OpenLinkReject::new(
                        ack.link_id,
                        open_link_delivery_reject_reason(&error),
                    ))
                    .map_err(|error| InboundDeliveryError::Handshake(error.to_string()));
                }
                DirectLinkFrame::open_link_ack(&ack)
                    .map_err(|error| InboundDeliveryError::Handshake(error.to_string()))
            }
            Err(reject) => DirectLinkFrame::open_link_reject(&reject)
                .map_err(|error| InboundDeliveryError::Handshake(error.to_string())),
        }
    }

    pub fn deliver_link_opened_to_target(
        &self,
        link_id: &LinkId,
    ) -> Result<(), InboundDeliveryError> {
        let snapshot = self
            .session_manager
            .link_snapshot(link_id)
            .ok_or(InboundDeliveryError::LinkOpenUnavailable)?;
        let opened = self
            .session_manager
            .link_opened_for_actor(link_id, &snapshot.target)
            .ok_or(InboundDeliveryError::LinkOpenUnavailable)?;
        let binding = self
            .bindings
            .get(&snapshot.target.actor_kind)
            .ok_or_else(|| InboundDeliveryError::UnboundActorKind {
                actor_kind: snapshot.target.actor_kind.clone(),
            })?;
        binding.deliver_link_opened(&snapshot.target, opened)
    }

    pub fn close_direction(
        &self,
        link_id: &LinkId,
        direction: LinkDirection,
        reason: LinkCloseReason,
    ) -> Result<(), InboundDeliveryError> {
        let snapshot = self
            .session_manager
            .link_snapshot(link_id)
            .ok_or(InboundDeliveryError::LinkOpenUnavailable)?;
        match self
            .session_manager
            .close_direction(link_id, direction, reason)?
        {
            CloseTransition::AlreadyClosed => Ok(()),
            CloseTransition::DirectionClosed(event) => {
                let actor_ref = actor_for_direction(&snapshot, event.direction);
                self.deliver_direction_closed(actor_ref, event)
            }
            CloseTransition::LinkClosed {
                direction_closed,
                link_closed,
            } => {
                let actor_ref = actor_for_direction(&snapshot, direction_closed.direction);
                self.deliver_direction_closed(actor_ref, direction_closed)?;
                self.deliver_link_closed_to_bound_actors(&snapshot, link_closed)
            }
        }
    }

    pub fn close_all(
        &self,
        link_id: &LinkId,
        reason: LinkCloseReason,
    ) -> Result<(), InboundDeliveryError> {
        let snapshot = self
            .session_manager
            .link_snapshot(link_id)
            .ok_or(InboundDeliveryError::LinkOpenUnavailable)?;
        match self.session_manager.close_all(link_id, reason)? {
            CloseAllTransition::AlreadyClosed => Ok(()),
            CloseAllTransition::Closed {
                direction_closed,
                link_closed,
            } => {
                for event in direction_closed {
                    let actor_ref = actor_for_direction(&snapshot, event.direction);
                    self.deliver_direction_closed(actor_ref, event)?;
                }
                self.deliver_link_closed_to_bound_actors(&snapshot, link_closed)
            }
        }
    }

    pub fn close_idle_links_at(&self, now: Instant) -> Result<usize, InboundDeliveryError> {
        let snapshots = self.session_manager.idle_link_snapshots_at(now);
        let mut closed = 0;
        for snapshot in snapshots {
            self.close_all(&snapshot.link_id, LinkCloseReason::HeartbeatTimeout)?;
            closed += 1;
        }
        Ok(closed)
    }

    pub fn close_active_links(
        &self,
        reason: LinkCloseReason,
    ) -> Result<usize, InboundDeliveryError> {
        let snapshots = self.session_manager.active_link_snapshots();
        let mut closed = 0;
        for snapshot in snapshots {
            self.close_all(&snapshot.link_id, reason.clone())?;
            closed += 1;
        }
        Ok(closed)
    }

    pub fn close_active_links_for_actor(
        &self,
        actor_kind: &ActorKind,
        actor_id: &ActorId,
        reason: LinkCloseReason,
    ) -> Result<usize, InboundDeliveryError> {
        let snapshots = self.session_manager.active_link_snapshots();
        let mut closed = 0;
        for snapshot in snapshots {
            let matches_source =
                snapshot.source.actor_kind == *actor_kind && snapshot.source.actor_id == *actor_id;
            let matches_target =
                snapshot.target.actor_kind == *actor_kind && snapshot.target.actor_id == *actor_id;
            if matches_source || matches_target {
                self.close_all(&snapshot.link_id, reason.clone())?;
                closed += 1;
            }
        }
        Ok(closed)
    }

    pub fn heartbeat_frames_due_at(&self, now: Instant) -> Vec<DirectLinkFrame> {
        self.session_manager
            .heartbeat_due_link_ids_at(now)
            .into_iter()
            .map(DirectLinkFrame::heartbeat)
            .collect()
    }

    fn deliver_direction_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), InboundDeliveryError> {
        let binding = self.bindings.get(&actor_ref.actor_kind).ok_or_else(|| {
            InboundDeliveryError::UnboundActorKind {
                actor_kind: actor_ref.actor_kind.clone(),
            }
        })?;
        binding.deliver_direction_closed(actor_ref, event)
    }

    fn deliver_link_closed_to_bound_actors(
        &self,
        snapshot: &ManagedLinkSnapshot,
        event: LinkClosed,
    ) -> Result<(), InboundDeliveryError> {
        for actor_ref in [&snapshot.source, &snapshot.target] {
            if let Some(binding) = self.bindings.get(&actor_ref.actor_kind) {
                binding.deliver_link_closed(actor_ref, event.clone())?;
            }
        }
        Ok(())
    }

    pub fn deliver_direction_closed_to_actor(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), InboundDeliveryError> {
        self.deliver_direction_closed(actor_ref, event)
    }

    pub fn deliver_link_closed_to_actor(
        &self,
        actor_ref: &ActorRef,
        event: LinkClosed,
    ) -> Result<(), InboundDeliveryError> {
        let binding = self.bindings.get(&actor_ref.actor_kind).ok_or_else(|| {
            InboundDeliveryError::UnboundActorKind {
                actor_kind: actor_ref.actor_kind.clone(),
            }
        })?;
        binding.deliver_link_closed(actor_ref, event)
    }

    fn apply_inbound_backpressure(
        &self,
        snapshot: &ManagedLinkSnapshot,
        direction: LinkDirection,
        message_id: DirectLinkMessageId,
    ) -> Result<InboundBackpressureAction, InboundDeliveryError> {
        let policy = self
            .session_manager
            .backpressure_policy(&snapshot.link_id, direction)
            .ok_or(MessageFrameError::WrongDirection)?;
        let (outcome, state) = {
            let mut states = self
                .backpressure
                .lock()
                .expect("inbound backpressure states poisoned");
            let state = states
                .entry((snapshot.link_id.clone(), direction))
                .or_insert_with(|| BackpressureQueue::new(policy));
            let outcome = state.try_enqueue(message_id);
            (outcome, state.snapshot())
        };

        match outcome {
            BackpressureOutcome::Enqueued => Ok(InboundBackpressureAction::Deliver),
            BackpressureOutcome::WouldBlock(_) | BackpressureOutcome::Rejected(_) => {
                self.record_and_emit_backpressure(snapshot, direction, &state)?;
                Err(InboundDeliveryError::BackpressureFull)
            }
            BackpressureOutcome::DroppedNewest(dropped)
            | BackpressureOutcome::DroppedOldest(dropped) => {
                self.session_manager.record_drop(&snapshot.link_id, dropped);
                self.record_and_emit_backpressure(snapshot, direction, &state)?;
                Ok(InboundBackpressureAction::Drop)
            }
            BackpressureOutcome::Coalesced(coalesced) => {
                self.session_manager
                    .record_coalesce(&snapshot.link_id, coalesced);
                self.record_and_emit_backpressure(snapshot, direction, &state)?;
                Ok(InboundBackpressureAction::Deliver)
            }
            BackpressureOutcome::Disconnect(_) => {
                self.record_and_emit_backpressure(snapshot, direction, &state)?;
                self.close_direction(
                    &snapshot.link_id,
                    direction,
                    LinkCloseReason::BackpressureExceeded,
                )?;
                Err(InboundDeliveryError::BackpressureExceeded)
            }
        }
    }

    fn complete_inbound_backpressure(&self, link_id: &LinkId, direction: LinkDirection) {
        if let Some(state) = self
            .backpressure
            .lock()
            .expect("inbound backpressure states poisoned")
            .get_mut(&(link_id.clone(), direction))
        {
            let _ = state.pop_front();
        }
    }

    fn emit_inbound_backpressure(
        &self,
        actor_ref: &ActorRef,
        link_id: &LinkId,
        direction: LinkDirection,
    ) -> Result<(), InboundDeliveryError> {
        let state = self
            .backpressure
            .lock()
            .expect("inbound backpressure states poisoned")
            .get(&(link_id.clone(), direction))
            .map(BackpressureQueue::snapshot);
        if let Some(state) = state {
            self.record_and_emit_backpressure_for_actor(actor_ref, link_id, &state)?;
        }
        Ok(())
    }

    fn record_and_emit_backpressure(
        &self,
        snapshot: &ManagedLinkSnapshot,
        direction: LinkDirection,
        state: &BackpressureSnapshot,
    ) -> Result<(), InboundDeliveryError> {
        let actor_ref = actor_for_direction(snapshot, direction);
        self.record_and_emit_backpressure_for_actor(actor_ref, &snapshot.link_id, state)
    }

    fn record_and_emit_backpressure_for_actor(
        &self,
        actor_ref: &ActorRef,
        link_id: &LinkId,
        state: &BackpressureSnapshot,
    ) -> Result<(), InboundDeliveryError> {
        self.session_manager
            .record_backpressure(link_id, &state.policy, state.pending);
        let binding = self.bindings.get(&actor_ref.actor_kind).ok_or_else(|| {
            InboundDeliveryError::UnboundActorKind {
                actor_kind: actor_ref.actor_kind.clone(),
            }
        })?;
        binding.deliver_backpressure(
            actor_ref,
            LinkBackpressure {
                link_id: link_id.clone(),
                policy: state.policy.clone(),
                pending: state.pending,
                dropped: state.dropped,
                coalesced: state.coalesced,
            },
        )
    }
}

pub struct DirectLinkInboundRouterBuilder {
    session_manager: Arc<DirectLinkSessionManager>,
    bindings: HashMap<ActorKind, Box<dyn ErasedInboundBinding>>,
    backpressure: HashMap<(LinkId, LinkDirection), BackpressureQueue<DirectLinkMessageId>>,
    outbound_senders: HashMap<LinkId, Arc<dyn DirectLinkSender>>,
}

impl DirectLinkInboundRouterBuilder {
    pub fn bind_actor<A, Messages, Metadata, F>(
        mut self,
        binding: DirectLinkActorBinding<A, Messages, Metadata>,
        resolver: F,
    ) -> Self
    where
        A: Actor,
        Metadata: DirectLinkMetadata,
        Messages: DirectLinkDispatch<A, Metadata>,
        A: Handler<LinkOpened>
            + Handler<LinkDirectionClosed>
            + Handler<LinkClosed>
            + Handler<LinkBackpressure>,
        F: Fn(&ActorRef) -> Option<ActorHandle<A>> + Send + Sync + 'static,
    {
        self.bindings.insert(
            binding.actor_kind().clone(),
            Box::new(TypedInboundBinding {
                binding,
                resolver: Arc::new(resolver),
                _actor: PhantomData,
            }),
        );
        self
    }

    pub fn build(self) -> DirectLinkInboundRouter {
        DirectLinkInboundRouter {
            session_manager: self.session_manager,
            bindings: self.bindings,
            backpressure: Mutex::new(self.backpressure),
            outbound_senders: Mutex::new(self.outbound_senders),
        }
    }
}

#[derive(Debug)]
pub struct InboundConnectionSender {
    link_id: LinkId,
    tx: mpsc::Sender<DirectLinkFrame>,
    next_sequence: Mutex<u64>,
    closed: AtomicBool,
}

impl InboundConnectionSender {
    pub fn new(link_id: LinkId, tx: mpsc::Sender<DirectLinkFrame>) -> Self {
        Self {
            link_id,
            tx,
            next_sequence: Mutex::new(1),
            closed: AtomicBool::new(false),
        }
    }

    fn next_message_frame(
        &self,
        message: OutboundDirectLinkMessage,
    ) -> Result<DirectLinkFrame, LinkSendError> {
        if message.direction != LinkDirection::TargetToSource {
            return Err(LinkSendError::Protocol(
                "inbound direct-link sender only supports TargetToSource messages".to_string(),
            ));
        }
        if message.link_id != self.link_id {
            return Err(LinkSendError::Protocol(format!(
                "direct-link sender for {} cannot send frame for {}",
                self.link_id, message.link_id
            )));
        }
        let mut next_sequence = self
            .next_sequence
            .lock()
            .expect("direct-link inbound sender sequence poisoned");
        let frame = DirectLinkFrame::directed_message_with_header(
            message.link_id,
            message.direction,
            LinkSequence(*next_sequence),
            message.message_id,
            message.metadata,
            message.payload,
        );
        *next_sequence += 1;
        Ok(frame)
    }
}

#[async_trait::async_trait]
impl DirectLinkSender for InboundConnectionSender {
    async fn tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(LinkSendError::Closed {
                reason: LinkCloseReason::Done,
            });
        }
        let frame = self.next_message_frame(message)?;
        self.tx
            .send(frame)
            .await
            .map_err(|_| LinkSendError::BackpressureFull)
    }

    fn try_tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(LinkSendError::Closed {
                reason: LinkCloseReason::Done,
            });
        }
        let frame = self.next_message_frame(message)?;
        self.tx
            .try_send(frame)
            .map_err(|_| LinkSendError::BackpressureFull)
    }

    async fn close(&self, _reason: LinkCloseReason) -> Result<(), LinkSendError> {
        self.closed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InboundBackpressureAction {
    Deliver,
    Drop,
}

trait ErasedInboundBinding: Send + Sync + 'static {
    fn deliver(
        &self,
        actor_ref: &ActorRef,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        metadata: &[u8],
        context: LinkMessageContext,
    ) -> Result<(), InboundDeliveryError>;

    fn deliver_link_opened(
        &self,
        actor_ref: &ActorRef,
        opened: LinkOpened,
    ) -> Result<(), InboundDeliveryError>;

    fn deliver_direction_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), InboundDeliveryError>;

    fn deliver_link_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkClosed,
    ) -> Result<(), InboundDeliveryError>;

    fn deliver_backpressure(
        &self,
        actor_ref: &ActorRef,
        event: LinkBackpressure,
    ) -> Result<(), InboundDeliveryError>;
}

type ActorResolver<A> = dyn Fn(&ActorRef) -> Option<ActorHandle<A>> + Send + Sync;

struct TypedInboundBinding<A, Messages, Metadata>
where
    A: Actor,
{
    binding: DirectLinkActorBinding<A, Messages, Metadata>,
    resolver: Arc<ActorResolver<A>>,
    _actor: PhantomData<fn() -> (A, Metadata)>,
}

impl<A, Messages, Metadata> ErasedInboundBinding for TypedInboundBinding<A, Messages, Metadata>
where
    A: Actor
        + Handler<LinkOpened>
        + Handler<LinkDirectionClosed>
        + Handler<LinkClosed>
        + Handler<LinkBackpressure>,
    Metadata: DirectLinkMetadata,
    Messages: DirectLinkDispatch<A, Metadata>,
{
    fn deliver(
        &self,
        actor_ref: &ActorRef,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        metadata: &[u8],
        context: LinkMessageContext,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        self.binding
            .try_deliver(&handle, message_id, payload, metadata, context)
            .map_err(Into::into)
    }

    fn deliver_link_opened(
        &self,
        actor_ref: &ActorRef,
        opened: LinkOpened,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        handle
            .try_tell(opened)
            .map_err(DirectLinkDeliveryError::from)
            .map_err(Into::into)
    }

    fn deliver_direction_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        handle
            .try_tell(event)
            .map_err(DirectLinkDeliveryError::from)
            .map_err(Into::into)
    }

    fn deliver_link_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkClosed,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        handle
            .try_tell(event)
            .map_err(DirectLinkDeliveryError::from)
            .map_err(Into::into)
    }

    fn deliver_backpressure(
        &self,
        actor_ref: &ActorRef,
        event: LinkBackpressure,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        handle
            .try_tell(event)
            .map_err(DirectLinkDeliveryError::from)
            .map_err(Into::into)
    }
}

fn actor_for_direction(snapshot: &ManagedLinkSnapshot, direction: LinkDirection) -> &ActorRef {
    match direction {
        LinkDirection::SourceToTarget => &snapshot.target,
        LinkDirection::TargetToSource => &snapshot.source,
    }
}

fn is_mailbox_full(error: &InboundDeliveryError) -> bool {
    matches!(
        error,
        InboundDeliveryError::Delivery(DirectLinkDeliveryError::Mailbox(
            ActorTellError::MailboxFull
        ))
    )
}

fn protocol_error_close_reason(error: &InboundDeliveryError) -> Option<String> {
    match error {
        InboundDeliveryError::Frame(MessageFrameError::UnknownLink) => {
            Some("unknown link".to_string())
        }
        InboundDeliveryError::Frame(MessageFrameError::WrongDirection) => {
            Some("wrong direction".to_string())
        }
        InboundDeliveryError::Frame(MessageFrameError::Closed) => Some("link closed".to_string()),
        InboundDeliveryError::Frame(MessageFrameError::UnsupportedMessageType) => {
            Some("unsupported message type".to_string())
        }
        InboundDeliveryError::Frame(MessageFrameError::NonActivatableTarget) => {
            Some("non-activatable target".to_string())
        }
        InboundDeliveryError::Frame(MessageFrameError::RateLimited) => {
            Some("message rate limit exceeded".to_string())
        }
        InboundDeliveryError::Frame(MessageFrameError::DecodeError(error)) => {
            Some(format!("decode error: {error}"))
        }
        InboundDeliveryError::Frame(MessageFrameError::InvalidSequence { expected, actual }) => {
            Some(format!(
                "invalid sequence: expected {expected:?}, actual {actual:?}"
            ))
        }
        InboundDeliveryError::Delivery(DirectLinkDeliveryError::UnsupportedMessageType) => {
            Some("unsupported message type".to_string())
        }
        InboundDeliveryError::Delivery(DirectLinkDeliveryError::Decode(error)) => {
            Some(format!("decode error: {error}"))
        }
        _ => None,
    }
}

fn open_link_delivery_reject_reason(error: &InboundDeliveryError) -> OpenLinkRejectReason {
    match error {
        InboundDeliveryError::ActorUnavailable
        | InboundDeliveryError::LinkOpenUnavailable
        | InboundDeliveryError::UnboundActorKind { .. } => OpenLinkRejectReason::ActorUnavailable,
        InboundDeliveryError::Delivery(DirectLinkDeliveryError::Mailbox(
            ActorTellError::MailboxFull,
        ))
        | InboundDeliveryError::BackpressureFull
        | InboundDeliveryError::BackpressureExceeded => OpenLinkRejectReason::Overloaded,
        _ => OpenLinkRejectReason::ActorUnavailable,
    }
}

#[cfg(test)]
mod tests;
