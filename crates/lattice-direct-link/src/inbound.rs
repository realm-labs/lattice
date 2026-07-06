// This module keeps inbound Direct Link router state and its white-box tests
// together while Phase 8 is still assembling stable service/runtime seams.
// Split the tests once inbound backpressure and lifecycle delivery have public
// integration fixtures that do not weaken coverage of private routing behavior.
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lattice_actor::{Actor, ActorHandle, ActorTellError, Handler};
use lattice_core::{
    ActorId, ActorKind, ActorRef, DirectLinkMessageId, LinkBackpressure, LinkCloseReason,
    LinkClosed, LinkDirection, LinkDirectionClosed, LinkId, LinkMessageContext, LinkMessageFlags,
    LinkOpened,
};
use thiserror::Error;

use crate::backpressure::{BackpressureOutcome, BackpressureQueue, BackpressureSnapshot};
use crate::codec::{DirectLinkFrame, DirectLinkFrameKind};
use crate::delivery::{DirectLinkDeliveryError, DirectLinkDispatch};
use crate::session::{
    CloseAllTransition, CloseTransition, DirectLinkPeerIdentity, DirectLinkSessionManager,
    ManagedLinkSnapshot, MessageFrameError, SessionManagerError,
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
        }
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
        match binding.deliver(&actor_ref, message_id, &frame.payload, context) {
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
                self.deliver_link_opened_to_target(&ack.link_id)?;
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
}

impl DirectLinkInboundRouterBuilder {
    pub fn bind_actor<A, Messages, F>(
        mut self,
        binding: DirectLinkActorBinding<A, Messages>,
        resolver: F,
    ) -> Self
    where
        A: Actor,
        Messages: DirectLinkDispatch<A>,
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
        }
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

struct TypedInboundBinding<A, Messages>
where
    A: Actor,
{
    binding: DirectLinkActorBinding<A, Messages>,
    resolver: Arc<ActorResolver<A>>,
    _actor: PhantomData<fn() -> A>,
}

impl<A, Messages> ErasedInboundBinding for TypedInboundBinding<A, Messages>
where
    A: Actor
        + Handler<LinkOpened>
        + Handler<LinkDirectionClosed>
        + Handler<LinkClosed>
        + Handler<LinkBackpressure>,
    Messages: DirectLinkDispatch<A>,
{
    fn deliver(
        &self,
        actor_ref: &ActorRef,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        context: LinkMessageContext,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        self.binding
            .try_deliver(&handle, message_id, payload, context)
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

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use lattice_actor::{
        ActorContext, ActorRuntime, ActorSpawnOptions, ActorTellError, Handler, MailboxConfig,
    };
    use lattice_core::{
        ActorId, ActorKind, ActorRef, BackpressurePolicy, DirectLinkMessage, DirectLinkMode,
        DirectLinkOptions, DirectLinkRuntime, DirectLinkRuntimeHandle, DirectLinkSender,
        DirectLinkSession, DirectLinkStreamDescriptor, DirectLinkStreamType, InstanceId,
        LinkBackpressure, LinkCloseReason, LinkClosed, LinkDirection, LinkDirectionClosed,
        LinkError, LinkId, LinkOpened, LinkSendError, LinkSequence, Linked,
        OutboundDirectLinkMessage, ServiceContext, ServiceKind, actor_kind, service_kind,
    };
    use prost::Message as _;
    use std::time::Instant;

    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::codec::DirectLinkFrame;
    use crate::session::{
        DIRECT_LINK_PROTOCOL_VERSION, DirectLinkActorPolicy, OpenLinkDirection,
        OpenLinkRejectReason, OpenLinkRequest, OpenLinkValidationPolicy,
    };
    use crate::stream::DirectLinkStream;

    #[derive(Clone, PartialEq, prost::Message)]
    struct PositionUpdate {
        #[prost(uint64, tag = "1")]
        tick: u64,
    }

    impl DirectLinkMessage for PositionUpdate {
        const PROTO_FULL_NAME: &'static str = "game.PositionUpdate";
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct InputCommand {
        #[prost(uint64, tag = "1")]
        command_id: u64,
    }

    impl DirectLinkMessage for InputCommand {
        const PROTO_FULL_NAME: &'static str = "game.InputCommand";
    }

    struct BattleActor {
        received: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl lattice_actor::Actor for BattleActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<PositionUpdate>> for BattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Linked<PositionUpdate>,
        ) -> Result<(), Self::Error> {
            self.received
                .lock()
                .expect("received mutex poisoned")
                .push(msg.payload.tick);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<Linked<InputCommand>> for BattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Linked<InputCommand>,
        ) -> Result<(), Self::Error> {
            self.received
                .lock()
                .expect("received mutex poisoned")
                .push(msg.payload.command_id);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkOpened> for BattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkOpened,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkDirectionClosed> for BattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkDirectionClosed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkClosed> for BattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkClosed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkBackpressure> for BattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkBackpressure,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    struct GatewayActor {
        received: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl lattice_actor::Actor for GatewayActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<PositionUpdate>> for GatewayActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Linked<PositionUpdate>,
        ) -> Result<(), Self::Error> {
            self.received
                .lock()
                .expect("received mutex poisoned")
                .push(msg.payload.tick);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkOpened> for GatewayActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkOpened,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkDirectionClosed> for GatewayActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkDirectionClosed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkClosed> for GatewayActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkClosed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkBackpressure> for GatewayActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkBackpressure,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingLinkRuntime {
        outbound_requests: Mutex<Vec<(LinkId, DirectLinkStreamDescriptor)>>,
        sender: Arc<RecordingLinkSender>,
    }

    #[async_trait]
    impl DirectLinkRuntime for RecordingLinkRuntime {
        async fn open_link(
            &self,
            _request: lattice_core::DirectLinkOpenRequest,
        ) -> Result<DirectLinkSession, LinkError> {
            Err(LinkError::Protocol(
                "open_link is not used by this test".to_string(),
            ))
        }

        async fn get_outbound(
            &self,
            link_id: LinkId,
            stream: DirectLinkStreamDescriptor,
        ) -> Result<DirectLinkSession, LinkError> {
            self.outbound_requests
                .lock()
                .expect("outbound requests mutex poisoned")
                .push((link_id.clone(), stream.clone()));
            Ok(DirectLinkSession {
                link_id,
                direction: LinkDirection::TargetToSource,
                accepted_message_ids: stream.accepted_message_ids(),
                stream,
                sender: self.sender.clone(),
            })
        }

        async fn close_all(
            &self,
            _link_id: LinkId,
            _reason: lattice_core::LinkCloseReason,
        ) -> Result<(), LinkError> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingLinkSender {
        sent: Mutex<Vec<OutboundDirectLinkMessage>>,
    }

    #[async_trait]
    impl DirectLinkSender for RecordingLinkSender {
        async fn tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
            self.try_tell(message)
        }

        fn try_tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
            self.sent
                .lock()
                .expect("sent messages mutex poisoned")
                .push(message);
            Ok(())
        }

        async fn close(&self, _reason: lattice_core::LinkCloseReason) -> Result<(), LinkSendError> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct BattleUpdateStream;

    impl DirectLinkStreamType for BattleUpdateStream {
        fn descriptor() -> DirectLinkStreamDescriptor {
            DirectLinkStream::new("battle-update")
                .message::<PositionUpdate>()
                .descriptor()
        }
    }

    struct OpeningBattleActor {
        opened: Arc<Mutex<Vec<LinkOpened>>>,
        outbound: Arc<Mutex<Option<(LinkDirection, String)>>>,
    }

    #[async_trait]
    impl lattice_actor::Actor for OpeningBattleActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<InputCommand>> for OpeningBattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: Linked<InputCommand>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkOpened> for OpeningBattleActor {
        async fn handle(
            &mut self,
            ctx: &mut ActorContext<Self>,
            msg: LinkOpened,
        ) -> Result<(), Self::Error> {
            let outbound = ctx
                .links()
                .get::<BattleUpdateStream>(msg.link_id.clone())
                .await
                .expect("target-to-source link should be available");
            *self
                .outbound
                .lock()
                .expect("outbound handle mutex poisoned") =
                Some((outbound.direction(), outbound.stream().stream_name.clone()));
            self.opened
                .lock()
                .expect("opened messages mutex poisoned")
                .push(msg);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkDirectionClosed> for OpeningBattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkDirectionClosed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkClosed> for OpeningBattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkClosed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkBackpressure> for OpeningBattleActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkBackpressure,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    struct ClosingActor {
        direction_closed: Arc<Mutex<Vec<LinkDirectionClosed>>>,
        link_closed: Arc<Mutex<Vec<LinkClosed>>>,
    }

    #[async_trait]
    impl lattice_actor::Actor for ClosingActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<InputCommand>> for ClosingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: Linked<InputCommand>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<Linked<PositionUpdate>> for ClosingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: Linked<PositionUpdate>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkOpened> for ClosingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkOpened,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkDirectionClosed> for ClosingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: LinkDirectionClosed,
        ) -> Result<(), Self::Error> {
            self.direction_closed
                .lock()
                .expect("direction closed mutex poisoned")
                .push(msg);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkClosed> for ClosingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: LinkClosed,
        ) -> Result<(), Self::Error> {
            self.link_closed
                .lock()
                .expect("link closed mutex poisoned")
                .push(msg);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkBackpressure> for ClosingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkBackpressure,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    struct BackpressureActor {
        received: Arc<Mutex<Vec<u64>>>,
        backpressure: Arc<Mutex<Vec<LinkBackpressure>>>,
        direction_closed: Arc<Mutex<Vec<LinkDirectionClosed>>>,
        link_closed: Arc<Mutex<Vec<LinkClosed>>>,
    }

    #[async_trait]
    impl lattice_actor::Actor for BackpressureActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<InputCommand>> for BackpressureActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Linked<InputCommand>,
        ) -> Result<(), Self::Error> {
            self.received
                .lock()
                .expect("received mutex poisoned")
                .push(msg.payload.command_id);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkOpened> for BackpressureActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkOpened,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkDirectionClosed> for BackpressureActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: LinkDirectionClosed,
        ) -> Result<(), Self::Error> {
            self.direction_closed
                .lock()
                .expect("direction closed mutex poisoned")
                .push(msg);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkClosed> for BackpressureActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: LinkClosed,
        ) -> Result<(), Self::Error> {
            self.link_closed
                .lock()
                .expect("link closed mutex poisoned")
                .push(msg);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkBackpressure> for BackpressureActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: LinkBackpressure,
        ) -> Result<(), Self::Error> {
            self.backpressure
                .lock()
                .expect("backpressure mutex poisoned")
                .push(msg);
            Ok(())
        }
    }

    struct BlockingActor {
        received: Arc<Mutex<Vec<u64>>>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl lattice_actor::Actor for BlockingActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<InputCommand>> for BlockingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Linked<InputCommand>,
        ) -> Result<(), Self::Error> {
            if msg.payload.command_id == 100 {
                self.entered.notify_waiters();
                self.release.notified().await;
            }
            self.received
                .lock()
                .expect("received mutex poisoned")
                .push(msg.payload.command_id);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkOpened> for BlockingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkOpened,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkDirectionClosed> for BlockingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkDirectionClosed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkClosed> for BlockingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkClosed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<LinkBackpressure> for BlockingActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: LinkBackpressure,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn inbound_router_delivers_message_frame_to_target_actor_mailbox() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let handle = ActorRuntime::default()
            .spawn_actor(
                BattleActor {
                    received: received.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let stream = DirectLinkStream::new("movement").message::<PositionUpdate>();
        let descriptor = stream.descriptor();
        let binding = stream.for_actor::<BattleActor>(actor_kind!("Battle"));
        manager
            .register_binding(actor_kind!("Battle"), descriptor.clone())
            .unwrap();
        let link_id = LinkId::new("link-inbound");
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(binding, move |_| Some(handle.clone()))
            .build();
        let message_id = descriptor.message_id_for::<PositionUpdate>().unwrap();
        let frame = DirectLinkFrame::message(
            link_id,
            LinkSequence(1),
            message_id,
            PositionUpdate { tick: 99 }.encode_to_vec(),
        );

        router.deliver_frame(frame).unwrap();

        timeout(Duration::from_secs(1), async {
            loop {
                if !received.lock().expect("received mutex poisoned").is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(*received.lock().expect("received mutex poisoned"), vec![99]);
    }

    #[tokio::test]
    async fn inbound_router_does_not_advance_sequence_when_mailbox_is_full() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handle = ActorRuntime::default()
            .spawn_actor(
                BlockingActor {
                    received: received.clone(),
                    entered: entered.clone(),
                    release: release.clone(),
                },
                ActorSpawnOptions {
                    mailbox: MailboxConfig::bounded(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        handle.try_tell(linked_command(100)).unwrap();
        timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        handle.try_tell(linked_command(101)).unwrap();

        let manager = Arc::new(DirectLinkSessionManager::new());
        let stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let descriptor = stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), descriptor.clone())
            .unwrap();
        let link_id = LinkId::new("link-mailbox-full-sequence");
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap();
        let handle_for_router = handle.clone();
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                stream.for_actor::<BlockingActor>(actor_kind!("Battle")),
                move |_| Some(handle_for_router.clone()),
            )
            .build();
        let message_id = descriptor.message_id_for::<InputCommand>().unwrap();
        let frame = || {
            DirectLinkFrame::message(
                link_id.clone(),
                LinkSequence(1),
                message_id,
                InputCommand { command_id: 11 }.encode_to_vec(),
            )
        };

        assert!(matches!(
            router.deliver_frame(frame()),
            Err(InboundDeliveryError::Delivery(
                DirectLinkDeliveryError::Mailbox(ActorTellError::MailboxFull)
            ))
        ));

        release.notify_waiters();
        wait_for_len(&received, 2).await;

        router.deliver_frame(frame()).unwrap();
        wait_for_len(&received, 3).await;
        assert_eq!(
            *received.lock().expect("received mutex poisoned"),
            vec![100, 101, 11]
        );
    }

    #[tokio::test]
    async fn inbound_router_delivers_bidirectional_frames_to_each_direction_actor() {
        let battle_received = Arc::new(Mutex::new(Vec::new()));
        let gateway_received = Arc::new(Mutex::new(Vec::new()));
        let runtime = ActorRuntime::default();
        let battle_handle = runtime
            .spawn_actor(
                BattleActor {
                    received: battle_received.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let gateway_handle = runtime
            .spawn_actor(
                GatewayActor {
                    received: gateway_received.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
        let input_descriptor = input_stream.descriptor();
        let update_descriptor = update_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        manager
            .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
            .unwrap();
        let link_id = LinkId::new("link-bidirectional");
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Bidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &input_descriptor,
                ),
                target_to_source: Some(OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &update_descriptor,
                )),
                options: DirectLinkOptions::bidirectional(),
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager.clone())
            .bind_actor(
                input_stream.for_actor::<BattleActor>(actor_kind!("Battle")),
                move |_| Some(battle_handle.clone()),
            )
            .bind_actor(
                update_stream.for_actor::<GatewayActor>(actor_kind!("GatewaySession")),
                move |_| Some(gateway_handle.clone()),
            )
            .build();

        router
            .deliver_frame(DirectLinkFrame::message(
                link_id.clone(),
                LinkSequence(1),
                input_descriptor.message_id_for::<InputCommand>().unwrap(),
                InputCommand { command_id: 11 }.encode_to_vec(),
            ))
            .unwrap();
        router
            .deliver_frame(DirectLinkFrame::directed_message(
                link_id.clone(),
                LinkDirection::TargetToSource,
                LinkSequence(1),
                update_descriptor
                    .message_id_for::<PositionUpdate>()
                    .unwrap(),
                PositionUpdate { tick: 22 }.encode_to_vec(),
            ))
            .unwrap();

        timeout(Duration::from_secs(1), async {
            loop {
                let battle_done = !battle_received
                    .lock()
                    .expect("received mutex poisoned")
                    .is_empty();
                let gateway_done = !gateway_received
                    .lock()
                    .expect("received mutex poisoned")
                    .is_empty();
                if battle_done && gateway_done {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            *battle_received.lock().expect("received mutex poisoned"),
            vec![11]
        );
        assert_eq!(
            *gateway_received.lock().expect("received mutex poisoned"),
            vec![22]
        );
        assert_eq!(
            manager.link_snapshot(&link_id).unwrap().directions,
            [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
                .into_iter()
                .collect()
        );
    }

    #[tokio::test]
    async fn inbound_router_delivers_link_opened_and_actor_gets_target_to_source_handle() {
        let opened = Arc::new(Mutex::new(Vec::new()));
        let outbound = Arc::new(Mutex::new(None));
        let runtime = Arc::new(RecordingLinkRuntime::default());
        let mut service =
            ServiceContext::builder(service_kind!("Battle"), InstanceId::new("battle-1"));
        service
            .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
            .unwrap();
        let link_id = LinkId::new("link-opened");
        let target_ref = actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9);
        let handle = ActorRuntime::default()
            .spawn_actor(
                OpeningBattleActor {
                    opened: opened.clone(),
                    outbound: outbound.clone(),
                },
                ActorSpawnOptions {
                    self_ref: Some(target_ref.clone()),
                    service: service.build(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
        let input_descriptor = input_stream.descriptor();
        let update_descriptor = update_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        manager
            .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
            .unwrap();
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: target_ref,
                mode: DirectLinkMode::Bidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &input_descriptor,
                ),
                target_to_source: Some(OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &update_descriptor,
                )),
                options: DirectLinkOptions::bidirectional(),
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                input_stream.for_actor::<OpeningBattleActor>(actor_kind!("Battle")),
                move |_| Some(handle.clone()),
            )
            .build();

        router.deliver_link_opened_to_target(&link_id).unwrap();

        timeout(Duration::from_secs(1), async {
            loop {
                if outbound
                    .lock()
                    .expect("outbound handle mutex poisoned")
                    .is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let opened = opened.lock().expect("opened messages mutex poisoned");
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].mode, DirectLinkMode::Bidirectional);
        assert_eq!(opened[0].inbound_stream, "gateway-input");
        assert_eq!(opened[0].outbound_stream.as_deref(), Some("battle-update"));
        assert_eq!(
            *outbound.lock().expect("outbound handle mutex poisoned"),
            Some((LinkDirection::TargetToSource, "battle-update".to_string()))
        );
        assert_eq!(
            runtime
                .outbound_requests
                .lock()
                .expect("outbound requests mutex poisoned")
                .as_slice(),
            &[(link_id, BattleUpdateStream::descriptor())]
        );
    }

    #[tokio::test]
    async fn process_open_link_frame_returns_ack_and_delivers_link_opened() {
        let opened = Arc::new(Mutex::new(Vec::new()));
        let outbound = Arc::new(Mutex::new(None));
        let runtime = Arc::new(RecordingLinkRuntime::default());
        let mut service =
            ServiceContext::builder(service_kind!("Battle"), InstanceId::new("battle-1"));
        service
            .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
            .unwrap();
        let link_id = LinkId::new("link-open-frame");
        let target_ref = actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9);
        let handle = ActorRuntime::default()
            .spawn_actor(
                OpeningBattleActor {
                    opened: opened.clone(),
                    outbound: outbound.clone(),
                },
                ActorSpawnOptions {
                    self_ref: Some(target_ref.clone()),
                    service: service.build(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
        let input_descriptor = input_stream.descriptor();
        let update_descriptor = update_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        manager
            .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
            .unwrap();
        manager.set_validation_policy(
            OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
                .authorize_sources([service_kind!("Gateway")])
                .require_peer_identity("lattice.test"),
        );
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                input_stream.for_actor::<OpeningBattleActor>(actor_kind!("Battle")),
                move |_| Some(handle.clone()),
            )
            .build();

        let response = router
            .process_open_link_frame(
                DirectLinkFrame::open_link_with_peer_identity(
                    &OpenLinkRequest {
                        protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                        link_id: link_id.clone(),
                        source: actor_ref(
                            service_kind!("Gateway"),
                            actor_kind!("GatewaySession"),
                            7,
                        ),
                        target: target_ref,
                        mode: DirectLinkMode::Bidirectional,
                        source_to_target: OpenLinkDirection::from_stream(
                            link_id.clone(),
                            &input_descriptor,
                        ),
                        target_to_source: Some(OpenLinkDirection::from_stream(
                            link_id.clone(),
                            &update_descriptor,
                        )),
                        options: DirectLinkOptions::bidirectional(),
                    },
                    DirectLinkPeerIdentity::new(
                        service_kind!("Gateway"),
                        InstanceId::new("instance-7"),
                        "spiffe://lattice.test/svc/gateway/instance/instance-7",
                    ),
                )
                .unwrap(),
                None,
            )
            .unwrap();

        assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkAck);
        let ack = response.decode_open_link_ack().unwrap();
        assert_eq!(ack.link_id, link_id);
        assert_eq!(ack.source_to_target.stream_name, "gateway-input");
        assert_eq!(
            ack.target_to_source
                .as_ref()
                .expect("target-to-source negotiation")
                .stream_name,
            "battle-update"
        );
        wait_for_len(&opened, 1).await;
        let opened = opened.lock().expect("opened messages mutex poisoned");
        assert_eq!(opened[0].link_id, link_id);
        assert_eq!(opened[0].mode, DirectLinkMode::Bidirectional);
        assert_eq!(opened[0].inbound_stream, "gateway-input");
        assert_eq!(opened[0].outbound_stream.as_deref(), Some("battle-update"));
        assert_eq!(
            *outbound.lock().expect("outbound handle mutex poisoned"),
            Some((LinkDirection::TargetToSource, "battle-update".to_string()))
        );
    }

    #[tokio::test]
    async fn process_open_link_frame_rejects_missing_required_peer_identity() {
        let manager = Arc::new(DirectLinkSessionManager::new());
        let stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let descriptor = stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), descriptor.clone())
            .unwrap();
        manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::lazy(None));
        manager.set_validation_policy(
            OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
                .authorize_sources([service_kind!("Gateway")])
                .require_peer_identity("lattice.test"),
        );
        let link_id = LinkId::new("link-open-missing-identity");
        let router = DirectLinkInboundRouter::builder(manager).build();

        let response = router
            .process_open_link_frame(
                DirectLinkFrame::open_link(&OpenLinkRequest {
                    protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                    link_id: link_id.clone(),
                    source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                    target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                    mode: DirectLinkMode::Unidirectional,
                    source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                    target_to_source: None,
                    options: DirectLinkOptions::default(),
                })
                .unwrap(),
                None,
            )
            .unwrap();

        assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkReject);
        let reject = response.decode_open_link_reject().unwrap();
        assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);
    }

    #[tokio::test]
    async fn inbound_router_emits_direction_and_link_closed_once_per_transition() {
        let target_direction_closed = Arc::new(Mutex::new(Vec::new()));
        let source_direction_closed = Arc::new(Mutex::new(Vec::new()));
        let target_link_closed = Arc::new(Mutex::new(Vec::new()));
        let source_link_closed = Arc::new(Mutex::new(Vec::new()));
        let runtime = ActorRuntime::default();
        let target_handle = runtime
            .spawn_actor(
                ClosingActor {
                    direction_closed: target_direction_closed.clone(),
                    link_closed: target_link_closed.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let source_handle = runtime
            .spawn_actor(
                ClosingActor {
                    direction_closed: source_direction_closed.clone(),
                    link_closed: source_link_closed.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
        let input_descriptor = input_stream.descriptor();
        let update_descriptor = update_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        manager
            .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
            .unwrap();
        let link_id = LinkId::new("link-close-events");
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Bidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &input_descriptor,
                ),
                target_to_source: Some(OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &update_descriptor,
                )),
                options: DirectLinkOptions::bidirectional(),
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
                move |_| Some(target_handle.clone()),
            )
            .bind_actor(
                update_stream.for_actor::<ClosingActor>(actor_kind!("GatewaySession")),
                move |_| Some(source_handle.clone()),
            )
            .build();

        router
            .close_direction(
                &link_id,
                LinkDirection::SourceToTarget,
                LinkCloseReason::Done,
            )
            .unwrap();
        router
            .close_direction(
                &link_id,
                LinkDirection::SourceToTarget,
                LinkCloseReason::Done,
            )
            .unwrap();
        wait_for_len(&target_direction_closed, 1).await;
        assert_eq!(
            target_direction_closed
                .lock()
                .expect("direction closed mutex poisoned")
                .len(),
            1
        );

        router
            .close_direction(
                &link_id,
                LinkDirection::TargetToSource,
                LinkCloseReason::Done,
            )
            .unwrap();
        router
            .close_direction(
                &link_id,
                LinkDirection::TargetToSource,
                LinkCloseReason::Done,
            )
            .unwrap();
        wait_for_len(&source_direction_closed, 1).await;
        wait_for_len(&target_link_closed, 1).await;
        wait_for_len(&source_link_closed, 1).await;

        assert_eq!(
            source_direction_closed
                .lock()
                .expect("direction closed mutex poisoned")
                .len(),
            1
        );
        assert_eq!(
            target_link_closed
                .lock()
                .expect("link closed mutex poisoned")
                .len(),
            1
        );
        assert_eq!(
            source_link_closed
                .lock()
                .expect("link closed mutex poisoned")
                .len(),
            1
        );
        assert_eq!(
            target_direction_closed
                .lock()
                .expect("direction closed mutex poisoned")[0]
                .stream,
            "gateway-input"
        );
        assert_eq!(
            source_direction_closed
                .lock()
                .expect("direction closed mutex poisoned")[0]
                .stream,
            "battle-update"
        );
    }

    #[tokio::test]
    async fn inbound_router_close_all_emits_structured_reasons_once() {
        for reason in [
            LinkCloseReason::HeartbeatTimeout,
            LinkCloseReason::ProtocolError("invalid sequence".to_string()),
            LinkCloseReason::TargetPassivated,
            LinkCloseReason::TargetMigrating,
            LinkCloseReason::NodeDraining,
            LinkCloseReason::ConnectionLost,
        ] {
            let target_direction_closed = Arc::new(Mutex::new(Vec::new()));
            let source_direction_closed = Arc::new(Mutex::new(Vec::new()));
            let target_link_closed = Arc::new(Mutex::new(Vec::new()));
            let source_link_closed = Arc::new(Mutex::new(Vec::new()));
            let runtime = ActorRuntime::default();
            let target_handle = runtime
                .spawn_actor(
                    ClosingActor {
                        direction_closed: target_direction_closed.clone(),
                        link_closed: target_link_closed.clone(),
                    },
                    Default::default(),
                )
                .await
                .unwrap();
            let source_handle = runtime
                .spawn_actor(
                    ClosingActor {
                        direction_closed: source_direction_closed.clone(),
                        link_closed: source_link_closed.clone(),
                    },
                    Default::default(),
                )
                .await
                .unwrap();
            let manager = Arc::new(DirectLinkSessionManager::new());
            let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
            let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
            let input_descriptor = input_stream.descriptor();
            let update_descriptor = update_stream.descriptor();
            manager
                .register_binding(actor_kind!("Battle"), input_descriptor.clone())
                .unwrap();
            manager
                .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
                .unwrap();
            let link_id = LinkId::new(format!("link-close-all-{reason:?}"));
            manager
                .open_link(OpenLinkRequest {
                    protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                    link_id: link_id.clone(),
                    source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                    target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                    mode: DirectLinkMode::Bidirectional,
                    source_to_target: OpenLinkDirection::from_stream(
                        link_id.clone(),
                        &input_descriptor,
                    ),
                    target_to_source: Some(OpenLinkDirection::from_stream(
                        link_id.clone(),
                        &update_descriptor,
                    )),
                    options: DirectLinkOptions::bidirectional(),
                })
                .unwrap();
            let router = DirectLinkInboundRouter::builder(manager)
                .bind_actor(
                    input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
                    move |_| Some(target_handle.clone()),
                )
                .bind_actor(
                    update_stream.for_actor::<ClosingActor>(actor_kind!("GatewaySession")),
                    move |_| Some(source_handle.clone()),
                )
                .build();

            router.close_all(&link_id, reason.clone()).unwrap();
            router.close_all(&link_id, reason.clone()).unwrap();

            wait_for_len(&target_direction_closed, 1).await;
            wait_for_len(&source_direction_closed, 1).await;
            wait_for_len(&target_link_closed, 1).await;
            wait_for_len(&source_link_closed, 1).await;
            assert_eq!(
                target_direction_closed
                    .lock()
                    .expect("direction closed mutex poisoned")
                    .len(),
                1
            );
            assert_eq!(
                source_direction_closed
                    .lock()
                    .expect("direction closed mutex poisoned")
                    .len(),
                1
            );
            assert_eq!(
                target_link_closed
                    .lock()
                    .expect("link closed mutex poisoned")
                    .as_slice(),
                &[LinkClosed {
                    link_id: link_id.clone(),
                    reason: reason.clone(),
                    closed_directions: [
                        LinkDirection::SourceToTarget,
                        LinkDirection::TargetToSource
                    ]
                    .into_iter()
                    .collect(),
                    last_sequence_seen: None,
                }]
            );
            assert_eq!(
                source_link_closed
                    .lock()
                    .expect("link closed mutex poisoned")
                    .as_slice(),
                &[LinkClosed {
                    link_id,
                    reason,
                    closed_directions: [
                        LinkDirection::SourceToTarget,
                        LinkDirection::TargetToSource
                    ]
                    .into_iter()
                    .collect(),
                    last_sequence_seen: None,
                }]
            );
        }
    }

    #[tokio::test]
    async fn heartbeat_and_ack_refresh_liveness_before_idle_timeout_close() {
        let direction_closed = Arc::new(Mutex::new(Vec::new()));
        let link_closed = Arc::new(Mutex::new(Vec::new()));
        let handle = ActorRuntime::default()
            .spawn_actor(
                ClosingActor {
                    direction_closed: direction_closed.clone(),
                    link_closed: link_closed.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let input_descriptor = input_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        let link_id = LinkId::new("link-heartbeat");
        let mut options = DirectLinkOptions::unidirectional();
        options.idle_timeout = Duration::from_secs(30);
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &input_descriptor,
                ),
                target_to_source: None,
                options,
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
                move |_| Some(handle.clone()),
            )
            .build();
        let heartbeat_at = Instant::now() + Duration::from_secs(10);

        router
            .process_frame_at(DirectLinkFrame::heartbeat(link_id.clone()), heartbeat_at)
            .unwrap();
        assert_eq!(
            router
                .close_idle_links_at(heartbeat_at + Duration::from_secs(29))
                .unwrap(),
            0
        );
        router
            .process_frame_at(
                DirectLinkFrame::heartbeat_ack(link_id.clone()),
                heartbeat_at + Duration::from_secs(29),
            )
            .unwrap();
        assert_eq!(
            router
                .close_idle_links_at(heartbeat_at + Duration::from_secs(58))
                .unwrap(),
            0
        );
        assert_eq!(
            router
                .close_idle_links_at(heartbeat_at + Duration::from_secs(59))
                .unwrap(),
            1
        );

        wait_for_len(&direction_closed, 1).await;
        wait_for_len(&link_closed, 1).await;
        assert_eq!(
            direction_closed
                .lock()
                .expect("direction closed mutex poisoned")[0]
                .reason,
            LinkCloseReason::HeartbeatTimeout
        );
        assert_eq!(
            link_closed.lock().expect("link closed mutex poisoned")[0].reason,
            LinkCloseReason::HeartbeatTimeout
        );
    }

    #[tokio::test]
    async fn process_frame_closes_invalid_message_frames_with_protocol_error() {
        for (name, frame) in [
            ("wrong direction", ProtocolErrorFrame::WrongDirection),
            (
                "unsupported message type",
                ProtocolErrorFrame::UnsupportedMessageType,
            ),
            ("decode error", ProtocolErrorFrame::DecodeError),
        ] {
            let link_id = LinkId::new(format!("link-protocol-error-{name}"));
            let (router, descriptor, received, link_closed) =
                protocol_error_test_router(link_id.clone()).await;
            let message_id = descriptor.message_id_for::<InputCommand>().unwrap();
            let frame = match frame {
                ProtocolErrorFrame::WrongDirection => DirectLinkFrame::directed_message(
                    link_id.clone(),
                    LinkDirection::TargetToSource,
                    LinkSequence(1),
                    message_id,
                    InputCommand { command_id: 11 }.encode_to_vec(),
                ),
                ProtocolErrorFrame::UnsupportedMessageType => DirectLinkFrame::message(
                    link_id.clone(),
                    LinkSequence(1),
                    DirectLinkMessageId(999),
                    InputCommand { command_id: 11 }.encode_to_vec(),
                ),
                ProtocolErrorFrame::DecodeError => DirectLinkFrame::message(
                    link_id.clone(),
                    LinkSequence(1),
                    message_id,
                    b"not protobuf".to_vec(),
                ),
            };

            assert!(router.process_frame(frame).is_err());
            wait_for_len(&link_closed, 1).await;
            assert!(received.lock().expect("received mutex poisoned").is_empty());
            let event = link_closed.lock().expect("link closed mutex poisoned")[0].clone();
            assert_eq!(event.link_id, link_id);
            assert!(matches!(
                event.reason,
                LinkCloseReason::ProtocolError(ref reason) if reason.contains(name)
            ));
        }
    }

    #[tokio::test]
    async fn process_frame_closes_remote_protocol_error_frame() {
        let link_id = LinkId::new("link-remote-protocol-error");
        let (router, _descriptor, _received, link_closed) =
            protocol_error_test_router(link_id.clone()).await;
        let frame = DirectLinkFrame {
            kind: DirectLinkFrameKind::ProtocolError,
            link_id: link_id.clone(),
            sequence: LinkSequence(0),
            message_id: None,
            flags: LinkMessageFlags::EMPTY,
            header: Vec::new(),
            payload: b"remote invalid sequence".to_vec(),
        };

        router.process_frame(frame).unwrap();
        wait_for_len(&link_closed, 1).await;
        let event = link_closed.lock().expect("link closed mutex poisoned")[0].clone();
        assert_eq!(event.link_id, link_id);
        assert_eq!(
            event.reason,
            LinkCloseReason::ProtocolError("remote invalid sequence".to_string())
        );
    }

    #[tokio::test]
    async fn inbound_backpressure_drop_newest_emits_event_without_mailbox_delivery() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let backpressure = Arc::new(Mutex::new(Vec::new()));
        let direction_closed = Arc::new(Mutex::new(Vec::new()));
        let link_closed = Arc::new(Mutex::new(Vec::new()));
        let handle = ActorRuntime::default()
            .spawn_actor(
                BackpressureActor {
                    received: received.clone(),
                    backpressure: backpressure.clone(),
                    direction_closed,
                    link_closed,
                },
                Default::default(),
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let input_descriptor = input_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        let link_id = LinkId::new("link-inbound-drop-newest");
        let mut options = DirectLinkOptions::unidirectional();
        options.backpressure = BackpressurePolicy::DropNewest { max_pending: 0 };
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &input_descriptor,
                ),
                target_to_source: None,
                options,
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager.clone())
            .bind_actor(
                input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
                move |_| Some(handle.clone()),
            )
            .build();

        router
            .deliver_frame(DirectLinkFrame::message(
                link_id.clone(),
                LinkSequence(1),
                input_descriptor.message_id_for::<InputCommand>().unwrap(),
                InputCommand { command_id: 11 }.encode_to_vec(),
            ))
            .unwrap();

        wait_for_len(&backpressure, 1).await;
        assert!(received.lock().expect("received mutex poisoned").is_empty());
        let events = backpressure.lock().expect("backpressure mutex poisoned");
        assert_eq!(events[0].link_id, link_id);
        assert_eq!(
            events[0].policy,
            BackpressurePolicy::DropNewest { max_pending: 0 }
        );
        assert_eq!(events[0].pending, 0);
        assert_eq!(events[0].dropped, 1);
        assert_eq!(manager.metrics().snapshot().dropped, 1);
        assert_eq!(manager.metrics().snapshot().backpressure_events, 1);
    }

    #[tokio::test]
    async fn inbound_backpressure_disconnect_closes_link_with_event() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let backpressure = Arc::new(Mutex::new(Vec::new()));
        let direction_closed = Arc::new(Mutex::new(Vec::new()));
        let link_closed = Arc::new(Mutex::new(Vec::new()));
        let handle = ActorRuntime::default()
            .spawn_actor(
                BackpressureActor {
                    received: received.clone(),
                    backpressure: backpressure.clone(),
                    direction_closed: direction_closed.clone(),
                    link_closed: link_closed.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let input_descriptor = input_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        let link_id = LinkId::new("link-inbound-disconnect");
        let mut options = DirectLinkOptions::unidirectional();
        options.backpressure = BackpressurePolicy::Disconnect { max_pending: 0 };
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &input_descriptor,
                ),
                target_to_source: None,
                options,
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager.clone())
            .bind_actor(
                input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
                move |_| Some(handle.clone()),
            )
            .build();

        assert!(matches!(
            router.deliver_frame(DirectLinkFrame::message(
                link_id.clone(),
                LinkSequence(1),
                input_descriptor.message_id_for::<InputCommand>().unwrap(),
                InputCommand { command_id: 11 }.encode_to_vec(),
            )),
            Err(InboundDeliveryError::BackpressureExceeded)
        ));

        wait_for_len(&backpressure, 1).await;
        wait_for_len(&direction_closed, 1).await;
        wait_for_len(&link_closed, 1).await;
        assert!(received.lock().expect("received mutex poisoned").is_empty());
        assert_eq!(
            direction_closed
                .lock()
                .expect("direction closed mutex poisoned")[0]
                .reason,
            LinkCloseReason::BackpressureExceeded
        );
        assert_eq!(
            link_closed.lock().expect("link closed mutex poisoned")[0].reason,
            LinkCloseReason::BackpressureExceeded
        );
        assert_eq!(manager.metrics().snapshot().closed, 1);
        assert_eq!(manager.metrics().snapshot().backpressure_events, 1);
    }

    #[test]
    fn inbound_router_rejects_unbound_actor_kind() {
        let manager = Arc::new(DirectLinkSessionManager::new());
        let stream = DirectLinkStream::new("movement").message::<PositionUpdate>();
        let descriptor = stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), descriptor.clone())
            .unwrap();
        let link_id = LinkId::new("link-unbound");
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager).build();
        let frame = DirectLinkFrame::message(
            link_id,
            LinkSequence(1),
            descriptor.message_id_for::<PositionUpdate>().unwrap(),
            PositionUpdate { tick: 1 }.encode_to_vec(),
        );

        assert!(matches!(
            router.deliver_frame(frame),
            Err(InboundDeliveryError::UnboundActorKind { .. })
        ));
    }

    enum ProtocolErrorFrame {
        WrongDirection,
        UnsupportedMessageType,
        DecodeError,
    }

    async fn protocol_error_test_router(
        link_id: LinkId,
    ) -> (
        DirectLinkInboundRouter,
        DirectLinkStreamDescriptor,
        Arc<Mutex<Vec<u64>>>,
        Arc<Mutex<Vec<LinkClosed>>>,
    ) {
        let received = Arc::new(Mutex::new(Vec::new()));
        let backpressure = Arc::new(Mutex::new(Vec::new()));
        let direction_closed = Arc::new(Mutex::new(Vec::new()));
        let link_closed = Arc::new(Mutex::new(Vec::new()));
        let handle = ActorRuntime::default()
            .spawn_actor(
                BackpressureActor {
                    received: received.clone(),
                    backpressure,
                    direction_closed,
                    link_closed: link_closed.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let input_descriptor = input_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id, &input_descriptor),
                target_to_source: None,
                options: DirectLinkOptions::unidirectional(),
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
                move |_| Some(handle.clone()),
            )
            .build();

        (router, input_descriptor, received, link_closed)
    }

    fn actor_ref(service_kind: ServiceKind, actor_kind: ActorKind, id: u64) -> ActorRef {
        ActorRef::direct(
            service_kind,
            actor_kind,
            ActorId::U64(id),
            InstanceId::new(format!("instance-{id}")),
            "http://127.0.0.1:10000".parse().unwrap(),
            None,
        )
    }

    fn linked_command(command_id: u64) -> Linked<InputCommand> {
        Linked {
            payload: InputCommand { command_id },
            context: LinkMessageContext {
                link_id: LinkId::new("prefill"),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                sequence: 0,
                received_at: Instant::now(),
                flags: LinkMessageFlags::EMPTY,
            },
        }
    }

    async fn wait_for_len<T>(items: &Arc<Mutex<Vec<T>>>, expected: usize) {
        timeout(Duration::from_secs(1), async {
            loop {
                if items.lock().expect("items mutex poisoned").len() >= expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
