use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// This module currently keeps Direct Link session state and its white-box tests
// together because the Phase 8 runtime surface is still being assembled. Split
// the tests into integration fixtures once mailbox delivery and service wiring
// expose stable public seams.
use lattice_core::{
    ActorKind, ActorRef, ActorRefTarget, BackpressurePolicy, DirectLinkMessage,
    DirectLinkMessageId, DirectLinkMode, DirectLinkOptions, DirectLinkSession,
    DirectLinkStreamDescriptor, Epoch, LinkCloseReason, LinkClosed, LinkDirection,
    LinkDirectionClosed, LinkError, LinkId, LinkOpened, LinkSequence, ServiceKind,
};
use thiserror::Error;

#[derive(Debug, Default, Clone)]
pub struct DirectLinkMetrics {
    inner: Arc<Mutex<DirectLinkMetricsInner>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DirectLinkMetricsSnapshot {
    pub opened: u64,
    pub closed: u64,
    pub sent: u64,
    pub received: u64,
    pub protocol_errors: u64,
    pub dropped: u64,
    pub coalesced: u64,
    pub decode_errors: u64,
    pub backpressure_events: u64,
}

#[derive(Debug, Default)]
struct DirectLinkMetricsInner {
    snapshot: DirectLinkMetricsSnapshot,
}

impl DirectLinkMetrics {
    pub fn snapshot(&self) -> DirectLinkMetricsSnapshot {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .clone()
    }

    pub fn record_open(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .opened += 1;
    }

    pub fn record_close(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .closed += 1;
    }

    pub fn record_receive(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .received += 1;
    }

    pub fn record_send(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .sent += 1;
    }

    pub fn record_protocol_error(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .protocol_errors += 1;
    }

    pub fn record_drop(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .dropped += 1;
    }

    pub fn record_coalesce(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .coalesced += 1;
    }

    pub fn record_decode_error(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .decode_errors += 1;
    }

    pub fn record_backpressure(&self) {
        self.inner
            .lock()
            .expect("direct link metrics poisoned")
            .snapshot
            .backpressure_events += 1;
    }
}

#[derive(Debug, Default)]
pub struct DirectLinkSessionManager {
    sessions: Mutex<BTreeMap<LinkId, DirectLinkSession>>,
    links: Mutex<BTreeMap<LinkId, ManagedLink>>,
    bindings: Mutex<HashMap<(ActorKind, String), DirectLinkStreamDescriptor>>,
    actors: Mutex<HashMap<ActorKind, DirectLinkActorPolicy>>,
    validation: Mutex<OpenLinkValidationPolicy>,
    metrics: DirectLinkMetrics,
}

impl DirectLinkSessionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn metrics(&self) -> DirectLinkMetrics {
        self.metrics.clone()
    }

    pub fn insert(&self, session: DirectLinkSession) -> Result<(), LinkError> {
        let duplicate =
            session
                .stream
                .duplicate_message_id()
                .map(|message_id| LinkError::DuplicateMessageId {
                    stream_name: session.stream.stream_name.clone(),
                    message_id,
                });
        if let Some(error) = duplicate {
            return Err(error);
        }
        self.sessions
            .lock()
            .expect("direct link sessions poisoned")
            .insert(session.link_id.clone(), session);
        self.metrics.record_open();
        Ok(())
    }

    pub fn register_binding(
        &self,
        actor_kind: ActorKind,
        stream: DirectLinkStreamDescriptor,
    ) -> Result<(), SessionManagerError> {
        if let Some(message_id) = stream.duplicate_message_id() {
            return Err(SessionManagerError::DuplicateMessageId {
                stream_name: stream.stream_name,
                message_id,
            });
        }
        let key = (actor_kind, stream.stream_name.clone());
        self.actors
            .lock()
            .expect("direct link actor policies poisoned")
            .entry(key.0.clone())
            .or_default();
        let replaced = self
            .bindings
            .lock()
            .expect("direct link bindings poisoned")
            .insert(key.clone(), stream)
            .is_some();
        if replaced {
            return Err(SessionManagerError::DuplicateBinding {
                actor_kind: key.0,
                stream_name: key.1,
            });
        }
        Ok(())
    }

    pub fn register_actor(&self, actor_kind: ActorKind, policy: DirectLinkActorPolicy) {
        self.actors
            .lock()
            .expect("direct link actor policies poisoned")
            .insert(actor_kind, policy);
    }

    pub fn set_validation_policy(&self, policy: OpenLinkValidationPolicy) {
        *self
            .validation
            .lock()
            .expect("direct link validation policy poisoned") = policy;
    }

    pub fn open_link(&self, request: OpenLinkRequest) -> Result<OpenLinkAck, OpenLinkReject> {
        if request.protocol_version != DIRECT_LINK_PROTOCOL_VERSION {
            return Err(OpenLinkReject::new(
                request.link_id,
                OpenLinkRejectReason::ProtocolVersionMismatch,
            ));
        }
        self.validate_open_request(&request)?;
        let source_to_target = self.negotiate_direction(
            &request.target.actor_kind,
            request.source_to_target.clone(),
            LinkDirection::SourceToTarget,
            request.options.backpressure.clone(),
        )?;
        let target_to_source = match request.mode {
            DirectLinkMode::Unidirectional => None,
            DirectLinkMode::Bidirectional => {
                let requested = request.target_to_source.clone().ok_or_else(|| {
                    OpenLinkReject::new(
                        request.link_id.clone(),
                        OpenLinkRejectReason::UnsupportedStream,
                    )
                })?;
                Some(self.negotiate_direction(
                    &request.source.actor_kind,
                    requested,
                    LinkDirection::TargetToSource,
                    request.options.backpressure.clone(),
                )?)
            }
        };

        let source_actor_kind = request.source.actor_kind.as_str().to_string();
        let target_actor_kind = request.target.actor_kind.as_str().to_string();
        let now = Instant::now();
        let link = ManagedLink {
            link_id: request.link_id.clone(),
            source: request.source,
            target: request.target,
            mode: request.mode,
            heartbeat_interval: request.options.heartbeat_interval,
            idle_timeout: request.options.idle_timeout,
            last_heartbeat_at: now,
            last_heartbeat_sent_at: now,
            directions: [Some(source_to_target.clone()), target_to_source.clone()]
                .into_iter()
                .flatten()
                .map(|direction| (direction.direction, direction))
                .collect(),
            closed: false,
        };
        self.links
            .lock()
            .expect("direct link managed links poisoned")
            .insert(request.link_id.clone(), link);
        self.metrics.record_open();
        tracing::debug!(
            link.id = request.link_id.as_str(),
            link.mode = ?request.mode,
            source.actor_kind = source_actor_kind.as_str(),
            target.actor_kind = target_actor_kind.as_str(),
            "direct link opened"
        );

        Ok(OpenLinkAck {
            link_id: request.link_id,
            source_to_target,
            target_to_source,
        })
    }

    pub fn validate_message_frame(
        &self,
        link_id: &LinkId,
        direction: LinkDirection,
        message_id: DirectLinkMessageId,
        sequence: LinkSequence,
    ) -> Result<(), MessageFrameError> {
        let mut links = self
            .links
            .lock()
            .expect("direct link managed links poisoned");
        let Some(link) = links.get_mut(link_id) else {
            return self.message_frame_error(link_id, MessageFrameError::UnknownLink);
        };
        if link.closed {
            return self.message_frame_error(link_id, MessageFrameError::Closed);
        }
        self.validate_frame_target(link_id, link)?;
        let Some(direction_state) = link.directions.get_mut(&direction) else {
            return self.message_frame_error(link_id, MessageFrameError::WrongDirection);
        };
        if direction_state.closed {
            return self.message_frame_error(link_id, MessageFrameError::Closed);
        }
        if !direction_state
            .accepted_message_type_ids
            .contains(&message_id)
        {
            return self.message_frame_error(link_id, MessageFrameError::UnsupportedMessageType);
        }
        let expected = direction_state.next_receive_sequence;
        if sequence != expected {
            return self.message_frame_error(
                link_id,
                MessageFrameError::InvalidSequence {
                    expected,
                    actual: sequence,
                },
            );
        }
        direction_state.next_receive_sequence = LinkSequence(expected.0 + 1);
        self.metrics.record_receive();
        tracing::trace!(
            link.id = link_id.as_str(),
            link.direction = ?direction,
            link.sequence = sequence.0,
            message.id = message_id.0,
            "direct link message frame accepted"
        );
        Ok(())
    }

    pub fn validate_and_decode_message<T>(
        &self,
        link_id: &LinkId,
        direction: LinkDirection,
        message_id: DirectLinkMessageId,
        sequence: LinkSequence,
        payload: &[u8],
    ) -> Result<T, MessageFrameError>
    where
        T: DirectLinkMessage,
    {
        self.validate_message_frame(link_id, direction, message_id, sequence)?;
        T::decode(payload).map_err(|error| {
            self.metrics.record_decode_error();
            tracing::warn!(
                link.id = link_id.as_str(),
                message.id = message_id.0,
                error = %error,
                "direct link message decode failed"
            );
            MessageFrameError::DecodeError(error.to_string())
        })
    }

    pub fn close_direction(
        &self,
        link_id: &LinkId,
        direction: LinkDirection,
        reason: LinkCloseReason,
    ) -> Result<CloseTransition, SessionManagerError> {
        let mut links = self
            .links
            .lock()
            .expect("direct link managed links poisoned");
        let link = links
            .get_mut(link_id)
            .ok_or(SessionManagerError::UnknownLink)?;
        let direction_state = link
            .directions
            .get_mut(&direction)
            .ok_or(SessionManagerError::WrongDirection)?;
        let stream = direction_state.stream_name.clone();
        let last_sequence_seen = direction_state
            .next_receive_sequence
            .0
            .checked_sub(1)
            .map(LinkSequence);
        if direction_state.closed {
            return Ok(CloseTransition::AlreadyClosed);
        }
        direction_state.closed = true;
        let closed_directions = link
            .directions
            .iter()
            .filter_map(|(direction, state)| state.closed.then_some(*direction))
            .collect::<BTreeSet<_>>();
        let direction_closed = LinkDirectionClosed {
            link_id: link_id.clone(),
            direction,
            stream,
            reason: reason.clone(),
            last_sequence_seen,
        };
        if link.directions.values().all(|state| state.closed) {
            link.closed = true;
            self.metrics.record_close();
            tracing::debug!(
                link.id = link_id.as_str(),
                link.reason = ?reason,
                "direct link closed"
            );
            Ok(CloseTransition::LinkClosed {
                direction_closed,
                link_closed: LinkClosed {
                    link_id: link_id.clone(),
                    reason,
                    closed_directions,
                    last_sequence_seen,
                },
            })
        } else {
            tracing::debug!(
                link.id = link_id.as_str(),
                link.direction = ?direction,
                link.reason = ?reason,
                "direct link direction closed"
            );
            Ok(CloseTransition::DirectionClosed(direction_closed))
        }
    }

    pub fn close_all(
        &self,
        link_id: &LinkId,
        reason: LinkCloseReason,
    ) -> Result<CloseAllTransition, SessionManagerError> {
        let mut links = self
            .links
            .lock()
            .expect("direct link managed links poisoned");
        let link = links
            .get_mut(link_id)
            .ok_or(SessionManagerError::UnknownLink)?;
        if link.closed {
            return Ok(CloseAllTransition::AlreadyClosed);
        }

        let mut direction_closed = Vec::new();
        for (direction, state) in &mut link.directions {
            if state.closed {
                continue;
            }
            let last_sequence_seen = state
                .next_receive_sequence
                .0
                .checked_sub(1)
                .map(LinkSequence);
            state.closed = true;
            direction_closed.push(LinkDirectionClosed {
                link_id: link_id.clone(),
                direction: *direction,
                stream: state.stream_name.clone(),
                reason: reason.clone(),
                last_sequence_seen,
            });
        }

        if direction_closed.is_empty() {
            link.closed = true;
            return Ok(CloseAllTransition::AlreadyClosed);
        }

        let closed_directions = link.directions.keys().copied().collect::<BTreeSet<_>>();
        link.closed = true;
        self.metrics.record_close();
        tracing::debug!(
            link.id = link_id.as_str(),
            link.reason = ?reason,
            "direct link closed"
        );
        Ok(CloseAllTransition::Closed {
            direction_closed,
            link_closed: LinkClosed {
                link_id: link_id.clone(),
                reason,
                closed_directions,
                last_sequence_seen: None,
            },
        })
    }

    pub fn accepted_message_ids(&self, link_id: &LinkId) -> Option<BTreeSet<DirectLinkMessageId>> {
        self.sessions
            .lock()
            .expect("direct link sessions poisoned")
            .get(link_id)
            .map(|session| session.accepted_message_ids.clone())
    }

    pub fn link_snapshot(&self, link_id: &LinkId) -> Option<ManagedLinkSnapshot> {
        self.links
            .lock()
            .expect("direct link managed links poisoned")
            .get(link_id)
            .map(ManagedLinkSnapshot::from)
    }

    pub fn link_opened_for_actor(
        &self,
        link_id: &LinkId,
        actor_ref: &ActorRef,
    ) -> Option<LinkOpened> {
        self.links
            .lock()
            .expect("direct link managed links poisoned")
            .get(link_id)?
            .opened_for_actor(actor_ref)
    }

    pub fn backpressure_policy(
        &self,
        link_id: &LinkId,
        direction: LinkDirection,
    ) -> Option<BackpressurePolicy> {
        self.links
            .lock()
            .expect("direct link managed links poisoned")
            .get(link_id)?
            .directions
            .get(&direction)
            .map(|direction| direction.backpressure.clone())
    }

    pub fn record_heartbeat_at(
        &self,
        link_id: &LinkId,
        now: Instant,
    ) -> Result<(), SessionManagerError> {
        let mut links = self
            .links
            .lock()
            .expect("direct link managed links poisoned");
        let link = links
            .get_mut(link_id)
            .ok_or(SessionManagerError::UnknownLink)?;
        link.last_heartbeat_at = now;
        tracing::trace!(link.id = link_id.as_str(), "direct link heartbeat received");
        Ok(())
    }

    pub fn idle_link_snapshots_at(&self, now: Instant) -> Vec<ManagedLinkSnapshot> {
        self.links
            .lock()
            .expect("direct link managed links poisoned")
            .values()
            .filter(|link| {
                !link.closed
                    && now.saturating_duration_since(link.last_heartbeat_at) >= link.idle_timeout
            })
            .map(ManagedLinkSnapshot::from)
            .collect()
    }

    pub fn heartbeat_due_link_ids_at(&self, now: Instant) -> Vec<LinkId> {
        self.links
            .lock()
            .expect("direct link managed links poisoned")
            .values_mut()
            .filter_map(|link| {
                if link.closed
                    || link.heartbeat_interval.is_zero()
                    || now.saturating_duration_since(link.last_heartbeat_sent_at)
                        < link.heartbeat_interval
                {
                    return None;
                }
                link.last_heartbeat_sent_at = now;
                Some(link.link_id.clone())
            })
            .collect()
    }

    pub fn close(&self, link_id: &LinkId, _reason: LinkCloseReason) -> bool {
        let removed_session = self
            .sessions
            .lock()
            .expect("direct link sessions poisoned")
            .remove(link_id)
            .is_some();
        let removed_link = self
            .links
            .lock()
            .expect("direct link managed links poisoned")
            .remove(link_id)
            .is_some();
        if removed_session || removed_link {
            self.metrics.record_close();
            tracing::debug!(link.id = link_id.as_str(), "direct link session removed");
        }
        removed_session || removed_link
    }

    pub fn record_decode_error(&self, link_id: Option<&LinkId>, details: &str) {
        self.metrics.record_decode_error();
        tracing::warn!(
            link.id = link_id.map(LinkId::as_str).unwrap_or(""),
            error = details,
            "direct link decode error"
        );
    }

    pub fn record_backpressure(
        &self,
        link_id: &LinkId,
        policy: &BackpressurePolicy,
        pending: usize,
    ) {
        self.metrics.record_backpressure();
        tracing::debug!(
            link.id = link_id.as_str(),
            policy = ?policy,
            pending,
            "direct link backpressure"
        );
    }

    pub fn record_drop(&self, link_id: &LinkId, message_id: DirectLinkMessageId) {
        self.metrics.record_drop();
        tracing::debug!(
            link.id = link_id.as_str(),
            message.id = message_id.0,
            "direct link message dropped"
        );
    }

    pub fn record_coalesce(&self, link_id: &LinkId, message_id: DirectLinkMessageId) {
        self.metrics.record_coalesce();
        tracing::trace!(
            link.id = link_id.as_str(),
            message.id = message_id.0,
            "direct link message coalesced"
        );
    }

    fn message_frame_error<T>(
        &self,
        link_id: &LinkId,
        error: MessageFrameError,
    ) -> Result<T, MessageFrameError> {
        self.metrics.record_protocol_error();
        tracing::warn!(
            link.id = link_id.as_str(),
            error = ?error,
            "direct link message frame rejected"
        );
        Err(error)
    }

    fn validate_frame_target(
        &self,
        link_id: &LinkId,
        link: &ManagedLink,
    ) -> Result<(), MessageFrameError> {
        let actors = self
            .actors
            .lock()
            .expect("direct link actor policies poisoned");
        let Some(policy) = actors.get(&link.target.actor_kind) else {
            return self.message_frame_error(link_id, MessageFrameError::NonActivatableTarget);
        };
        if !policy.active && policy.activation == DirectLinkActivationPolicy::ExistingOnly {
            return self.message_frame_error(link_id, MessageFrameError::NonActivatableTarget);
        }
        Ok(())
    }

    fn negotiate_direction(
        &self,
        actor_kind: &ActorKind,
        requested: OpenLinkDirection,
        direction: LinkDirection,
        backpressure: BackpressurePolicy,
    ) -> Result<NegotiatedDirection, OpenLinkReject> {
        let bindings = self.bindings.lock().expect("direct link bindings poisoned");
        let binding = bindings
            .get(&(actor_kind.clone(), requested.stream_name.clone()))
            .ok_or_else(|| {
                OpenLinkReject::new(
                    requested.link_id.clone(),
                    OpenLinkRejectReason::UnsupportedStream,
                )
            })?;
        let accepted = binding.accepted_message_ids();
        if !requested
            .supported_message_type_ids
            .iter()
            .all(|id| accepted.contains(id))
        {
            return Err(OpenLinkReject::new(
                requested.link_id,
                OpenLinkRejectReason::UnsupportedMessageType,
            ));
        }
        Ok(NegotiatedDirection {
            direction,
            stream_name: requested.stream_name,
            accepted_message_type_ids: requested.supported_message_type_ids,
            next_receive_sequence: LinkSequence(1),
            backpressure,
            closed: false,
        })
    }

    fn validate_open_request(&self, request: &OpenLinkRequest) -> Result<(), OpenLinkReject> {
        let validation = self
            .validation
            .lock()
            .expect("direct link validation policy poisoned")
            .clone();
        if let Some(hosted_service) = &validation.hosted_service
            && &request.target.service_kind != hosted_service
        {
            return Err(OpenLinkReject::new(
                request.link_id.clone(),
                OpenLinkRejectReason::NotOwner,
            ));
        }
        if !validation.accepting_links {
            return Err(OpenLinkReject::new(
                request.link_id.clone(),
                OpenLinkRejectReason::Overloaded,
            ));
        }
        if !validation
            .auth_policy
            .authorizes(&request.source.service_kind)
        {
            return Err(OpenLinkReject::new(
                request.link_id.clone(),
                OpenLinkRejectReason::Unauthorized,
            ));
        }
        if validation
            .max_frame_size
            .is_some_and(|max| request.options.max_frame_size > max)
        {
            return Err(OpenLinkReject::new(
                request.link_id.clone(),
                OpenLinkRejectReason::Overloaded,
            ));
        }
        if validation
            .max_pending
            .is_some_and(|max| request.options.backpressure.max_pending() > max)
        {
            return Err(OpenLinkReject::new(
                request.link_id.clone(),
                OpenLinkRejectReason::Overloaded,
            ));
        }

        let actors = self
            .actors
            .lock()
            .expect("direct link actor policies poisoned");
        let target_policy = actors.get(&request.target.actor_kind).ok_or_else(|| {
            OpenLinkReject::new(
                request.link_id.clone(),
                OpenLinkRejectReason::ActorUnavailable,
            )
        })?;
        target_policy.validate_target(request)?;
        if matches!(request.mode, DirectLinkMode::Bidirectional) {
            let source_policy = actors.get(&request.source.actor_kind).ok_or_else(|| {
                OpenLinkReject::new(
                    request.link_id.clone(),
                    OpenLinkRejectReason::ActorUnavailable,
                )
            })?;
            if !source_policy.active {
                return Err(OpenLinkReject::new(
                    request.link_id.clone(),
                    OpenLinkRejectReason::ActorUnavailable,
                ));
            }
        }
        Ok(())
    }
}

pub const DIRECT_LINK_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct OpenLinkRequest {
    pub protocol_version: u16,
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: ActorRef,
    pub mode: DirectLinkMode,
    pub source_to_target: OpenLinkDirection,
    pub target_to_source: Option<OpenLinkDirection>,
    pub options: DirectLinkOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenLinkValidationPolicy {
    pub hosted_service: Option<ServiceKind>,
    pub accepting_links: bool,
    pub auth_policy: DirectLinkAuthPolicy,
    pub max_frame_size: Option<usize>,
    pub max_pending: Option<usize>,
}

impl OpenLinkValidationPolicy {
    pub fn hosted(service_kind: ServiceKind) -> Self {
        Self {
            hosted_service: Some(service_kind),
            ..Self::default()
        }
    }

    pub fn authorize_sources(mut self, sources: impl IntoIterator<Item = ServiceKind>) -> Self {
        self.auth_policy = DirectLinkAuthPolicy::AllowServices(sources.into_iter().collect());
        self
    }

    pub fn accepting_links(mut self, accepting_links: bool) -> Self {
        self.accepting_links = accepting_links;
        self
    }

    pub fn max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = Some(max_frame_size);
        self
    }

    pub fn max_pending(mut self, max_pending: usize) -> Self {
        self.max_pending = Some(max_pending);
        self
    }
}

impl Default for OpenLinkValidationPolicy {
    fn default() -> Self {
        Self {
            hosted_service: None,
            accepting_links: true,
            auth_policy: DirectLinkAuthPolicy::AllowAll,
            max_frame_size: None,
            max_pending: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectLinkAuthPolicy {
    AllowAll,
    AllowServices(HashSet<ServiceKind>),
}

impl DirectLinkAuthPolicy {
    fn authorizes(&self, source: &ServiceKind) -> bool {
        match self {
            Self::AllowAll => true,
            Self::AllowServices(allowed) => allowed.contains(source),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectLinkActivationPolicy {
    ExistingOnly,
    AllowLazyActivation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectLinkActorPolicy {
    pub activation: DirectLinkActivationPolicy,
    pub active: bool,
    pub owner_epoch: Option<Epoch>,
}

impl DirectLinkActorPolicy {
    pub fn active(owner_epoch: Option<Epoch>) -> Self {
        Self {
            activation: DirectLinkActivationPolicy::ExistingOnly,
            active: true,
            owner_epoch,
        }
    }

    pub fn lazy(owner_epoch: Option<Epoch>) -> Self {
        Self {
            activation: DirectLinkActivationPolicy::AllowLazyActivation,
            active: false,
            owner_epoch,
        }
    }

    fn validate_target(&self, request: &OpenLinkRequest) -> Result<(), OpenLinkReject> {
        if !self.active && self.activation == DirectLinkActivationPolicy::ExistingOnly {
            return Err(OpenLinkReject::new(
                request.link_id.clone(),
                OpenLinkRejectReason::ActorUnavailable,
            ));
        }
        if let Some(current_epoch) = self.owner_epoch
            && let ActorRefTarget::Direct {
                owner_epoch: Some(request_epoch),
                ..
            } = &request.target.target
            && *request_epoch != current_epoch
        {
            return Err(OpenLinkReject::new(
                request.link_id.clone(),
                OpenLinkRejectReason::Fenced,
            ));
        }
        Ok(())
    }
}

impl Default for DirectLinkActorPolicy {
    fn default() -> Self {
        Self {
            activation: DirectLinkActivationPolicy::AllowLazyActivation,
            active: true,
            owner_epoch: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenLinkDirection {
    pub link_id: LinkId,
    pub stream_name: String,
    pub supported_message_type_ids: BTreeSet<DirectLinkMessageId>,
}

impl OpenLinkDirection {
    pub fn from_stream(link_id: LinkId, stream: &DirectLinkStreamDescriptor) -> Self {
        Self {
            link_id,
            stream_name: stream.stream_name.clone(),
            supported_message_type_ids: stream.accepted_message_ids(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenLinkAck {
    pub link_id: LinkId,
    pub source_to_target: NegotiatedDirection,
    pub target_to_source: Option<NegotiatedDirection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenLinkReject {
    pub link_id: LinkId,
    pub reason: OpenLinkRejectReason,
    pub optional_redirect: Option<Box<ActorRef>>,
}

impl OpenLinkReject {
    pub fn new(link_id: LinkId, reason: OpenLinkRejectReason) -> Self {
        Self {
            link_id,
            reason,
            optional_redirect: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenLinkRejectReason {
    NotOwner,
    Fenced,
    ActorUnavailable,
    UnsupportedStream,
    UnsupportedMessageType,
    Unauthorized,
    Overloaded,
    ProtocolVersionMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedDirection {
    pub direction: LinkDirection,
    pub stream_name: String,
    pub accepted_message_type_ids: BTreeSet<DirectLinkMessageId>,
    pub next_receive_sequence: LinkSequence,
    pub backpressure: BackpressurePolicy,
    pub closed: bool,
}

#[derive(Debug, Clone)]
struct ManagedLink {
    link_id: LinkId,
    source: ActorRef,
    target: ActorRef,
    mode: DirectLinkMode,
    heartbeat_interval: Duration,
    idle_timeout: Duration,
    last_heartbeat_at: Instant,
    last_heartbeat_sent_at: Instant,
    directions: BTreeMap<LinkDirection, NegotiatedDirection>,
    closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedLinkSnapshot {
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: ActorRef,
    pub mode: DirectLinkMode,
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
    pub directions: BTreeSet<LinkDirection>,
    pub closed: bool,
}

impl From<&ManagedLink> for ManagedLinkSnapshot {
    fn from(value: &ManagedLink) -> Self {
        Self {
            link_id: value.link_id.clone(),
            source: value.source.clone(),
            target: value.target.clone(),
            mode: value.mode,
            heartbeat_interval: value.heartbeat_interval,
            idle_timeout: value.idle_timeout,
            directions: value.directions.keys().copied().collect(),
            closed: value.closed,
        }
    }
}

impl ManagedLink {
    fn opened_for_actor(&self, actor_ref: &ActorRef) -> Option<LinkOpened> {
        if *actor_ref == self.target {
            let inbound = self.directions.get(&LinkDirection::SourceToTarget)?;
            let outbound = self.directions.get(&LinkDirection::TargetToSource);
            return Some(LinkOpened {
                link_id: self.link_id.clone(),
                source: self.source.clone(),
                target: self.target.clone(),
                mode: self.mode,
                inbound_stream: inbound.stream_name.clone(),
                inbound_accepted_message_types: inbound.accepted_message_type_ids.clone(),
                outbound_stream: outbound.map(|direction| direction.stream_name.clone()),
                outbound_accepted_message_types: outbound
                    .map(|direction| direction.accepted_message_type_ids.clone())
                    .unwrap_or_default(),
            });
        }

        if *actor_ref == self.source {
            let inbound = self.directions.get(&LinkDirection::TargetToSource)?;
            let outbound = self.directions.get(&LinkDirection::SourceToTarget);
            return Some(LinkOpened {
                link_id: self.link_id.clone(),
                source: self.source.clone(),
                target: self.target.clone(),
                mode: self.mode,
                inbound_stream: inbound.stream_name.clone(),
                inbound_accepted_message_types: inbound.accepted_message_type_ids.clone(),
                outbound_stream: outbound.map(|direction| direction.stream_name.clone()),
                outbound_accepted_message_types: outbound
                    .map(|direction| direction.accepted_message_type_ids.clone())
                    .unwrap_or_default(),
            });
        }

        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseTransition {
    DirectionClosed(LinkDirectionClosed),
    LinkClosed {
        direction_closed: LinkDirectionClosed,
        link_closed: LinkClosed,
    },
    AlreadyClosed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseAllTransition {
    Closed {
        direction_closed: Vec<LinkDirectionClosed>,
        link_closed: LinkClosed,
    },
    AlreadyClosed,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SessionManagerError {
    #[error("duplicate direct-link stream binding for actor {actor_kind:?} stream {stream_name}")]
    DuplicateBinding {
        actor_kind: ActorKind,
        stream_name: String,
    },
    #[error("direct-link stream {stream_name} has duplicate message id {message_id:?}")]
    DuplicateMessageId {
        stream_name: String,
        message_id: DirectLinkMessageId,
    },
    #[error("direct-link session does not exist")]
    UnknownLink,
    #[error("direct-link direction does not exist")]
    WrongDirection,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MessageFrameError {
    #[error("direct-link session does not exist")]
    UnknownLink,
    #[error("direct-link direction does not exist")]
    WrongDirection,
    #[error("direct-link direction is closed")]
    Closed,
    #[error("direct-link message type is not negotiated")]
    UnsupportedMessageType,
    #[error("direct-link target actor is not active or activatable")]
    NonActivatableTarget,
    #[error("direct-link message payload failed to decode: {0}")]
    DecodeError(String),
    #[error("direct-link sequence is invalid: expected {expected:?}, actual {actual:?}")]
    InvalidSequence {
        expected: LinkSequence,
        actual: LinkSequence,
    },
}
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use http::Uri;
    use lattice_core::{
        ActorId, DirectLinkMessageDescriptor, DirectLinkMessageId, Epoch, InstanceId, ServiceKind,
        actor_kind, service_kind,
    };

    use super::*;

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestPayload {
        #[prost(uint64, tag = "1")]
        value: u64,
    }

    impl DirectLinkMessage for TestPayload {
        const PROTO_FULL_NAME: &'static str = "game.TestPayload";
    }

    #[test]
    fn open_link_negotiates_unidirectional_session_and_sequence() {
        let manager = DirectLinkSessionManager::new();
        let stream = stream("movement", &[1, 2]);
        manager
            .register_binding(actor_kind!("Battle"), stream.clone())
            .unwrap();
        let link_id = LinkId::new("link-1");

        let ack = manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &stream),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap();

        assert_eq!(ack.source_to_target.stream_name, "movement");
        assert_eq!(
            ack.source_to_target.accepted_message_type_ids,
            BTreeSet::from([DirectLinkMessageId(1), DirectLinkMessageId(2)])
        );
        let snapshot = manager.link_snapshot(&link_id).unwrap();
        assert_eq!(snapshot.mode, DirectLinkMode::Unidirectional);
        assert_eq!(
            snapshot.directions,
            BTreeSet::from([LinkDirection::SourceToTarget])
        );
        manager
            .validate_message_frame(
                &link_id,
                LinkDirection::SourceToTarget,
                DirectLinkMessageId(1),
                LinkSequence(1),
            )
            .unwrap();
        assert_eq!(
            manager.validate_message_frame(
                &link_id,
                LinkDirection::SourceToTarget,
                DirectLinkMessageId(1),
                LinkSequence(1),
            ),
            Err(MessageFrameError::InvalidSequence {
                expected: LinkSequence(2),
                actual: LinkSequence(1)
            })
        );
        let metrics = manager.metrics().snapshot();
        assert_eq!(metrics.opened, 1);
        assert_eq!(metrics.received, 1);
        assert_eq!(metrics.protocol_errors, 1);
    }

    #[test]
    fn message_frame_validation_rejects_invalid_frames_before_delivery() {
        let manager = DirectLinkSessionManager::new();
        let stream = stream("movement", &[1]);
        manager
            .register_binding(actor_kind!("Battle"), stream.clone())
            .unwrap();
        let link_id = LinkId::new("link-frames");
        manager
            .open_link(open_request_with_id(&stream, link_id.clone()))
            .unwrap();

        assert_eq!(
            manager.validate_message_frame(
                &LinkId::new("missing"),
                LinkDirection::SourceToTarget,
                DirectLinkMessageId(1),
                LinkSequence(1),
            ),
            Err(MessageFrameError::UnknownLink)
        );
        assert_eq!(
            manager.validate_message_frame(
                &link_id,
                LinkDirection::TargetToSource,
                DirectLinkMessageId(1),
                LinkSequence(1),
            ),
            Err(MessageFrameError::WrongDirection)
        );
        assert_eq!(
            manager.validate_message_frame(
                &link_id,
                LinkDirection::SourceToTarget,
                DirectLinkMessageId(2),
                LinkSequence(1),
            ),
            Err(MessageFrameError::UnsupportedMessageType)
        );
        assert!(matches!(
            manager.validate_and_decode_message::<TestPayload>(
                &link_id,
                LinkDirection::SourceToTarget,
                DirectLinkMessageId(1),
                LinkSequence(1),
                b"not protobuf",
            ),
            Err(MessageFrameError::DecodeError(_))
        ));

        let inactive = DirectLinkSessionManager::new();
        inactive
            .register_binding(actor_kind!("Battle"), stream.clone())
            .unwrap();
        let inactive_id = LinkId::new("link-inactive");
        inactive
            .open_link(open_request_with_id(&stream, inactive_id.clone()))
            .unwrap();
        inactive.register_actor(
            actor_kind!("Battle"),
            DirectLinkActorPolicy {
                activation: DirectLinkActivationPolicy::ExistingOnly,
                active: false,
                owner_epoch: None,
            },
        );
        assert_eq!(
            inactive.validate_message_frame(
                &inactive_id,
                LinkDirection::SourceToTarget,
                DirectLinkMessageId(1),
                LinkSequence(1),
            ),
            Err(MessageFrameError::NonActivatableTarget)
        );
    }

    #[test]
    fn heartbeat_due_tracking_emits_once_per_interval_and_stops_after_close() {
        let manager = DirectLinkSessionManager::new();
        let stream = stream("movement", &[1]);
        manager
            .register_binding(actor_kind!("Battle"), stream.clone())
            .unwrap();
        let link_id = LinkId::new("link-heartbeat-due");
        let mut request = open_request_with_id(&stream, link_id.clone());
        request.options.heartbeat_interval = Duration::from_secs(10);
        manager.open_link(request).unwrap();

        let opened_at = Instant::now();
        assert!(
            manager
                .heartbeat_due_link_ids_at(opened_at + Duration::from_secs(1))
                .is_empty()
        );
        assert_eq!(
            manager.heartbeat_due_link_ids_at(opened_at + Duration::from_secs(10)),
            vec![link_id.clone()]
        );
        assert!(
            manager
                .heartbeat_due_link_ids_at(opened_at + Duration::from_secs(19))
                .is_empty()
        );
        assert_eq!(
            manager.heartbeat_due_link_ids_at(opened_at + Duration::from_secs(20)),
            vec![link_id.clone()]
        );

        manager
            .close_all(&link_id, LinkCloseReason::Done)
            .expect("close link");
        assert!(
            manager
                .heartbeat_due_link_ids_at(opened_at + Duration::from_secs(30))
                .is_empty()
        );
    }

    #[test]
    fn open_link_rejects_unavailable_actor_unsupported_stream_and_message() {
        let manager = DirectLinkSessionManager::new();
        let requested_stream = stream("movement", &[1]);
        let link_id = LinkId::new("link-1");

        let reject = manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &requested_stream,
                ),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::ActorUnavailable);

        manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::default());
        let reject = manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &requested_stream,
                ),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::UnsupportedStream);

        manager
            .register_binding(actor_kind!("Battle"), stream("movement", &[1]))
            .unwrap();
        let reject = manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id,
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection {
                    link_id: LinkId::new("link-2"),
                    stream_name: "movement".to_string(),
                    supported_message_type_ids: BTreeSet::from([DirectLinkMessageId(2)]),
                },
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::UnsupportedMessageType);
    }

    #[test]
    fn open_link_validates_service_auth_epoch_activation_and_backpressure() {
        let stream = stream("movement", &[1]);

        let protocol = configured_manager(&stream);
        let mut request = open_request(&stream);
        request.protocol_version = DIRECT_LINK_PROTOCOL_VERSION + 1;
        let reject = protocol.open_link(request).unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::ProtocolVersionMismatch);

        let wrong_service = configured_manager(&stream);
        let mut request = open_request(&stream);
        request.target.service_kind = service_kind!("Wrong");
        let reject = wrong_service.open_link(request).unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::NotOwner);

        let unauthorized = configured_manager(&stream);
        let mut request = open_request(&stream);
        request.source.service_kind = service_kind!("Intruder");
        let reject = unauthorized.open_link(request).unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);

        let overloaded = configured_manager(&stream);
        overloaded.set_validation_policy(
            OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
                .authorize_sources([service_kind!("Gateway")])
                .max_pending(4)
                .max_frame_size(128),
        );
        let mut request = open_request(&stream);
        request.options.backpressure = BackpressurePolicy::DropOldest { max_pending: 8 };
        let reject = overloaded.open_link(request).unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::Overloaded);

        let fenced = configured_manager(&stream);
        fenced.register_actor(
            actor_kind!("Battle"),
            DirectLinkActorPolicy::active(Some(Epoch(2))),
        );
        let mut request = open_request(&stream);
        request.target = actor_ref_with_epoch(
            service_kind!("Battle"),
            actor_kind!("Battle"),
            9,
            Some(Epoch(1)),
        );
        let reject = fenced.open_link(request).unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::Fenced);

        let inactive = configured_manager(&stream);
        inactive.register_actor(
            actor_kind!("Battle"),
            DirectLinkActorPolicy {
                activation: DirectLinkActivationPolicy::ExistingOnly,
                active: false,
                owner_epoch: None,
            },
        );
        let reject = inactive.open_link(open_request(&stream)).unwrap_err();
        assert_eq!(reject.reason, OpenLinkRejectReason::ActorUnavailable);

        let lazy = configured_manager(&stream);
        lazy.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::lazy(None));
        assert!(lazy.open_link(open_request(&stream)).is_ok());
    }

    #[test]
    fn bidirectional_close_keeps_opposite_direction_until_closed() {
        let manager = DirectLinkSessionManager::new();
        let outbound = stream("input", &[10]);
        let inbound = stream("updates", &[20]);
        manager
            .register_binding(actor_kind!("Battle"), outbound.clone())
            .unwrap();
        manager
            .register_binding(actor_kind!("GatewaySession"), inbound.clone())
            .unwrap();
        let link_id = LinkId::new("link-1");
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Bidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &outbound),
                target_to_source: Some(OpenLinkDirection::from_stream(link_id.clone(), &inbound)),
                options: DirectLinkOptions::bidirectional(),
            })
            .unwrap();

        match manager
            .close_direction(
                &link_id,
                LinkDirection::SourceToTarget,
                LinkCloseReason::Done,
            )
            .unwrap()
        {
            CloseTransition::DirectionClosed(event) => {
                assert_eq!(event.reason, LinkCloseReason::Done);
                assert_eq!(event.direction, LinkDirection::SourceToTarget);
                assert_eq!(event.stream, "input");
            }
            other => panic!("expected direction close, got {other:?}"),
        }
        manager
            .validate_message_frame(
                &link_id,
                LinkDirection::TargetToSource,
                DirectLinkMessageId(20),
                LinkSequence(1),
            )
            .unwrap();
        match manager
            .close_direction(
                &link_id,
                LinkDirection::TargetToSource,
                LinkCloseReason::Done,
            )
            .unwrap()
        {
            CloseTransition::LinkClosed {
                direction_closed,
                link_closed,
            } => {
                assert_eq!(direction_closed.direction, LinkDirection::TargetToSource);
                assert_eq!(direction_closed.stream, "updates");
                assert_eq!(
                    link_closed.closed_directions,
                    [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
                        .into_iter()
                        .collect()
                );
            }
            other => panic!("expected link close, got {other:?}"),
        }
        assert_eq!(manager.metrics().snapshot().closed, 1);
    }

    #[test]
    fn observability_hooks_increment_metrics() {
        let manager = DirectLinkSessionManager::new();
        let link_id = LinkId::new("link-1");

        manager.record_decode_error(Some(&link_id), "bad payload");
        manager.record_backpressure(
            &link_id,
            &BackpressurePolicy::DropOldest { max_pending: 1 },
            1,
        );
        manager.record_drop(&link_id, DirectLinkMessageId(10));
        manager.record_coalesce(&link_id, DirectLinkMessageId(10));

        let metrics = manager.metrics().snapshot();
        assert_eq!(metrics.decode_errors, 1);
        assert_eq!(metrics.backpressure_events, 1);
        assert_eq!(metrics.dropped, 1);
        assert_eq!(metrics.coalesced, 1);
    }

    fn stream(name: &str, ids: &[u64]) -> DirectLinkStreamDescriptor {
        DirectLinkStreamDescriptor {
            stream_name: name.to_string(),
            messages: ids
                .iter()
                .map(|id| DirectLinkMessageDescriptor {
                    message_id: DirectLinkMessageId(*id),
                    proto_full_name: format!("game.Message{id}"),
                    rust_type_name: format!("Message{id}"),
                })
                .collect(),
        }
    }

    fn configured_manager(stream: &DirectLinkStreamDescriptor) -> DirectLinkSessionManager {
        let manager = DirectLinkSessionManager::new();
        manager.set_validation_policy(
            OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
                .authorize_sources([service_kind!("Gateway")])
                .max_pending(1024)
                .max_frame_size(256 * 1024),
        );
        manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::active(None));
        manager
            .register_binding(actor_kind!("Battle"), stream.clone())
            .unwrap();
        manager
    }

    fn open_request(stream: &DirectLinkStreamDescriptor) -> OpenLinkRequest {
        open_request_with_id(stream, LinkId::new("link-policy"))
    }

    fn open_request_with_id(
        stream: &DirectLinkStreamDescriptor,
        link_id: LinkId,
    ) -> OpenLinkRequest {
        OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id, stream),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        }
    }

    fn actor_ref(service_kind: ServiceKind, actor_kind: ActorKind, id: u64) -> ActorRef {
        actor_ref_with_epoch(service_kind, actor_kind, id, None)
    }

    fn actor_ref_with_epoch(
        service_kind: ServiceKind,
        actor_kind: ActorKind,
        id: u64,
        owner_epoch: Option<Epoch>,
    ) -> ActorRef {
        ActorRef::direct(
            service_kind,
            actor_kind,
            ActorId::U64(id),
            InstanceId::new(format!("instance-{id}")),
            Uri::from_str("http://127.0.0.1:10000").unwrap(),
            owner_epoch,
        )
    }
}
