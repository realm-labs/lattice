use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use lattice_core::{
    ActorKind, ActorRef, BackpressurePolicy, DirectLinkMessageId, DirectLinkMode,
    DirectLinkOptions, DirectLinkSession, DirectLinkStreamDescriptor, LinkCloseReason,
    LinkDirection, LinkError, LinkId, LinkSequence,
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
}

#[derive(Debug, Default)]
pub struct DirectLinkSessionManager {
    sessions: Mutex<BTreeMap<LinkId, DirectLinkSession>>,
    links: Mutex<BTreeMap<LinkId, ManagedLink>>,
    bindings: Mutex<HashMap<(ActorKind, String), DirectLinkStreamDescriptor>>,
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

    pub fn open_link(&self, request: OpenLinkRequest) -> Result<OpenLinkAck, OpenLinkReject> {
        if request.protocol_version != DIRECT_LINK_PROTOCOL_VERSION {
            return Err(OpenLinkReject::new(
                request.link_id,
                OpenLinkRejectReason::ProtocolVersionMismatch,
            ));
        }
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

        let link = ManagedLink {
            link_id: request.link_id.clone(),
            source: request.source,
            target: request.target,
            mode: request.mode,
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
        let link = links
            .get_mut(link_id)
            .ok_or(MessageFrameError::UnknownLink)?;
        if link.closed {
            return Err(MessageFrameError::Closed);
        }
        let direction_state = link
            .directions
            .get_mut(&direction)
            .ok_or(MessageFrameError::WrongDirection)?;
        if direction_state.closed {
            return Err(MessageFrameError::Closed);
        }
        if !direction_state
            .accepted_message_type_ids
            .contains(&message_id)
        {
            return Err(MessageFrameError::UnsupportedMessageType);
        }
        let expected = direction_state.next_receive_sequence;
        if sequence != expected {
            return Err(MessageFrameError::InvalidSequence {
                expected,
                actual: sequence,
            });
        }
        direction_state.next_receive_sequence = LinkSequence(expected.0 + 1);
        Ok(())
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
        if direction_state.closed {
            return Ok(CloseTransition::AlreadyClosed);
        }
        direction_state.closed = true;
        let closed_directions = link
            .directions
            .iter()
            .filter_map(|(direction, state)| state.closed.then_some(*direction))
            .collect::<BTreeSet<_>>();
        if link.directions.values().all(|state| state.closed) {
            link.closed = true;
            self.metrics.record_close();
            Ok(CloseTransition::LinkClosed {
                reason,
                closed_directions,
            })
        } else {
            Ok(CloseTransition::DirectionClosed { reason, direction })
        }
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
        }
        removed_session || removed_link
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
    directions: BTreeMap<LinkDirection, NegotiatedDirection>,
    closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedLinkSnapshot {
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: ActorRef,
    pub mode: DirectLinkMode,
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
            directions: value.directions.keys().copied().collect(),
            closed: value.closed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseTransition {
    DirectionClosed {
        reason: LinkCloseReason,
        direction: LinkDirection,
    },
    LinkClosed {
        reason: LinkCloseReason,
        closed_directions: BTreeSet<LinkDirection>,
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
        ActorId, DirectLinkMessageDescriptor, DirectLinkMessageId, InstanceId, ServiceKind,
        actor_kind, service_kind,
    };

    use super::*;

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
    }

    #[test]
    fn open_link_rejects_unsupported_stream_and_message() {
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

        assert_eq!(
            manager
                .close_direction(
                    &link_id,
                    LinkDirection::SourceToTarget,
                    LinkCloseReason::Done
                )
                .unwrap(),
            CloseTransition::DirectionClosed {
                reason: LinkCloseReason::Done,
                direction: LinkDirection::SourceToTarget,
            }
        );
        manager
            .validate_message_frame(
                &link_id,
                LinkDirection::TargetToSource,
                DirectLinkMessageId(20),
                LinkSequence(1),
            )
            .unwrap();
        assert!(matches!(
            manager
                .close_direction(
                    &link_id,
                    LinkDirection::TargetToSource,
                    LinkCloseReason::Done
                )
                .unwrap(),
            CloseTransition::LinkClosed { .. }
        ));
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

    fn actor_ref(service_kind: ServiceKind, actor_kind: ActorKind, id: u64) -> ActorRef {
        ActorRef::direct(
            service_kind,
            actor_kind,
            ActorId::U64(id),
            InstanceId::new(format!("instance-{id}")),
            Uri::from_str("http://127.0.0.1:10000").unwrap(),
            None,
        )
    }
}
