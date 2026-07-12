use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
};
use prost::Message;
use thiserror::Error;
use tokio::sync::oneshot;

use crate::association::{Association, AssociationError, AssociationId};
use crate::protocol::{CatalogueDecision, ProtocolFingerprint};
use crate::transport::{FramedConnection, RemotingIo};
use crate::wire::{Frame, FrameKind};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SenderIdentity {
    Actor {
        path: ActorPath,
        activation_id: ActivationId,
    },
    Process(u128),
}

impl SenderIdentity {
    fn stable_bytes(&self) -> Vec<u8> {
        match self {
            Self::Actor {
                path,
                activation_id,
            } => format!(
                "a:{}:{}:{}",
                activation_id.node_incarnation().get(),
                activation_id.local_sequence(),
                path
            )
            .into_bytes(),
            Self::Process(value) => value.to_be_bytes().to_vec(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExactActorTarget {
    pub cluster_id: ClusterId,
    pub node_address: NodeAddress,
    pub node_incarnation: NodeIncarnation,
    pub actor_path: ActorPath,
    pub activation_id: ActivationId,
    pub protocol_id: ProtocolId,
}

impl<A> From<&ActorRef<A>> for ExactActorTarget {
    fn from(value: &ActorRef<A>) -> Self {
        Self {
            cluster_id: value.cluster_id().clone(),
            node_address: value.node_address().clone(),
            node_incarnation: value.node_incarnation(),
            actor_path: value.actor_path().clone(),
            activation_id: value.activation_id(),
            protocol_id: value.protocol_id(),
        }
    }
}

impl ExactActorTarget {
    fn stable_bytes(&self) -> Vec<u8> {
        format!(
            "{}:{}:{}:{}:{}",
            self.node_address,
            self.node_incarnation.get(),
            self.actor_path,
            self.activation_id.local_sequence(),
            self.protocol_id.get()
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CorrelationId {
    caller_incarnation: u128,
    sequence: u64,
}

impl CorrelationId {
    pub const fn new(caller_incarnation: u128, sequence: u64) -> Option<Self> {
        if caller_incarnation == 0 || sequence == 0 {
            None
        } else {
            Some(Self {
                caller_incarnation,
                sequence,
            })
        }
    }

    pub const fn caller_incarnation(self) -> u128 {
        self.caller_incarnation
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    fn to_bytes(self) -> [u8; 24] {
        let mut bytes = [0_u8; 24];
        bytes[..16].copy_from_slice(&self.caller_incarnation.to_be_bytes());
        bytes[16..].copy_from_slice(&self.sequence.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 24 {
            return None;
        }
        let caller_incarnation = u128::from_be_bytes(bytes[..16].try_into().ok()?);
        let sequence = u64::from_be_bytes(bytes[16..].try_into().ok()?);
        Self::new(caller_incarnation, sequence)
    }
}

pub fn ask_correlation(frame: &Frame) -> Option<CorrelationId> {
    if frame.kind != FrameKind::Ask {
        return None;
    }
    let ask = frame.decode_message::<AskWire>().ok()?;
    CorrelationId::from_bytes(&ask.correlation_id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundTell {
    pub target: ExactActorTarget,
    pub message_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundAsk {
    pub target: ExactActorTarget,
    pub correlation_id: CorrelationId,
    pub timeout_budget: Duration,
    pub message_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFailure {
    pub correlation_id: CorrelationId,
    pub code: RemoteFailureCode,
    pub safe_detail: Option<String>,
}

pub fn decode_tell(frame: &Frame) -> Result<InboundTell, RemoteMessageError> {
    if frame.kind != FrameKind::Tell {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<TellWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(InboundTell {
        target: target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        message_id: wire.message_id,
        payload: Bytes::from(wire.payload),
    })
}

pub fn decode_ask(frame: &Frame) -> Result<InboundAsk, RemoteMessageError> {
    if frame.kind != FrameKind::Ask {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let wire = frame
        .decode_message::<AskWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let correlation_id = CorrelationId::from_bytes(&wire.correlation_id)
        .ok_or(RemoteMessageError::InvalidPayload)?;
    if wire.message_id == 0 || wire.timeout_nanos == 0 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(InboundAsk {
        target: target_from_wire(wire.target.ok_or(RemoteMessageError::InvalidPayload)?)?,
        correlation_id,
        timeout_budget: Duration::from_nanos(wire.timeout_nanos),
        message_id: wire.message_id,
        payload: Bytes::from(wire.payload),
    })
}

pub fn reply_frame(correlation_id: CorrelationId, payload: Bytes) -> Frame {
    Frame::encode_message(
        FrameKind::Reply,
        &ReplyWire {
            correlation_id: correlation_id.to_bytes().to_vec(),
            payload: payload.to_vec(),
        },
    )
}

pub fn decode_reply(frame: &Frame) -> Result<(CorrelationId, Bytes), RemoteMessageError> {
    if frame.kind != FrameKind::Reply {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let reply = frame
        .decode_message::<ReplyWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    Ok((
        CorrelationId::from_bytes(&reply.correlation_id)
            .ok_or(RemoteMessageError::InvalidPayload)?,
        Bytes::from(reply.payload),
    ))
}

pub fn failure_frame(failure: &RemoteFailure) -> Frame {
    let detail = failure.safe_detail.as_deref().unwrap_or("");
    Frame::encode_message(
        FrameKind::Failure,
        &FailureWire {
            correlation_id: failure.correlation_id.to_bytes().to_vec(),
            code: failure.code as u32,
            safe_detail: detail.chars().take(256).collect(),
        },
    )
}

pub fn decode_failure(frame: &Frame) -> Result<RemoteFailure, RemoteMessageError> {
    if frame.kind != FrameKind::Failure {
        return Err(RemoteMessageError::InvalidPayload);
    }
    let failure = frame
        .decode_message::<FailureWire>()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let code = RemoteFailureCode::try_from(failure.code)
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    if failure.safe_detail.len() > 256 {
        return Err(RemoteMessageError::InvalidPayload);
    }
    Ok(RemoteFailure {
        correlation_id: CorrelationId::from_bytes(&failure.correlation_id)
            .ok_or(RemoteMessageError::InvalidPayload)?,
        code,
        safe_detail: (!failure.safe_detail.is_empty()).then_some(failure.safe_detail),
    })
}

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

    pub fn mark_socket_write_started(&self, correlation: CorrelationId) -> bool {
        let mut entries = self.pending.entries.lock().expect("pending asks poisoned");
        let Some(pending) = entries.get_mut(&correlation) else {
            return false;
        };
        pending.commitment = Commitment::SocketWriteStarted;
        true
    }

    pub fn prepare_ask_for_socket_write(&self, frame: &mut Frame) -> bool {
        if frame.kind != FrameKind::Ask {
            return true;
        }
        let Ok(mut ask) = frame.decode_message::<AskWire>() else {
            return false;
        };
        let Some(correlation) = CorrelationId::from_bytes(&ask.correlation_id) else {
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
        ask.timeout_nanos = duration_nanos(remaining);
        *frame = Frame::encode_message(FrameKind::Ask, &ask);
        true
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

#[derive(Clone, PartialEq, Message)]
struct ExactActorTargetWire {
    #[prost(string, tag = "1")]
    cluster_id: String,
    #[prost(string, tag = "2")]
    host: String,
    #[prost(uint32, tag = "3")]
    port: u32,
    #[prost(bytes = "vec", tag = "4")]
    node_incarnation: Vec<u8>,
    #[prost(string, tag = "5")]
    actor_path: String,
    #[prost(uint64, tag = "6")]
    activation_sequence: u64,
    #[prost(uint64, tag = "7")]
    protocol_id: u64,
}

#[derive(Clone, PartialEq, Message)]
struct TellWire {
    #[prost(bytes = "vec", tag = "1")]
    sender: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    target: Option<ExactActorTargetWire>,
    #[prost(uint64, tag = "3")]
    message_id: u64,
    #[prost(bytes = "vec", tag = "4")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct AskWire {
    #[prost(bytes = "vec", tag = "1")]
    sender: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    target: Option<ExactActorTargetWire>,
    #[prost(bytes = "vec", tag = "3")]
    correlation_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    timeout_nanos: u64,
    #[prost(uint64, tag = "5")]
    message_id: u64,
    #[prost(bytes = "vec", tag = "6")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ReplyWire {
    #[prost(bytes = "vec", tag = "1")]
    correlation_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct FailureWire {
    #[prost(bytes = "vec", tag = "1")]
    correlation_id: Vec<u8>,
    #[prost(uint32, tag = "2")]
    code: u32,
    #[prost(string, tag = "3")]
    safe_detail: String,
}

fn target_to_wire(target: &ExactActorTarget) -> ExactActorTargetWire {
    ExactActorTargetWire {
        cluster_id: target.cluster_id.as_str().to_owned(),
        host: target.node_address.host().to_owned(),
        port: u32::from(target.node_address.port()),
        node_incarnation: target.node_incarnation.get().to_be_bytes().to_vec(),
        actor_path: target.actor_path.to_string(),
        activation_sequence: target.activation_id.local_sequence(),
        protocol_id: target.protocol_id.get(),
    }
}

fn target_from_wire(wire: ExactActorTargetWire) -> Result<ExactActorTarget, RemoteMessageError> {
    let node_bytes: [u8; 16] = wire
        .node_incarnation
        .as_slice()
        .try_into()
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let node_incarnation = NodeIncarnation::new(u128::from_be_bytes(node_bytes))
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
    let port = u16::try_from(wire.port).map_err(|_| RemoteMessageError::InvalidPayload)?;
    Ok(ExactActorTarget {
        cluster_id: ClusterId::new(wire.cluster_id)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        node_address: NodeAddress::new(wire.host, port)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        node_incarnation,
        actor_path: ActorPath::try_from(wire.actor_path)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        activation_id: ActivationId::new(node_incarnation, wire.activation_sequence)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
        protocol_id: ProtocolId::new(wire.protocol_id)
            .map_err(|_| RemoteMessageError::InvalidPayload)?,
    })
}

#[async_trait]
pub trait InboundDispatch: Send + Sync + 'static {
    async fn tell(
        &self,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;

    async fn ask(
        &self,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError>;
}

pub async fn serve_inbound_connection<S, D>(
    mut connection: FramedConnection<S>,
    dispatch: Arc<D>,
    outbound: Option<Arc<OutboundMessaging>>,
) -> Result<(), InboundConnectionError>
where
    S: RemotingIo,
    D: InboundDispatch + ?Sized,
{
    loop {
        let frame = connection.read_frame().await?;
        match frame.kind {
            FrameKind::Tell => {
                let tell = decode_tell(&frame)?;
                let _ = dispatch
                    .tell(tell.target, tell.message_id, tell.payload)
                    .await;
            }
            FrameKind::Ask => {
                let ask = decode_ask(&frame)?;
                let deadline = Instant::now()
                    .checked_add(ask.timeout_budget)
                    .ok_or(RemoteMessageError::DeadlineExceeded)?;
                let response = match dispatch
                    .ask(ask.target, ask.message_id, ask.payload, deadline)
                    .await
                {
                    Ok(payload) => reply_frame(ask.correlation_id, payload),
                    Err(error) => failure_frame(&RemoteFailure {
                        correlation_id: ask.correlation_id,
                        code: failure_code(&error),
                        safe_detail: None,
                    }),
                };
                connection.write_frame(&response).await?;
            }
            FrameKind::Reply => {
                let (correlation, payload) = decode_reply(&frame)?;
                if let Some(outbound) = &outbound {
                    outbound.complete_reply(correlation, payload);
                }
            }
            FrameKind::Failure => {
                let failure = decode_failure(&frame)?;
                if let Some(outbound) = &outbound {
                    outbound
                        .complete_failure(failure.correlation_id, AskError::Remote(failure.code));
                }
            }
            FrameKind::Heartbeat => {
                connection
                    .write_frame(&Frame {
                        kind: FrameKind::HeartbeatAck,
                        payload: Bytes::new(),
                    })
                    .await?;
            }
            FrameKind::HeartbeatAck | FrameKind::Backpressure => {}
            FrameKind::Close => return Ok(()),
            _ => return Err(InboundConnectionError::UnexpectedFrame(frame.kind)),
        }
    }
}

pub(crate) fn failure_code(error: &RemoteMessageError) -> RemoteFailureCode {
    match error {
        RemoteMessageError::StaleActivation => RemoteFailureCode::StaleActivation,
        RemoteMessageError::UnsupportedProtocol | RemoteMessageError::UnknownMessage => {
            RemoteFailureCode::UnknownMessage
        }
        RemoteMessageError::ProtocolFingerprintMismatch => RemoteFailureCode::ProtocolMismatch,
        RemoteMessageError::MailboxRejected => RemoteFailureCode::MailboxFull,
        RemoteMessageError::InvalidPayload => RemoteFailureCode::DecodeFailed,
        RemoteMessageError::DeadlineExceeded => RemoteFailureCode::DeadlineExceeded,
        RemoteMessageError::Unauthorized => RemoteFailureCode::Unauthorized,
        RemoteMessageError::ZeroPendingLimit | RemoteMessageError::HandlerFailed => {
            RemoteFailureCode::HandlerFailed
        }
    }
}

#[derive(Debug, Error)]
pub enum InboundConnectionError {
    #[error("inbound remoting socket failed")]
    Wire(#[from] crate::wire::WireError),
    #[error("inbound remoting message is invalid")]
    Message(#[from] RemoteMessageError),
    #[error("frame kind {0:?} is invalid on the actor data loop")]
    UnexpectedFrame(FrameKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RemoteFailureCode {
    StaleActivation = 1,
    UnknownMessage = 2,
    DecodeFailed = 3,
    MailboxFull = 4,
    MailboxClosed = 5,
    Unauthorized = 6,
    DeadlineExceeded = 7,
    HandlerFailed = 8,
    ProtocolMismatch = 9,
    Internal = 10,
}

impl TryFrom<u32> for RemoteFailureCode {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::StaleActivation),
            2 => Ok(Self::UnknownMessage),
            3 => Ok(Self::DecodeFailed),
            4 => Ok(Self::MailboxFull),
            5 => Ok(Self::MailboxClosed),
            6 => Ok(Self::Unauthorized),
            7 => Ok(Self::DeadlineExceeded),
            8 => Ok(Self::HandlerFailed),
            9 => Ok(Self::ProtocolMismatch),
            10 => Ok(Self::Internal),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Error)]
pub enum TellError {
    #[error("remote actor protocol is unavailable")]
    Protocol(#[source] RemoteMessageError),
    #[error("association rejected tell admission")]
    Association(#[source] AssociationError),
    #[error("remote or logical target rejected tell")]
    Remote(#[source] RemoteMessageError),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AskError {
    #[error("ask deadline exceeded")]
    DeadlineExceeded,
    #[error("pending ask limit reached")]
    PendingLimit,
    #[error("ask correlation sequence exhausted")]
    CorrelationExhausted,
    #[error("association failed before socket write commitment")]
    AssociationLostBeforeWrite,
    #[error("ask result is unknown after socket write commitment")]
    UnknownResult,
    #[error("remote actor protocol is unavailable")]
    Protocol(RemoteMessageError),
    #[error("association rejected ask admission: {0}")]
    Association(String),
    #[error("remote execution failed with code {0:?}")]
    Remote(RemoteFailureCode),
}

impl From<AssociationError> for AskError {
    fn from(value: AssociationError) -> Self {
        Self::Association(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RemoteMessageError {
    #[error("pending ask limit must be nonzero")]
    ZeroPendingLimit,
    #[error("peer does not support the actor protocol")]
    UnsupportedProtocol,
    #[error("peer actor protocol fingerprint differs")]
    ProtocolFingerprintMismatch,
    #[error("actor protocol does not register the message ID")]
    UnknownMessage,
    #[error("target actor activation is stale or absent")]
    StaleActivation,
    #[error("target actor mailbox rejected the message")]
    MailboxRejected,
    #[error("message payload is invalid")]
    InvalidPayload,
    #[error("message deadline elapsed")]
    DeadlineExceeded,
    #[error("message is unauthorized")]
    Unauthorized,
    #[error("remote handler failed")]
    HandlerFailed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::association::{AssociationKey, AssociationState, LaneAttachment, LaneKind};
    use crate::config::RemotingConfig;
    use crate::protocol::ProtocolDescriptor;
    use crate::transport::FramedConnection;
    use crate::wire::FrameCodec;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn active_association(
        protocol_id: ProtocolId,
        fingerprint: ProtocolFingerprint,
    ) -> Arc<Association> {
        let key = AssociationKey {
            cluster_id: ClusterId::new("test").unwrap(),
            local_incarnation: NodeIncarnation::new(1).unwrap(),
            remote_address: NodeAddress::new("remote", 25520).unwrap(),
            remote_incarnation: NodeIncarnation::new(2).unwrap(),
        };
        let association =
            Arc::new(Association::new(key.clone(), RemotingConfig::default()).unwrap());
        for (lane, nonce) in [
            (LaneKind::Control, 1),
            (LaneKind::Interactive, 2),
            (LaneKind::Bulk(0), 3),
        ] {
            association
                .attach(LaneAttachment {
                    association_id: association.id(),
                    key: key.clone(),
                    lane,
                    connection_nonce: nonce,
                })
                .unwrap();
        }
        association
            .install_peer_catalogue([ProtocolDescriptor {
                protocol_id,
                fingerprint,
            }])
            .unwrap();
        assert_eq!(association.state(), AssociationState::Active);
        association
    }

    fn target(protocol_id: ProtocolId) -> ActorRef<()> {
        let node = NodeIncarnation::new(2).unwrap();
        ActorRef::new(
            ClusterId::new("test").unwrap(),
            NodeAddress::new("remote", 25520).unwrap(),
            node,
            ActorPath::user(["user", "target"]).unwrap(),
            ActivationId::new(node, 1).unwrap(),
            protocol_id,
        )
        .unwrap()
    }

    struct RecordingDispatch {
        activation: ActivationId,
        tells: AtomicUsize,
    }

    #[async_trait]
    impl InboundDispatch for RecordingDispatch {
        async fn tell(
            &self,
            target: ExactActorTarget,
            _message_id: u64,
            _payload: Bytes,
        ) -> Result<(), RemoteMessageError> {
            if target.activation_id != self.activation {
                return Err(RemoteMessageError::StaleActivation);
            }
            self.tells.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }

        async fn ask(
            &self,
            target: ExactActorTarget,
            _message_id: u64,
            payload: Bytes,
            deadline: Instant,
        ) -> Result<Bytes, RemoteMessageError> {
            if target.activation_id != self.activation {
                return Err(RemoteMessageError::StaleActivation);
            }
            if Instant::now() >= deadline {
                return Err(RemoteMessageError::DeadlineExceeded);
            }
            Ok(payload)
        }
    }

    #[tokio::test]
    async fn real_tcp_tell_and_ask_dispatch_exact_activation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let protocol_id = ProtocolId::new(7).unwrap();
        let actor_ref = target(protocol_id);
        let dispatch = Arc::new(RecordingDispatch {
            activation: actor_ref.activation_id(),
            tells: AtomicUsize::new(0),
        });
        let server_dispatch = dispatch.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_inbound_connection(
                FramedConnection::new(stream, FrameCodec::new(4096).unwrap()),
                server_dispatch,
                None,
            )
            .await
            .unwrap();
        });
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut client = FramedConnection::new(stream, FrameCodec::new(4096).unwrap());
        let exact = ExactActorTarget::from(&actor_ref);
        client
            .write_frame(&Frame::encode_message(
                FrameKind::Tell,
                &TellWire {
                    sender: 9_u128.to_be_bytes().to_vec(),
                    target: Some(target_to_wire(&exact)),
                    message_id: 1,
                    payload: b"tell".to_vec(),
                },
            ))
            .await
            .unwrap();
        let correlation = CorrelationId::new(9, 1).unwrap();
        client
            .write_frame(&Frame::encode_message(
                FrameKind::Ask,
                &AskWire {
                    sender: 9_u128.to_be_bytes().to_vec(),
                    target: Some(target_to_wire(&exact)),
                    correlation_id: correlation.to_bytes().to_vec(),
                    timeout_nanos: Duration::from_secs(1).as_nanos() as u64,
                    message_id: 2,
                    payload: b"ask".to_vec(),
                },
            ))
            .await
            .unwrap();
        let reply = client.read_frame().await.unwrap();
        assert_eq!(
            decode_reply(&reply).unwrap(),
            (correlation, Bytes::from_static(b"ask"))
        );
        client
            .write_frame(&Frame {
                kind: FrameKind::Close,
                payload: Bytes::new(),
            })
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(dispatch.tells.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disconnect_result_changes_only_at_socket_write_boundary() {
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"test/v1");
        for (committed, expected) in [
            (false, AskError::AssociationLostBeforeWrite),
            (true, AskError::UnknownResult),
        ] {
            let association = active_association(protocol_id, fingerprint);
            let messaging = Arc::new(OutboundMessaging::new(4).unwrap());
            let task_messaging = messaging.clone();
            let task_association = association.clone();
            let actor_ref = target(protocol_id);
            let task = tokio::spawn(async move {
                task_messaging
                    .ask(
                        &task_association,
                        &SenderIdentity::Process(9),
                        &actor_ref,
                        fingerprint,
                        1,
                        Bytes::new(),
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await
            });
            tokio::task::yield_now().await;
            let correlation = messaging.pending_correlations()[0];
            if committed {
                assert!(messaging.mark_socket_write_started(correlation));
            }
            assert_eq!(messaging.fail_association(association.id()), 1);
            assert_eq!(task.await.unwrap().unwrap_err(), expected);
        }
    }

    #[tokio::test]
    async fn expired_queued_ask_is_dropped_before_socket_write() {
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"test/v1");
        let association = active_association(protocol_id, fingerprint);
        let mut interactive = association.take_receivers().unwrap().interactive;
        let messaging = Arc::new(OutboundMessaging::new(4).unwrap());
        let task_messaging = messaging.clone();
        let task_association = association.clone();
        let actor_ref = target(protocol_id);
        let task = tokio::spawn(async move {
            task_messaging
                .ask(
                    &task_association,
                    &SenderIdentity::Process(9),
                    &actor_ref,
                    fingerprint,
                    1,
                    Bytes::new(),
                    Instant::now() + Duration::from_millis(10),
                )
                .await
        });
        let mut frame = interactive.recv().await.unwrap();
        assert_eq!(task.await.unwrap().unwrap_err(), AskError::DeadlineExceeded);
        assert!(!messaging.prepare_ask_for_socket_write(&mut frame));
        assert_eq!(messaging.pending_count(), 0);
    }

    #[test]
    fn one_protocol_mismatch_does_not_close_the_association() {
        let protocol_id = ProtocolId::new(7).unwrap();
        let fingerprint = ProtocolFingerprint::digest(b"test/v1");
        let association = active_association(protocol_id, fingerprint);
        let messaging = OutboundMessaging::new(4).unwrap();
        let actor_ref = target(protocol_id);
        let mismatch = messaging.tell(
            &association,
            &SenderIdentity::Process(9),
            &actor_ref,
            ProtocolFingerprint::digest(b"other"),
            1,
            Bytes::new(),
        );
        assert!(matches!(
            mismatch,
            Err(TellError::Protocol(
                RemoteMessageError::ProtocolFingerprintMismatch
            ))
        ));
        assert_eq!(association.state(), AssociationState::Active);
        assert!(
            messaging
                .tell(
                    &association,
                    &SenderIdentity::Process(9),
                    &actor_ref,
                    fingerprint,
                    1,
                    Bytes::new(),
                )
                .is_ok()
        );
    }
}
