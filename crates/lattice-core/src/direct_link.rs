use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use http::Uri;
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ActorRef, ServiceContext, TraceContext};

static NEXT_LINK_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LinkId(String);

impl LinkId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn next_local() -> Self {
        Self(format!(
            "local-{}",
            NEXT_LINK_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LinkId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DirectLinkMessageId(pub u64);

impl DirectLinkMessageId {
    pub fn for_proto(stream_name: &str, proto_full_name: &str) -> Self {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in stream_name
            .as_bytes()
            .iter()
            .copied()
            .chain([0])
            .chain(proto_full_name.as_bytes().iter().copied())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self(hash)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LinkSequence(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DirectLinkMode {
    Unidirectional,
    Bidirectional,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconnectPolicy {
    BusinessOwned,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CoalesceKey(pub String);

impl CoalesceKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackpressurePolicy {
    Block {
        max_pending: usize,
    },
    FailFast {
        max_pending: usize,
    },
    DropNewest {
        max_pending: usize,
    },
    DropOldest {
        max_pending: usize,
    },
    Coalesce {
        max_pending: usize,
        key: CoalesceKey,
    },
    Disconnect {
        max_pending: usize,
    },
}

impl BackpressurePolicy {
    pub fn max_pending(&self) -> usize {
        match self {
            Self::Block { max_pending }
            | Self::FailFast { max_pending }
            | Self::DropNewest { max_pending }
            | Self::DropOldest { max_pending }
            | Self::Coalesce { max_pending, .. }
            | Self::Disconnect { max_pending } => *max_pending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectLinkOptions {
    pub mode: DirectLinkMode,
    pub reconnect: ReconnectPolicy,
    pub backpressure: BackpressurePolicy,
    pub heartbeat_interval: Duration,
    pub idle_timeout: Duration,
    pub max_frame_size: usize,
}

impl DirectLinkOptions {
    pub fn unidirectional() -> Self {
        Self::default()
    }

    pub fn bidirectional() -> Self {
        Self {
            mode: DirectLinkMode::Bidirectional,
            ..Self::default()
        }
    }
}

impl Default for DirectLinkOptions {
    fn default() -> Self {
        Self {
            mode: DirectLinkMode::Unidirectional,
            reconnect: ReconnectPolicy::BusinessOwned,
            backpressure: BackpressurePolicy::FailFast { max_pending: 1024 },
            heartbeat_interval: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            max_frame_size: 256 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkCloseReason {
    Done,
    LocalClose,
    RemoteClose,
    HeartbeatTimeout,
    BackpressureExceeded,
    ProtocolError(String),
    Unauthorized,
    TargetPassivated,
    TargetMigrating,
    NodeDraining,
    ConnectionLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LinkDirection {
    SourceToTarget,
    TargetToSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectLinkEndpoint {
    #[serde(with = "crate::uri_serde")]
    pub uri: Uri,
}

impl DirectLinkEndpoint {
    pub fn new(uri: Uri) -> Self {
        Self { uri }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkTarget {
    Actor(ActorRef),
    Endpoint {
        endpoint: DirectLinkEndpoint,
        target: ActorRef,
    },
}

impl From<ActorRef> for LinkTarget {
    fn from(value: ActorRef) -> Self {
        Self::Actor(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectLinkMessageDescriptor {
    pub message_id: DirectLinkMessageId,
    pub proto_full_name: String,
    pub rust_type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectLinkStreamDescriptor {
    pub stream_name: String,
    pub messages: Vec<DirectLinkMessageDescriptor>,
}

impl DirectLinkStreamDescriptor {
    pub fn new(stream_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            messages: Vec::new(),
        }
    }

    pub fn message_id_for<T>(&self) -> Option<DirectLinkMessageId>
    where
        T: DirectLinkMessage,
    {
        self.messages
            .iter()
            .find(|message| message.proto_full_name == T::PROTO_FULL_NAME)
            .map(|message| message.message_id)
    }

    pub fn accepted_message_ids(&self) -> BTreeSet<DirectLinkMessageId> {
        self.messages
            .iter()
            .map(|message| message.message_id)
            .collect()
    }

    pub fn duplicate_message_id(&self) -> Option<DirectLinkMessageId> {
        let mut seen = BTreeSet::new();
        self.messages
            .iter()
            .map(|message| message.message_id)
            .find(|id| !seen.insert(*id))
    }
}

pub trait DirectLinkMessage: ProstMessage + Default + Send + Sync + 'static {
    const PROTO_FULL_NAME: &'static str;
}

pub trait DirectLinkStreamSpec: Clone + Send + Sync + 'static {
    fn descriptor(&self) -> DirectLinkStreamDescriptor;
}

pub trait DirectLinkStreamType: Clone + Send + Sync + 'static {
    fn descriptor() -> DirectLinkStreamDescriptor;
}

impl<T> DirectLinkStreamSpec for T
where
    T: DirectLinkStreamType,
{
    fn descriptor(&self) -> DirectLinkStreamDescriptor {
        T::descriptor()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkMessageFlags {
    bits: u32,
}

impl LinkMessageFlags {
    pub const EMPTY: Self = Self { bits: 0 };

    pub fn from_bits(bits: u32) -> Self {
        Self { bits }
    }

    pub fn bits(&self) -> u32 {
        self.bits
    }
}

impl Default for LinkMessageFlags {
    fn default() -> Self {
        Self::EMPTY
    }
}

#[derive(Debug, Clone)]
pub struct LinkMessageContext {
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: ActorRef,
    pub sequence: u64,
    pub received_at: Instant,
    pub flags: LinkMessageFlags,
}

#[derive(Debug, Clone)]
pub struct Linked<T> {
    pub payload: T,
    pub context: LinkMessageContext,
}

#[derive(Debug, Clone)]
pub struct LinkOpened {
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: ActorRef,
    pub mode: DirectLinkMode,
    pub inbound_stream: String,
    pub inbound_accepted_message_types: BTreeSet<DirectLinkMessageId>,
    pub outbound_stream: Option<String>,
    pub outbound_accepted_message_types: BTreeSet<DirectLinkMessageId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkDirectionClosed {
    pub link_id: LinkId,
    pub direction: LinkDirection,
    pub stream: String,
    pub reason: LinkCloseReason,
    pub last_sequence_seen: Option<LinkSequence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkClosed {
    pub link_id: LinkId,
    pub reason: LinkCloseReason,
    pub closed_directions: BTreeSet<LinkDirection>,
    pub last_sequence_seen: Option<LinkSequence>,
}

#[derive(Debug, Clone)]
pub struct LinkBackpressure {
    pub link_id: LinkId,
    pub policy: BackpressurePolicy,
    pub pending: usize,
    pub dropped: u64,
    pub coalesced: u64,
}

#[derive(Debug, Clone)]
pub struct LinkProtocolError {
    pub link_id: LinkId,
    pub error: String,
    pub close_action: LinkCloseReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundDirectLinkMessage {
    pub link_id: LinkId,
    pub direction: LinkDirection,
    pub message_id: DirectLinkMessageId,
    pub proto_full_name: &'static str,
    pub payload: Vec<u8>,
    pub flags: LinkMessageFlags,
}

#[derive(Debug, Clone)]
pub struct DirectLinkSession {
    pub link_id: LinkId,
    pub direction: LinkDirection,
    pub stream: DirectLinkStreamDescriptor,
    pub accepted_message_ids: BTreeSet<DirectLinkMessageId>,
    pub sender: Arc<dyn DirectLinkSender>,
}

#[async_trait]
pub trait DirectLinkSender: Send + Sync + fmt::Debug + 'static {
    async fn tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError>;
    fn try_tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError>;
    async fn close(&self, reason: LinkCloseReason) -> Result<(), LinkSendError>;
}

#[derive(Debug, Clone)]
pub struct DirectLink<S> {
    session: DirectLinkSession,
    _stream: PhantomData<fn() -> S>,
}

impl<S> DirectLink<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(session: DirectLinkSession) -> Self {
        Self {
            session,
            _stream: PhantomData,
        }
    }

    pub fn id(&self) -> &LinkId {
        &self.session.link_id
    }

    pub fn direction(&self) -> LinkDirection {
        self.session.direction
    }

    pub fn stream(&self) -> &DirectLinkStreamDescriptor {
        &self.session.stream
    }

    pub async fn tell<T>(&self, payload: T) -> Result<(), LinkSendError>
    where
        T: DirectLinkMessage,
    {
        let message = self.encode_message(payload)?;
        self.session.sender.tell(message).await
    }

    pub fn try_tell<T>(&self, payload: T) -> Result<(), LinkSendError>
    where
        T: DirectLinkMessage,
    {
        let message = self.encode_message(payload)?;
        self.session.sender.try_tell(message)
    }

    pub async fn close(&self, reason: LinkCloseReason) -> Result<(), LinkSendError> {
        self.session.sender.close(reason).await
    }

    fn encode_message<T>(&self, payload: T) -> Result<OutboundDirectLinkMessage, LinkSendError>
    where
        T: DirectLinkMessage,
    {
        let message_id = self
            .session
            .stream
            .message_id_for::<T>()
            .ok_or(LinkSendError::UnsupportedMessageType)?;
        if !self.session.accepted_message_ids.contains(&message_id) {
            return Err(LinkSendError::UnsupportedMessageType);
        }
        let mut encoded = Vec::with_capacity(payload.encoded_len());
        payload
            .encode(&mut encoded)
            .map_err(|error| LinkSendError::Encode(error.to_string()))?;
        Ok(OutboundDirectLinkMessage {
            link_id: self.session.link_id.clone(),
            direction: self.session.direction,
            message_id,
            proto_full_name: T::PROTO_FULL_NAME,
            payload: encoded,
            flags: LinkMessageFlags::EMPTY,
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LinkSendError {
    #[error("direct link is closed: {reason:?}")]
    Closed { reason: LinkCloseReason },
    #[error("direct link backpressure queue is full")]
    BackpressureFull,
    #[error("message type is not supported by this direct link stream")]
    UnsupportedMessageType,
    #[error("encoded message is larger than the negotiated direct link frame size")]
    MessageTooLarge,
    #[error("failed to encode direct link message: {0}")]
    Encode(String),
    #[error("direct link protocol error: {0}")]
    Protocol(String),
}

#[derive(Debug, Error)]
pub enum LinkError {
    #[error("direct link runtime is not configured")]
    Unavailable,
    #[error("actor context has no source ActorRef")]
    MissingSourceActor,
    #[error("direct link stream {stream_name} has duplicate message id {message_id:?}")]
    DuplicateMessageId {
        stream_name: String,
        message_id: DirectLinkMessageId,
    },
    #[error("target is not the current direct link owner")]
    NotOwner { redirect: Option<Box<ActorRef>> },
    #[error("target owner epoch is fenced")]
    Fenced,
    #[error("target actor is unavailable")]
    ActorUnavailable,
    #[error("direct link stream is unsupported")]
    UnsupportedStream,
    #[error("direct link message type is unsupported")]
    UnsupportedMessageType,
    #[error("direct link is unauthorized")]
    Unauthorized,
    #[error("target is overloaded")]
    Overloaded,
    #[error("direct link protocol version is unsupported")]
    ProtocolVersionMismatch,
    #[error("direct link protocol error: {0}")]
    Protocol(String),
}

#[derive(Debug, Clone)]
pub struct DirectLinkOpenRequest {
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: LinkTarget,
    pub mode: DirectLinkMode,
    pub source_to_target: DirectLinkStreamDescriptor,
    pub target_to_source: Option<DirectLinkStreamDescriptor>,
    pub options: DirectLinkOptions,
    pub trace: TraceContext,
}

#[async_trait]
pub trait DirectLinkRuntime: Send + Sync + fmt::Debug + 'static {
    async fn open_link(
        &self,
        request: DirectLinkOpenRequest,
    ) -> Result<DirectLinkSession, LinkError>;

    async fn get_outbound(
        &self,
        link_id: LinkId,
        stream: DirectLinkStreamDescriptor,
    ) -> Result<DirectLinkSession, LinkError>;

    async fn close_all(&self, link_id: LinkId, reason: LinkCloseReason) -> Result<(), LinkError>;
}

#[derive(Clone)]
pub struct DirectLinkRuntimeHandle {
    runtime: Arc<dyn DirectLinkRuntime>,
}

impl DirectLinkRuntimeHandle {
    pub fn new(runtime: Arc<dyn DirectLinkRuntime>) -> Self {
        Self { runtime }
    }

    pub fn runtime(&self) -> Arc<dyn DirectLinkRuntime> {
        self.runtime.clone()
    }
}

impl fmt::Debug for DirectLinkRuntimeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectLinkRuntimeHandle")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct DirectLinkManager {
    service: ServiceContext,
    source: Option<ActorRef>,
    runtime: Option<Arc<dyn DirectLinkRuntime>>,
}

impl DirectLinkManager {
    pub fn new(service: ServiceContext, source: Option<ActorRef>) -> Self {
        let runtime = service
            .extension::<DirectLinkRuntimeHandle>()
            .map(|handle| handle.runtime());
        Self {
            service,
            source,
            runtime,
        }
    }

    pub fn service(&self) -> &ServiceContext {
        &self.service
    }

    pub async fn connect<S, T>(
        &self,
        target: T,
        stream: S,
        options: DirectLinkOptions,
    ) -> Result<DirectLink<S>, LinkError>
    where
        S: DirectLinkStreamSpec,
        T: Into<LinkTarget>,
    {
        let mut options = options;
        options.mode = DirectLinkMode::Unidirectional;
        let source_to_target = stream.descriptor();
        validate_stream(&source_to_target)?;
        let session = self
            .runtime()?
            .open_link(DirectLinkOpenRequest {
                link_id: LinkId::next_local(),
                source: self.source.clone().ok_or(LinkError::MissingSourceActor)?,
                target: target.into(),
                mode: DirectLinkMode::Unidirectional,
                source_to_target,
                target_to_source: None,
                options,
                trace: TraceContext::default(),
            })
            .await?;
        Ok(DirectLink::new(session))
    }

    pub async fn connect_bidirectional<Out, In, T>(
        &self,
        target: T,
        source_to_target: Out,
        target_to_source: In,
        options: DirectLinkOptions,
    ) -> Result<DirectLink<Out>, LinkError>
    where
        Out: DirectLinkStreamSpec,
        In: DirectLinkStreamSpec,
        T: Into<LinkTarget>,
    {
        let mut options = options;
        options.mode = DirectLinkMode::Bidirectional;
        let outbound = source_to_target.descriptor();
        let inbound = target_to_source.descriptor();
        validate_stream(&outbound)?;
        validate_stream(&inbound)?;
        let session = self
            .runtime()?
            .open_link(DirectLinkOpenRequest {
                link_id: LinkId::next_local(),
                source: self.source.clone().ok_or(LinkError::MissingSourceActor)?,
                target: target.into(),
                mode: DirectLinkMode::Bidirectional,
                source_to_target: outbound,
                target_to_source: Some(inbound),
                options,
                trace: TraceContext::default(),
            })
            .await?;
        Ok(DirectLink::new(session))
    }

    pub async fn get<S>(&self, link_id: LinkId) -> Result<DirectLink<S>, LinkError>
    where
        S: DirectLinkStreamType,
    {
        let stream = S::descriptor();
        validate_stream(&stream)?;
        let session = self.runtime()?.get_outbound(link_id, stream).await?;
        Ok(DirectLink::new(session))
    }

    pub async fn close_all(
        &self,
        link_id: LinkId,
        reason: LinkCloseReason,
    ) -> Result<(), LinkError> {
        self.runtime()?.close_all(link_id, reason).await
    }

    fn runtime(&self) -> Result<Arc<dyn DirectLinkRuntime>, LinkError> {
        self.runtime.clone().ok_or(LinkError::Unavailable)
    }
}

fn validate_stream(stream: &DirectLinkStreamDescriptor) -> Result<(), LinkError> {
    if let Some(message_id) = stream.duplicate_message_id() {
        return Err(LinkError::DuplicateMessageId {
            stream_name: stream.stream_name.clone(),
            message_id,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::{ActorId, ActorRef, InstanceId, ServiceKind};

    #[derive(Clone, PartialEq, prost::Message)]
    struct InputCommand {
        #[prost(uint64, tag = "1")]
        command_id: u64,
    }

    impl DirectLinkMessage for InputCommand {
        const PROTO_FULL_NAME: &'static str = "game.InputCommand";
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct StateDelta {
        #[prost(uint64, tag = "1")]
        tick: u64,
    }

    impl DirectLinkMessage for StateDelta {
        const PROTO_FULL_NAME: &'static str = "game.StateDelta";
    }

    #[derive(Clone)]
    struct SourceToTargetStream;

    impl DirectLinkStreamType for SourceToTargetStream {
        fn descriptor() -> DirectLinkStreamDescriptor {
            stream("gateway-input", &[message::<InputCommand>("gateway-input")])
        }
    }

    #[derive(Clone)]
    struct TargetToSourceStream;

    impl DirectLinkStreamType for TargetToSourceStream {
        fn descriptor() -> DirectLinkStreamDescriptor {
            stream("battle-update", &[message::<StateDelta>("battle-update")])
        }
    }

    #[derive(Debug, Default)]
    struct RecordingRuntime {
        requests: Mutex<Vec<DirectLinkOpenRequest>>,
        sender: Arc<RecordingSender>,
    }

    #[async_trait]
    impl DirectLinkRuntime for RecordingRuntime {
        async fn open_link(
            &self,
            request: DirectLinkOpenRequest,
        ) -> Result<DirectLinkSession, LinkError> {
            let session = DirectLinkSession {
                link_id: request.link_id.clone(),
                direction: LinkDirection::SourceToTarget,
                stream: request.source_to_target.clone(),
                accepted_message_ids: request.source_to_target.accepted_message_ids(),
                sender: self.sender.clone(),
            };
            self.requests
                .lock()
                .expect("open requests mutex poisoned")
                .push(request);
            Ok(session)
        }

        async fn get_outbound(
            &self,
            link_id: LinkId,
            stream: DirectLinkStreamDescriptor,
        ) -> Result<DirectLinkSession, LinkError> {
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
            _reason: LinkCloseReason,
        ) -> Result<(), LinkError> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingSender {
        messages: Mutex<Vec<OutboundDirectLinkMessage>>,
    }

    #[async_trait]
    impl DirectLinkSender for RecordingSender {
        async fn tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
            self.try_tell(message)
        }

        fn try_tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
            self.messages
                .lock()
                .expect("sent messages mutex poisoned")
                .push(message);
            Ok(())
        }

        async fn close(&self, _reason: LinkCloseReason) -> Result<(), LinkSendError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn connect_bidirectional_returns_source_to_target_send_handle() {
        let runtime = Arc::new(RecordingRuntime::default());
        let mut context = ServiceContext::builder(
            ServiceKind::from_static("Gateway"),
            InstanceId::new("gateway-1"),
        );
        context
            .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
            .unwrap();
        let source = actor_ref("Gateway", "GatewaySession", 7);
        let target = actor_ref("Battle", "Battle", 9);
        let manager = DirectLinkManager::new(context.build(), Some(source.clone()));

        let link = manager
            .connect_bidirectional(
                target.clone(),
                SourceToTargetStream,
                TargetToSourceStream,
                DirectLinkOptions::default(),
            )
            .await
            .unwrap();
        link.try_tell(InputCommand { command_id: 42 }).unwrap();

        assert_eq!(link.direction(), LinkDirection::SourceToTarget);
        assert_eq!(
            link.stream(),
            &<SourceToTargetStream as DirectLinkStreamType>::descriptor()
        );
        let requests = runtime
            .requests
            .lock()
            .expect("open requests mutex poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].source, source);
        assert_eq!(requests[0].target, LinkTarget::Actor(target));
        assert_eq!(requests[0].mode, DirectLinkMode::Bidirectional);
        assert_eq!(requests[0].options.mode, DirectLinkMode::Bidirectional);
        assert_eq!(
            requests[0].source_to_target,
            <SourceToTargetStream as DirectLinkStreamType>::descriptor()
        );
        assert_eq!(
            requests[0].target_to_source,
            Some(<TargetToSourceStream as DirectLinkStreamType>::descriptor())
        );

        let messages = runtime
            .sender
            .messages
            .lock()
            .expect("sent messages mutex poisoned");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].link_id, *link.id());
        assert_eq!(messages[0].direction, LinkDirection::SourceToTarget);
        assert_eq!(
            messages[0].message_id,
            DirectLinkMessageId::for_proto("gateway-input", InputCommand::PROTO_FULL_NAME)
        );
        assert_eq!(messages[0].proto_full_name, InputCommand::PROTO_FULL_NAME);
    }

    fn stream(
        stream_name: impl Into<String>,
        messages: &[DirectLinkMessageDescriptor],
    ) -> DirectLinkStreamDescriptor {
        DirectLinkStreamDescriptor {
            stream_name: stream_name.into(),
            messages: messages.to_vec(),
        }
    }

    fn message<T>(stream_name: &str) -> DirectLinkMessageDescriptor
    where
        T: DirectLinkMessage,
    {
        DirectLinkMessageDescriptor {
            message_id: DirectLinkMessageId::for_proto(stream_name, T::PROTO_FULL_NAME),
            proto_full_name: T::PROTO_FULL_NAME.to_string(),
            rust_type_name: std::any::type_name::<T>().to_string(),
        }
    }

    fn actor_ref(service_kind: &'static str, actor_kind: &'static str, id: u64) -> ActorRef {
        ActorRef::direct(
            ServiceKind::from_static(service_kind),
            crate::ActorKind::from_static(actor_kind),
            ActorId::U64(id),
            InstanceId::new(format!("{service_kind}-{id}")),
            "http://127.0.0.1:10000".parse().unwrap(),
            None,
        )
    }

    #[test]
    fn stream_descriptors_have_expected_message_ids() {
        assert_eq!(
            <SourceToTargetStream as DirectLinkStreamType>::descriptor().accepted_message_ids(),
            BTreeSet::from([DirectLinkMessageId::for_proto(
                "gateway-input",
                InputCommand::PROTO_FULL_NAME
            )])
        );
        assert_eq!(
            <TargetToSourceStream as DirectLinkStreamType>::descriptor().accepted_message_ids(),
            BTreeSet::from([DirectLinkMessageId::for_proto(
                "battle-update",
                StateDelta::PROTO_FULL_NAME
            )])
        );
    }
}
