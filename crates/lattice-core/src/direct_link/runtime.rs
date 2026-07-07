use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;

use crate::{ActorRef, ServiceContext, TraceContext};

use super::{
    DirectLinkMessage, DirectLinkMessageId, DirectLinkMetadata, DirectLinkMode, DirectLinkOptions,
    DirectLinkStreamDescriptor, DirectLinkStreamSpec, DirectLinkStreamType, LinkCloseReason,
    LinkDirection, LinkError, LinkId, LinkMessageFlags, LinkSendError, LinkTarget,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundDirectLinkMessage {
    pub link_id: LinkId,
    pub direction: LinkDirection,
    pub message_id: DirectLinkMessageId,
    pub proto_full_name: &'static str,
    pub metadata: Vec<u8>,
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
        S: DirectLinkStreamSpec<Metadata = ()>,
        T: DirectLinkMessage,
    {
        let message = self.encode_message_with_metadata(payload, ())?;
        self.session.sender.tell(message).await
    }

    pub fn try_tell<T>(&self, payload: T) -> Result<(), LinkSendError>
    where
        S: DirectLinkStreamSpec<Metadata = ()>,
        T: DirectLinkMessage,
    {
        let message = self.encode_message_with_metadata(payload, ())?;
        self.session.sender.try_tell(message)
    }

    pub async fn tell_with_metadata<T>(
        &self,
        payload: T,
        metadata: S::Metadata,
    ) -> Result<(), LinkSendError>
    where
        S: DirectLinkStreamSpec,
        T: DirectLinkMessage,
    {
        let message = self.encode_message_with_metadata(payload, metadata)?;
        self.session.sender.tell(message).await
    }

    pub fn try_tell_with_metadata<T>(
        &self,
        payload: T,
        metadata: S::Metadata,
    ) -> Result<(), LinkSendError>
    where
        S: DirectLinkStreamSpec,
        T: DirectLinkMessage,
    {
        let message = self.encode_message_with_metadata(payload, metadata)?;
        self.session.sender.try_tell(message)
    }

    pub async fn close(&self, reason: LinkCloseReason) -> Result<(), LinkSendError> {
        self.session.sender.close(reason).await
    }

    fn encode_message_with_metadata<T>(
        &self,
        payload: T,
        metadata: S::Metadata,
    ) -> Result<OutboundDirectLinkMessage, LinkSendError>
    where
        S: DirectLinkStreamSpec,
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
        let metadata = metadata
            .encode_metadata()
            .map_err(|error| LinkSendError::EncodeMetadata(error.to_string()))?;
        Ok(OutboundDirectLinkMessage {
            link_id: self.session.link_id.clone(),
            direction: self.session.direction,
            message_id,
            proto_full_name: T::PROTO_FULL_NAME,
            metadata,
            payload: encoded,
            flags: LinkMessageFlags::EMPTY,
        })
    }
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

#[async_trait]
pub trait DirectLinkLifecycleRuntime: Send + Sync + fmt::Debug + 'static {
    async fn close_for_actor(
        &self,
        actor: ActorRef,
        reason: LinkCloseReason,
    ) -> Result<usize, LinkError>;
}

#[derive(Clone)]
pub struct DirectLinkLifecycleRuntimeHandle {
    runtime: Arc<dyn DirectLinkLifecycleRuntime>,
}

impl DirectLinkLifecycleRuntimeHandle {
    pub fn new(runtime: Arc<dyn DirectLinkLifecycleRuntime>) -> Self {
        Self { runtime }
    }

    pub fn runtime(&self) -> Arc<dyn DirectLinkLifecycleRuntime> {
        self.runtime.clone()
    }
}

impl fmt::Debug for DirectLinkLifecycleRuntimeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectLinkLifecycleRuntimeHandle")
            .finish_non_exhaustive()
    }
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
