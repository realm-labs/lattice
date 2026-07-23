use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::Instant,
};

use bytes::{Bytes, BytesMut};
#[doc(hidden)]
pub use lattice_core::actor_ref::ProtocolTag as __ProtocolTag;
use lattice_core::actor_ref::{ActorRef, ProtocolId, ProtocolTag};
use lattice_remoting::protocol::{ProtocolDescriptor, ProtocolFingerprint};
use thiserror::Error;

use crate::{
    error::ActorCallError,
    handle::ActorHandle,
    traits::{Actor, Message, MessageKind, Request, Responder},
};

pub(crate) mod tell;

use tell::ProtocolTellDispatch;

mod helpers;

use helpers::{bounded_error, canonical_descriptor, protocol_failure};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CodecDescriptor {
    pub id: u64,
    pub version: u32,
}

impl CodecDescriptor {
    pub const fn new(id: u64, version: u32) -> Self {
        Self { id, version }
    }
}

pub trait WireCodec<T>: Send + Sync + 'static {
    const DESCRIPTOR: CodecDescriptor;

    fn encode(&self, value: &T, output: &mut BytesMut) -> Result<(), EncodeError>;
    fn decode(&self, input: &[u8]) -> Result<T, DecodeError>;

    /// Returns the encoded size when the codec can determine it without
    /// encoding the value.
    ///
    /// The default keeps custom codecs source-compatible. Size-aware codecs
    /// let the protocol allocate the final payload buffer exactly once.
    fn encoded_len(&self, _value: &T) -> Option<usize> {
        None
    }

    /// Encodes one value into an immutable wire payload.
    ///
    /// Codecs with an already-owned immutable representation may override
    /// this method to avoid copying it into an intermediate buffer.
    fn encode_to_bytes(&self, value: &T) -> Result<Bytes, EncodeError> {
        let mut output = self
            .encoded_len(value)
            .map_or_else(BytesMut::new, BytesMut::with_capacity);
        self.encode(value, &mut output)?;
        Ok(output.freeze())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UnitCodec;

#[derive(Debug, Clone, Copy, Default)]
pub struct ProstCodec;

impl<T> WireCodec<T> for ProstCodec
where
    T: prost::Message + Default + Send + Sync + 'static,
{
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(0x7072_6f73_7400_0001, 1);

    fn encode(&self, value: &T, output: &mut BytesMut) -> Result<(), EncodeError> {
        prost::Message::encode(value, output).map_err(|error| EncodeError::new(error.to_string()))
    }

    fn decode(&self, input: &[u8]) -> Result<T, DecodeError> {
        T::decode(input).map_err(|error| DecodeError::new(error.to_string()))
    }

    fn encoded_len(&self, value: &T) -> Option<usize> {
        Some(prost::Message::encoded_len(value))
    }
}

impl WireCodec<()> for UnitCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, _value: &(), _output: &mut BytesMut) -> Result<(), EncodeError> {
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<(), DecodeError> {
        if input.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::new("unit payload must be empty"))
        }
    }

    fn encoded_len(&self, _value: &()) -> Option<usize> {
        Some(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("wire encoding failed: {message}")]
pub struct EncodeError {
    message: String,
}

impl EncodeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: bounded_error(message.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("wire decoding failed: {message}")]
pub struct DecodeError {
    message: String,
}

impl DecodeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: bounded_error(message.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DispatchMode {
    Tell,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchReply {
    TellAccepted,
    Ask(Bytes),
}

pub(crate) type DispatchFuture =
    Pin<Box<dyn Future<Output = Result<DispatchReply, DispatchError>> + Send + 'static>>;
type DispatchFn<A> = dyn Fn(ActorHandle<A>, Bytes, Option<Instant>, Option<ActorRef>) -> DispatchFuture
    + Send
    + Sync
    + 'static;
type TellDispatchFn<A> = dyn Fn(&ActorHandle<A>, Bytes, Option<ActorRef>) -> ProtocolTellDispatch
    + Send
    + Sync
    + 'static;

enum ServerDispatch<A: Actor> {
    Tell(Arc<TellDispatchFn<A>>),
    Async(Arc<DispatchFn<A>>),
}
type EncodeRequestFn = dyn Fn(&dyn Any) -> Result<Bytes, EncodeError> + Send + Sync;
type DecodeReplyFn = dyn Fn(&[u8]) -> Result<Box<dyn Any + Send>, DecodeError> + Send + Sync;

pub trait Protocol: ProtocolTag {
    const ID: u64;

    fn build_protocol() -> Result<ActorProtocol<Self>, ProtocolBuildError>
    where
        Self: Sized;
}

/// Builds the server-side dispatcher for a protocol and actor pair.
///
/// Implementations are generated by [macro@crate::actor_protocol]. Keeping this as a
/// trait lets higher-level service builders bind protocols without requiring
/// application code to construct and retain an `ActorProtocolBinding`.
pub trait ActorProtocolBinder<A: Actor>: Protocol {
    fn bind_actor() -> Result<ActorProtocolBinding<A, Self>, ProtocolBuildError>
    where
        Self: Sized;
}

pub trait SupportsTell<M: Message>: Protocol {}

pub trait SupportsAsk<R: Request>: Protocol {}

struct ClientBinding {
    descriptor: MessageDescriptor,
    request_type: TypeId,
    encode_request: Box<EncodeRequestFn>,
    decode_reply: Option<Box<DecodeReplyFn>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MessageDescriptor {
    message_id: u64,
    mode: DispatchMode,
    request_codec: CodecDescriptor,
    request_schema_version: u32,
    response_codec: Option<CodecDescriptor>,
    response_schema_version: Option<u32>,
    max_payload: usize,
}

pub struct ActorProtocol<P: Protocol> {
    protocol_id: ProtocolId,
    name: String,
    fingerprint: ProtocolFingerprint,
    bindings: Box<[ClientBinding]>,
    bindings_by_id: HashMap<u64, usize>,
    bindings_by_type: HashMap<TypeId, usize>,
    protocol: PhantomData<fn() -> P>,
}

impl<P: Protocol> ActorProtocol<P> {
    pub fn builder(name: impl Into<String>) -> ActorProtocolBuilder<P> {
        ActorProtocolBuilder {
            name: name.into(),
            max_payload: 256 * 1024,
            bindings: Vec::new(),
            protocol: PhantomData,
        }
    }

    pub fn protocol_id(&self) -> ProtocolId {
        self.protocol_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn fingerprint(&self) -> ProtocolFingerprint {
        self.fingerprint
    }

    pub fn catalogue_descriptor(&self) -> ProtocolDescriptor {
        ProtocolDescriptor {
            protocol_id: self.protocol_id,
            fingerprint: self.fingerprint,
        }
    }

    pub fn encode_request<T: Send + 'static>(
        &self,
        mode: DispatchMode,
        message: &T,
    ) -> Result<(u64, Bytes), DispatchError> {
        let binding_index = self
            .bindings_by_type
            .get(&TypeId::of::<T>())
            .ok_or(DispatchError::UnregisteredType)?;
        let binding = &self.bindings[*binding_index];
        if binding.descriptor.mode != mode {
            return Err(DispatchError::ModeMismatch);
        }
        let payload = (binding.encode_request)(message).map_err(DispatchError::Encode)?;
        if payload.len() > binding.descriptor.max_payload {
            return Err(DispatchError::PayloadTooLarge {
                actual: payload.len(),
                maximum: binding.descriptor.max_payload,
            });
        }
        Ok((binding.descriptor.message_id, payload))
    }

    pub fn decode_response<R: Request>(
        &self,
        message_id: u64,
        payload: &[u8],
    ) -> Result<R::Response, DispatchError> {
        let binding_index = self
            .bindings_by_id
            .get(&message_id)
            .ok_or(DispatchError::UnknownMessage(message_id))?;
        let binding = &self.bindings[*binding_index];
        let decode = binding
            .decode_reply
            .as_ref()
            .ok_or(DispatchError::ModeMismatch)?;
        decode(payload)
            .map_err(DispatchError::Decode)?
            .downcast::<R::Response>()
            .map(|reply| *reply)
            .map_err(|_| DispatchError::ReplyTypeMismatch)
    }
}

pub struct ActorProtocolBuilder<P: Protocol> {
    name: String,
    max_payload: usize,
    bindings: Vec<ClientBinding>,
    protocol: PhantomData<fn() -> P>,
}

impl<P: Protocol> ActorProtocolBuilder<P> {
    pub fn max_payload(mut self, maximum: usize) -> Self {
        self.max_payload = maximum;
        self
    }

    pub fn tell<M, C>(mut self, message_id: u64, schema_version: u32, codec: C) -> Self
    where
        M: Message,
        C: WireCodec<M>,
    {
        let codec = Arc::new(codec);
        self.bindings.push(ClientBinding {
            descriptor: MessageDescriptor {
                message_id,
                mode: DispatchMode::Tell,
                request_codec: C::DESCRIPTOR,
                request_schema_version: schema_version,
                response_codec: None,
                response_schema_version: None,
                max_payload: self.max_payload,
            },
            request_type: TypeId::of::<M>(),
            encode_request: {
                let codec = codec.clone();
                Box::new(move |message| {
                    let message = message
                        .downcast_ref::<M>()
                        .ok_or_else(|| EncodeError::new("message type does not match binding"))?;
                    codec.encode_to_bytes(message)
                })
            },
            decode_reply: None,
        });
        self
    }

    pub fn ask<Q, C, RC>(
        mut self,
        message_id: u64,
        request_schema_version: u32,
        response_schema_version: u32,
        codec: C,
        reply_codec: RC,
    ) -> Self
    where
        Q: Request,
        C: WireCodec<Q>,
        RC: WireCodec<Q::Response>,
    {
        let codec = Arc::new(codec);
        let reply_codec = Arc::new(reply_codec);
        self.bindings.push(ClientBinding {
            descriptor: MessageDescriptor {
                message_id,
                mode: DispatchMode::Ask,
                request_codec: C::DESCRIPTOR,
                request_schema_version,
                response_codec: Some(RC::DESCRIPTOR),
                response_schema_version: Some(response_schema_version),
                max_payload: self.max_payload,
            },
            request_type: TypeId::of::<Q>(),
            encode_request: {
                let codec = codec.clone();
                Box::new(move |message| {
                    let message = message
                        .downcast_ref::<Q>()
                        .ok_or_else(|| EncodeError::new("message type does not match binding"))?;
                    codec.encode_to_bytes(message)
                })
            },
            decode_reply: {
                let reply_codec = reply_codec.clone();
                Some(Box::new(move |input: &[u8]| {
                    let reply = reply_codec.decode(input)?;
                    Ok(Box::new(reply) as Box<dyn Any + Send>)
                }))
            },
        });
        self
    }

    pub fn build(self) -> Result<ActorProtocol<P>, ProtocolBuildError> {
        if self.name.is_empty() || self.name.len() > 128 || self.name.chars().any(char::is_control)
        {
            return Err(ProtocolBuildError::InvalidName);
        }
        if self.max_payload == 0 {
            return Err(ProtocolBuildError::ZeroPayloadLimit);
        }
        let mut bindings = BTreeMap::new();
        let mut request_types = BTreeSet::new();
        for binding in self.bindings {
            if binding.descriptor.message_id == 0 {
                return Err(ProtocolBuildError::ReservedMessageId);
            }
            let message_id = binding.descriptor.message_id;
            if binding.descriptor.request_schema_version == 0
                || binding.descriptor.response_schema_version == Some(0)
            {
                return Err(ProtocolBuildError::ZeroSchemaVersion(message_id));
            }
            if !request_types.insert(binding.request_type) {
                return Err(ProtocolBuildError::DuplicateMessageType);
            }
            if bindings.insert(message_id, binding).is_some() {
                return Err(ProtocolBuildError::DuplicateMessageId(message_id));
            }
        }
        if bindings.is_empty() {
            return Err(ProtocolBuildError::Empty);
        }
        let protocol_id = protocol_id::<P>()?;
        let canonical = canonical_descriptor(protocol_id, &self.name, &bindings);
        let mut bindings_by_id = HashMap::with_capacity(bindings.len());
        let mut bindings_by_type = HashMap::with_capacity(bindings.len());
        let bindings = bindings
            .into_iter()
            .enumerate()
            .map(|(binding_index, (message_id, binding))| {
                bindings_by_type.insert(binding.request_type, binding_index);
                bindings_by_id.insert(message_id, binding_index);
                binding
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(ActorProtocol {
            protocol_id,
            name: self.name,
            fingerprint: ProtocolFingerprint::digest(&canonical),
            bindings,
            bindings_by_id,
            bindings_by_type,
            protocol: PhantomData,
        })
    }
}

pub struct ActorProtocolBinding<A: Actor, P: Protocol> {
    protocol: Arc<ActorProtocol<P>>,
    dispatch: BTreeMap<u64, ServerDispatch<A>>,
}

impl<A: Actor, P: Protocol> ActorProtocolBinding<A, P> {
    pub fn builder(name: impl Into<String>) -> ActorProtocolBindingBuilder<A, P> {
        ActorProtocolBindingBuilder {
            client: ActorProtocol::<P>::builder(name),
            dispatch: Vec::new(),
            actor: PhantomData,
        }
    }

    pub fn protocol(&self) -> &Arc<ActorProtocol<P>> {
        &self.protocol
    }

    pub fn protocol_id(&self) -> ProtocolId {
        self.protocol.protocol_id()
    }

    pub fn fingerprint(&self) -> ProtocolFingerprint {
        self.protocol.fingerprint()
    }

    pub async fn dispatch(
        &self,
        handle: ActorHandle<A>,
        message_id: u64,
        mode: DispatchMode,
        payload: Bytes,
        deadline: Option<Instant>,
    ) -> Result<DispatchReply, DispatchError> {
        self.dispatch_with_sender(handle, message_id, mode, payload, deadline, None)
            .await
    }

    #[doc(hidden)]
    pub async fn dispatch_with_sender(
        &self,
        handle: ActorHandle<A>,
        message_id: u64,
        mode: DispatchMode,
        payload: Bytes,
        deadline: Option<Instant>,
        sender: Option<ActorRef>,
    ) -> Result<DispatchReply, DispatchError> {
        if mode == DispatchMode::Tell {
            return match self.try_dispatch_tell(&handle, message_id, payload, sender) {
                ProtocolTellDispatch::Accepted => Ok(DispatchReply::TellAccepted),
                ProtocolTellDispatch::Deferred { completion, .. } => completion.await,
                ProtocolTellDispatch::Rejected(error) => Err(error),
            };
        }
        let observer = handle.observer().clone();
        let actor = handle.observation_metadata().clone();
        let payload_size = payload.len();
        let kind = MessageKind::Request;
        let result = async {
            let binding_index = self
                .protocol
                .bindings_by_id
                .get(&message_id)
                .ok_or(DispatchError::UnknownMessage(message_id))?;
            let client = &self.protocol.bindings[*binding_index];
            if client.descriptor.mode != mode {
                return Err(DispatchError::ModeMismatch);
            }
            if payload.len() > client.descriptor.max_payload {
                return Err(DispatchError::PayloadTooLarge {
                    actual: payload.len(),
                    maximum: client.descriptor.max_payload,
                });
            }
            let dispatch = self
                .dispatch
                .get(&message_id)
                .ok_or(DispatchError::UnknownMessage(message_id))?;
            match dispatch {
                ServerDispatch::Async(dispatch) => {
                    dispatch(handle, payload, deadline, sender).await
                }
                ServerDispatch::Tell(_) => Err(DispatchError::ModeMismatch),
            }
        }
        .await;
        if let Err(error) = &result {
            observer.protocol_failed(
                &actor,
                message_id,
                kind,
                payload_size,
                protocol_failure(error),
            );
        }
        result
    }

    #[doc(hidden)]
    pub fn try_dispatch_tell_with_sender(
        &self,
        handle: ActorHandle<A>,
        message_id: u64,
        payload: Bytes,
        sender: Option<ActorRef>,
    ) -> Result<DispatchReply, DispatchError> {
        let payload_size = payload.len();
        match self.try_dispatch_tell(&handle, message_id, payload, sender) {
            ProtocolTellDispatch::Accepted => Ok(DispatchReply::TellAccepted),
            ProtocolTellDispatch::Deferred { .. } => {
                let error = DispatchError::MailboxRejected;
                handle.observer().protocol_failed(
                    handle.observation_metadata(),
                    message_id,
                    MessageKind::Tell,
                    payload_size,
                    protocol_failure(&error),
                );
                Err(error)
            }
            ProtocolTellDispatch::Rejected(error) => Err(error),
        }
    }
}

pub struct ActorProtocolBindingBuilder<A: Actor, P: Protocol> {
    client: ActorProtocolBuilder<P>,
    dispatch: Vec<(u64, ServerDispatch<A>)>,
    actor: PhantomData<fn() -> A>,
}

impl<A: Actor, P: Protocol> ActorProtocolBindingBuilder<A, P> {
    pub fn max_payload(mut self, maximum: usize) -> Self {
        self.client = self.client.max_payload(maximum);
        self
    }

    pub fn ask<Q, C, RC>(
        mut self,
        message_id: u64,
        request_schema_version: u32,
        response_schema_version: u32,
        codec: C,
        reply_codec: RC,
    ) -> Self
    where
        A: Responder<Q>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<Q>,
        Q: Request,
        C: WireCodec<Q>,
        RC: WireCodec<Q::Response>,
    {
        let codec = Arc::new(codec);
        let reply_codec = Arc::new(reply_codec);
        self.client = self.client.ask::<Q, _, _>(
            message_id,
            request_schema_version,
            response_schema_version,
            SharedCodec(codec.clone()),
            SharedCodec(reply_codec.clone()),
        );
        self.dispatch.push((
            message_id,
            ServerDispatch::Async(Arc::new(move |handle, payload, deadline, _sender| {
                let codec = codec.clone();
                let reply_codec = reply_codec.clone();
                Box::pin(async move {
                    let message = codec.decode(&payload).map_err(DispatchError::Decode)?;
                    let deadline = deadline.ok_or(DispatchError::MissingDeadline)?;
                    let reply = handle
                        .ask_until_owned(message, deadline)
                        .await
                        .map_err(DispatchError::Actor)?;
                    let mut output = BytesMut::new();
                    reply_codec
                        .encode(&reply, &mut output)
                        .map_err(DispatchError::Encode)?;
                    Ok(DispatchReply::Ask(output.freeze()))
                })
            })),
        ));
        self
    }

    pub fn build(self) -> Result<ActorProtocolBinding<A, P>, ProtocolBuildError> {
        let protocol = Arc::new(self.client.build()?);
        let dispatch = self.dispatch.into_iter().collect();
        Ok(ActorProtocolBinding { protocol, dispatch })
    }
}

struct SharedCodec<C>(Arc<C>);

impl<T, C: WireCodec<T>> WireCodec<T> for SharedCodec<C> {
    const DESCRIPTOR: CodecDescriptor = C::DESCRIPTOR;

    fn encode(&self, value: &T, output: &mut BytesMut) -> Result<(), EncodeError> {
        self.0.encode(value, output)
    }

    fn decode(&self, input: &[u8]) -> Result<T, DecodeError> {
        self.0.decode(input)
    }

    fn encoded_len(&self, value: &T) -> Option<usize> {
        self.0.encoded_len(value)
    }

    fn encode_to_bytes(&self, value: &T) -> Result<Bytes, EncodeError> {
        self.0.encode_to_bytes(value)
    }
}

fn protocol_id<P: Protocol>() -> Result<ProtocolId, ProtocolBuildError> {
    if P::PROTOCOL_ID != Some(P::ID) {
        return Err(ProtocolBuildError::ProtocolTagMismatch);
    }
    __protocol_id(P::ID)
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("message Rust type is not registered in this actor protocol")]
    UnregisteredType,
    #[error("unknown message ID {0}")]
    UnknownMessage(u64),
    #[error("message mode does not match protocol registration")]
    ModeMismatch,
    #[error("payload size {actual} exceeds maximum {maximum}")]
    PayloadTooLarge { actual: usize, maximum: usize },
    #[error("message decoding failed")]
    Decode(#[source] DecodeError),
    #[error("reply encoding failed")]
    Encode(#[source] EncodeError),
    #[error("ask frame omitted its deadline")]
    MissingDeadline,
    #[error("actor mailbox rejected the message")]
    MailboxRejected,
    #[error("actor execution failed")]
    Actor(#[source] ActorCallError),
    #[error("reply codec returned a different Rust type")]
    ReplyTypeMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolBuildError {
    #[error("protocol ID zero is reserved")]
    ReservedProtocolId,
    #[error("protocol marker ID does not match its reference tag ID")]
    ProtocolTagMismatch,
    #[error("actor protocol name is empty, oversized, or contains control characters")]
    InvalidName,
    #[error("actor protocol payload limit must be nonzero")]
    ZeroPayloadLimit,
    #[error("actor protocol has no messages")]
    Empty,
    #[error("message ID zero is reserved")]
    ReservedMessageId,
    #[error("schema version zero is reserved for message ID {0}")]
    ZeroSchemaVersion(u64),
    #[error("duplicate message ID {0}")]
    DuplicateMessageId(u64),
    #[error("one Rust message type is registered more than once")]
    DuplicateMessageType,
}

#[macro_export]
macro_rules! actor_protocol {
    (
        $(#[$meta:meta])*
        $visibility:vis $name:ident {
            protocol_id: $protocol_id:expr;
            name: $protocol_name:expr;
            $($bindings:tt)*
        }
    ) => {
        $crate::actor_protocol!(@collect
            [$(#[$meta])*]
            [$visibility]
            [$name]
            [$protocol_id]
            [$protocol_name]
            [$($bindings)*]
            []
            [$($bindings)*]
        );
    };

    (@collect
        [$($meta:tt)*]
        [$visibility:vis]
        [$name:ident]
        [$protocol_id:expr]
        [$protocol_name:expr]
        [$($bindings:tt)*]
        [$($bounds:tt)*]
        []
    ) => {
        $($meta)*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        $visibility struct $name;

        impl $crate::protocol::__ProtocolTag for $name {
            const PROTOCOL_ID: Option<u64> = Some($protocol_id);
        }

        impl $crate::protocol::Protocol for $name {
            const ID: u64 = $protocol_id;

            fn build_protocol() -> Result<
                $crate::protocol::ActorProtocol<Self>,
                $crate::protocol::ProtocolBuildError,
            > {
                let builder = $crate::protocol::ActorProtocol::<Self>::builder($protocol_name);
                $crate::actor_protocol!(@apply_client builder; $($bindings)*).build()
            }
        }

        impl<A> $crate::protocol::ActorProtocolBinder<A> for $name
        where
            A: $crate::traits::Actor,
            $($bounds)*
        {
            fn bind_actor() -> Result<
                $crate::protocol::ActorProtocolBinding<A, Self>,
                $crate::protocol::ProtocolBuildError,
            > {
                let builder = $crate::protocol::ActorProtocolBinding::<A, Self>::builder(
                    $protocol_name,
                );
                $crate::actor_protocol!(@apply_server builder; $($bindings)*).build()
            }
        }

        impl $name {
            $visibility fn build() -> Result<
                $crate::protocol::ActorProtocol<Self>,
                $crate::protocol::ProtocolBuildError,
            > {
                <Self as $crate::protocol::Protocol>::build_protocol()
            }

            $visibility fn bind<A>() -> Result<
                $crate::protocol::ActorProtocolBinding<A, Self>,
                $crate::protocol::ProtocolBuildError,
            >
            where
                A: $crate::traits::Actor,
                $($bounds)*
            {
                <Self as $crate::protocol::ActorProtocolBinder<A>>::bind_actor()
            }
        }

        $crate::actor_protocol!(@support_impls $name; $($bindings)*);
    };

    (@collect
        [$($meta:tt)*] [$visibility:vis] [$name:ident] [$protocol_id:expr] [$protocol_name:expr]
        [$($bindings:tt)*] [$($bounds:tt)*]
        [
            tell $message_id:literal => $message:ty { $($config:tt)* }
            $($remaining:tt)*
        ]
    ) => {
        $crate::actor_protocol!(@collect
            [$($meta)*] [$visibility] [$name] [$protocol_id] [$protocol_name]
            [$($bindings)*]
            [
                $($bounds)*
                A: $crate::traits::Handler<$message>,
                <A as $crate::traits::Actor>::Behavior:
                    $crate::state_machine::Accepts<$message>,
            ]
            [$($remaining)*]
        );
    };

    (@collect
        [$($meta:tt)*] [$visibility:vis] [$name:ident] [$protocol_id:expr] [$protocol_name:expr]
        [$($bindings:tt)*] [$($bounds:tt)*]
        [
            ask $message_id:literal => $message:ty { $($config:tt)* }
            $($remaining:tt)*
        ]
    ) => {
        $crate::actor_protocol!(@collect
            [$($meta)*] [$visibility] [$name] [$protocol_id] [$protocol_name]
            [$($bindings)*]
            [
                $($bounds)*
                A: $crate::traits::Responder<$message>,
                <A as $crate::traits::Actor>::Behavior:
                    $crate::state_machine::Accepts<$message>,
            ]
            [$($remaining)*]
        );
    };

    (@apply_client $builder:expr;) => { $builder };

    (@apply_client $builder:expr;
        tell $message_id:literal => $message:ty {
            schema_version: $schema_version:expr,
            codec: $codec:expr $(,)?
        }
        $($remaining:tt)*
    ) => {
        $crate::actor_protocol!(@apply_client
            $builder.tell::<$message, _>($message_id, $schema_version, $codec);
            $($remaining)*
        )
    };

    (@apply_client $builder:expr;
        ask $message_id:literal => $message:ty {
            request_schema_version: $request_schema_version:expr,
            response_schema_version: $response_schema_version:expr,
            request_codec: $request_codec:expr,
            response_codec: $response_codec:expr $(,)?
        }
        $($remaining:tt)*
    ) => {
        $crate::actor_protocol!(@apply_client
            $builder.ask::<$message, _, _>(
                $message_id,
                $request_schema_version,
                $response_schema_version,
                $request_codec,
                $response_codec,
            );
            $($remaining)*
        )
    };

    (@apply_server $builder:expr;) => { $builder };

    (@apply_server $builder:expr;
        tell $message_id:literal => $message:ty {
            schema_version: $schema_version:expr,
            codec: $codec:expr $(,)?
        }
        $($remaining:tt)*
    ) => {
        $crate::actor_protocol!(@apply_server
            $builder.tell::<$message, _>($message_id, $schema_version, $codec);
            $($remaining)*
        )
    };

    (@apply_server $builder:expr;
        ask $message_id:literal => $message:ty {
            request_schema_version: $request_schema_version:expr,
            response_schema_version: $response_schema_version:expr,
            request_codec: $request_codec:expr,
            response_codec: $response_codec:expr $(,)?
        }
        $($remaining:tt)*
    ) => {
        $crate::actor_protocol!(@apply_server
            $builder.ask::<$message, _, _>(
                $message_id,
                $request_schema_version,
                $response_schema_version,
                $request_codec,
                $response_codec,
            );
            $($remaining)*
        )
    };

    (@support_impls $protocol:ty;) => {};

    (@support_impls $protocol:ty;
        tell $message_id:literal => $message:ty { $($config:tt)* }
        $($remaining:tt)*
    ) => {
        impl $crate::protocol::SupportsTell<$message> for $protocol {}
        $crate::actor_protocol!(@support_impls $protocol; $($remaining)*);
    };

    (@support_impls $protocol:ty;
        ask $message_id:literal => $message:ty { $($config:tt)* }
        $($remaining:tt)*
    ) => {
        impl $crate::protocol::SupportsAsk<$message> for $protocol {}
        $crate::actor_protocol!(@support_impls $protocol; $($remaining)*);
    };
}

#[doc(hidden)]
pub fn __protocol_id(value: u64) -> Result<ProtocolId, ProtocolBuildError> {
    ProtocolId::new(value).map_err(|_| ProtocolBuildError::ReservedProtocolId)
}

#[cfg(test)]
mod tests {
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ClusterId, NodeAddress, NodeIncarnation,
    };
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        context::HandlerContext, error::ActorError, mailbox::MailboxConfig, reply::ReplyTo,
        runtime::spawn_actor, traits::Handler,
    };

    struct TestActor {
        observed_sender: Option<oneshot::Sender<Option<ActorRef>>>,
    }

    impl Actor for TestActor {
        type Error = ActorError;
        type Behavior = ::lattice_actor::state_machine::Stateless;
    }

    #[derive(Clone, crate::Message)]
    struct Tell(u64);

    #[derive(Clone, crate::Request)]
    #[request(response = u64)]
    struct Ask(u64);

    #[derive(Clone, Copy)]
    struct U64Codec;

    impl WireCodec<Tell> for U64Codec {
        const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(20, 1);

        fn encode(&self, value: &Tell, output: &mut BytesMut) -> Result<(), EncodeError> {
            output.extend_from_slice(&value.0.to_be_bytes());
            Ok(())
        }

        fn decode(&self, input: &[u8]) -> Result<Tell, DecodeError> {
            decode_u64(input).map(Tell)
        }
    }

    impl WireCodec<Ask> for U64Codec {
        const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(20, 1);

        fn encode(&self, value: &Ask, output: &mut BytesMut) -> Result<(), EncodeError> {
            output.extend_from_slice(&value.0.to_be_bytes());
            Ok(())
        }

        fn decode(&self, input: &[u8]) -> Result<Ask, DecodeError> {
            decode_u64(input).map(Ask)
        }
    }

    impl WireCodec<u64> for U64Codec {
        const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(20, 1);

        fn encode(&self, value: &u64, output: &mut BytesMut) -> Result<(), EncodeError> {
            output.extend_from_slice(&value.to_be_bytes());
            Ok(())
        }

        fn decode(&self, input: &[u8]) -> Result<u64, DecodeError> {
            decode_u64(input)
        }
    }

    fn decode_u64(input: &[u8]) -> Result<u64, DecodeError> {
        let bytes: [u8; 8] = input
            .try_into()
            .map_err(|_| DecodeError::new("expected eight bytes"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    impl Handler<Tell> for TestActor {
        async fn handle(
            &mut self,
            ctx: &mut HandlerContext<'_, Self>,
            _msg: Tell,
        ) -> Result<(), Self::Error> {
            if let Some(observed_sender) = self.observed_sender.take() {
                let _ = observed_sender.send(ctx.sender().cloned());
            }
            Ok(())
        }
    }

    impl Responder<Ask> for TestActor {
        async fn respond(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            request: Ask,
            reply_to: ReplyTo<u64>,
        ) -> Result<(), Self::Error> {
            let _ = reply_to.send(request.0 + 1);
            Ok(())
        }
    }

    actor_protocol! {
        TestProtocol {
            protocol_id: 77;
            name: "test/v1";
            tell 1 => Tell {
                schema_version: 1,
                codec: U64Codec,
            }
            ask 2 => Ask {
                request_schema_version: 1,
                response_schema_version: 1,
                request_codec: U64Codec,
                response_codec: U64Codec,
            }
        }
    }

    #[test]
    fn macro_and_builder_produce_the_same_fingerprint() {
        let generated = TestProtocol::build().unwrap();
        let manual = ActorProtocol::<TestProtocol>::builder("test/v1")
            .tell::<Tell, _>(1, 1, U64Codec)
            .ask::<Ask, _, _>(2, 1, 1, U64Codec, U64Codec)
            .build()
            .unwrap();
        assert_eq!(generated.fingerprint(), manual.fingerprint());
        let binding = TestProtocol::bind::<TestActor>().unwrap();
        assert_eq!(generated.fingerprint(), binding.fingerprint());
    }

    #[test]
    fn duplicate_message_ids_fail_construction() {
        let result = ActorProtocol::<TestProtocol>::builder("test/v1")
            .tell::<Tell, _>(1, 1, U64Codec)
            .ask::<Ask, _, _>(1, 1, 1, U64Codec, U64Codec)
            .build();
        assert!(matches!(
            result,
            Err(ProtocolBuildError::DuplicateMessageId(1))
        ));
    }

    #[test]
    fn request_and_response_schema_versions_change_the_fingerprint() {
        let build = |request_schema_version, response_schema_version| {
            ActorProtocol::<TestProtocol>::builder("test/v1")
                .ask::<Ask, _, _>(
                    1,
                    request_schema_version,
                    response_schema_version,
                    U64Codec,
                    U64Codec,
                )
                .build()
                .unwrap()
                .fingerprint()
        };

        let baseline = build(1, 1);
        assert_ne!(baseline, build(2, 1));
        assert_ne!(baseline, build(1, 2));
    }

    #[test]
    fn zero_schema_version_fails_construction() {
        let result = ActorProtocol::<TestProtocol>::builder("test/v1")
            .tell::<Tell, _>(1, 0, U64Codec)
            .build();

        assert!(matches!(
            result,
            Err(ProtocolBuildError::ZeroSchemaVersion(1))
        ));
    }

    #[tokio::test]
    async fn protocol_tell_dispatch_preserves_actor_sender_metadata() {
        let (observed_tx, observed_rx) = oneshot::channel();
        let handle = spawn_actor(
            TestActor {
                observed_sender: Some(observed_tx),
            },
            MailboxConfig::default(),
        );
        let incarnation = NodeIncarnation::new(9).unwrap();
        let sender = ActorRef::new(
            ClusterId::new("test").unwrap(),
            NodeAddress::new("sender", 25521).unwrap(),
            incarnation,
            ActorPath::user(["user", "sender"]).unwrap(),
            ActivationId::new(incarnation, 1).unwrap(),
            ProtocolId::new(77).unwrap(),
        )
        .unwrap();
        let protocol = TestProtocol::bind::<TestActor>().unwrap();

        let result = protocol
            .dispatch_with_sender(
                handle,
                1,
                DispatchMode::Tell,
                Bytes::copy_from_slice(&5_u64.to_be_bytes()),
                None,
                Some(sender.clone()),
            )
            .await
            .unwrap();

        assert!(matches!(result, DispatchReply::TellAccepted));
        assert!(
            observed_rx
                .await
                .unwrap()
                .is_some_and(|actual| actual.same_activation(&sender))
        );
    }
}
