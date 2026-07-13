use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use lattice_core::actor_ref::{ActorRef, ProtocolId};
use lattice_remoting::protocol::{ProtocolDescriptor, ProtocolFingerprint};
use thiserror::Error;

use crate::error::ActorCallError;
use crate::handle::ActorHandle;
use crate::traits::{Actor, Handler, Message, Request, Responder};

pub trait WireSchema: Send + 'static {
    const SCHEMA_ID: u64;
    const SCHEMA_VERSION: u32;
}

pub trait WireCodec<T>: Send + Sync + 'static {
    const CODEC_ID: u64;
    const CODEC_VERSION: u32;

    fn encode(&self, value: &T, output: &mut BytesMut) -> Result<(), EncodeError>;
    fn decode(&self, input: &[u8]) -> Result<T, DecodeError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UnitCodec;

#[derive(Debug, Clone, Copy, Default)]
pub struct ProstCodec;

impl<T> WireCodec<T> for ProstCodec
where
    T: prost::Message + Default + Send + Sync + 'static,
{
    const CODEC_ID: u64 = 0x7072_6f73_7400_0001;
    const CODEC_VERSION: u32 = 1;

    fn encode(&self, value: &T, output: &mut BytesMut) -> Result<(), EncodeError> {
        prost::Message::encode(value, output).map_err(|error| EncodeError::new(error.to_string()))
    }

    fn decode(&self, input: &[u8]) -> Result<T, DecodeError> {
        T::decode(input).map_err(|error| DecodeError::new(error.to_string()))
    }
}

impl WireSchema for () {
    const SCHEMA_ID: u64 = 1;
    const SCHEMA_VERSION: u32 = 1;
}

impl WireCodec<()> for UnitCodec {
    const CODEC_ID: u64 = 1;
    const CODEC_VERSION: u32 = 1;

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

fn bounded_error(mut message: String) -> String {
    message.truncate(256);
    message
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

type DispatchFuture =
    Pin<Box<dyn Future<Output = Result<DispatchReply, DispatchError>> + Send + 'static>>;
type DispatchFn<A> = dyn Fn(ActorHandle<A>, Bytes, Option<Instant>, Option<ActorRef<()>>) -> DispatchFuture
    + Send
    + Sync
    + 'static;
type EncodeRequestFn = dyn Fn(&dyn Any) -> Result<Bytes, EncodeError> + Send + Sync;
type DecodeReplyFn = dyn Fn(&[u8]) -> Result<Box<dyn Any + Send>, DecodeError> + Send + Sync;

struct Binding<A: Actor> {
    descriptor: MessageDescriptor,
    dispatch: Arc<DispatchFn<A>>,
    request_type: TypeId,
    encode_request: Arc<EncodeRequestFn>,
    decode_reply: Option<Arc<DecodeReplyFn>>,
}

impl<A: Actor> Clone for Binding<A> {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            dispatch: self.dispatch.clone(),
            request_type: self.request_type,
            encode_request: self.encode_request.clone(),
            decode_reply: self.decode_reply.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MessageDescriptor {
    message_id: u64,
    mode: DispatchMode,
    request_codec_id: u64,
    request_codec_version: u32,
    request_schema_id: u64,
    request_schema_version: u32,
    reply_codec_id: Option<u64>,
    reply_codec_version: Option<u32>,
    reply_schema_id: Option<u64>,
    reply_schema_version: Option<u32>,
    max_payload: usize,
}

pub struct ActorProtocol<A: Actor> {
    protocol_id: ProtocolId,
    name: String,
    fingerprint: ProtocolFingerprint,
    bindings: BTreeMap<u64, Binding<A>>,
}

impl<A: Actor> ActorProtocol<A> {
    pub fn builder(protocol_id: ProtocolId, name: impl Into<String>) -> ActorProtocolBuilder<A> {
        ActorProtocolBuilder {
            protocol_id,
            name: name.into(),
            max_payload: 256 * 1024,
            bindings: Vec::new(),
            actor: PhantomData,
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
        sender: Option<ActorRef<()>>,
    ) -> Result<DispatchReply, DispatchError> {
        let binding = self
            .bindings
            .get(&message_id)
            .ok_or(DispatchError::UnknownMessage(message_id))?;
        if binding.descriptor.mode != mode {
            return Err(DispatchError::ModeMismatch);
        }
        if payload.len() > binding.descriptor.max_payload {
            return Err(DispatchError::PayloadTooLarge {
                actual: payload.len(),
                maximum: binding.descriptor.max_payload,
            });
        }
        (binding.dispatch)(handle, payload, deadline, sender).await
    }

    pub fn encode_request<T: Send + 'static>(
        &self,
        mode: DispatchMode,
        message: &T,
    ) -> Result<(u64, Bytes), DispatchError> {
        let binding = self
            .bindings
            .values()
            .find(|binding| binding.request_type == TypeId::of::<T>())
            .ok_or(DispatchError::UnregisteredType)?;
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
        let binding = self
            .bindings
            .get(&message_id)
            .ok_or(DispatchError::UnknownMessage(message_id))?;
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

pub struct ActorProtocolBuilder<A: Actor> {
    protocol_id: ProtocolId,
    name: String,
    max_payload: usize,
    bindings: Vec<Binding<A>>,
    actor: PhantomData<fn() -> A>,
}

impl<A: Actor> ActorProtocolBuilder<A> {
    pub fn max_payload(mut self, maximum: usize) -> Self {
        self.max_payload = maximum;
        self
    }

    pub fn tell<M, C>(mut self, message_id: u64, codec: C) -> Self
    where
        A: Handler<M>,
        M: Message + WireSchema,
        C: WireCodec<M>,
    {
        let codec = Arc::new(codec);
        self.bindings.push(Binding {
            descriptor: MessageDescriptor {
                message_id,
                mode: DispatchMode::Tell,
                request_codec_id: C::CODEC_ID,
                request_codec_version: C::CODEC_VERSION,
                request_schema_id: M::SCHEMA_ID,
                request_schema_version: M::SCHEMA_VERSION,
                reply_codec_id: None,
                reply_codec_version: None,
                reply_schema_id: None,
                reply_schema_version: None,
                max_payload: self.max_payload,
            },
            request_type: TypeId::of::<M>(),
            encode_request: {
                let codec = codec.clone();
                Arc::new(move |message| {
                    let message = message
                        .downcast_ref::<M>()
                        .ok_or_else(|| EncodeError::new("message type does not match binding"))?;
                    let mut output = BytesMut::new();
                    codec.encode(message, &mut output)?;
                    Ok(output.freeze())
                })
            },
            decode_reply: None,
            dispatch: Arc::new(move |handle, payload, _deadline, sender| {
                let codec = codec.clone();
                Box::pin(async move {
                    let message = codec.decode(&payload).map_err(DispatchError::Decode)?;
                    handle
                        .try_tell_from(message, sender)
                        .map_err(|_| DispatchError::MailboxRejected)?;
                    Ok(DispatchReply::TellAccepted)
                })
            }),
        });
        self
    }

    pub fn ask<Q, C, RC>(mut self, message_id: u64, codec: C, reply_codec: RC) -> Self
    where
        A: Responder<Q>,
        Q: Request + WireSchema,
        Q::Response: WireSchema,
        C: WireCodec<Q>,
        RC: WireCodec<Q::Response>,
    {
        let codec = Arc::new(codec);
        let reply_codec = Arc::new(reply_codec);
        self.bindings.push(Binding {
            descriptor: MessageDescriptor {
                message_id,
                mode: DispatchMode::Ask,
                request_codec_id: C::CODEC_ID,
                request_codec_version: C::CODEC_VERSION,
                request_schema_id: Q::SCHEMA_ID,
                request_schema_version: Q::SCHEMA_VERSION,
                reply_codec_id: Some(RC::CODEC_ID),
                reply_codec_version: Some(RC::CODEC_VERSION),
                reply_schema_id: Some(Q::Response::SCHEMA_ID),
                reply_schema_version: Some(Q::Response::SCHEMA_VERSION),
                max_payload: self.max_payload,
            },
            request_type: TypeId::of::<Q>(),
            encode_request: {
                let codec = codec.clone();
                Arc::new(move |message| {
                    let message = message
                        .downcast_ref::<Q>()
                        .ok_or_else(|| EncodeError::new("message type does not match binding"))?;
                    let mut output = BytesMut::new();
                    codec.encode(message, &mut output)?;
                    Ok(output.freeze())
                })
            },
            decode_reply: {
                let reply_codec = reply_codec.clone();
                Some(Arc::new(move |input: &[u8]| {
                    let reply = reply_codec.decode(input)?;
                    Ok(Box::new(reply) as Box<dyn Any + Send>)
                }))
            },
            dispatch: Arc::new(move |handle, payload, deadline, _sender| {
                let codec = codec.clone();
                let reply_codec = reply_codec.clone();
                Box::pin(async move {
                    let message = codec.decode(&payload).map_err(DispatchError::Decode)?;
                    let deadline = deadline.ok_or(DispatchError::MissingDeadline)?;
                    let reply = handle
                        .ask_before_owned(message, deadline)
                        .await
                        .map_err(DispatchError::Actor)?;
                    let mut output = BytesMut::new();
                    reply_codec
                        .encode(&reply, &mut output)
                        .map_err(DispatchError::Encode)?;
                    Ok(DispatchReply::Ask(output.freeze()))
                })
            }),
        });
        self
    }

    pub fn build(self) -> Result<ActorProtocol<A>, ProtocolBuildError> {
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
        let canonical = canonical_descriptor(self.protocol_id, &self.name, &bindings);
        Ok(ActorProtocol {
            protocol_id: self.protocol_id,
            name: self.name,
            fingerprint: ProtocolFingerprint::digest(&canonical),
            bindings,
        })
    }
}

fn canonical_descriptor<A: Actor>(
    protocol_id: ProtocolId,
    name: &str,
    bindings: &BTreeMap<u64, Binding<A>>,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&protocol_id.get().to_be_bytes());
    output.extend_from_slice(&(name.len() as u32).to_be_bytes());
    output.extend_from_slice(name.as_bytes());
    output.extend_from_slice(&(bindings.len() as u32).to_be_bytes());
    for descriptor in bindings.values().map(|binding| &binding.descriptor) {
        output.extend_from_slice(&descriptor.message_id.to_be_bytes());
        output.push(match descriptor.mode {
            DispatchMode::Tell => 0,
            DispatchMode::Ask => 1,
        });
        for value in [
            descriptor.request_codec_id,
            u64::from(descriptor.request_codec_version),
            descriptor.request_schema_id,
            u64::from(descriptor.request_schema_version),
            descriptor.reply_codec_id.unwrap_or(0),
            u64::from(descriptor.reply_codec_version.unwrap_or(0)),
            descriptor.reply_schema_id.unwrap_or(0),
            u64::from(descriptor.reply_schema_version.unwrap_or(0)),
            descriptor.max_payload as u64,
        ] {
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
    output
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
    #[error("actor protocol name is empty, oversized, or contains control characters")]
    InvalidName,
    #[error("actor protocol payload limit must be nonzero")]
    ZeroPayloadLimit,
    #[error("actor protocol has no messages")]
    Empty,
    #[error("message ID zero is reserved")]
    ReservedMessageId,
    #[error("duplicate message ID {0}")]
    DuplicateMessageId(u64),
    #[error("one Rust message type is registered more than once")]
    DuplicateMessageType,
}

#[macro_export]
macro_rules! actor_protocol {
    (
        $(#[$meta:meta])*
        $visibility:vis $name:ident for $actor:ty {
            protocol_id: $protocol_id:expr;
            name: $protocol_name:expr;
            $($bindings:tt)*
        }
    ) => {
        $(#[$meta])*
        $visibility struct $name;

        impl $name {
            $visibility fn build() -> Result<
                $crate::protocol::ActorProtocol<$actor>,
                $crate::protocol::ProtocolBuildError,
            > {
                let builder = $crate::protocol::ActorProtocol::<$actor>::builder(
                    $crate::protocol::__protocol_id($protocol_id)?,
                    $protocol_name,
                );
                $crate::actor_protocol!(@apply builder; $($bindings)*).build()
            }
        }
    };

    (@apply $builder:expr;) => { $builder };

    (@apply $builder:expr;
        tell $message_id:literal => $message:ty {
            codec: $codec:expr $(,)?
        }
        $($remaining:tt)*
    ) => {
        $crate::actor_protocol!(@apply
            $builder.tell::<$message, _>($message_id, $codec);
            $($remaining)*
        )
    };

    (@apply $builder:expr;
        ask $message_id:literal => $message:ty {
            request_codec: $request_codec:expr,
            reply_codec: $reply_codec:expr $(,)?
        }
        $($remaining:tt)*
    ) => {
        $crate::actor_protocol!(@apply
            $builder.ask::<$message, _, _>($message_id, $request_codec, $reply_codec);
            $($remaining)*
        )
    };
}

#[doc(hidden)]
pub fn __protocol_id(value: u64) -> Result<ProtocolId, ProtocolBuildError> {
    ProtocolId::new(value).map_err(|_| ProtocolBuildError::ReservedProtocolId)
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::context::ActorContext;
    use crate::error::ActorError;
    use crate::mailbox::MailboxConfig;
    use crate::reply::ReplyTo;
    use crate::runtime::spawn_actor;
    use lattice_core::actor_ref::{
        ActivationId, ActorPath, ClusterId, NodeAddress, NodeIncarnation,
    };
    use tokio::sync::oneshot;

    struct TestActor {
        observed_sender: Option<oneshot::Sender<Option<ActorRef<()>>>>,
    }

    #[async_trait]
    impl Actor for TestActor {
        type Error = ActorError;
    }

    #[derive(Clone)]
    struct Tell(u64);

    impl Message for Tell {}

    impl WireSchema for Tell {
        const SCHEMA_ID: u64 = 10;
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone)]
    struct Ask(u64);

    impl Request for Ask {
        type Response = u64;
    }

    impl WireSchema for Ask {
        const SCHEMA_ID: u64 = 11;
        const SCHEMA_VERSION: u32 = 1;
    }

    impl WireSchema for u64 {
        const SCHEMA_ID: u64 = 12;
        const SCHEMA_VERSION: u32 = 1;
    }

    #[derive(Clone, Copy)]
    struct U64Codec;

    impl WireCodec<Tell> for U64Codec {
        const CODEC_ID: u64 = 20;
        const CODEC_VERSION: u32 = 1;

        fn encode(&self, value: &Tell, output: &mut BytesMut) -> Result<(), EncodeError> {
            output.extend_from_slice(&value.0.to_be_bytes());
            Ok(())
        }

        fn decode(&self, input: &[u8]) -> Result<Tell, DecodeError> {
            decode_u64(input).map(Tell)
        }
    }

    impl WireCodec<Ask> for U64Codec {
        const CODEC_ID: u64 = 20;
        const CODEC_VERSION: u32 = 1;

        fn encode(&self, value: &Ask, output: &mut BytesMut) -> Result<(), EncodeError> {
            output.extend_from_slice(&value.0.to_be_bytes());
            Ok(())
        }

        fn decode(&self, input: &[u8]) -> Result<Ask, DecodeError> {
            decode_u64(input).map(Ask)
        }
    }

    impl WireCodec<u64> for U64Codec {
        const CODEC_ID: u64 = 20;
        const CODEC_VERSION: u32 = 1;

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

    #[async_trait]
    impl Handler<Tell> for TestActor {
        async fn handle(
            &mut self,
            ctx: &mut ActorContext<Self>,
            _msg: Tell,
        ) -> Result<(), Self::Error> {
            if let Some(observed_sender) = self.observed_sender.take() {
                let _ = observed_sender.send(ctx.sender().cloned());
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Responder<Ask> for TestActor {
        async fn respond(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            request: Ask,
            reply_to: ReplyTo<u64>,
        ) -> Result<(), Self::Error> {
            let _ = reply_to.send(request.0 + 1);
            Ok(())
        }
    }

    actor_protocol! {
        TestProtocol for TestActor {
            protocol_id: 77;
            name: "test/v1";
            tell 1 => Tell { codec: U64Codec }
            ask 2 => Ask {
                request_codec: U64Codec,
                reply_codec: U64Codec,
            }
        }
    }

    #[test]
    fn macro_and_builder_produce_the_same_fingerprint() {
        let generated = TestProtocol::build().unwrap();
        let manual = ActorProtocol::<TestActor>::builder(ProtocolId::new(77).unwrap(), "test/v1")
            .tell::<Tell, _>(1, U64Codec)
            .ask::<Ask, _, _>(2, U64Codec, U64Codec)
            .build()
            .unwrap();
        assert_eq!(generated.fingerprint(), manual.fingerprint());
    }

    #[test]
    fn duplicate_message_ids_fail_construction() {
        let result = ActorProtocol::<TestActor>::builder(ProtocolId::new(77).unwrap(), "test/v1")
            .tell::<Tell, _>(1, U64Codec)
            .ask::<Ask, _, _>(1, U64Codec, U64Codec)
            .build();
        assert!(matches!(
            result,
            Err(ProtocolBuildError::DuplicateMessageId(1))
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
        let protocol = TestProtocol::build().unwrap();

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
