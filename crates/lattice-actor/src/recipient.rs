use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{
    ActorRef, EntityRef, ProtocolId, ProtocolTag, RecipientRef, SingletonRef,
};
use lattice_remoting::messaging::error::{AskError, TellError};
use lattice_remoting::protocol::ProtocolFingerprint;
use lattice_remoting::watch::{WatchError, WatchId};
use thiserror::Error;

use crate::error::ActorError;
use crate::protocol::{
    ActorProtocol, DispatchError, DispatchMode, Protocol, SupportsAsk, SupportsTell,
};
use crate::traits::{Message, Request};

#[async_trait]
#[doc(hidden)]
pub trait RecipientBackend: Send + Sync + 'static {
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: RecipientRef,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), TellError>;

    async fn ask(
        &self,
        target: RecipientRef,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;

    async fn watch_actor(&self, target: ActorRef) -> Result<WatchId, WatchError>;

    async fn watch_entity_current(&self, target: EntityRef) -> Result<WatchId, WatchError>;

    async fn watch_singleton_current(&self, target: SingletonRef) -> Result<WatchId, WatchError>;

    async fn unwatch(&self, watch_id: WatchId) -> Result<(), WatchError>;
}

/// Process-level actor messaging capability.
///
/// Applications normally access this through `LatticeService` or
/// `ActorContext`; actor references themselves remain plain serializable data.
#[derive(Clone)]
pub struct ActorSystem {
    backend: Arc<dyn RecipientBackend>,
    protocols: Arc<BTreeMap<u64, RegisteredActorProtocol>>,
}

impl fmt::Debug for ActorSystem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorSystem")
            .field("protocol_count", &self.protocols.len())
            .finish_non_exhaustive()
    }
}

impl ActorSystem {
    #[doc(hidden)]
    pub fn new(
        backend: Arc<dyn RecipientBackend>,
        protocols: impl IntoIterator<Item = RegisteredActorProtocol>,
    ) -> Result<Self, ProtocolRegistrationError> {
        let mut registered = BTreeMap::new();
        for protocol in protocols {
            let protocol_id = protocol.protocol_id().get();
            if registered.insert(protocol_id, protocol).is_some() {
                return Err(ProtocolRegistrationError::DuplicateProtocol(
                    ProtocolId::new(protocol_id).expect("registered protocol IDs are nonzero"),
                ));
            }
        }
        Ok(Self {
            backend,
            protocols: Arc::new(registered),
        })
    }

    /// Sends a one-way message from process code. The receiver observes no
    /// actor sender.
    pub async fn tell<P, M>(
        &self,
        target: impl Into<RecipientRef<P>>,
        message: M,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        self.tell_with_sender(target.into(), message, None).await
    }

    pub(crate) async fn tell_with_sender<P, M>(
        &self,
        target: RecipientRef<P>,
        message: M,
        sender: Option<ActorRef>,
    ) -> Result<(), RecipientError>
    where
        P: SupportsTell<M>,
        M: Message,
    {
        let protocol = self.protocol::<P>(target_protocol_id(&target))?;
        let (message_id, payload) = protocol
            .encode_request(DispatchMode::Tell, &message)
            .map_err(RecipientError::Dispatch)?;
        self.backend
            .tell(
                sender,
                target.erase(),
                protocol.fingerprint(),
                message_id,
                payload,
            )
            .await
            .map_err(RecipientError::Tell)
    }

    /// Sends a typed request from process code and waits until `deadline` for
    /// its typed response.
    pub async fn ask<P, R>(
        &self,
        target: impl Into<RecipientRef<P>>,
        request: R,
        deadline: Instant,
    ) -> Result<R::Response, RecipientError>
    where
        P: SupportsAsk<R>,
        R: Request,
    {
        if Instant::now() >= deadline {
            return Err(RecipientError::Ask(AskError::DeadlineExceeded));
        }
        let target = target.into();
        let protocol = self.protocol::<P>(target_protocol_id(&target))?;
        let (message_id, payload) = protocol
            .encode_request(DispatchMode::Ask, &request)
            .map_err(RecipientError::Dispatch)?;
        let reply = self
            .backend
            .ask(
                target.erase(),
                protocol.fingerprint(),
                message_id,
                payload,
                deadline,
            )
            .await
            .map_err(RecipientError::Ask)?;
        protocol
            .decode_response::<R>(message_id, &reply)
            .map_err(RecipientError::Dispatch)
    }

    pub async fn watch<P: ProtocolTag>(
        &self,
        target: &ActorRef<P>,
    ) -> Result<WatchId, RecipientError> {
        self.backend
            .watch_actor(target.erase())
            .await
            .map_err(RecipientError::Watch)
    }

    pub async fn watch_entity_current<P: ProtocolTag>(
        &self,
        target: &EntityRef<P>,
    ) -> Result<WatchId, RecipientError> {
        self.backend
            .watch_entity_current(target.erase())
            .await
            .map_err(RecipientError::Watch)
    }

    pub async fn watch_singleton_current<P: ProtocolTag>(
        &self,
        target: &SingletonRef<P>,
    ) -> Result<WatchId, RecipientError> {
        self.backend
            .watch_singleton_current(target.erase())
            .await
            .map_err(RecipientError::Watch)
    }

    pub async fn unwatch(&self, watch_id: WatchId) -> Result<(), RecipientError> {
        self.backend
            .unwatch(watch_id)
            .await
            .map_err(RecipientError::Watch)
    }

    fn protocol<P: Protocol>(
        &self,
        protocol_id: ProtocolId,
    ) -> Result<Arc<ActorProtocol<P>>, RecipientError> {
        let registered = self.protocols.get(&protocol_id.get()).ok_or(
            RecipientError::ProtocolNotRegistered {
                protocol_id: protocol_id.get(),
            },
        )?;
        registered
            .protocol
            .clone()
            .downcast::<ActorProtocol<P>>()
            .map_err(|_| RecipientError::ProtocolTypeMismatch {
                protocol_id: protocol_id.get(),
            })
    }
}

fn target_protocol_id<P: Protocol>(target: &RecipientRef<P>) -> ProtocolId {
    match target {
        RecipientRef::Actor(reference) => reference.protocol_id(),
        RecipientRef::Entity(reference) => reference.protocol_id(),
        RecipientRef::Singleton(reference) => reference.protocol_id(),
    }
}

#[derive(Clone)]
#[doc(hidden)]
pub struct RegisteredActorProtocol {
    protocol_id: ProtocolId,
    fingerprint: ProtocolFingerprint,
    protocol: Arc<dyn Any + Send + Sync>,
}

impl fmt::Debug for RegisteredActorProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredActorProtocol")
            .field("protocol_id", &self.protocol_id)
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

impl RegisteredActorProtocol {
    pub fn new<P: Protocol>(protocol: Arc<ActorProtocol<P>>) -> Self {
        Self {
            protocol_id: protocol.protocol_id(),
            fingerprint: protocol.fingerprint(),
            protocol,
        }
    }

    pub fn protocol_id(&self) -> ProtocolId {
        self.protocol_id
    }

    pub fn fingerprint(&self) -> ProtocolFingerprint {
        self.fingerprint
    }

    pub fn is_for<P: Protocol>(&self) -> bool {
        self.protocol.is::<ActorProtocol<P>>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProtocolRegistrationError {
    #[error("actor protocol {0:?} is already registered")]
    DuplicateProtocol(ProtocolId),
    #[error("actor protocol {protocol_id} was registered for a different protocol marker type")]
    ProtocolTypeMismatch { protocol_id: u64 },
    #[error("actor registry is already attached to an actor system")]
    ActorSystemAlreadyInstalled,
    #[error(
        "actor registry protocol ID {registry_protocol_id:?} does not match binding protocol ID {binding_protocol_id}"
    )]
    RegistryProtocolMismatch {
        registry_protocol_id: Option<u64>,
        binding_protocol_id: u64,
    },
}

#[derive(Debug, Error)]
pub enum RecipientError {
    #[error("actor protocol {protocol_id} is not registered with this actor system")]
    ProtocolNotRegistered { protocol_id: u64 },
    #[error("actor protocol {protocol_id} is registered for a different protocol marker type")]
    ProtocolTypeMismatch { protocol_id: u64 },
    #[error("actor context has no actor system")]
    ActorSystemUnavailable,
    #[error("typed protocol dispatch failed")]
    Dispatch(#[source] DispatchError),
    #[error("tell admission failed")]
    Tell(#[source] TellError),
    #[error("ask failed")]
    Ask(#[source] AskError),
    #[error("watch operation failed")]
    Watch(#[source] WatchError),
}

impl From<RecipientError> for ActorError {
    fn from(error: RecipientError) -> Self {
        Self::new(error.to_string())
    }
}
