use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{ActorRef, EntityRef, RecipientRef, SingletonRef};
use lattice_remoting::protocol::ProtocolFingerprint;
use lattice_remoting::{AskError, TellError, WatchError, WatchId};
use thiserror::Error;

use crate::protocol::{ActorProtocol, DispatchError, DispatchMode};
use crate::traits::{Actor, Message};

#[async_trait]
pub trait RecipientBackend: Send + Sync + 'static {
    async fn tell(
        &self,
        target: RecipientRef<()>,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), TellError>;

    async fn ask(
        &self,
        target: RecipientRef<()>,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;

    async fn watch_actor(&self, target: ActorRef<()>) -> Result<WatchId, WatchError>;

    async fn watch_entity_current(&self, target: EntityRef<()>) -> Result<WatchId, WatchError>;

    async fn watch_singleton_current(
        &self,
        target: SingletonRef<()>,
    ) -> Result<WatchId, WatchError>;

    async fn unwatch(&self, watch_id: WatchId) -> Result<(), WatchError>;
}

pub struct BoundRecipient<A: Actor> {
    target: RecipientRef<A>,
    protocol: Arc<ActorProtocol<A>>,
    backend: Arc<dyn RecipientBackend>,
}

impl<A: Actor> Clone for BoundRecipient<A> {
    fn clone(&self) -> Self {
        let target = match &self.target {
            RecipientRef::Actor(reference) => RecipientRef::Actor(reference.cast()),
            RecipientRef::Entity(reference) => RecipientRef::Entity(reference.cast()),
            RecipientRef::Singleton(reference) => RecipientRef::Singleton(reference.cast()),
        };
        Self {
            target,
            protocol: self.protocol.clone(),
            backend: self.backend.clone(),
        }
    }
}

impl<A: Actor> BoundRecipient<A> {
    pub fn new(
        target: RecipientRef<A>,
        protocol: Arc<ActorProtocol<A>>,
        backend: Arc<dyn RecipientBackend>,
    ) -> Result<Self, RecipientError> {
        let target_protocol = match &target {
            RecipientRef::Actor(reference) => reference.protocol_id(),
            RecipientRef::Entity(reference) => reference.protocol_id(),
            RecipientRef::Singleton(reference) => reference.protocol_id(),
        };
        if target_protocol != protocol.protocol_id() {
            return Err(RecipientError::ProtocolMismatch);
        }
        Ok(Self {
            target,
            protocol,
            backend,
        })
    }

    pub async fn tell<M>(&self, message: M) -> Result<(), RecipientError>
    where
        M: Message<Reply = ()>,
    {
        let (message_id, payload) = self
            .protocol
            .encode_request(DispatchMode::Tell, &message)
            .map_err(RecipientError::Dispatch)?;
        self.backend
            .tell(
                self.target.erase(),
                self.protocol.fingerprint(),
                message_id,
                payload,
            )
            .await
            .map_err(RecipientError::Tell)
    }

    pub async fn ask<M>(&self, message: M, deadline: Instant) -> Result<M::Reply, RecipientError>
    where
        M: Message,
    {
        if Instant::now() >= deadline {
            return Err(RecipientError::Ask(AskError::DeadlineExceeded));
        }
        let (message_id, payload) = self
            .protocol
            .encode_request(DispatchMode::Ask, &message)
            .map_err(RecipientError::Dispatch)?;
        let reply = self
            .backend
            .ask(
                self.target.erase(),
                self.protocol.fingerprint(),
                message_id,
                payload,
                deadline,
            )
            .await
            .map_err(RecipientError::Ask)?;
        self.protocol
            .decode_reply::<M>(message_id, &reply)
            .map_err(RecipientError::Dispatch)
    }

    pub async fn watch(&self) -> Result<WatchId, RecipientError> {
        let RecipientRef::Actor(reference) = &self.target else {
            return Err(RecipientError::UseWatchCurrent);
        };
        self.backend
            .watch_actor(reference.erase())
            .await
            .map_err(RecipientError::Watch)
    }

    pub async fn watch_current(&self) -> Result<WatchId, RecipientError> {
        match &self.target {
            RecipientRef::Actor(_) => Err(RecipientError::UseWatch),
            RecipientRef::Entity(reference) => self
                .backend
                .watch_entity_current(reference.erase())
                .await
                .map_err(RecipientError::Watch),
            RecipientRef::Singleton(reference) => self
                .backend
                .watch_singleton_current(reference.erase())
                .await
                .map_err(RecipientError::Watch),
        }
    }

    pub async fn unwatch(&self, watch_id: WatchId) -> Result<(), RecipientError> {
        self.backend
            .unwatch(watch_id)
            .await
            .map_err(RecipientError::Watch)
    }
}

#[derive(Debug, Error)]
pub enum RecipientError {
    #[error("recipient protocol ID differs from its ActorProtocol")]
    ProtocolMismatch,
    #[error("typed protocol dispatch failed")]
    Dispatch(#[source] DispatchError),
    #[error("tell admission failed")]
    Tell(#[source] TellError),
    #[error("ask failed")]
    Ask(#[source] AskError),
    #[error("watch operation failed")]
    Watch(#[source] WatchError),
    #[error("logical recipients require watch_current")]
    UseWatchCurrent,
    #[error("concrete ActorRef recipients require watch")]
    UseWatch,
}
