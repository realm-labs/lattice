use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use lattice_actor::{ProtocolHostRegistry, RecipientBackend};
use lattice_core::actor_ref::{ActorRef, EntityRef, RecipientRef, SingletonRef};
use lattice_remoting::protocol::ProtocolFingerprint;
use lattice_remoting::{
    AskError, AssociationManager, InboundDispatch, OutboundMessaging, RemoteFailureCode,
    RemoteMessageError, SenderIdentity, TellError, WatchError, WatchId, WatchRegistry,
};

#[async_trait]
pub trait LogicalRouter: Send + Sync + 'static {
    async fn tell_entity(
        &self,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;

    async fn ask_entity(
        &self,
        target: EntityRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;

    async fn tell_singleton(
        &self,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError>;

    async fn ask_singleton(
        &self,
        target: SingletonRef<()>,
        fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError>;

    async fn resolve_entity_current(
        &self,
        target: EntityRef<()>,
    ) -> Result<Option<ActorRef<()>>, WatchError>;

    async fn resolve_singleton_current(
        &self,
        target: SingletonRef<()>,
    ) -> Result<Option<ActorRef<()>>, WatchError>;
}

pub(crate) struct ServiceRecipientBackend {
    pub local_cluster: lattice_core::actor_ref::ClusterId,
    pub local_address: lattice_core::actor_ref::NodeAddress,
    pub local_incarnation: lattice_core::actor_ref::NodeIncarnation,
    pub hosts: Arc<ProtocolHostRegistry>,
    pub associations: Arc<AssociationManager>,
    pub messaging: Arc<OutboundMessaging>,
    pub watches: Mutex<WatchRegistry>,
    pub logical: Option<Arc<dyn LogicalRouter>>,
}

impl ServiceRecipientBackend {
    fn is_local(&self, reference: &ActorRef<()>) -> bool {
        reference.cluster_id() == &self.local_cluster
            && reference.node_address() == &self.local_address
            && reference.node_incarnation() == self.local_incarnation
    }

    fn association(
        &self,
        reference: &ActorRef<()>,
    ) -> Result<Arc<lattice_remoting::Association>, lattice_remoting::association::AssociationError>
    {
        self.associations.get_or_create(
            reference.cluster_id().clone(),
            reference.node_address().clone(),
            reference.node_incarnation(),
        )
    }
}

#[async_trait]
impl RecipientBackend for ServiceRecipientBackend {
    async fn tell(
        &self,
        target: RecipientRef<()>,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), TellError> {
        match target {
            RecipientRef::Actor(reference) if self.is_local(&reference) => self
                .hosts
                .tell((&reference).into(), message_id, payload)
                .await
                .map_err(TellError::Remote),
            RecipientRef::Actor(reference) => {
                let association = self
                    .association(&reference)
                    .map_err(TellError::Association)?;
                self.messaging
                    .tell(
                        &association,
                        &SenderIdentity::Process(self.local_incarnation.get()),
                        &reference,
                        protocol_fingerprint,
                        message_id,
                        payload,
                    )
                    .map(|_| ())
            }
            RecipientRef::Entity(reference) => self
                .logical
                .as_ref()
                .ok_or(TellError::Remote(RemoteMessageError::Unauthorized))?
                .tell_entity(reference, protocol_fingerprint, message_id, payload)
                .await
                .map_err(TellError::Remote),
            RecipientRef::Singleton(reference) => self
                .logical
                .as_ref()
                .ok_or(TellError::Remote(RemoteMessageError::Unauthorized))?
                .tell_singleton(reference, protocol_fingerprint, message_id, payload)
                .await
                .map_err(TellError::Remote),
        }
    }

    async fn ask(
        &self,
        target: RecipientRef<()>,
        protocol_fingerprint: ProtocolFingerprint,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, AskError> {
        match target {
            RecipientRef::Actor(reference) if self.is_local(&reference) => self
                .hosts
                .ask((&reference).into(), message_id, payload, deadline)
                .await
                .map_err(map_remote_ask),
            RecipientRef::Actor(reference) => {
                let association = self.association(&reference).map_err(AskError::from)?;
                self.messaging
                    .ask(
                        &association,
                        &SenderIdentity::Process(self.local_incarnation.get()),
                        &reference,
                        protocol_fingerprint,
                        message_id,
                        payload,
                        deadline,
                    )
                    .await
            }
            RecipientRef::Entity(reference) => {
                self.logical
                    .as_ref()
                    .ok_or(AskError::Protocol(RemoteMessageError::Unauthorized))?
                    .ask_entity(
                        reference,
                        protocol_fingerprint,
                        message_id,
                        payload,
                        deadline,
                    )
                    .await
            }
            RecipientRef::Singleton(reference) => {
                self.logical
                    .as_ref()
                    .ok_or(AskError::Protocol(RemoteMessageError::Unauthorized))?
                    .ask_singleton(
                        reference,
                        protocol_fingerprint,
                        message_id,
                        payload,
                        deadline,
                    )
                    .await
            }
        }
    }

    async fn watch_actor(&self, target: ActorRef<()>) -> Result<WatchId, WatchError> {
        if self.is_local(&target) {
            return Err(WatchError::InvalidCommand);
        }
        let association = self
            .association(&target)
            .map_err(|_| WatchError::InvalidCommand)?;
        let (watch_id, _command) = self
            .watches
            .lock()
            .expect("watch registry poisoned")
            .watch(association.id(), &target)?;
        // The command is admitted to reliable control by the association runtime.
        Ok(watch_id)
    }

    async fn watch_entity_current(&self, target: EntityRef<()>) -> Result<WatchId, WatchError> {
        let current = self
            .logical
            .as_ref()
            .ok_or(WatchError::NotActive)?
            .resolve_entity_current(target)
            .await?
            .ok_or(WatchError::NotActive)?;
        self.watch_actor(current).await
    }

    async fn watch_singleton_current(
        &self,
        target: SingletonRef<()>,
    ) -> Result<WatchId, WatchError> {
        let current = self
            .logical
            .as_ref()
            .ok_or(WatchError::Unavailable)?
            .resolve_singleton_current(target)
            .await?
            .ok_or(WatchError::Unavailable)?;
        self.watch_actor(current).await
    }

    async fn unwatch(&self, watch_id: WatchId) -> Result<(), WatchError> {
        self.watches
            .lock()
            .expect("watch registry poisoned")
            .unwatch(watch_id)
            .map(|_| ())
            .ok_or(WatchError::InvalidCommand)
    }
}

fn map_remote_ask(error: RemoteMessageError) -> AskError {
    let code = match error {
        RemoteMessageError::StaleActivation => RemoteFailureCode::StaleActivation,
        RemoteMessageError::UnknownMessage | RemoteMessageError::UnsupportedProtocol => {
            RemoteFailureCode::UnknownMessage
        }
        RemoteMessageError::ProtocolFingerprintMismatch => RemoteFailureCode::ProtocolMismatch,
        RemoteMessageError::MailboxRejected => RemoteFailureCode::MailboxFull,
        RemoteMessageError::InvalidPayload => RemoteFailureCode::DecodeFailed,
        RemoteMessageError::DeadlineExceeded => RemoteFailureCode::DeadlineExceeded,
        RemoteMessageError::Unauthorized => RemoteFailureCode::Unauthorized,
        RemoteMessageError::HandlerFailed | RemoteMessageError::ZeroPendingLimit => {
            RemoteFailureCode::HandlerFailed
        }
    };
    AskError::Remote(code)
}
