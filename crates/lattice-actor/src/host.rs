use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{ActorRef, ProtocolId};
use lattice_remoting::messaging::error::RemoteMessageError;
use lattice_remoting::messaging::inbound::InboundDispatch;
use lattice_remoting::messaging::target::ExactActorTarget;
use thiserror::Error;

use crate::protocol::{ActorProtocolBinding, DispatchError, DispatchMode, DispatchReply, Protocol};
use crate::registry::{ActorCellDiagnostics, ActorRegistry};
use crate::traits::Actor;

#[async_trait]
trait ErasedActorHost: Send + Sync {
    fn protocol_id(&self) -> ProtocolId;
    fn is_current(&self, target: &ExactActorTarget) -> bool;
    fn subscribe_terminated(
        &self,
        target: &ExactActorTarget,
    ) -> Option<crate::handle::ActorTerminationSubscription>;
    async fn drain_all(&self) -> Vec<ActorCellDiagnostics>;
    async fn force_shutdown_all(&self, reason: &str, ticket: &str) -> Vec<ActorCellDiagnostics>;
    fn live_cells(&self) -> Vec<ActorCellDiagnostics>;
    async fn retry_stop(
        &self,
        local_ref: crate::watch::LocalActorRef,
    ) -> Option<Result<(), crate::registry::ActorQuarantineError>>;
    async fn force_stop(
        &self,
        local_ref: crate::watch::LocalActorRef,
        reason: &str,
        ticket: &str,
    ) -> Option<Result<(), crate::registry::ActorQuarantineError>>;

    async fn tell(
        &self,
        sender: Option<ActorRef>,
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

pub struct ActorHost<A: Actor, P: Protocol> {
    registry: Arc<ActorRegistry<A>>,
    protocol: Arc<ActorProtocolBinding<A, P>>,
}

impl<A: Actor, P: Protocol> ActorHost<A, P> {
    pub fn new(registry: Arc<ActorRegistry<A>>, protocol: Arc<ActorProtocolBinding<A, P>>) -> Self {
        Self { registry, protocol }
    }

    fn resolve(
        &self,
        target: &ExactActorTarget,
    ) -> Result<crate::handle::ActorHandle<A>, RemoteMessageError> {
        let reference = ActorRef::new(
            target.cluster_id.clone(),
            target.node_address.clone(),
            target.node_incarnation,
            target.actor_path.clone(),
            target.activation_id,
            target.protocol_id,
        )
        .map_err(|_| RemoteMessageError::InvalidPayload)?;
        self.registry
            .get_exact(&reference)
            .ok_or(RemoteMessageError::StaleActivation)
    }
}

#[async_trait]
impl<A: Actor, P: Protocol> ErasedActorHost for ActorHost<A, P> {
    fn protocol_id(&self) -> ProtocolId {
        self.protocol.protocol_id()
    }

    fn is_current(&self, target: &ExactActorTarget) -> bool {
        self.resolve(target).is_ok()
    }

    fn subscribe_terminated(
        &self,
        target: &ExactActorTarget,
    ) -> Option<crate::handle::ActorTerminationSubscription> {
        self.resolve(target)
            .ok()
            .map(|handle| handle.subscribe_terminated())
    }

    async fn drain_all(&self) -> Vec<ActorCellDiagnostics> {
        let _ = self.registry.drain().await;
        self.registry.live_cells()
    }

    async fn force_shutdown_all(&self, reason: &str, ticket: &str) -> Vec<ActorCellDiagnostics> {
        self.registry.force_shutdown(reason, ticket).await
    }

    fn live_cells(&self) -> Vec<ActorCellDiagnostics> {
        self.registry.live_cells()
    }

    async fn retry_stop(
        &self,
        local_ref: crate::watch::LocalActorRef,
    ) -> Option<Result<(), crate::registry::ActorQuarantineError>> {
        self.registry
            .live_cells()
            .iter()
            .any(|cell| cell.local_ref == local_ref)
            .then(|| self.registry.retry_stop_exact(local_ref))?
            .await
            .into()
    }

    async fn force_stop(
        &self,
        local_ref: crate::watch::LocalActorRef,
        reason: &str,
        ticket: &str,
    ) -> Option<Result<(), crate::registry::ActorQuarantineError>> {
        self.registry
            .live_cells()
            .iter()
            .any(|cell| cell.local_ref == local_ref)
            .then(|| {
                self.registry
                    .force_stop_exact(local_ref, reason.to_owned(), ticket.to_owned())
            })?
            .await
            .into()
    }

    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if sender
            .as_ref()
            .is_some_and(|sender| sender.cluster_id() != &target.cluster_id)
        {
            return Err(RemoteMessageError::Unauthorized);
        }
        let handle = self.resolve(&target)?;
        match self
            .protocol
            .dispatch_with_sender(
                handle,
                message_id,
                DispatchMode::Tell,
                payload,
                None,
                sender,
            )
            .await
            .map_err(map_dispatch)?
        {
            DispatchReply::TellAccepted => Ok(()),
            DispatchReply::Ask(_) => Err(RemoteMessageError::InvalidPayload),
        }
    }

    async fn ask(
        &self,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if Instant::now() >= deadline {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        let handle = self.resolve(&target)?;
        match self
            .protocol
            .dispatch(
                handle,
                message_id,
                DispatchMode::Ask,
                payload,
                Some(deadline),
            )
            .await
            .map_err(map_dispatch)?
        {
            DispatchReply::Ask(reply) => Ok(reply),
            DispatchReply::TellAccepted => Err(RemoteMessageError::InvalidPayload),
        }
    }
}

pub struct ProtocolHostRegistry {
    maximum: usize,
    hosts: BTreeMap<u64, Arc<dyn ErasedActorHost>>,
}

impl ProtocolHostRegistry {
    pub fn new(maximum: usize) -> Result<Self, HostRegistryError> {
        if maximum == 0 {
            return Err(HostRegistryError::ZeroLimit);
        }
        Ok(Self {
            maximum,
            hosts: BTreeMap::new(),
        })
    }

    pub fn register<A: Actor, P: Protocol>(
        &mut self,
        host: ActorHost<A, P>,
    ) -> Result<(), HostRegistryError> {
        if self.hosts.len() == self.maximum {
            return Err(HostRegistryError::Capacity);
        }
        let protocol_id = host.protocol_id().get();
        if self.hosts.insert(protocol_id, Arc::new(host)).is_some() {
            return Err(HostRegistryError::DuplicateProtocol(protocol_id));
        }
        Ok(())
    }

    pub fn is_current(&self, target: &ExactActorTarget) -> bool {
        self.hosts
            .get(&target.protocol_id.get())
            .is_some_and(|host| host.is_current(target))
    }

    pub fn subscribe_terminated(
        &self,
        target: &ExactActorTarget,
    ) -> Option<crate::handle::ActorTerminationSubscription> {
        self.hosts
            .get(&target.protocol_id.get())
            .and_then(|host| host.subscribe_terminated(target))
    }

    pub async fn drain_all(&self) -> Vec<ActorCellDiagnostics> {
        let mut cells = Vec::new();
        for host in self.hosts.values() {
            cells.extend(host.drain_all().await);
        }
        cells
    }

    pub async fn force_shutdown_all(
        &self,
        reason: &str,
        ticket: &str,
    ) -> Vec<ActorCellDiagnostics> {
        let mut cells = Vec::new();
        for host in self.hosts.values() {
            cells.extend(host.force_shutdown_all(reason, ticket).await);
        }
        cells
    }

    pub fn live_cells(&self) -> Vec<ActorCellDiagnostics> {
        self.hosts
            .values()
            .flat_map(|host| host.live_cells())
            .collect()
    }

    pub async fn retry_stop(
        &self,
        local_ref: crate::watch::LocalActorRef,
    ) -> Result<(), HostAdminError> {
        for host in self.hosts.values() {
            if let Some(result) = host.retry_stop(local_ref).await {
                return result.map_err(HostAdminError::Actor);
            }
        }
        Err(HostAdminError::NotFound(local_ref.id()))
    }

    pub async fn force_stop(
        &self,
        local_ref: crate::watch::LocalActorRef,
        reason: &str,
        ticket: &str,
    ) -> Result<(), HostAdminError> {
        for host in self.hosts.values() {
            if let Some(result) = host.force_stop(local_ref, reason, ticket).await {
                return result.map_err(HostAdminError::Actor);
            }
        }
        Err(HostAdminError::NotFound(local_ref.id()))
    }
}

#[async_trait]
impl InboundDispatch for ProtocolHostRegistry {
    async fn tell(
        &self,
        sender: Option<ActorRef>,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.hosts
            .get(&target.protocol_id.get())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .tell(sender, target, message_id, payload)
            .await
    }

    async fn ask(
        &self,
        target: ExactActorTarget,
        message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        self.hosts
            .get(&target.protocol_id.get())
            .ok_or(RemoteMessageError::UnsupportedProtocol)?
            .ask(target, message_id, payload, deadline)
            .await
    }
}

fn map_dispatch(error: DispatchError) -> RemoteMessageError {
    match error {
        DispatchError::UnknownMessage(_) | DispatchError::UnregisteredType => {
            RemoteMessageError::UnknownMessage
        }
        DispatchError::Decode(_)
        | DispatchError::ModeMismatch
        | DispatchError::ReplyTypeMismatch => RemoteMessageError::InvalidPayload,
        DispatchError::PayloadTooLarge { .. } | DispatchError::Encode(_) => {
            RemoteMessageError::InvalidPayload
        }
        DispatchError::MissingDeadline => RemoteMessageError::DeadlineExceeded,
        DispatchError::MailboxRejected => RemoteMessageError::MailboxRejected,
        DispatchError::Actor(crate::error::ActorCallError::DeadlineExceeded) => {
            RemoteMessageError::DeadlineExceeded
        }
        DispatchError::Actor(crate::error::ActorCallError::ActorPanicked) => {
            RemoteMessageError::ActorPanicked
        }
        DispatchError::Actor(_) => RemoteMessageError::HandlerFailed,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HostRegistryError {
    #[error("actor host registry limit must be nonzero")]
    ZeroLimit,
    #[error("actor host registry is full")]
    Capacity,
    #[error("actor host registry contains duplicate ProtocolId {0}")]
    DuplicateProtocol(u64),
}

#[derive(Debug, Error)]
pub enum HostAdminError {
    #[error("Actor cell {0} is not registered on this service")]
    NotFound(u64),
    #[error(transparent)]
    Actor(#[from] crate::registry::ActorQuarantineError),
}
