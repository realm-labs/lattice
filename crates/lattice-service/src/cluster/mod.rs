use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_actor::protocol::{
    ActorProtocolBinding, DispatchError, DispatchMode, DispatchReply, Protocol,
};
use lattice_actor::registry::{ActorLoader, ActorRegistry};
use lattice_actor::traits::Actor;
use lattice_actor::{error::ActorCallError, handle::ActorHandle};
use lattice_core::actor_ref::{
    ActorRef, ConfigFingerprint, EntityRef, EntityType, PlacementDomainId, ProtocolId,
    SingletonKind, SingletonRef,
};
use lattice_core::id::ActorId;
use lattice_placement::coordinator::SingletonConfig;
use lattice_placement::region::EntityConfig;
use lattice_placement::session::LogicPlacementState;
use lattice_placement::types::NodeKey;
use lattice_placement::types::PlacementSlot;
use lattice_placement::types::PlacementSlotKey;
use lattice_placement::types::PlacementSlotState;
use lattice_remoting::association::AssociationKey;
use lattice_remoting::association::AssociationManager;
use lattice_remoting::association::AssociationState;
use lattice_remoting::messaging::error::AskError;
use lattice_remoting::messaging::error::RemoteMessageError;
use lattice_remoting::messaging::outbound::OutboundMessaging;
use lattice_remoting::messaging::target::LogicalEntityTarget;
use lattice_remoting::messaging::target::LogicalSingletonTarget;
use lattice_remoting::messaging::target::SenderIdentity;
use lattice_remoting::protocol::ProtocolFingerprint;
use lattice_remoting::watch::WatchError;

use crate::backend::LogicalRouter;

mod buffer;
mod entity;
pub mod join;
pub mod members;
pub(crate) mod membership_runtime;
pub mod peers;
mod proxy;
mod router;
pub(crate) mod runtime;
mod singleton;
mod singleton_proxy;

use buffer::RouteBuffer;
use entity::EntityRoute;
use singleton::SingletonRoute;

static NEXT_LOGICAL_RESOLUTION: AtomicU64 = AtomicU64::new(1);
const LOGICAL_RESOLVE_MESSAGE_ID: u64 = u64::MAX;

#[derive(Debug, Clone)]
pub struct LogicalBufferConfig {
    pub maximum_messages_per_slot: usize,
    pub maximum_messages: usize,
    pub maximum_bytes: usize,
    pub maximum_residence: Duration,
    pub maximum_control_payload: usize,
}

impl Default for LogicalBufferConfig {
    fn default() -> Self {
        Self {
            maximum_messages_per_slot: 1_024,
            maximum_messages: 10_000,
            maximum_bytes: 64 * 1024 * 1024,
            maximum_residence: Duration::from_secs(30),
            maximum_control_payload: lattice_placement::control::DEFAULT_MAX_CONTROL_PAYLOAD,
        }
    }
}

impl LogicalBufferConfig {
    fn validate(&self) -> Result<(), ClusterRouterError> {
        if self.maximum_messages_per_slot == 0
            || self.maximum_messages == 0
            || self.maximum_messages_per_slot > self.maximum_messages
            || self.maximum_bytes == 0
            || self.maximum_residence.is_zero()
            || self.maximum_control_payload == 0
        {
            return Err(ClusterRouterError::InvalidBufferConfig);
        }
        Ok(())
    }
}

pub struct DomainLogicalRouter {
    local_node: NodeKey,
    state: Arc<Mutex<LogicPlacementState>>,
    associations: Arc<AssociationManager>,
    peers: Option<Arc<peers::PeerReconciler>>,
    messaging: Arc<OutboundMessaging>,
    coordinator: AssociationKey,
    buffer_config: LogicalBufferConfig,
    entities: BTreeMap<(PlacementDomainId, EntityType), Arc<dyn EntityRoute>>,
    singletons: BTreeMap<(PlacementDomainId, SingletonKind), Arc<dyn SingletonRoute>>,
    maximum_registrations: usize,
}

async fn drain_actor_ids<A, I>(
    registry: &ActorRegistry<A>,
    actor_ids: I,
    timeout: Duration,
) -> Result<bool, RemoteMessageError>
where
    A: Actor,
    I: IntoIterator<Item = ActorId>,
{
    let _ = timeout;
    Ok(registry.drain_actor_ids(actor_ids).await.completed())
}

fn map_dispatch(error: DispatchError) -> RemoteMessageError {
    match error {
        DispatchError::UnknownMessage(_) | DispatchError::UnregisteredType => {
            RemoteMessageError::UnknownMessage
        }
        DispatchError::Decode(_)
        | DispatchError::ModeMismatch
        | DispatchError::ReplyTypeMismatch
        | DispatchError::PayloadTooLarge { .. }
        | DispatchError::Encode(_) => RemoteMessageError::InvalidPayload,
        DispatchError::MissingDeadline | DispatchError::Actor(ActorCallError::DeadlineExceeded) => {
            RemoteMessageError::DeadlineExceeded
        }
        DispatchError::MailboxRejected => RemoteMessageError::MailboxRejected,
        DispatchError::Actor(_) => RemoteMessageError::HandlerFailed,
    }
}

fn decode_resolved_actor(
    payload: &[u8],
    cluster: &lattice_core::actor_ref::ClusterId,
    address: &lattice_core::actor_ref::NodeAddress,
    incarnation: lattice_core::actor_ref::NodeIncarnation,
    protocol_id: ProtocolId,
) -> Result<ActorRef, WatchError> {
    let actor: ActorRef =
        serde_json::from_slice(payload).map_err(|_| WatchError::InvalidCommand)?;
    if actor.cluster_id() != cluster
        || actor.node_address() != address
        || actor.node_incarnation() != incarnation
        || actor.protocol_id() != protocol_id
    {
        return Err(WatchError::InvalidCommand);
    }
    Ok(actor)
}

fn map_tell(error: lattice_remoting::messaging::error::TellError) -> RemoteMessageError {
    match error {
        lattice_remoting::messaging::error::TellError::Protocol(error)
        | lattice_remoting::messaging::error::TellError::Remote(error) => error,
        lattice_remoting::messaging::error::TellError::Association(_) => {
            RemoteMessageError::HandlerFailed
        }
    }
}

fn map_ask(error: RemoteMessageError) -> AskError {
    AskError::Protocol(error)
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterRouterError {
    #[error("cluster logical router node identity is invalid")]
    InvalidNode,
    #[error("cluster logical router limit must be nonzero")]
    ZeroLimit,
    #[error("cluster logical router buffer configuration is invalid")]
    InvalidBufferConfig,
    #[error("cluster logical router Coordinator identity is invalid")]
    InvalidCoordinator,
    #[error("cluster logical router registration capacity reached")]
    Capacity,
    #[error("cluster logical router protocol does not match its config")]
    ProtocolMismatch,
    #[error("entity type {entity_type:?} is already registered in placement domain {domain}")]
    DuplicateEntity {
        domain: PlacementDomainId,
        entity_type: EntityType,
    },
    #[error("singleton kind {kind:?} is already registered in placement domain {domain}")]
    DuplicateSingleton {
        domain: PlacementDomainId,
        kind: SingletonKind,
    },
}

#[cfg(test)]
mod tests;
