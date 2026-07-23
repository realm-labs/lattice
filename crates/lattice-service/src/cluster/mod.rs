use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_actor::{
    error::ActorCallError,
    handle::ActorHandle,
    protocol::{ActorProtocolBinding, DispatchError, DispatchMode, DispatchReply, Protocol},
    registry::{ActorLoader, ActorRegistry},
    traits::Actor,
};
use lattice_core::{
    actor_ref::{
        ActorRef, ClusterId, ConfigFingerprint, EntityRef, EntityType, NodeAddress,
        NodeIncarnation, PlacementDomainId, ProtocolId, SingletonKind, SingletonRef,
    },
    id::ActorId,
};
use lattice_placement::{
    coordinator::SingletonConfig,
    mapping::{ShardMapper, ShardMapperBinding, ShardMappingError, Xxh3V1ShardMapper},
    region::EntityConfig,
    session::LogicPlacementState,
    types::{NodeKey, PlacementSlot, PlacementSlotKey, PlacementSlotState},
};
use lattice_remoting::{
    association::{AssociationKey, AssociationManager, AssociationState},
    messaging::{
        error::{AskError, RemoteMessageError, TellError},
        outbound::{OutboundMessage, OutboundMessaging},
        target::{LogicalEntityTarget, LogicalSingletonTarget, SenderIdentity},
    },
    protocol::ProtocolFingerprint,
    watch::WatchError,
};

use crate::backend::LogicalRouter;

pub mod api;
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

pub use api::{Cluster, ClusterEvent, ClusterEvents, ClusterState, ClusterWaitError};

use buffer::RouteBuffer;
use entity::EntityRoute;
use singleton::SingletonRoute;

static NEXT_LOGICAL_RESOLUTION: AtomicU64 = AtomicU64::new(1);
const LOGICAL_RESOLVE_MESSAGE_ID: u64 = u64::MAX;

fn next_logical_resolution(local: NodeIncarnation) -> u128 {
    let sequence = NEXT_LOGICAL_RESOLUTION.fetch_add(1, Ordering::Relaxed);
    (local.get() << 64) ^ u128::from(sequence)
}

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
        DispatchError::Actor(ActorCallError::ActorPanicked) => RemoteMessageError::ActorPanicked,
        DispatchError::MailboxRejected => RemoteMessageError::MailboxRejected,
        DispatchError::Actor(_) => RemoteMessageError::HandlerFailed,
    }
}

fn decode_resolved_actor(
    payload: &[u8],
    cluster: &ClusterId,
    address: &NodeAddress,
    incarnation: NodeIncarnation,
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

fn map_tell(error: TellError) -> RemoteMessageError {
    match error {
        TellError::Protocol(error) | TellError::Remote(error) => error,
        TellError::Association(_) => RemoteMessageError::HandlerFailed,
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
    #[error("cluster logical router shard mapper does not match its config")]
    ShardMapping(#[from] ShardMappingError),
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
