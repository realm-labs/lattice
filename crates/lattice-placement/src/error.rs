use lattice_core::actor_ref::Epoch;
use lattice_core::instance::{InstanceId, InstanceIncarnation};

use crate::registry::InstanceState;
use crate::storage::{LeaseId, PlacementVersion};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlacementError {
    #[error("no route found")]
    NoRoute,
    #[error("static placement supports only u64 route keys in phase 3")]
    UnsupportedRouteKey,
    #[error("virtual shard count must be greater than zero")]
    InvalidShardCount,
    #[error("no ready instances are available for placement")]
    NoReadyInstances,
    #[error("duplicate virtual shard assigner {name}")]
    DuplicateAssigner { name: &'static str },
    #[error("instance {instance_id} was not found")]
    InstanceNotFound { instance_id: InstanceId },
    #[error("instance {instance_id} is not ready: {state:?}")]
    InstanceNotReady {
        instance_id: InstanceId,
        state: InstanceState,
    },
    #[error("placement compare-and-put failed")]
    CompareAndPutFailed,
    #[error("this placement store does not support durable epoch reservations")]
    EpochReservationsUnsupported,
    #[error("placement epoch is exhausted")]
    EpochExhausted,
    #[error("placement epoch regressed from {current:?} to {incoming:?}")]
    EpochRegression { current: Epoch, incoming: Epoch },
    #[error("placement authority changed without advancing epoch {epoch:?}")]
    EpochAuthorityConflict { epoch: Epoch },
    #[error("stopped placement reactivated without advancing epoch {epoch:?}")]
    EpochReactivation { epoch: Epoch },
    #[error("placement epoch must be {expected:?}, got {incoming:?}")]
    EpochMismatch { expected: Epoch, incoming: Epoch },
    #[error("durable epoch floor {floor:?} is behind placement epoch {record:?}")]
    EpochFloorCorrupt { floor: Epoch, record: Epoch },
    #[error(
        "placement record token {record:?} is not proven by durable epoch floor token {floor:?}"
    )]
    EpochFloorUnproven {
        record: PlacementVersion,
        floor: Option<PlacementVersion>,
    },
    #[error("placement epoch reservation does not match its record")]
    EpochReservationMismatch,
    #[error("activation lock is already held for actor")]
    ActivationLockHeld,
    #[error("activation lock was lost for actor")]
    ActivationLockLost,
    #[error("instance lease {lease_id:?} was not found")]
    InstanceLeaseNotFound { lease_id: LeaseId },
    #[error("instance {instance_id} lease changed from expected {expected:?} to {actual:?}")]
    InstanceLeaseMismatch {
        instance_id: InstanceId,
        expected: LeaseId,
        actual: LeaseId,
    },
    #[error("instance {instance_id} incarnation changed from expected {expected} to {actual}")]
    InstanceIncarnationMismatch {
        instance_id: InstanceId,
        expected: InstanceIncarnation,
        actual: InstanceIncarnation,
    },
    #[error("instance {instance_id} is already registered")]
    InstanceAlreadyRegistered { instance_id: InstanceId },
    #[error("instance {instance_id} cannot transition from {current:?} to {requested:?}")]
    InvalidInstanceStateTransition {
        instance_id: InstanceId,
        current: crate::registry::InstanceState,
        requested: crate::registry::InstanceState,
    },
    #[error("coordinator leadership has been lost")]
    CoordinatorLeadershipLost,
    #[error("singleton activation lock is already held")]
    SingletonLockHeld,
    #[error("singleton activation lock was lost")]
    SingletonLockLost,
    #[error("placement watch closed")]
    PlacementWatchClosed,
    #[error("etcd placement store error: {message}")]
    Etcd { message: String },
    #[error("invalid etcd authentication configuration")]
    InvalidEtcdAuthentication,
    #[error("failed to read etcd password file: {kind:?}")]
    EtcdPasswordFile { kind: std::io::ErrorKind },
    #[error("failed to read etcd TLS CA file: {kind:?}")]
    EtcdTlsCaFile { kind: std::io::ErrorKind },
    #[error("authenticated etcd connection failed")]
    AuthenticatedEtcdConnect,
    #[error("etcd authentication failed")]
    EtcdAuthenticationFailed,
    #[error("invalid etcd endpoint")]
    InvalidEtcdEndpoint,
    #[error("authenticated etcd endpoints must all use HTTPS")]
    InsecureEtcdAuthenticationTransport,
    #[error("the dangerous unauthenticated etcd escape accepts loopback HTTP endpoints only")]
    InsecureEtcdUnauthenticatedTransport,
    #[error("userinfo in etcd endpoint URLs is unsupported; configure authentication separately")]
    EtcdEndpointUserinfoUnsupported,
    #[error("placement codec error: {message}")]
    PlacementCodec { message: String },
    #[error("logic control error: {message}")]
    LogicControl { message: String },
    #[error("placement authority RPC timeout must be in the range 1ns..=60s")]
    InvalidPlacementAuthorityTimeout,
    #[error("placement authority RPC timed out")]
    PlacementAuthorityTimeout,
    #[error("placement authority RPC failed with status {code:?}")]
    PlacementAuthorityRpc { code: tonic::Code },
    #[error("placement authority reply field {field} is invalid")]
    InvalidPlacementAuthorityReply { field: &'static str },
}
