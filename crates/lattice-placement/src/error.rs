use lattice_core::InstanceId;

use crate::instance::InstanceState;
use crate::store::LeaseId;

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
    #[error("activation lock is already held for actor")]
    ActivationLockHeld,
    #[error("instance lease {lease_id:?} was not found")]
    InstanceLeaseNotFound { lease_id: LeaseId },
    #[error("singleton activation lock is already held")]
    SingletonLockHeld,
    #[error("placement watch closed")]
    PlacementWatchClosed,
    #[error("etcd placement store error: {message}")]
    Etcd { message: String },
    #[error("placement codec error: {message}")]
    PlacementCodec { message: String },
    #[error("logic control error: {message}")]
    LogicControl { message: String },
}
