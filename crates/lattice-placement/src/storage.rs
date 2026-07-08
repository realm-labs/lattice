use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::sharding::VirtualShardId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorPlacementKey {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlacementVersion(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementState {
    Activating,
    Running,
    Draining,
    Migrating,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorPlacementRecord {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
    pub owner: InstanceId,
    pub epoch: Epoch,
    pub lease_id: LeaseId,
    pub state: PlacementState,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VirtualShardPlacementKey {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_id: VirtualShardId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualShardPlacementRecord {
    pub service_kind: ServiceKind,
    pub actor_kind: ActorKind,
    pub shard_id: VirtualShardId,
    pub owner: InstanceId,
    pub epoch: Epoch,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SingletonKey {
    pub service_kind: ServiceKind,
    pub singleton_kind: ActorKind,
    pub scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingletonPlacementRecord {
    pub service_kind: ServiceKind,
    pub singleton_kind: ActorKind,
    pub scope: String,
    pub owner: InstanceId,
    pub epoch: Epoch,
    pub lease_id: LeaseId,
    pub state: PlacementState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinatorLeadership {
    pub candidate_id: InstanceId,
    pub lease_id: LeaseId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementWatchEvent {
    InstanceUpdated {
        record: InstanceRecord,
    },
    ActorUpdated {
        key: ActorPlacementKey,
        version: PlacementVersion,
        record: ActorPlacementRecord,
    },
    VirtualShardUpdated {
        key: VirtualShardPlacementKey,
        version: PlacementVersion,
        record: VirtualShardPlacementRecord,
    },
    SingletonUpdated {
        key: SingletonKey,
        version: PlacementVersion,
        record: SingletonPlacementRecord,
    },
}

#[derive(Debug)]
pub struct PlacementWatch {
    rx: broadcast::Receiver<PlacementWatchEvent>,
}

impl PlacementWatch {
    pub async fn next(&mut self) -> Result<PlacementWatchEvent, PlacementError> {
        loop {
            match self.rx.recv().await {
                Ok(event) => return Ok(event),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(PlacementError::PlacementWatchClosed);
                }
            }
        }
    }

    pub(crate) fn new(rx: broadcast::Receiver<PlacementWatchEvent>) -> Self {
        Self { rx }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlacementPrefix(String);

impl PlacementPrefix {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[async_trait]
pub trait PlacementStore: Clone + Send + Sync + 'static {
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError>;
    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError>;
    async fn campaign_coordinator_leader(
        &self,
        candidate_id: InstanceId,
    ) -> Result<Option<CoordinatorLeadership>, PlacementError>;
    async fn keepalive_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError>;
    async fn resign_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError>;
    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError>;
    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError>;
    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError>;
    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError>;
    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError>;
    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError>;
    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError>;
    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError>;
    async fn list_singletons(
        &self,
    ) -> Result<Vec<(PlacementVersion, SingletonPlacementRecord)>, PlacementError>;
    async fn compare_and_put_singleton(
        &self,
        key: SingletonKey,
        expected: Option<PlacementVersion>,
        value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn acquire_singleton_lock(&self, key: SingletonKey) -> Result<LeaseId, PlacementError>;
    async fn validate_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn release_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError>;
    async fn validate_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn release_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError>;
    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError>;
    fn prefix(&self) -> &PlacementPrefix;
}

pub mod etcd;
pub mod memory;
