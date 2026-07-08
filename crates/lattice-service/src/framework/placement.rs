use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_placement::control::TonicLogicControl;
use lattice_placement::coordination::actor::PlacementCoordinator;
use lattice_placement::coordination::reports::DrainReport;
use lattice_placement::error::PlacementError;
use lattice_placement::registry::InstanceRecord;
use lattice_placement::storage::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementPrefix, PlacementStore,
    PlacementVersion, PlacementWatch, SingletonKey, SingletonPlacementRecord,
    VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

#[async_trait]
pub trait DynPlacementStore: Send + Sync + 'static {
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError>;
    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError>;
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
    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Result<DrainReport, PlacementError>;
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
        actor_kind: &lattice_core::kind::ActorKind,
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
    fn prefix(&self) -> PlacementPrefix;
}

#[async_trait]
impl<T> DynPlacementStore for T
where
    T: PlacementStore,
{
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        PlacementStore::grant_instance_lease(self).await
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        PlacementStore::keepalive_instance_lease(self, lease_id).await
    }

    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError> {
        PlacementStore::upsert_instance(self, record).await
    }

    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        PlacementStore::get_instance(self, instance_id).await
    }

    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        PlacementStore::list_instances(self, service_kind).await
    }

    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError> {
        PlacementStore::list_all_instances(self).await
    }

    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Result<DrainReport, PlacementError> {
        PlacementCoordinator::new(self.clone(), TonicLogicControl)
            .drain_instance(service_kind, instance_id)
            .await
    }

    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        PlacementStore::get_actor(self, key).await
    }

    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        PlacementStore::list_actors(self).await
    }

    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        PlacementStore::compare_and_put_actor(self, key, expected, value).await
    }

    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementStore::get_virtual_shard(self, key).await
    }

    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &lattice_core::kind::ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementStore::list_virtual_shards(self, service_kind, actor_kind).await
    }

    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementStore::list_virtual_shards_for_service(self, service_kind).await
    }

    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        PlacementStore::compare_and_put_virtual_shard(self, key, expected, value).await
    }

    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        PlacementStore::get_singleton(self, key).await
    }

    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError> {
        PlacementStore::acquire_activation_lock(self, key).await
    }

    async fn validate_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        PlacementStore::validate_activation_lock(self, key, lease_id).await
    }

    async fn release_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        PlacementStore::release_activation_lock(self, key, lease_id).await
    }

    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError> {
        PlacementStore::watch(self, prefix).await
    }

    fn prefix(&self) -> PlacementPrefix {
        PlacementStore::prefix(self).clone()
    }
}

pub struct PlacementStoreComponent {
    inner: Arc<dyn DynPlacementStore>,
}

impl PlacementStoreComponent {
    pub fn new<T>(store: T) -> Self
    where
        T: PlacementStore,
    {
        Self {
            inner: Arc::new(store),
        }
    }

    pub fn inner(&self) -> Arc<dyn DynPlacementStore> {
        self.inner.clone()
    }
}

impl std::fmt::Debug for PlacementStoreComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlacementStoreComponent")
            .finish_non_exhaustive()
    }
}
