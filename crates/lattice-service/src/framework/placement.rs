use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_placement::error::PlacementError;
use lattice_placement::registry::InstanceRecord;
use lattice_placement::storage::{
    ActorPlacementKey, ActorPlacementRecord, PlacementPrefix, PlacementReadStore, PlacementVersion,
    PlacementWatch, SingletonKey, SingletonPlacementRecord, VirtualShardPlacementKey,
    VirtualShardPlacementRecord,
};

/// Read/watch-only placement view exposed to ordinary service extensions.
///
/// Placement mutations cross the separately configured semantic authority;
/// lifecycle-owned liveness writes stay private to the service runtime.
#[async_trait]
pub trait DynPlacementStore: Send + Sync + 'static {
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
    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError>;
    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError>;
    fn prefix(&self) -> PlacementPrefix;
}

#[async_trait]
impl<T> DynPlacementStore for T
where
    T: PlacementReadStore,
{
    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        PlacementReadStore::get_instance(self, instance_id).await
    }

    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        PlacementReadStore::list_instances(self, service_kind).await
    }

    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError> {
        PlacementReadStore::list_all_instances(self).await
    }

    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        PlacementReadStore::get_actor(self, key).await
    }

    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        PlacementReadStore::list_actors(self).await
    }

    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementReadStore::get_virtual_shard(self, key).await
    }

    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &lattice_core::kind::ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementReadStore::list_virtual_shards(self, service_kind, actor_kind).await
    }

    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        PlacementReadStore::list_virtual_shards_for_service(self, service_kind).await
    }

    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        PlacementReadStore::get_singleton(self, key).await
    }

    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError> {
        PlacementReadStore::watch(self, prefix).await
    }

    fn prefix(&self) -> PlacementPrefix {
        PlacementReadStore::prefix(self).clone()
    }
}

pub struct PlacementStoreComponent {
    inner: Arc<dyn DynPlacementStore>,
}

impl PlacementStoreComponent {
    pub fn new<T>(store: T) -> Self
    where
        T: PlacementReadStore,
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
