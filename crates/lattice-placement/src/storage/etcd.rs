use async_trait::async_trait;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_core::service_context::ConfiguredComponent;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::storage::etcd::client::{
    ActivationLockTtl, EtcdKv, InMemoryEtcdClient, InstanceLeaseTtl, RealEtcdClient,
};
use crate::storage::etcd::codec::{
    EtcdValue, activation_lock_key, actor_key, clean_prefix, coordinator_leader_key,
    default_instance_lease_ttl_secs, instance_key, singleton_key, singleton_lock_key, vshard_key,
};
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, CoordinatorLeadership, LeaseId, PlacementPrefix,
    PlacementStore, PlacementVersion, PlacementWatch, PlacementWatchEvent, SingletonKey,
    SingletonPlacementRecord, VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

pub mod client;
pub mod codec;

#[derive(Debug, Clone)]
pub struct EtcdPlacementStore<C> {
    prefix: PlacementPrefix,
    client: C,
}

impl<C> EtcdPlacementStore<C> {
    pub fn new(prefix: PlacementPrefix, client: C) -> Self {
        Self { prefix, client }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtcdPlacementStoreConfig {
    pub key_prefix: String,
    pub endpoints: Vec<String>,
    #[serde(default = "default_instance_lease_ttl_secs")]
    pub instance_lease_ttl_secs: i64,
    pub activation_lock_ttl_secs: i64,
}

impl EtcdPlacementStore<RealEtcdClient> {
    pub fn from_config() -> ConfiguredComponent<Self> {
        ConfiguredComponent::from_section("placement_store", Self::connect)
    }

    pub async fn connect(config: EtcdPlacementStoreConfig) -> Result<Self, PlacementError> {
        let client = RealEtcdClient::connect(
            config.endpoints,
            InstanceLeaseTtl::new(config.instance_lease_ttl_secs),
            ActivationLockTtl::new(config.activation_lock_ttl_secs),
        )
        .await?;
        Ok(Self::new(PlacementPrefix::new(config.key_prefix), client))
    }

    pub async fn from_options(config: EtcdPlacementStoreConfig) -> Result<Self, PlacementError> {
        Self::connect(config).await
    }
}

impl EtcdPlacementStore<InMemoryEtcdClient> {
    pub fn in_memory_from_config(config: EtcdPlacementStoreConfig) -> Self {
        Self::new(
            PlacementPrefix::new(config.key_prefix),
            InMemoryEtcdClient::new(),
        )
    }
}

#[async_trait]
impl<C> PlacementStore for EtcdPlacementStore<C>
where
    C: EtcdKv,
{
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        self.client.grant_instance_lease().await
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        self.client.keepalive_instance_lease(lease_id).await
    }

    async fn campaign_coordinator_leader(
        &self,
        candidate_id: InstanceId,
    ) -> Result<Option<CoordinatorLeadership>, PlacementError> {
        let lease_id = self.client.grant_instance_lease().await?;
        let leadership = CoordinatorLeadership {
            candidate_id,
            lease_id,
        };
        match self
            .client
            .compare_and_put(
                coordinator_leader_key(&self.prefix),
                None,
                EtcdValue::CoordinatorLeader(Box::new(leadership.clone())),
            )
            .await
        {
            Ok(_) => Ok(Some(leadership)),
            Err(PlacementError::CompareAndPutFailed) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn keepalive_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError> {
        let Some((_, EtcdValue::CoordinatorLeader(current))) = self
            .client
            .get(&coordinator_leader_key(&self.prefix))
            .await?
        else {
            return Err(PlacementError::CoordinatorLeadershipLost);
        };
        if current.as_ref() != leadership {
            return Err(PlacementError::CoordinatorLeadershipLost);
        }
        self.client
            .keepalive_instance_lease(leadership.lease_id)
            .await
    }

    async fn resign_coordinator_leader(
        &self,
        leadership: &CoordinatorLeadership,
    ) -> Result<(), PlacementError> {
        let Some((_, EtcdValue::CoordinatorLeader(current))) = self
            .client
            .get(&coordinator_leader_key(&self.prefix))
            .await?
        else {
            return Ok(());
        };
        if current.as_ref() == leadership {
            self.client
                .delete(&coordinator_leader_key(&self.prefix))
                .await?;
        }
        Ok(())
    }

    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError> {
        self.client
            .put(
                instance_key(&self.prefix, &record.service_kind, &record.instance_id),
                EtcdValue::Instance(Box::new(record)),
            )
            .await
    }

    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        let prefix = format!("{}/logic/instances/", clean_prefix(&self.prefix));
        for (_key, _version, value) in self.client.list_prefix(&prefix).await? {
            if let EtcdValue::Instance(record) = value
                && &record.instance_id == instance_id
            {
                return Ok(Some(*record));
            }
        }
        Ok(None)
    }

    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        let prefix = format!(
            "{}/logic/instances/{}/",
            clean_prefix(&self.prefix),
            service_kind.as_str()
        );
        Ok(self
            .client
            .list_prefix(&prefix)
            .await?
            .into_iter()
            .filter_map(|(_key, _version, value)| match value {
                EtcdValue::Instance(record) => Some(*record),
                _ => None,
            })
            .collect())
    }

    async fn list_all_instances(&self) -> Result<Vec<InstanceRecord>, PlacementError> {
        let prefix = format!("{}/logic/instances/", clean_prefix(&self.prefix));
        Ok(self
            .client
            .list_prefix(&prefix)
            .await?
            .into_iter()
            .filter_map(|(_key, _version, value)| match value {
                EtcdValue::Instance(record) => Some(*record),
                _ => None,
            })
            .collect())
    }

    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        let Some((version, value)) = self.client.get(&actor_key(&self.prefix, key)).await? else {
            return Ok(None);
        };
        match value {
            EtcdValue::Actor(record) => Ok(Some((version, *record))),
            _ => Ok(None),
        }
    }

    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        let prefix = format!("{}/logic/actors/", clean_prefix(&self.prefix));
        Ok(self
            .client
            .list_prefix(&prefix)
            .await?
            .into_iter()
            .filter_map(|(_key, version, value)| match value {
                EtcdValue::Actor(record) => Some((version, *record)),
                _ => None,
            })
            .collect())
    }

    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        self.client
            .compare_and_put(
                actor_key(&self.prefix, &key),
                expected,
                EtcdValue::Actor(Box::new(value)),
            )
            .await
    }

    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        let Some((version, value)) = self.client.get(&vshard_key(&self.prefix, key)).await? else {
            return Ok(None);
        };
        match value {
            EtcdValue::VirtualShard(record) => Ok(Some((version, *record))),
            _ => Ok(None),
        }
    }

    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        let prefix = format!(
            "{}/logic/vshards/{}/{}/",
            clean_prefix(&self.prefix),
            service_kind.as_str(),
            actor_kind.as_str()
        );
        Ok(self
            .client
            .list_prefix(&prefix)
            .await?
            .into_iter()
            .filter_map(|(_key, version, value)| match value {
                EtcdValue::VirtualShard(record) => Some((version, *record)),
                _ => None,
            })
            .collect())
    }

    async fn list_virtual_shards_for_service(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        let prefix = format!(
            "{}/logic/vshards/{}/",
            clean_prefix(&self.prefix),
            service_kind.as_str()
        );
        Ok(self
            .client
            .list_prefix(&prefix)
            .await?
            .into_iter()
            .filter_map(|(_key, version, value)| match value {
                EtcdValue::VirtualShard(record) => Some((version, *record)),
                _ => None,
            })
            .collect())
    }

    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        self.client
            .compare_and_put(
                vshard_key(&self.prefix, &key),
                expected,
                EtcdValue::VirtualShard(Box::new(value)),
            )
            .await
    }

    async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        let Some((version, value)) = self.client.get(&singleton_key(&self.prefix, key)).await?
        else {
            return Ok(None);
        };
        match value {
            EtcdValue::Singleton(record) => Ok(Some((version, *record))),
            _ => Ok(None),
        }
    }

    async fn list_singletons(
        &self,
    ) -> Result<Vec<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        let prefix = format!("{}/logic/singletons/", clean_prefix(&self.prefix));
        Ok(self
            .client
            .list_prefix(&prefix)
            .await?
            .into_iter()
            .filter_map(|(_key, version, value)| match value {
                EtcdValue::Singleton(record) => Some((version, *record)),
                _ => None,
            })
            .collect())
    }

    async fn compare_and_put_singleton(
        &self,
        key: SingletonKey,
        expected: Option<PlacementVersion>,
        value: SingletonPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        self.client
            .compare_and_put(
                singleton_key(&self.prefix, &key),
                expected,
                EtcdValue::Singleton(Box::new(value)),
            )
            .await
    }

    async fn acquire_singleton_lock(&self, key: SingletonKey) -> Result<LeaseId, PlacementError> {
        let lease_id = self.client.next_lease_id().await?;
        match self
            .client
            .compare_and_put(
                singleton_lock_key(&self.prefix, &key),
                None,
                EtcdValue::SingletonLock(lease_id),
            )
            .await
        {
            Ok(_) => Ok(lease_id),
            Err(PlacementError::CompareAndPutFailed) => Err(PlacementError::SingletonLockHeld),
            Err(error) => Err(error),
        }
    }

    async fn validate_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        match self
            .client
            .get(&singleton_lock_key(&self.prefix, key))
            .await?
        {
            Some((_, EtcdValue::SingletonLock(current))) if current == lease_id => Ok(()),
            _ => Err(PlacementError::SingletonLockLost),
        }
    }

    async fn release_singleton_lock(
        &self,
        key: &SingletonKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        self.client
            .compare_and_delete(
                singleton_lock_key(&self.prefix, key),
                EtcdValue::SingletonLock(lease_id),
            )
            .await
            .map_err(|error| match error {
                PlacementError::CompareAndPutFailed => PlacementError::SingletonLockLost,
                error => error,
            })
    }

    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError> {
        let lease_id = self.client.next_lease_id().await?;
        match self
            .client
            .compare_and_put(
                activation_lock_key(&self.prefix, &key),
                None,
                EtcdValue::ActivationLock(lease_id),
            )
            .await
        {
            Ok(_) => Ok(lease_id),
            Err(PlacementError::CompareAndPutFailed) => Err(PlacementError::ActivationLockHeld),
            Err(error) => Err(error),
        }
    }

    async fn validate_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        match self
            .client
            .get(&activation_lock_key(&self.prefix, key))
            .await?
        {
            Some((_, EtcdValue::ActivationLock(current))) if current == lease_id => Ok(()),
            _ => Err(PlacementError::ActivationLockLost),
        }
    }

    async fn release_activation_lock(
        &self,
        key: &ActorPlacementKey,
        lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        self.client
            .compare_and_delete(
                activation_lock_key(&self.prefix, key),
                EtcdValue::ActivationLock(lease_id),
            )
            .await
            .map_err(|error| match error {
                PlacementError::CompareAndPutFailed => PlacementError::ActivationLockLost,
                error => error,
            })
    }

    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError> {
        let logic_prefix = format!("{}/logic/", clean_prefix(&prefix));
        let mut etcd_watch = self.client.watch_prefix(&logic_prefix).await?;
        let (tx, rx) = broadcast::channel(128);
        tokio::spawn(async move {
            while let Ok(event) = etcd_watch.next().await {
                match event.value {
                    Some(EtcdValue::Instance(record)) => {
                        let _ = tx.send(PlacementWatchEvent::InstanceUpdated { record: *record });
                    }
                    Some(EtcdValue::Actor(record)) => {
                        let record = *record;
                        let key = ActorPlacementKey {
                            service_kind: record.service_kind.clone(),
                            actor_kind: record.actor_kind.clone(),
                            actor_id: record.actor_id.clone(),
                        };
                        let _ = tx.send(PlacementWatchEvent::ActorUpdated {
                            key,
                            version: event.version,
                            record,
                        });
                    }
                    Some(EtcdValue::VirtualShard(record)) => {
                        let record = *record;
                        let key = VirtualShardPlacementKey {
                            service_kind: record.service_kind.clone(),
                            actor_kind: record.actor_kind.clone(),
                            shard_id: record.shard_id,
                        };
                        let _ = tx.send(PlacementWatchEvent::VirtualShardUpdated {
                            key,
                            version: event.version,
                            record,
                        });
                    }
                    Some(EtcdValue::Singleton(record)) => {
                        let record = *record;
                        let key = SingletonKey {
                            service_kind: record.service_kind.clone(),
                            singleton_kind: record.singleton_kind.clone(),
                            scope: record.scope.clone(),
                        };
                        let _ = tx.send(PlacementWatchEvent::SingletonUpdated {
                            key,
                            version: event.version,
                            record,
                        });
                    }
                    _ => {}
                }
            }
        });
        Ok(PlacementWatch::new(rx))
    }

    fn prefix(&self) -> &PlacementPrefix {
        &self.prefix
    }
}

#[cfg(test)]
mod tests;
