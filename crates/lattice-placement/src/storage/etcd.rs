use std::num::NonZeroUsize;

use async_trait::async_trait;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_core::service_context::ConfiguredComponent;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::storage::etcd::client::{
    ActivationLockTtl, EtcdKv, EtcdOwnershipRanges, EtcdOwnershipWatchEvent,
    EtcdOwnershipWatchUpdate, InMemoryEtcdClient, InstanceLeaseTtl, RealEtcdClient,
};
use crate::storage::etcd::codec::{
    EtcdValue, activation_lock_key, actor_key, actor_service_prefix, clean_prefix,
    coordinator_leader_key, default_instance_lease_ttl_secs, instance_key, logic_prefix,
    singleton_key, singleton_lock_key, singleton_service_prefix, vshard_key, vshard_service_prefix,
};
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, CoordinatorLeadership, LeaseId, OwnershipView,
    OwnershipViewError, OwnershipViewRecord, OwnershipViewSnapshot, OwnershipWatch,
    OwnershipWatchBatch, OwnershipWatchError, OwnershipWatchEvent, OwnershipWatchMessage,
    OwnershipWatchUpdate, PlacementPrefix, PlacementStore, PlacementVersion, PlacementWatch,
    PlacementWatchEvent, SingletonKey, SingletonPlacementRecord, VirtualShardPlacementKey,
    VirtualShardPlacementRecord,
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

    async fn open_ownership_view(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        max_entries: NonZeroUsize,
    ) -> Result<OwnershipView, OwnershipViewError> {
        let mut raw = self
            .client
            .open_ownership_view(
                EtcdOwnershipRanges {
                    local_instance_key: instance_key(&self.prefix, service_kind, instance_id),
                    record_prefixes: vec![
                        actor_service_prefix(&self.prefix, service_kind),
                        vshard_service_prefix(&self.prefix, service_kind),
                        singleton_service_prefix(&self.prefix, service_kind),
                    ],
                    watch_prefix: logic_prefix(&self.prefix),
                },
                max_entries,
            )
            .await?;

        let mut local_instance = None;
        let mut records = Vec::new();
        for entry in raw.snapshot.entries {
            validate_etcd_value_key(&self.prefix, &entry.key, &entry.value).map_err(|error| {
                OwnershipViewError::Protocol {
                    message: error.to_string(),
                }
            })?;
            match entry.value {
                EtcdValue::Instance(record) => {
                    if record.service_kind != *service_kind || record.instance_id != *instance_id {
                        return Err(OwnershipViewError::Protocol {
                            message: format!(
                                "etcd ownership snapshot returned unexpected instance {} for service {}",
                                record.instance_id, record.service_kind
                            ),
                        });
                    }
                    local_instance = Some(*record);
                }
                EtcdValue::Actor(record) => {
                    if record.service_kind != *service_kind {
                        return Err(snapshot_service_mismatch(
                            service_kind,
                            &record.service_kind,
                        ));
                    }
                    if record.owner == *instance_id {
                        records.push(OwnershipViewRecord::Actor {
                            revision: entry.revision,
                            record: *record,
                        });
                    }
                }
                EtcdValue::VirtualShard(record) => {
                    if record.service_kind != *service_kind {
                        return Err(snapshot_service_mismatch(
                            service_kind,
                            &record.service_kind,
                        ));
                    }
                    if record.owner == *instance_id {
                        records.push(OwnershipViewRecord::VirtualShard {
                            revision: entry.revision,
                            record: *record,
                        });
                    }
                }
                EtcdValue::Singleton(record) => {
                    if record.service_kind != *service_kind {
                        return Err(snapshot_service_mismatch(
                            service_kind,
                            &record.service_kind,
                        ));
                    }
                    if record.owner == *instance_id {
                        records.push(OwnershipViewRecord::Singleton {
                            revision: entry.revision,
                            record: *record,
                        });
                    }
                }
                EtcdValue::CoordinatorLeader(_)
                | EtcdValue::ActivationLock(_)
                | EtcdValue::SingletonLock(_) => {
                    return Err(OwnershipViewError::Protocol {
                        message: format!(
                            "etcd ownership snapshot returned non-ownership key {}",
                            entry.key
                        ),
                    });
                }
            }
        }
        if records.len() > max_entries.get() {
            return Err(OwnershipViewError::CapacityExceeded {
                max_entries: max_entries.get(),
            });
        }

        let snapshot = OwnershipViewSnapshot {
            revision: raw.snapshot.revision,
            local_instance,
            records,
        };
        let prefix = self.prefix.clone();
        let expected_service = service_kind.clone();
        let expected_instance = instance_id.clone();
        let snapshot_revision = snapshot.revision;
        let max_watch_entries = max_entries.get();
        let (tx, rx) = broadcast::channel(128);
        tokio::spawn(async move {
            let mut high_water = snapshot_revision;
            loop {
                match raw.watch.next_update().await {
                    Ok(EtcdOwnershipWatchUpdate::Progress { revision }) => {
                        if let Err(error) = advance_etcd_watch_progress(revision, &mut high_water) {
                            let _ = tx.send(OwnershipWatchMessage::Failed(error));
                            break;
                        }
                        if tx
                            .send(OwnershipWatchMessage::Update(
                                OwnershipWatchUpdate::Progress { revision },
                            ))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(EtcdOwnershipWatchUpdate::Batch(batch)) => {
                        if batch.events.len() > max_watch_entries {
                            let _ = tx.send(OwnershipWatchMessage::Failed(
                                OwnershipWatchError::CapacityExceeded {
                                    max_entries: max_watch_entries,
                                },
                            ));
                            break;
                        }
                        if let Err(error) =
                            advance_etcd_watch_batch(batch.revision, &mut high_water)
                        {
                            let _ = tx.send(OwnershipWatchMessage::Failed(error));
                            break;
                        }
                        match map_etcd_watch_batch(
                            &prefix,
                            &expected_service,
                            &expected_instance,
                            batch.revision,
                            batch.events,
                        ) {
                            Ok(Some(batch)) => {
                                if tx
                                    .send(OwnershipWatchMessage::Update(
                                        OwnershipWatchUpdate::Batch(batch),
                                    ))
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Ok(None) => {
                                if tx
                                    .send(OwnershipWatchMessage::Update(
                                        OwnershipWatchUpdate::Progress {
                                            revision: batch.revision,
                                        },
                                    ))
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = tx.send(OwnershipWatchMessage::Failed(error));
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(OwnershipWatchMessage::Failed(error));
                        break;
                    }
                }
            }
        });
        Ok(OwnershipView {
            snapshot,
            watch: OwnershipWatch::new(rx),
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

fn snapshot_service_mismatch(expected: &ServiceKind, actual: &ServiceKind) -> OwnershipViewError {
    OwnershipViewError::Protocol {
        message: format!("etcd ownership snapshot expected service {expected}, got {actual}"),
    }
}

fn validate_etcd_value_key(
    prefix: &PlacementPrefix,
    actual_key: &str,
    value: &EtcdValue,
) -> Result<(), OwnershipWatchError> {
    let expected_key = match value {
        EtcdValue::Instance(record) => {
            instance_key(prefix, &record.service_kind, &record.instance_id)
        }
        EtcdValue::Actor(record) => actor_key(
            prefix,
            &ActorPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                actor_id: record.actor_id.clone(),
            },
        ),
        EtcdValue::VirtualShard(record) => vshard_key(
            prefix,
            &VirtualShardPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                shard_id: record.shard_id,
            },
        ),
        EtcdValue::Singleton(record) => singleton_key(
            prefix,
            &SingletonKey {
                service_kind: record.service_kind.clone(),
                singleton_kind: record.singleton_kind.clone(),
                scope: record.scope.clone(),
            },
        ),
        EtcdValue::CoordinatorLeader(_) => coordinator_leader_key(prefix),
        EtcdValue::ActivationLock(_) => {
            let namespace = format!("{}/logic/activation_locks/", clean_prefix(prefix));
            if actual_key.starts_with(&namespace) {
                return Ok(());
            }
            return Err(OwnershipWatchError::Protocol {
                message: format!("etcd activation lock appeared outside {namespace}: {actual_key}"),
            });
        }
        EtcdValue::SingletonLock(_) => {
            let namespace = format!("{}/logic/singleton_locks/", clean_prefix(prefix));
            if actual_key.starts_with(&namespace) {
                return Ok(());
            }
            return Err(OwnershipWatchError::Protocol {
                message: format!("etcd singleton lock appeared outside {namespace}: {actual_key}"),
            });
        }
    };
    if actual_key != expected_key {
        return Err(OwnershipWatchError::Protocol {
            message: format!(
                "etcd ownership value key mismatch: expected {expected_key}, got {actual_key}"
            ),
        });
    }
    Ok(())
}

fn map_etcd_watch_batch(
    prefix: &PlacementPrefix,
    expected_service: &ServiceKind,
    expected_instance: &InstanceId,
    revision: crate::storage::PlacementRevision,
    raw_events: Vec<EtcdOwnershipWatchEvent>,
) -> Result<Option<OwnershipWatchBatch>, OwnershipWatchError> {
    let mut events = Vec::new();
    for event in raw_events {
        let mapped = match event {
            EtcdOwnershipWatchEvent::Upserted { key, value, .. } => map_etcd_watch_value(
                prefix,
                expected_service,
                expected_instance,
                &key,
                value,
                false,
            )?,
            EtcdOwnershipWatchEvent::Deleted {
                key,
                previous_value,
                ..
            } => map_etcd_watch_value(
                prefix,
                expected_service,
                expected_instance,
                &key,
                previous_value,
                true,
            )?,
        };
        if let Some(event) = mapped {
            events.push(event);
        }
    }
    if events.is_empty() {
        Ok(None)
    } else {
        Ok(Some(OwnershipWatchBatch { revision, events }))
    }
}

fn advance_etcd_watch_batch(
    revision: crate::storage::PlacementRevision,
    high_water: &mut crate::storage::PlacementRevision,
) -> Result<(), OwnershipWatchError> {
    if revision <= *high_water {
        return Err(OwnershipWatchError::Protocol {
            message: format!(
                "etcd ownership batch revision {revision:?} did not advance beyond {high_water:?}"
            ),
        });
    }
    *high_water = revision;
    Ok(())
}

fn advance_etcd_watch_progress(
    revision: crate::storage::PlacementRevision,
    high_water: &mut crate::storage::PlacementRevision,
) -> Result<(), OwnershipWatchError> {
    if revision < *high_water {
        return Err(OwnershipWatchError::Protocol {
            message: format!(
                "etcd ownership progress revision {revision:?} regressed behind {high_water:?}"
            ),
        });
    }
    *high_water = revision;
    Ok(())
}

fn map_etcd_watch_value(
    prefix: &PlacementPrefix,
    expected_service: &ServiceKind,
    expected_instance: &InstanceId,
    key: &str,
    value: EtcdValue,
    deleted: bool,
) -> Result<Option<OwnershipWatchEvent>, OwnershipWatchError> {
    validate_etcd_value_key(prefix, key, &value)?;
    let event = match value {
        EtcdValue::Instance(record) => {
            if record.service_kind != *expected_service || record.instance_id != *expected_instance
            {
                return Ok(None);
            }
            if deleted {
                OwnershipWatchEvent::InstanceDeleted { record: *record }
            } else {
                OwnershipWatchEvent::InstanceUpserted { record: *record }
            }
        }
        EtcdValue::Actor(record) => {
            if record.service_kind != *expected_service {
                return Ok(None);
            }
            let key = ActorPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                actor_id: record.actor_id.clone(),
            };
            if deleted {
                OwnershipWatchEvent::ActorDeleted { key }
            } else {
                OwnershipWatchEvent::ActorUpserted {
                    key,
                    record: *record,
                }
            }
        }
        EtcdValue::VirtualShard(record) => {
            if record.service_kind != *expected_service {
                return Ok(None);
            }
            let key = VirtualShardPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                shard_id: record.shard_id,
            };
            if deleted {
                OwnershipWatchEvent::VirtualShardDeleted { key }
            } else {
                OwnershipWatchEvent::VirtualShardUpserted {
                    key,
                    record: *record,
                }
            }
        }
        EtcdValue::Singleton(record) => {
            if record.service_kind != *expected_service {
                return Ok(None);
            }
            let key = SingletonKey {
                service_kind: record.service_kind.clone(),
                singleton_kind: record.singleton_kind.clone(),
                scope: record.scope.clone(),
            };
            if deleted {
                OwnershipWatchEvent::SingletonDeleted { key }
            } else {
                OwnershipWatchEvent::SingletonUpserted {
                    key,
                    record: *record,
                }
            }
        }
        EtcdValue::CoordinatorLeader(_)
        | EtcdValue::ActivationLock(_)
        | EtcdValue::SingletonLock(_) => return Ok(None),
    };
    Ok(Some(event))
}

#[cfg(test)]
mod tests;
