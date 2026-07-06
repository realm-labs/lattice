use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, EventType, GetOptions, PutOptions, Txn, TxnOp, WatchOptions,
};
use lattice_core::{ActorId, ActorKind, ConfiguredComponent, InstanceId, ServiceKind};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::instance::InstanceRecord;
use crate::store::{
    ActorPlacementKey, ActorPlacementRecord, CoordinatorLeadership, LeaseId, PlacementPrefix,
    PlacementStore, PlacementVersion, PlacementWatch, PlacementWatchEvent, SingletonKey,
    SingletonPlacementRecord, VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

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

#[async_trait]
pub trait EtcdKv: Clone + Send + Sync + 'static {
    async fn put(&self, key: String, value: EtcdValue) -> Result<(), PlacementError>;
    async fn get(&self, key: &str)
    -> Result<Option<(PlacementVersion, EtcdValue)>, PlacementError>;
    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, PlacementVersion, EtcdValue)>, PlacementError>;
    async fn compare_and_put(
        &self,
        key: String,
        expected: Option<PlacementVersion>,
        value: EtcdValue,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn compare_and_delete(
        &self,
        key: String,
        expected: EtcdValue,
    ) -> Result<(), PlacementError>;
    async fn delete(&self, key: &str) -> Result<(), PlacementError>;
    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError>;
    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError>;
    async fn next_lease_id(&self) -> Result<LeaseId, PlacementError>;
    async fn watch_prefix(&self, prefix: &str) -> Result<EtcdWatch, PlacementError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EtcdValue {
    Instance(Box<InstanceRecord>),
    Actor(Box<ActorPlacementRecord>),
    VirtualShard(Box<VirtualShardPlacementRecord>),
    Singleton(Box<SingletonPlacementRecord>),
    CoordinatorLeader(Box<CoordinatorLeadership>),
    ActivationLock(LeaseId),
    SingletonLock(LeaseId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdWatchEvent {
    pub key: String,
    pub version: PlacementVersion,
    pub value: Option<EtcdValue>,
}

#[derive(Debug)]
pub struct EtcdWatch {
    rx: broadcast::Receiver<EtcdWatchEvent>,
}

impl EtcdWatch {
    pub async fn next(&mut self) -> Result<EtcdWatchEvent, PlacementError> {
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
}

#[derive(Clone)]
pub struct RealEtcdClient {
    client: Client,
    instance_lease_ttl: InstanceLeaseTtl,
    activation_lock_ttl: ActivationLockTtl,
}

impl fmt::Debug for RealEtcdClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealEtcdClient")
            .field("instance_lease_ttl", &self.instance_lease_ttl)
            .field("activation_lock_ttl", &self.activation_lock_ttl)
            .finish_non_exhaustive()
    }
}

impl RealEtcdClient {
    pub async fn connect(
        endpoints: Vec<String>,
        instance_lease_ttl: InstanceLeaseTtl,
        activation_lock_ttl: ActivationLockTtl,
    ) -> Result<Self, PlacementError> {
        let client = Client::connect(endpoints, None).await.map_err(etcd_error)?;
        Ok(Self {
            client,
            instance_lease_ttl,
            activation_lock_ttl,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InstanceLeaseTtl(i64);

impl InstanceLeaseTtl {
    const DEFAULT_SECS: i64 = 30;

    pub fn new(seconds: i64) -> Self {
        if seconds > 0 {
            Self(seconds)
        } else {
            Self(Self::DEFAULT_SECS)
        }
    }

    fn as_secs(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ActivationLockTtl(i64);

impl ActivationLockTtl {
    const DEFAULT_SECS: i64 = 30;

    pub fn new(seconds: i64) -> Self {
        if seconds > 0 {
            Self(seconds)
        } else {
            Self(Self::DEFAULT_SECS)
        }
    }

    fn as_secs(self) -> i64 {
        self.0
    }
}

#[async_trait]
impl EtcdKv for RealEtcdClient {
    async fn put(&self, key: String, value: EtcdValue) -> Result<(), PlacementError> {
        let mut client = self.client.clone();
        client
            .put(key, encode_etcd_value(&value)?, put_options_for(&value)?)
            .await
            .map_err(etcd_error)?;
        Ok(())
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<(PlacementVersion, EtcdValue)>, PlacementError> {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(etcd_error)?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        Ok(Some((
            placement_version(kv.version())?,
            decode_etcd_value(kv.value())?,
        )))
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, PlacementVersion, EtcdValue)>, PlacementError> {
        let mut client = self.client.clone();
        let response = client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(etcd_error)?;
        response
            .kvs()
            .iter()
            .map(|kv| {
                Ok((
                    String::from_utf8(kv.key().to_vec()).map_err(codec_error)?,
                    placement_version(kv.version())?,
                    decode_etcd_value(kv.value())?,
                ))
            })
            .collect()
    }

    async fn compare_and_put(
        &self,
        key: String,
        expected: Option<PlacementVersion>,
        value: EtcdValue,
    ) -> Result<PlacementVersion, PlacementError> {
        let expected_version = expected.map_or(0, |version| version.0 as i64);
        let bytes = encode_etcd_value(&value)?;
        let put_options = put_options_for(&value)?;
        let txn = Txn::new()
            .when(vec![Compare::version(
                key.as_bytes(),
                CompareOp::Equal,
                expected_version,
            )])
            .and_then(vec![TxnOp::put(key.clone(), bytes, put_options)]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await.map_err(etcd_error)?;
        if !response.succeeded() {
            return Err(PlacementError::CompareAndPutFailed);
        }
        self.get(&key)
            .await?
            .map(|(version, _)| version)
            .ok_or_else(|| PlacementError::Etcd {
                message: format!("compare-and-put succeeded but key {key} was not readable"),
            })
    }

    async fn delete(&self, key: &str) -> Result<(), PlacementError> {
        let mut client = self.client.clone();
        client.delete(key, None).await.map_err(etcd_error)?;
        Ok(())
    }

    async fn compare_and_delete(
        &self,
        key: String,
        expected: EtcdValue,
    ) -> Result<(), PlacementError> {
        let expected = encode_etcd_value(&expected)?;
        let txn = Txn::new()
            .when(vec![Compare::value(
                key.as_bytes(),
                CompareOp::Equal,
                expected,
            )])
            .and_then(vec![TxnOp::delete(key, None)]);
        let mut client = self.client.clone();
        let response = client.txn(txn).await.map_err(etcd_error)?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(PlacementError::CompareAndPutFailed)
        }
    }

    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        let mut client = self.client.clone();
        let response = client
            .lease_grant(self.instance_lease_ttl.as_secs(), None)
            .await
            .map_err(etcd_error)?;
        lease_id(response.id())
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        let lease_id = i64::try_from(lease_id.0).map_err(codec_error)?;
        let mut client = self.client.clone();
        let (mut keeper, mut stream) = client
            .lease_keep_alive(lease_id)
            .await
            .map_err(etcd_error)?;
        keeper.keep_alive().await.map_err(etcd_error)?;
        stream.message().await.map_err(etcd_error)?;
        Ok(())
    }

    async fn next_lease_id(&self) -> Result<LeaseId, PlacementError> {
        let mut client = self.client.clone();
        let response = client
            .lease_grant(self.activation_lock_ttl.as_secs(), None)
            .await
            .map_err(etcd_error)?;
        lease_id(response.id())
    }

    async fn watch_prefix(&self, prefix: &str) -> Result<EtcdWatch, PlacementError> {
        let mut client = self.client.clone();
        let mut stream = client
            .watch(prefix, Some(WatchOptions::new().with_prefix()))
            .await
            .map_err(etcd_error)?;
        let (tx, rx) = broadcast::channel(128);
        tokio::spawn(async move {
            while let Ok(Some(response)) = stream.message().await {
                for event in response.events() {
                    let Some(kv) = event.kv() else {
                        continue;
                    };
                    let Ok(key) = String::from_utf8(kv.key().to_vec()) else {
                        continue;
                    };
                    let Ok(version) = placement_version(kv.version()) else {
                        continue;
                    };
                    let value = match event.event_type() {
                        EventType::Put => decode_etcd_value(kv.value()).ok(),
                        EventType::Delete => None,
                    };
                    let _ = tx.send(EtcdWatchEvent {
                        key,
                        version,
                        value,
                    });
                }
            }
        });
        Ok(EtcdWatch { rx })
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryEtcdClient {
    inner: Arc<std::sync::Mutex<HashMap<String, (PlacementVersion, EtcdValue)>>>,
    watchers: Arc<std::sync::Mutex<HashMap<String, broadcast::Sender<EtcdWatchEvent>>>>,
    instance_leases: Arc<std::sync::Mutex<HashMap<LeaseId, u64>>>,
    next_lease_id: Arc<AtomicU64>,
}

impl InMemoryEtcdClient {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(HashMap::new())),
            watchers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            instance_leases: Arc::new(std::sync::Mutex::new(HashMap::new())),
            next_lease_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn keys(&self) -> Vec<String> {
        let mut keys = self
            .inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }
}

#[async_trait]
impl EtcdKv for InMemoryEtcdClient {
    async fn put(&self, key: String, value: EtcdValue) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        let version = inner.get(&key).map_or(PlacementVersion(1), |(version, _)| {
            PlacementVersion(version.0 + 1)
        });
        inner.insert(key.clone(), (version, value.clone()));
        drop(inner);
        self.notify_watchers(EtcdWatchEvent {
            key,
            version,
            value: Some(value),
        });
        Ok(())
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<(PlacementVersion, EtcdValue)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .get(key)
            .cloned())
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, PlacementVersion, EtcdValue)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, (version, value))| (key.clone(), *version, value.clone()))
            .collect())
    }

    async fn compare_and_put(
        &self,
        key: String,
        expected: Option<PlacementVersion>,
        value: EtcdValue,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        let current = inner.get(&key).map(|(version, _)| *version);
        if current != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let next = PlacementVersion(current.map_or(1, |version| version.0 + 1));
        inner.insert(key.clone(), (next, value.clone()));
        drop(inner);
        self.notify_watchers(EtcdWatchEvent {
            key,
            version: next,
            value: Some(value),
        });
        Ok(next)
    }

    async fn delete(&self, key: &str) -> Result<(), PlacementError> {
        let removed = self
            .inner
            .lock()
            .expect("in-memory etcd mutex poisoned")
            .remove(key);
        if let Some((version, _)) = removed {
            self.notify_watchers(EtcdWatchEvent {
                key: key.to_string(),
                version,
                value: None,
            });
        }
        Ok(())
    }

    async fn compare_and_delete(
        &self,
        key: String,
        expected: EtcdValue,
    ) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("in-memory etcd mutex poisoned");
        match inner.get(&key) {
            Some((_, current)) if current == &expected => {}
            _ => return Err(PlacementError::CompareAndPutFailed),
        }
        let removed = inner.remove(&key);
        drop(inner);
        if let Some((version, _)) = removed {
            self.notify_watchers(EtcdWatchEvent {
                key,
                version,
                value: None,
            });
        }
        Ok(())
    }

    async fn grant_instance_lease(&self) -> Result<LeaseId, PlacementError> {
        let lease_id = LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst));
        self.instance_leases
            .lock()
            .expect("in-memory etcd leases mutex poisoned")
            .insert(lease_id, 0);
        Ok(lease_id)
    }

    async fn keepalive_instance_lease(&self, lease_id: LeaseId) -> Result<(), PlacementError> {
        let mut leases = self
            .instance_leases
            .lock()
            .expect("in-memory etcd leases mutex poisoned");
        let Some(keepalives) = leases.get_mut(&lease_id) else {
            return Err(PlacementError::InstanceLeaseNotFound { lease_id });
        };
        *keepalives += 1;
        Ok(())
    }

    async fn next_lease_id(&self) -> Result<LeaseId, PlacementError> {
        Ok(LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst)))
    }

    async fn watch_prefix(&self, prefix: &str) -> Result<EtcdWatch, PlacementError> {
        let mut watchers = self
            .watchers
            .lock()
            .expect("in-memory etcd watchers mutex poisoned");
        let rx = watchers
            .entry(prefix.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(128);
                tx
            })
            .subscribe();
        Ok(EtcdWatch { rx })
    }
}

impl InMemoryEtcdClient {
    fn notify_watchers(&self, event: EtcdWatchEvent) {
        let watchers = self
            .watchers
            .lock()
            .expect("in-memory etcd watchers mutex poisoned")
            .iter()
            .filter(|(prefix, _)| event.key.starts_with(prefix.as_str()))
            .map(|(_, tx)| tx.clone())
            .collect::<Vec<_>>();
        for tx in watchers {
            let _ = tx.send(event.clone());
        }
    }
}

fn clean_prefix(prefix: &PlacementPrefix) -> &str {
    prefix.as_str().trim_end_matches('/')
}

fn encode_etcd_value(value: &EtcdValue) -> Result<Vec<u8>, PlacementError> {
    serde_json::to_vec(value).map_err(codec_error)
}

fn decode_etcd_value(bytes: &[u8]) -> Result<EtcdValue, PlacementError> {
    serde_json::from_slice(bytes).map_err(codec_error)
}

fn put_options_for(value: &EtcdValue) -> Result<Option<PutOptions>, PlacementError> {
    match value {
        EtcdValue::Instance(record) => {
            let lease_id = i64::try_from(record.lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::CoordinatorLeader(leadership) => {
            let lease_id = i64::try_from(leadership.lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::ActivationLock(lease_id) | EtcdValue::SingletonLock(lease_id) => {
            let lease_id = i64::try_from(lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::Singleton(record) => {
            let lease_id = i64::try_from(record.lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::Actor(_) | EtcdValue::VirtualShard(_) => Ok(None),
    }
}

fn default_instance_lease_ttl_secs() -> i64 {
    InstanceLeaseTtl::DEFAULT_SECS
}

fn placement_version(version: i64) -> Result<PlacementVersion, PlacementError> {
    let version = u64::try_from(version).map_err(codec_error)?;
    Ok(PlacementVersion(version))
}

fn lease_id(id: i64) -> Result<LeaseId, PlacementError> {
    let id = u64::try_from(id).map_err(codec_error)?;
    Ok(LeaseId(id))
}

fn etcd_error(error: etcd_client::Error) -> PlacementError {
    PlacementError::Etcd {
        message: error.to_string(),
    }
}

fn codec_error(error: impl std::fmt::Display) -> PlacementError {
    PlacementError::PlacementCodec {
        message: error.to_string(),
    }
}

fn instance_key(
    prefix: &PlacementPrefix,
    service_kind: &ServiceKind,
    instance_id: &InstanceId,
) -> String {
    format!(
        "{}/logic/instances/{}/{}",
        clean_prefix(prefix),
        service_kind.as_str(),
        instance_id.as_str()
    )
}

fn actor_key(prefix: &PlacementPrefix, key: &ActorPlacementKey) -> String {
    format!(
        "{}/logic/actors/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.actor_kind.as_str(),
        actor_id_segment(&key.actor_id)
    )
}

fn vshard_key(prefix: &PlacementPrefix, key: &VirtualShardPlacementKey) -> String {
    format!(
        "{}/logic/vshards/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.actor_kind.as_str(),
        key.shard_id.0
    )
}

fn singleton_key(prefix: &PlacementPrefix, key: &SingletonKey) -> String {
    format!(
        "{}/logic/singletons/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.singleton_kind.as_str(),
        scope_segment(&key.scope)
    )
}

fn activation_lock_key(prefix: &PlacementPrefix, key: &ActorPlacementKey) -> String {
    format!(
        "{}/logic/activation_locks/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.actor_kind.as_str(),
        actor_id_segment(&key.actor_id)
    )
}

fn singleton_lock_key(prefix: &PlacementPrefix, key: &SingletonKey) -> String {
    format!(
        "{}/logic/singleton_locks/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.singleton_kind.as_str(),
        scope_segment(&key.scope)
    )
}

fn coordinator_leader_key(prefix: &PlacementPrefix) -> String {
    format!("{}/coordinator/leader", clean_prefix(prefix))
}

fn scope_segment(scope: &str) -> String {
    hex_encode(scope.as_bytes())
}

fn actor_id_segment(actor_id: &ActorId) -> String {
    match actor_id {
        ActorId::Str(value) => format!("str:{value}"),
        ActorId::U64(value) => format!("u64:{value}"),
        ActorId::I64(value) => format!("i64:{value}"),
        ActorId::Bytes(value) => format!("bytes:{}", hex_encode(value)),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

#[cfg(test)]
mod tests;
