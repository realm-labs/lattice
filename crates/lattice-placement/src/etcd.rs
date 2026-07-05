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
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementPrefix, PlacementStore,
    PlacementVersion, PlacementWatch, PlacementWatchEvent, VirtualShardPlacementKey,
    VirtualShardPlacementRecord,
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
    pub activation_lock_ttl_secs: i64,
}

impl EtcdPlacementStore<RealEtcdClient> {
    pub fn from_config() -> ConfiguredComponent<Self> {
        ConfiguredComponent::from_section("placement_store", Self::connect)
    }

    pub async fn connect(config: EtcdPlacementStoreConfig) -> Result<Self, PlacementError> {
        let client = RealEtcdClient::connect(
            config.endpoints,
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

    async fn release_activation_lock(&self, key: &ActorPlacementKey) -> Result<(), PlacementError> {
        self.client
            .delete(&activation_lock_key(&self.prefix, key))
            .await
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
    async fn delete(&self, key: &str) -> Result<(), PlacementError>;
    async fn next_lease_id(&self) -> Result<LeaseId, PlacementError>;
    async fn watch_prefix(&self, prefix: &str) -> Result<EtcdWatch, PlacementError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EtcdValue {
    Instance(Box<InstanceRecord>),
    Actor(Box<ActorPlacementRecord>),
    VirtualShard(Box<VirtualShardPlacementRecord>),
    ActivationLock(LeaseId),
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
    activation_lock_ttl: ActivationLockTtl,
}

impl fmt::Debug for RealEtcdClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RealEtcdClient")
            .field("activation_lock_ttl", &self.activation_lock_ttl)
            .finish_non_exhaustive()
    }
}

impl RealEtcdClient {
    pub async fn connect(
        endpoints: Vec<String>,
        activation_lock_ttl: ActivationLockTtl,
    ) -> Result<Self, PlacementError> {
        let client = Client::connect(endpoints, None).await.map_err(etcd_error)?;
        Ok(Self {
            client,
            activation_lock_ttl,
        })
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
    next_lease_id: Arc<AtomicU64>,
}

impl InMemoryEtcdClient {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(HashMap::new())),
            watchers: Arc::new(std::sync::Mutex::new(HashMap::new())),
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
        EtcdValue::ActivationLock(lease_id) => {
            let lease_id = i64::try_from(lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::Instance(_) | EtcdValue::Actor(_) | EtcdValue::VirtualShard(_) => Ok(None),
    }
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
        "{}/logic/actors/{}/{}",
        clean_prefix(prefix),
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

fn activation_lock_key(prefix: &PlacementPrefix, key: &ActorPlacementKey) -> String {
    format!(
        "{}/logic/activation_locks/{}/{}",
        clean_prefix(prefix),
        key.actor_kind.as_str(),
        actor_id_segment(&key.actor_id)
    )
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
mod tests {
    use std::collections::BTreeMap;

    use lattice_core::instance::InstanceCapacity;
    use lattice_core::{ActorId, Epoch, actor_kind, service_kind};

    use super::*;
    use crate::instance::InstanceState;
    use crate::store::PlacementState;

    #[tokio::test]
    async fn etcd_store_writes_under_cluster_prefix_and_isolates_reads() {
        let client = InMemoryEtcdClient::new();
        let first =
            EtcdPlacementStore::new(PlacementPrefix::new("/lattice/cluster-a"), client.clone());
        let second =
            EtcdPlacementStore::new(PlacementPrefix::new("/lattice/cluster-b"), client.clone());
        let key = actor_key_for(7);

        first
            .upsert_instance(instance_record("world-a", InstanceState::Ready))
            .await
            .unwrap();
        first
            .compare_and_put_actor(key.clone(), None, actor_record(7, "world-a", 1, LeaseId(1)))
            .await
            .unwrap();
        first
            .compare_and_put_virtual_shard(vshard_key_for(3), None, vshard_record(3, "world-a", 1))
            .await
            .unwrap();

        assert_eq!(
            client.keys(),
            vec![
                "/lattice/cluster-a/logic/actors/World/u64:7".to_string(),
                "/lattice/cluster-a/logic/instances/World/world-a".to_string(),
                "/lattice/cluster-a/logic/vshards/World/World/3".to_string(),
            ]
        );
        assert!(second.get_actor(&key).await.unwrap().is_none());
        assert!(
            second
                .get_virtual_shard(&vshard_key_for(3))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            second
                .list_instances(&service_kind!("World"))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn etcd_store_compare_and_put_uses_versions() {
        let store = EtcdPlacementStore::new(
            PlacementPrefix::new("/lattice/test"),
            InMemoryEtcdClient::new(),
        );
        let key = actor_key_for(7);
        let record = actor_record(7, "world-a", 1, LeaseId(1));

        let version = store
            .compare_and_put_actor(key.clone(), None, record.clone())
            .await
            .unwrap();
        let stale = store
            .compare_and_put_actor(key.clone(), None, record.clone())
            .await;
        let updated = ActorPlacementRecord {
            epoch: Epoch(2),
            ..record
        };
        let next = store
            .compare_and_put_actor(key.clone(), Some(version), updated.clone())
            .await
            .unwrap();

        assert_eq!(version, PlacementVersion(1));
        assert_eq!(stale, Err(PlacementError::CompareAndPutFailed));
        assert_eq!(next, PlacementVersion(2));
        assert_eq!(store.get_actor(&key).await.unwrap().unwrap().1, updated);
    }

    #[tokio::test]
    async fn etcd_store_persists_virtual_shards_with_versions() {
        let store = EtcdPlacementStore::new(
            PlacementPrefix::new("/lattice/test"),
            InMemoryEtcdClient::new(),
        );
        let key = vshard_key_for(9);
        let record = vshard_record(9, "world-a", 1);

        let version = store
            .compare_and_put_virtual_shard(key.clone(), None, record.clone())
            .await
            .unwrap();
        let stale = store
            .compare_and_put_virtual_shard(key.clone(), None, record.clone())
            .await;
        let updated = VirtualShardPlacementRecord {
            owner: InstanceId::new("world-b"),
            epoch: Epoch(2),
            ..record
        };
        let next = store
            .compare_and_put_virtual_shard(key.clone(), Some(version), updated.clone())
            .await
            .unwrap();

        assert_eq!(version, PlacementVersion(1));
        assert_eq!(stale, Err(PlacementError::CompareAndPutFailed));
        assert_eq!(next, PlacementVersion(2));
        assert_eq!(
            store.get_virtual_shard(&key).await.unwrap().unwrap().1,
            updated
        );
        assert_eq!(
            store
                .list_virtual_shards(&service_kind!("World"), &actor_kind!("World"))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn etcd_watch_reports_virtual_shard_updates() {
        let store = EtcdPlacementStore::new(
            PlacementPrefix::new("/lattice/test"),
            InMemoryEtcdClient::new(),
        );
        let mut watch = store.watch(store.prefix().clone()).await.unwrap();
        let key = vshard_key_for(5);
        let record = vshard_record(5, "world-a", 1);
        let version = store
            .compare_and_put_virtual_shard(key.clone(), None, record.clone())
            .await
            .unwrap();

        let event = watch.next().await.unwrap();
        assert_eq!(
            event,
            PlacementWatchEvent::VirtualShardUpdated {
                key,
                version,
                record,
            }
        );
    }

    #[tokio::test]
    async fn etcd_store_activation_lock_is_exclusive_until_release() {
        let store = EtcdPlacementStore::new(
            PlacementPrefix::new("/lattice/test"),
            InMemoryEtcdClient::new(),
        );
        let key = actor_key_for(7);

        let first = store.acquire_activation_lock(key.clone()).await.unwrap();
        let second = store.acquire_activation_lock(key.clone()).await;
        store.release_activation_lock(&key).await.unwrap();
        let third = store.acquire_activation_lock(key).await.unwrap();

        assert_eq!(first, LeaseId(1));
        assert_eq!(second, Err(PlacementError::ActivationLockHeld));
        assert_eq!(third, LeaseId(3));
    }

    #[test]
    fn etcd_store_builds_from_config() {
        let store = EtcdPlacementStore::in_memory_from_config(EtcdPlacementStoreConfig {
            key_prefix: "/lattice/test".to_string(),
            endpoints: vec!["http://127.0.0.1:2379".to_string()],
            activation_lock_ttl_secs: 30,
        });

        assert_eq!(store.prefix().as_str(), "/lattice/test");
        assert_eq!(
            EtcdPlacementStore::from_config().section(),
            "placement_store"
        );
    }

    #[test]
    fn etcd_value_codec_round_trips_placement_metadata() {
        let instance =
            EtcdValue::Instance(Box::new(instance_record("world-a", InstanceState::Ready)));
        let actor = EtcdValue::Actor(Box::new(actor_record(7, "world-a", 3, LeaseId(5))));
        let lock = EtcdValue::ActivationLock(LeaseId(42));

        for value in [instance, actor, lock] {
            let encoded = encode_etcd_value(&value).unwrap();
            let decoded = decode_etcd_value(&encoded).unwrap();
            assert_eq!(decoded, value);
        }
    }

    fn actor_key_for(actor_id: u64) -> ActorPlacementKey {
        ActorPlacementKey {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(actor_id),
        }
    }

    fn vshard_key_for(shard_id: u32) -> VirtualShardPlacementKey {
        VirtualShardPlacementKey {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            shard_id: crate::vshard::VirtualShardId(shard_id),
        }
    }

    fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
        InstanceRecord {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new(instance_id),
            advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
            control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
            version: "test".to_string(),
            state,
            capacity: InstanceCapacity::default(),
            labels: BTreeMap::new(),
        }
    }

    fn actor_record(
        actor_id: u64,
        owner: &str,
        epoch: u64,
        lease_id: LeaseId,
    ) -> ActorPlacementRecord {
        ActorPlacementRecord {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(actor_id),
            owner: InstanceId::new(owner),
            epoch: Epoch(epoch),
            lease_id,
            state: PlacementState::Running,
        }
    }

    fn vshard_record(shard_id: u32, owner: &str, epoch: u64) -> VirtualShardPlacementRecord {
        VirtualShardPlacementRecord {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            shard_id: crate::vshard::VirtualShardId(shard_id),
            owner: InstanceId::new(owner),
            epoch: Epoch(epoch),
        }
    }
}
