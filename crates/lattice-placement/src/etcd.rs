use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use lattice_core::{ActorId, InstanceId, ServiceKind};
use tokio::sync::broadcast;

use crate::{
    ActorPlacementKey, ActorPlacementRecord, InstanceRecord, LeaseId, PlacementError,
    PlacementPrefix, PlacementStore, PlacementVersion, PlacementWatch, PlacementWatchEvent,
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
        let actor_prefix = format!("{}/logic/actors/", clean_prefix(&prefix));
        let mut etcd_watch = self.client.watch_prefix(&actor_prefix).await?;
        let (tx, rx) = broadcast::channel(128);
        tokio::spawn(async move {
            while let Ok(event) = etcd_watch.next().await {
                let Some(EtcdValue::Actor(record)) = event.value else {
                    continue;
                };
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EtcdValue {
    Instance(Box<InstanceRecord>),
    Actor(Box<ActorPlacementRecord>),
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

    use lattice_core::{ActorId, Epoch, InstanceCapacity, actor_kind, service_kind};

    use super::*;
    use crate::{InstanceState, PlacementState};

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

        assert_eq!(
            client.keys(),
            vec![
                "/lattice/cluster-a/logic/actors/World/u64:7".to_string(),
                "/lattice/cluster-a/logic/instances/World/world-a".to_string(),
            ]
        );
        assert!(second.get_actor(&key).await.unwrap().is_none());
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

    fn actor_key_for(actor_id: u64) -> ActorPlacementKey {
        ActorPlacementKey {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(actor_id),
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
}
