use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use lattice_core::{ActorId, ActorKind, Epoch, InstanceId, ServiceKind};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::instance::InstanceRecord;
use crate::vshard::VirtualShardId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorPlacementKey {
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
    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError>;
    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError>;
    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError>;
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
    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError>;
    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError>;
    async fn release_activation_lock(&self, key: &ActorPlacementKey) -> Result<(), PlacementError>;
    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError>;
    fn prefix(&self) -> &PlacementPrefix;
}

#[derive(Debug, Clone)]
pub struct InMemoryPlacementStore {
    prefix: PlacementPrefix,
    inner: Arc<std::sync::Mutex<PlacementStoreInner>>,
    next_lease_id: Arc<AtomicU64>,
}

impl InMemoryPlacementStore {
    pub fn new(prefix: PlacementPrefix) -> Self {
        Self {
            prefix,
            inner: Arc::new(std::sync::Mutex::new(PlacementStoreInner::default())),
            next_lease_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn with_shared_inner(prefix: PlacementPrefix, other: &Self) -> Self {
        Self {
            prefix,
            inner: other.inner.clone(),
            next_lease_id: other.next_lease_id.clone(),
        }
    }

    fn prefixed_actor_key(&self, key: &ActorPlacementKey) -> PrefixedActorKey {
        PrefixedActorKey {
            prefix: self.prefix.clone(),
            key: key.clone(),
        }
    }

    fn prefixed_vshard_key(&self, key: &VirtualShardPlacementKey) -> PrefixedVShardKey {
        PrefixedVShardKey {
            prefix: self.prefix.clone(),
            key: key.clone(),
        }
    }

    fn prefixed_instance_key(&self, instance_id: &InstanceId) -> PrefixedInstanceKey {
        PrefixedInstanceKey {
            prefix: self.prefix.clone(),
            instance_id: instance_id.clone(),
        }
    }
}

#[async_trait]
impl PlacementStore for InMemoryPlacementStore {
    async fn upsert_instance(&self, record: InstanceRecord) -> Result<(), PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        inner.instances.insert(
            self.prefixed_instance_key(&record.instance_id),
            record.clone(),
        );
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::InstanceUpdated { record },
        );
        Ok(())
    }

    async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .instances
            .get(&self.prefixed_instance_key(instance_id))
            .cloned())
    }

    async fn list_instances(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .instances
            .iter()
            .filter(|(key, record)| {
                key.prefix == self.prefix && &record.service_kind == service_kind
            })
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .actors
            .get(&self.prefixed_actor_key(key))
            .cloned())
    }

    async fn list_actors(
        &self,
    ) -> Result<Vec<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .actors
            .iter()
            .filter(|(key, _)| key.prefix == self.prefix)
            .map(|(_, value)| value.clone())
            .collect())
    }

    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_actor_key(&key);
        let current = inner.actors.get(&key).map(|(version, _)| *version);
        if current != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let next = PlacementVersion(current.map_or(1, |version| version.0 + 1));
        let watch_key = key.key.clone();
        inner.actors.insert(key, (next, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::ActorUpdated {
                key: watch_key,
                version: next,
                record: value,
            },
        );
        Ok(next)
    }

    async fn get_virtual_shard(
        &self,
        key: &VirtualShardPlacementKey,
    ) -> Result<Option<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .vshards
            .get(&self.prefixed_vshard_key(key))
            .cloned())
    }

    async fn list_virtual_shards(
        &self,
        service_kind: &ServiceKind,
        actor_kind: &ActorKind,
    ) -> Result<Vec<(PlacementVersion, VirtualShardPlacementRecord)>, PlacementError> {
        Ok(self
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .vshards
            .iter()
            .filter(|(key, _)| {
                key.prefix == self.prefix
                    && &key.key.service_kind == service_kind
                    && &key.key.actor_kind == actor_kind
            })
            .map(|(_, value)| value.clone())
            .collect())
    }

    async fn compare_and_put_virtual_shard(
        &self,
        key: VirtualShardPlacementKey,
        expected: Option<PlacementVersion>,
        value: VirtualShardPlacementRecord,
    ) -> Result<PlacementVersion, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_vshard_key(&key);
        let current = inner.vshards.get(&key).map(|(version, _)| *version);
        if current != expected {
            return Err(PlacementError::CompareAndPutFailed);
        }
        let next = PlacementVersion(current.map_or(1, |version| version.0 + 1));
        let watch_key = key.key.clone();
        inner.vshards.insert(key, (next, value.clone()));
        inner.notify(
            &self.prefix,
            PlacementWatchEvent::VirtualShardUpdated {
                key: watch_key,
                version: next,
                record: value,
            },
        );
        Ok(next)
    }

    async fn acquire_activation_lock(
        &self,
        key: ActorPlacementKey,
    ) -> Result<LeaseId, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let key = self.prefixed_actor_key(&key);
        if inner.activation_locks.contains_key(&key) {
            return Err(PlacementError::ActivationLockHeld);
        }
        let lease = LeaseId(self.next_lease_id.fetch_add(1, Ordering::SeqCst));
        inner.activation_locks.insert(key, lease);
        Ok(lease)
    }

    async fn release_activation_lock(&self, key: &ActorPlacementKey) -> Result<(), PlacementError> {
        self.inner
            .lock()
            .expect("placement store mutex poisoned")
            .activation_locks
            .remove(&self.prefixed_actor_key(key));
        Ok(())
    }

    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatch, PlacementError> {
        let mut inner = self.inner.lock().expect("placement store mutex poisoned");
        let rx = inner
            .watchers
            .entry(prefix)
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(128);
                tx
            })
            .subscribe();
        Ok(PlacementWatch::new(rx))
    }

    fn prefix(&self) -> &PlacementPrefix {
        &self.prefix
    }
}

#[derive(Debug, Default)]
struct PlacementStoreInner {
    instances: HashMap<PrefixedInstanceKey, InstanceRecord>,
    actors: HashMap<PrefixedActorKey, (PlacementVersion, ActorPlacementRecord)>,
    vshards: HashMap<PrefixedVShardKey, (PlacementVersion, VirtualShardPlacementRecord)>,
    activation_locks: HashMap<PrefixedActorKey, LeaseId>,
    watchers: HashMap<PlacementPrefix, broadcast::Sender<PlacementWatchEvent>>,
}

impl PlacementStoreInner {
    fn notify(&self, prefix: &PlacementPrefix, event: PlacementWatchEvent) {
        if let Some(tx) = self.watchers.get(prefix) {
            let _ = tx.send(event);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrefixedInstanceKey {
    prefix: PlacementPrefix,
    instance_id: InstanceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrefixedActorKey {
    prefix: PlacementPrefix,
    key: ActorPlacementKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrefixedVShardKey {
    prefix: PlacementPrefix,
    key: VirtualShardPlacementKey,
}
