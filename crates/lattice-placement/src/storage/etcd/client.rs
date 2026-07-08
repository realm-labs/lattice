use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, EventType, GetOptions, Txn, TxnOp, WatchOptions};
use tokio::sync::broadcast;

use crate::error::PlacementError;
use crate::storage::etcd::codec::{
    EtcdValue, codec_error, decode_etcd_value, encode_etcd_value, etcd_error, lease_id,
    placement_version, put_options_for,
};
use crate::storage::{LeaseId, PlacementVersion};

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
