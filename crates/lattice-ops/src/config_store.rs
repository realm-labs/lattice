use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

use crate::OpsError;

#[async_trait]
pub trait ConfigStore: Clone + Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, OpsError>;
    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), OpsError>;
    async fn watch(&self, key: &str) -> Result<ConfigWatch, OpsError>;
}

#[derive(Debug, Clone, Default)]
pub struct LocalConfigStore {
    values: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    watches: Arc<Mutex<HashMap<String, watch::Sender<Option<serde_json::Value>>>>>,
}

#[async_trait]
impl ConfigStore for LocalConfigStore {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, OpsError> {
        Ok(self.values.lock().await.get(key).cloned())
    }

    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), OpsError> {
        self.values.lock().await.insert(key.clone(), value.clone());
        let mut watches = self.watches.lock().await;
        let tx = watches.entry(key).or_insert_with(|| {
            let (tx, _rx) = watch::channel(None);
            tx
        });
        tx.send_replace(Some(value));
        Ok(())
    }

    async fn watch(&self, key: &str) -> Result<ConfigWatch, OpsError> {
        let current = self.values.lock().await.get(key).cloned();
        let mut watches = self.watches.lock().await;
        let rx = watches
            .entry(key.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = watch::channel(current.clone());
                tx
            })
            .subscribe();
        Ok(ConfigWatch { rx })
    }
}

#[derive(Debug, Clone)]
pub struct EtcdConfigStore {
    client: InMemoryEtcdConfigClient,
    key_prefix: String,
}

impl EtcdConfigStore {
    pub fn new(client: InMemoryEtcdConfigClient, key_prefix: impl Into<String>) -> Self {
        Self {
            client,
            key_prefix: normalize_prefix(&key_prefix.into()),
        }
    }

    pub fn from_config(config: EtcdConfigStoreConfig) -> Self {
        Self::new(InMemoryEtcdConfigClient::new(), config.key_prefix)
    }

    pub fn client(&self) -> InMemoryEtcdConfigClient {
        self.client.clone()
    }

    fn storage_key(&self, key: &str) -> String {
        format!("{}/{}", self.key_prefix, key.trim_start_matches('/'))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtcdConfigStoreConfig {
    pub key_prefix: String,
}

#[async_trait]
impl ConfigStore for EtcdConfigStore {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, OpsError> {
        self.client.get(&self.storage_key(key)).await
    }

    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), OpsError> {
        self.client.put(self.storage_key(&key), value).await
    }

    async fn watch(&self, key: &str) -> Result<ConfigWatch, OpsError> {
        self.client.watch(&self.storage_key(key)).await
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryEtcdConfigClient {
    values: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    watches: Arc<Mutex<HashMap<String, watch::Sender<Option<serde_json::Value>>>>>,
}

impl InMemoryEtcdConfigClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, OpsError> {
        Ok(self.values.lock().await.get(key).cloned())
    }

    pub async fn put(&self, key: String, value: serde_json::Value) -> Result<(), OpsError> {
        self.values.lock().await.insert(key.clone(), value.clone());
        let mut watches = self.watches.lock().await;
        let tx = watches.entry(key).or_insert_with(|| {
            let (tx, _rx) = watch::channel(None);
            tx
        });
        tx.send_replace(Some(value));
        Ok(())
    }

    pub async fn watch(&self, key: &str) -> Result<ConfigWatch, OpsError> {
        let current = self.values.lock().await.get(key).cloned();
        let mut watches = self.watches.lock().await;
        let rx = watches
            .entry(key.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = watch::channel(current.clone());
                tx
            })
            .subscribe();
        Ok(ConfigWatch { rx })
    }
}

#[derive(Debug)]
pub struct ConfigWatch {
    rx: watch::Receiver<Option<serde_json::Value>>,
}

impl ConfigWatch {
    pub async fn changed(&mut self) -> Result<Option<serde_json::Value>, OpsError> {
        self.rx
            .changed()
            .await
            .map_err(|_| OpsError::ConfigWatchClosed)?;
        Ok(self.rx.borrow().clone())
    }
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        "/lattice/config".to_string()
    } else {
        format!("/{trimmed}")
    }
}
