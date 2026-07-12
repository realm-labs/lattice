use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, watch};

#[async_trait]
pub trait ConfigStore: Clone + Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError>;
    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError>;
    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigStoreError {
    #[error("config watch closed")]
    WatchClosed,
    #[error("config backend {backend} does not support {operation}")]
    UnsupportedOperation {
        operation: &'static str,
        backend: &'static str,
    },
    #[error("config backend error: {message}")]
    Backend { message: String },
    #[error("config codec error: {message}")]
    Codec { message: String },
}

#[derive(Debug)]
pub struct ConfigWatch {
    rx: watch::Receiver<Option<serde_json::Value>>,
}

impl ConfigWatch {
    pub fn channel(
        initial: Option<serde_json::Value>,
    ) -> (watch::Sender<Option<serde_json::Value>>, Self) {
        let (tx, rx) = watch::channel(initial);
        (tx, Self { rx })
    }

    pub fn from_receiver(rx: watch::Receiver<Option<serde_json::Value>>) -> Self {
        Self { rx }
    }

    pub fn current(&self) -> Option<serde_json::Value> {
        self.rx.borrow().clone()
    }

    pub async fn changed(&mut self) -> Result<Option<serde_json::Value>, ConfigStoreError> {
        self.rx
            .changed()
            .await
            .map_err(|_| ConfigStoreError::WatchClosed)?;
        Ok(self.rx.borrow().clone())
    }
}

#[derive(Debug, Clone, Default)]
pub struct LocalConfigStore {
    values: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    watches: Arc<Mutex<HashMap<String, watch::Sender<Option<serde_json::Value>>>>>,
}

#[async_trait]
impl ConfigStore for LocalConfigStore {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
        Ok(self.values.lock().await.get(key).cloned())
    }

    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError> {
        self.values.lock().await.insert(key.clone(), value.clone());
        let mut watches = self.watches.lock().await;
        let tx = watches.entry(key).or_insert_with(|| {
            let (tx, _rx) = watch::channel(None);
            tx
        });
        tx.send_replace(Some(value));
        Ok(())
    }

    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError> {
        let current = self.values.lock().await.get(key).cloned();
        let mut watches = self.watches.lock().await;
        let rx = watches
            .entry(key.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = watch::channel(current.clone());
                tx
            })
            .subscribe();
        Ok(ConfigWatch::from_receiver(rx))
    }
}
