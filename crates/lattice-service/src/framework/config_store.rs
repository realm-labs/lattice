use std::sync::Arc;

use async_trait::async_trait;
use lattice_config::store::{ConfigStore, ConfigStoreError, ConfigWatch};

#[async_trait]
pub trait DynConfigStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError>;
    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError>;
    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError>;
}

#[async_trait]
impl<T> DynConfigStore for T
where
    T: ConfigStore,
{
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
        ConfigStore::get(self, key).await
    }

    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError> {
        ConfigStore::put(self, key, value).await
    }

    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError> {
        ConfigStore::watch(self, key).await
    }
}

pub struct ConfigStoreComponent {
    inner: Arc<dyn DynConfigStore>,
}

impl ConfigStoreComponent {
    pub fn new<T>(store: T) -> Self
    where
        T: ConfigStore,
    {
        Self {
            inner: Arc::new(store),
        }
    }

    pub fn inner(&self) -> Arc<dyn DynConfigStore> {
        self.inner.clone()
    }
}

impl std::fmt::Debug for ConfigStoreComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfigStoreComponent")
            .finish_non_exhaustive()
    }
}
