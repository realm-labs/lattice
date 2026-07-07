use async_trait::async_trait;
use lattice_config::store::{ConfigStore, ConfigStoreError, ConfigWatch};
use lattice_core::service_context::ConfiguredComponent;
use std::fmt;

use crate::client::{EtcdConfigClient, RealEtcdConfigClient};
use crate::codec::{decode_value, encode_value, normalize_prefix};
use crate::config::EtcdConfigStoreConfig;

#[derive(Debug, Clone)]
pub struct EtcdConfigStore {
    inner: EtcdConfigStoreInner<RealEtcdConfigClient>,
}

impl EtcdConfigStore {
    pub fn from_config() -> ConfiguredComponent<Self> {
        ConfiguredComponent::from_section("config_store", Self::connect)
    }

    pub async fn connect(config: EtcdConfigStoreConfig) -> Result<Self, ConfigStoreError> {
        let client = RealEtcdConfigClient::connect(config.endpoints).await?;
        Ok(Self {
            inner: EtcdConfigStoreInner::new(client, config.key_prefix),
        })
    }

    pub async fn from_options(config: EtcdConfigStoreConfig) -> Result<Self, ConfigStoreError> {
        Self::connect(config).await
    }
}

#[async_trait]
impl ConfigStore for EtcdConfigStore {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
        self.inner.get(key).await
    }

    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError> {
        self.inner.put(key, value).await
    }

    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError> {
        self.inner.watch(key).await
    }
}

#[derive(Clone)]
pub(crate) struct EtcdConfigStoreInner<C> {
    client: C,
    key_prefix: String,
}

impl<C> fmt::Debug for EtcdConfigStoreInner<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdConfigStoreInner")
            .field("key_prefix", &self.key_prefix)
            .finish_non_exhaustive()
    }
}

impl<C> EtcdConfigStoreInner<C> {
    pub(crate) fn new(client: C, key_prefix: impl Into<String>) -> Self {
        Self {
            client,
            key_prefix: normalize_prefix(&key_prefix.into()),
        }
    }

    pub(crate) fn storage_key(&self, key: &str) -> String {
        format!("{}/{}", self.key_prefix, key.trim_start_matches('/'))
    }
}

#[async_trait]
impl<C> ConfigStore for EtcdConfigStoreInner<C>
where
    C: EtcdConfigClient,
{
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
        let Some(bytes) = self.client.get(&self.storage_key(key)).await? else {
            return Ok(None);
        };
        decode_value(&bytes).map(Some)
    }

    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError> {
        self.client
            .put(self.storage_key(&key), encode_value(&value)?)
            .await
    }

    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError> {
        let mut raw_watch = self.client.watch(&self.storage_key(key)).await?;
        let initial = raw_watch
            .borrow()
            .as_deref()
            .map(decode_value)
            .transpose()?;
        let (tx, watch) = ConfigWatch::channel(initial);

        tokio::spawn(async move {
            while raw_watch.changed().await.is_ok() {
                let value = match raw_watch.borrow().as_deref().map(decode_value).transpose() {
                    Ok(value) => value,
                    Err(_) => break,
                };
                tx.send_replace(value);
            }
        });

        Ok(watch)
    }
}
