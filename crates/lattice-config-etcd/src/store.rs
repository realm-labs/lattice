use async_trait::async_trait;
use etcd_client::ConnectOptions;
use lattice_config::store::{ConfigStore, ConfigStoreError, ConfigWatch};
use lattice_core::service_context::ConfiguredComponent;
use std::fmt;

use crate::client::{EtcdConfigClient, RealEtcdConfigClient};
use crate::codec::{decode_value, encode_value, normalize_prefix};
use crate::config::EtcdConfigStoreConfig;
use crate::watch::{ConfigStalenessWatch, WATCH_TARGET};

#[derive(Debug, Clone)]
pub struct EtcdConfigStore {
    inner: EtcdConfigStoreInner<RealEtcdConfigClient>,
}

impl EtcdConfigStore {
    pub fn from_config() -> ConfiguredComponent<Self> {
        ConfiguredComponent::from_section("config_store", Self::connect)
    }

    pub async fn connect(config: EtcdConfigStoreConfig) -> Result<Self, ConfigStoreError> {
        Self::connect_with_options(config, None).await
    }

    pub async fn connect_with_options(
        config: EtcdConfigStoreConfig,
        options: Option<ConnectOptions>,
    ) -> Result<Self, ConfigStoreError> {
        let client = RealEtcdConfigClient::connect(config.endpoints, options).await?;
        Ok(Self {
            inner: EtcdConfigStoreInner::new(client, config.key_prefix)?,
        })
    }

    pub async fn watch_with_staleness(
        &self,
        key: &str,
    ) -> Result<(ConfigWatch, ConfigStalenessWatch), ConfigStoreError> {
        self.inner.watch_with_staleness(key).await
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
    pub(crate) fn new(client: C, key_prefix: impl Into<String>) -> Result<Self, ConfigStoreError> {
        Ok(Self {
            client,
            key_prefix: normalize_prefix(&key_prefix.into())?,
        })
    }

    pub(crate) fn storage_key(&self, key: &str) -> String {
        format!("{}/{}", self.key_prefix, key.trim_start_matches('/'))
    }
}

impl<C> EtcdConfigStoreInner<C>
where
    C: EtcdConfigClient,
{
    pub(crate) async fn watch_with_staleness(
        &self,
        key: &str,
    ) -> Result<(ConfigWatch, ConfigStalenessWatch), ConfigStoreError> {
        let storage_key = self.storage_key(key);
        let raw_watch = self.client.watch(&storage_key).await?;
        let mut values = raw_watch.values;
        let initial = values.borrow().as_deref().map(decode_value).transpose()?;
        let (tx, watch) = ConfigWatch::channel(initial);

        tokio::spawn(async move {
            loop {
                let changed = tokio::select! {
                    () = tx.closed() => return,
                    changed = values.changed() => changed,
                };
                if changed.is_err() {
                    return;
                }
                let decoded = values.borrow().as_deref().map(decode_value).transpose();
                match decoded {
                    Ok(value) => {
                        tx.send_replace(value);
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: WATCH_TARGET,
                            key = %storage_key,
                            error = %error,
                            "config watch update failed to decode; closing the watch",
                        );
                        return;
                    }
                }
            }
        });

        Ok((watch, ConfigStalenessWatch::new(raw_watch.staleness)))
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
        Ok(self.watch_with_staleness(key).await?.0)
    }
}
