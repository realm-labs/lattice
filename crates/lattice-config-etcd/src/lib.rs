use async_trait::async_trait;
use etcd_client::{Client, EventType};
use lattice_config::{ConfigStore, ConfigStoreError, ConfigWatch};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtcdConfigStoreConfig {
    pub key_prefix: String,
    pub endpoints: Vec<String>,
}

#[derive(Clone)]
pub struct EtcdConfigStore {
    inner: EtcdConfigStoreInner<RealEtcdConfigClient>,
}

impl EtcdConfigStore {
    pub async fn connect(config: EtcdConfigStoreConfig) -> Result<Self, ConfigStoreError> {
        let client = RealEtcdConfigClient::connect(config.endpoints).await?;
        Ok(Self {
            inner: EtcdConfigStoreInner::new(client, config.key_prefix),
        })
    }

    pub async fn from_config(config: EtcdConfigStoreConfig) -> Result<Self, ConfigStoreError> {
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
struct EtcdConfigStoreInner<C> {
    client: C,
    key_prefix: String,
}

impl<C> EtcdConfigStoreInner<C> {
    fn new(client: C, key_prefix: impl Into<String>) -> Self {
        Self {
            client,
            key_prefix: normalize_prefix(&key_prefix.into()),
        }
    }

    fn storage_key(&self, key: &str) -> String {
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
                let value = raw_watch.borrow().as_deref().map(decode_value).transpose();
                if let Ok(value) = value {
                    tx.send_replace(value);
                }
            }
        });

        Ok(watch)
    }
}

#[async_trait]
trait EtcdConfigClient: Clone + Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigStoreError>;
    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), ConfigStoreError>;
    async fn watch(&self, key: &str) -> Result<watch::Receiver<Option<Vec<u8>>>, ConfigStoreError>;
}

#[derive(Clone)]
struct RealEtcdConfigClient {
    client: Client,
}

impl RealEtcdConfigClient {
    async fn connect(endpoints: Vec<String>) -> Result<Self, ConfigStoreError> {
        let client = Client::connect(endpoints, None).await.map_err(etcd_error)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl EtcdConfigClient for RealEtcdConfigClient {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigStoreError> {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(etcd_error)?;
        Ok(response.kvs().first().map(|kv| kv.value().to_vec()))
    }

    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), ConfigStoreError> {
        let mut client = self.client.clone();
        client.put(key, value, None).await.map_err(etcd_error)?;
        Ok(())
    }

    async fn watch(&self, key: &str) -> Result<watch::Receiver<Option<Vec<u8>>>, ConfigStoreError> {
        let initial = self.get(key).await?;
        let (tx, rx) = watch::channel(initial);
        let watch_key = key.to_string();
        let mut client = self.client.clone();
        let mut stream = client.watch(watch_key, None).await.map_err(etcd_error)?;

        tokio::spawn(async move {
            while let Ok(Some(response)) = stream.message().await {
                for event in response.events() {
                    let value = match event.event_type() {
                        EventType::Put => event.kv().map(|kv| kv.value().to_vec()),
                        EventType::Delete => None,
                    };
                    tx.send_replace(value);
                }
            }
        });

        Ok(rx)
    }
}

fn encode_value(value: &serde_json::Value) -> Result<Vec<u8>, ConfigStoreError> {
    serde_json::to_vec(value).map_err(codec_error)
}

fn decode_value(bytes: &[u8]) -> Result<serde_json::Value, ConfigStoreError> {
    serde_json::from_slice(bytes).map_err(codec_error)
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        "/lattice/config".to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn etcd_error(error: etcd_client::Error) -> ConfigStoreError {
    ConfigStoreError::Backend {
        message: error.to_string(),
    }
}

fn codec_error(error: impl std::fmt::Display) -> ConfigStoreError {
    ConfigStoreError::Codec {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use serde_json::json;
    use tokio::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn etcd_config_store_supports_watch_reload() {
        let store = test_store("/lattice/test/config");
        let mut watch = store.watch("gateway.rate_limit").await.unwrap();

        store
            .put(
                "gateway.rate_limit".to_string(),
                json!({ "per_second": 100 }),
            )
            .await
            .unwrap();
        let value = watch.changed().await.unwrap();

        assert_eq!(value, Some(json!({ "per_second": 100 })));
        assert_eq!(
            store.get("gateway.rate_limit").await.unwrap(),
            Some(json!({ "per_second": 100 }))
        );
    }

    #[tokio::test]
    async fn etcd_config_store_isolates_cluster_prefixes() {
        let client = InMemoryEtcdConfigClient::new();
        let prod = EtcdConfigStoreInner::new(client.clone(), "/lattice/prod/config");
        let staging = EtcdConfigStoreInner::new(client, "/lattice/staging/config");

        prod.put("feature.matchmaking".to_string(), json!(true))
            .await
            .unwrap();
        staging
            .put("feature.matchmaking".to_string(), json!(false))
            .await
            .unwrap();

        assert_eq!(
            prod.get("feature.matchmaking").await.unwrap(),
            Some(json!(true))
        );
        assert_eq!(
            staging.get("feature.matchmaking").await.unwrap(),
            Some(json!(false))
        );
    }

    #[tokio::test]
    async fn malformed_config_value_returns_codec_error() {
        let client = InMemoryEtcdConfigClient::new();
        let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config");

        client
            .put(
                "/lattice/test/config/broken".to_string(),
                b"not-json".to_vec(),
            )
            .await
            .unwrap();

        let error = store.get("broken").await;

        assert!(matches!(error, Err(ConfigStoreError::Codec { .. })));
    }

    #[test]
    fn config_builds_from_normalized_prefix() {
        let store = test_store("lattice/test/config");

        assert_eq!(
            store.storage_key("/feature/foo"),
            "/lattice/test/config/feature/foo"
        );
    }

    fn test_store(prefix: &str) -> EtcdConfigStoreInner<InMemoryEtcdConfigClient> {
        EtcdConfigStoreInner::new(InMemoryEtcdConfigClient::new(), prefix)
    }

    type RawConfigWatchers = HashMap<String, watch::Sender<Option<Vec<u8>>>>;

    #[derive(Debug, Clone, Default)]
    struct InMemoryEtcdConfigClient {
        values: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        watches: Arc<Mutex<RawConfigWatchers>>,
    }

    impl InMemoryEtcdConfigClient {
        fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl EtcdConfigClient for InMemoryEtcdConfigClient {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigStoreError> {
            Ok(self.values.lock().await.get(key).cloned())
        }

        async fn put(&self, key: String, value: Vec<u8>) -> Result<(), ConfigStoreError> {
            self.values.lock().await.insert(key.clone(), value.clone());
            let mut watches = self.watches.lock().await;
            let tx = watches.entry(key).or_insert_with(|| {
                let (tx, _rx) = watch::channel(None);
                tx
            });
            tx.send_replace(Some(value));
            Ok(())
        }

        async fn watch(
            &self,
            key: &str,
        ) -> Result<watch::Receiver<Option<Vec<u8>>>, ConfigStoreError> {
            let current = self.values.lock().await.get(key).cloned();
            let mut watches = self.watches.lock().await;
            let rx = watches
                .entry(key.to_string())
                .or_insert_with(|| {
                    let (tx, _rx) = watch::channel(current.clone());
                    tx
                })
                .subscribe();
            Ok(rx)
        }
    }
}
