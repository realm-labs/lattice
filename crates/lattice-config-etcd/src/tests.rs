use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lattice_config::store::{ConfigStore, ConfigStoreError};
use serde_json::json;
use tokio::sync::{Mutex, watch};

use crate::client::EtcdConfigClient;
use crate::store::{EtcdConfigStore, EtcdConfigStoreInner};

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

#[tokio::test]
async fn malformed_watch_update_closes_config_watch() {
    let client = InMemoryEtcdConfigClient::new();
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config");
    store
        .put("feature.flag".to_string(), json!(true))
        .await
        .unwrap();
    let mut watch = store.watch("feature.flag").await.unwrap();

    client
        .put(
            "/lattice/test/config/feature.flag".to_string(),
            b"not-json".to_vec(),
        )
        .await
        .unwrap();

    let error = watch.changed().await.unwrap_err();
    assert!(matches!(error, ConfigStoreError::WatchClosed));
}

#[test]
fn config_builds_from_normalized_prefix() {
    let store = test_store("lattice/test/config");

    assert_eq!(
        store.storage_key("/feature/foo"),
        "/lattice/test/config/feature/foo"
    );
    assert_eq!(EtcdConfigStore::from_config().section(), "config_store");
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

    async fn watch(&self, key: &str) -> Result<watch::Receiver<Option<Vec<u8>>>, ConfigStoreError> {
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
