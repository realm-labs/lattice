use std::time::Duration;

use etcd_client::Client;
use lattice_config::store::{ConfigStore, ConfigWatch};
use lattice_config_etcd::{config::EtcdConfigStoreConfig, store::EtcdConfigStore};
use serde_json::json;

fn endpoints() -> Option<Vec<String>> {
    std::env::var("LATTICE_ETCD_ENDPOINTS")
        .ok()
        .map(|value| value.split(',').map(str::to_owned).collect())
}

fn key_prefix() -> String {
    format!("/lattice-config-tests/{}", uuid::Uuid::new_v4().simple())
}

async fn next_value(watch: &mut ConfigWatch) -> Option<serde_json::Value> {
    tokio::time::timeout(Duration::from_secs(15), watch.changed())
        .await
        .expect("config watch did not deliver an update in time")
        .expect("config watch closed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_etcd_config_watch_observes_updates_racing_registration() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let store = EtcdConfigStore::connect(EtcdConfigStoreConfig {
        key_prefix: key_prefix(),
        endpoints,
    })
    .await
    .unwrap();
    store
        .put("gateway.rate_limit".to_string(), json!({ "per_second": 1 }))
        .await
        .unwrap();

    let (mut watch, staleness) = store
        .watch_with_staleness("gateway.rate_limit")
        .await
        .unwrap();
    assert!(!staleness.current().is_stale());
    store
        .put("gateway.rate_limit".to_string(), json!({ "per_second": 2 }))
        .await
        .unwrap();

    assert_eq!(
        next_value(&mut watch).await,
        Some(json!({ "per_second": 2 }))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_etcd_config_watch_recovers_from_compaction() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = key_prefix();
    let store = EtcdConfigStore::connect(EtcdConfigStoreConfig {
        key_prefix: prefix.clone(),
        endpoints: endpoints.clone(),
    })
    .await
    .unwrap();
    store
        .put("feature.flag".to_string(), json!(0))
        .await
        .unwrap();

    let (mut watch, staleness) = store.watch_with_staleness("feature.flag").await.unwrap();
    for revision in 1..=8 {
        store
            .put("feature.flag".to_string(), json!(revision))
            .await
            .unwrap();
    }

    let mut raw = Client::connect(endpoints, None).await.unwrap();
    let head = raw
        .get(format!("{prefix}/feature.flag"), None)
        .await
        .unwrap()
        .header()
        .map(|header| header.revision())
        .unwrap();
    raw.compact(head, None).await.unwrap();

    store
        .put("feature.flag".to_string(), json!(99))
        .await
        .unwrap();

    let mut observed = None;
    while observed != Some(json!(99)) {
        observed = next_value(&mut watch).await;
    }
    assert!(!staleness.current().is_stale());
}
