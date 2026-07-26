use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_config::store::{ConfigStore, ConfigStoreError, ConfigWatch};
use serde_json::json;
use tokio::sync::{Mutex, mpsc};

use crate::client::EtcdConfigClient;
use crate::codec::normalize_prefix;
use crate::store::{EtcdConfigStore, EtcdConfigStoreInner};
use crate::watch::{
    ConfigStaleness, ConfigStalenessWatch, EtcdSnapshot, EtcdWatchBackend, EtcdWatchSession,
    RawConfigWatch, RetryBackoff, WatchRetryPolicy, spawn_config_watch,
};

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
    let value = await_change(&mut watch).await.unwrap();

    assert_eq!(value, Some(json!({ "per_second": 100 })));
    assert_eq!(
        store.get("gateway.rate_limit").await.unwrap(),
        Some(json!({ "per_second": 100 }))
    );
}

#[tokio::test]
async fn etcd_config_store_isolates_cluster_prefixes() {
    let client = InMemoryEtcdConfigClient::new();
    let prod = EtcdConfigStoreInner::new(client.clone(), "/lattice/prod/config").unwrap();
    let staging = EtcdConfigStoreInner::new(client, "/lattice/staging/config").unwrap();

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
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config").unwrap();

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
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config").unwrap();
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

    let error = await_change(&mut watch).await.unwrap_err();
    assert!(matches!(error, ConfigStoreError::WatchClosed));
}

#[tokio::test]
async fn config_watch_registers_from_the_snapshot_revision() {
    let client = InMemoryEtcdConfigClient::new();
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config").unwrap();
    store
        .put("gateway.rate_limit".to_string(), json!(1))
        .await
        .unwrap();

    client.hold_registration().await;
    let mut watch = store.watch("gateway.rate_limit").await.unwrap();
    store
        .put("gateway.rate_limit".to_string(), json!(2))
        .await
        .unwrap();
    client.release().await;

    assert_eq!(await_change(&mut watch).await.unwrap(), Some(json!(2)));
}

#[tokio::test]
async fn config_watch_recovers_updates_missed_while_disconnected() {
    let client = InMemoryEtcdConfigClient::new();
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config").unwrap();
    store
        .put("gateway.rate_limit".to_string(), json!(1))
        .await
        .unwrap();
    let mut watch = store.watch("gateway.rate_limit").await.unwrap();
    client.await_open_sessions(1).await;

    client.hold().await;
    client.disconnect().await;
    store
        .put("gateway.rate_limit".to_string(), json!(2))
        .await
        .unwrap();
    client.compact().await;
    client.release().await;

    assert_eq!(await_change(&mut watch).await.unwrap(), Some(json!(2)));
}

#[tokio::test]
async fn config_watch_resubscribes_after_compaction() {
    let client = InMemoryEtcdConfigClient::new();
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config").unwrap();
    store
        .put("feature.flag".to_string(), json!(1))
        .await
        .unwrap();
    let mut watch = store.watch("feature.flag").await.unwrap();
    client.await_open_sessions(1).await;

    client.compact().await;
    store
        .put("feature.flag".to_string(), json!(2))
        .await
        .unwrap();

    assert_eq!(await_change(&mut watch).await.unwrap(), Some(json!(2)));
    assert!(client.cancelled_sessions() >= 1);
}

#[tokio::test]
async fn config_watch_exposes_staleness_across_reconnects() {
    let client = InMemoryEtcdConfigClient::new();
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config").unwrap();
    store
        .put("feature.flag".to_string(), json!(1))
        .await
        .unwrap();
    let (mut watch, mut staleness) = store.watch_with_staleness("feature.flag").await.unwrap();
    client.await_open_sessions(1).await;
    assert!(!staleness.current().is_stale());

    client.hold().await;
    client.disconnect().await;
    let stale = await_staleness(&mut staleness, true).await;
    assert_eq!(stale.disconnects(), 1);
    assert!(stale.stale_since().is_some());

    client.release().await;
    let recovered = await_staleness(&mut staleness, false).await;
    assert!(recovered.stale_since().is_none());
    assert_eq!(recovered.disconnects(), 1);

    store
        .put("feature.flag".to_string(), json!(2))
        .await
        .unwrap();
    assert_eq!(await_change(&mut watch).await.unwrap(), Some(json!(2)));
}

#[tokio::test]
async fn dropping_the_config_watch_cancels_the_etcd_watcher() {
    let client = InMemoryEtcdConfigClient::new();
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config").unwrap();
    store
        .put("feature.flag".to_string(), json!(true))
        .await
        .unwrap();
    let watch = store.watch("feature.flag").await.unwrap();
    client.await_open_sessions(1).await;

    drop(watch);

    client.await_cancelled_sessions(1).await;
    client.await_open_sessions(0).await;
}

#[tokio::test]
async fn dropping_the_config_watch_stops_a_reconnecting_watcher() {
    let client = InMemoryEtcdConfigClient::new();
    let store = EtcdConfigStoreInner::new(client.clone(), "/lattice/test/config").unwrap();
    store
        .put("feature.flag".to_string(), json!(true))
        .await
        .unwrap();
    let watch = store.watch("feature.flag").await.unwrap();
    client.await_open_sessions(1).await;

    client.hold().await;
    client.disconnect().await;
    client.await_open_sessions(0).await;

    drop(watch);
    client.release().await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.open_sessions(), 0);
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

#[test]
fn empty_key_prefix_is_rejected() {
    assert!(matches!(
        normalize_prefix("///"),
        Err(ConfigStoreError::Backend { .. })
    ));
    assert!(EtcdConfigStoreInner::new(InMemoryEtcdConfigClient::new(), "").is_err());
}

#[test]
fn watch_retry_backoff_applies_jitter_and_caps() {
    let policy = WatchRetryPolicy {
        initial: Duration::from_millis(100),
        max: Duration::from_millis(800),
        multiplier: 2.0,
        jitter: 0.25,
    };
    let mut backoff = RetryBackoff::new(policy);

    let first = backoff.next_delay();
    let second = backoff.next_delay();

    assert!((Duration::from_millis(75)..=Duration::from_millis(125)).contains(&first));
    assert!((Duration::from_millis(150)..=Duration::from_millis(250)).contains(&second));
    assert_ne!(first.mul_f64(2.0), second);

    for _ in 0..8 {
        assert!(backoff.next_delay() <= policy.max);
    }

    backoff.reset();
    assert!(backoff.next_delay() <= Duration::from_millis(125));
}

fn test_store(prefix: &str) -> EtcdConfigStoreInner<InMemoryEtcdConfigClient> {
    EtcdConfigStoreInner::new(InMemoryEtcdConfigClient::new(), prefix).unwrap()
}

fn test_policy() -> WatchRetryPolicy {
    WatchRetryPolicy {
        initial: Duration::from_millis(1),
        max: Duration::from_millis(5),
        multiplier: 2.0,
        jitter: 0.25,
    }
}

const SETTLE_TIMEOUT: Duration = Duration::from_secs(5);

async fn await_change(
    watch: &mut ConfigWatch,
) -> Result<Option<serde_json::Value>, ConfigStoreError> {
    tokio::time::timeout(SETTLE_TIMEOUT, watch.changed())
        .await
        .expect("config watch did not settle in time")
}

async fn await_staleness(watch: &mut ConfigStalenessWatch, stale: bool) -> ConfigStaleness {
    tokio::time::timeout(SETTLE_TIMEOUT, async {
        loop {
            let current = watch.current();
            if current.is_stale() == stale {
                return current;
            }
            watch.changed().await.unwrap();
        }
    })
    .await
    .expect("config watch staleness did not settle in time")
}

#[derive(Debug)]
enum FakeWatchEvent {
    Value(Option<Vec<u8>>),
    Fault(String),
}

#[derive(Debug)]
struct FakeRevision {
    revision: i64,
    key: String,
    value: Option<Vec<u8>>,
}

#[derive(Debug)]
struct FakeSubscription {
    key: String,
    events: mpsc::UnboundedSender<FakeWatchEvent>,
}

#[derive(Debug, Default)]
struct FakeEtcdState {
    revision: i64,
    compact_revision: i64,
    values: HashMap<String, Vec<u8>>,
    history: Vec<FakeRevision>,
    subscriptions: Vec<FakeSubscription>,
    hold_snapshot: bool,
    hold_registration: bool,
}

#[derive(Debug, Clone, Default)]
struct InMemoryEtcdConfigClient {
    state: Arc<Mutex<FakeEtcdState>>,
    open_sessions: Arc<AtomicU64>,
    cancelled_sessions: Arc<AtomicU64>,
}

impl InMemoryEtcdConfigClient {
    fn new() -> Self {
        Self::default()
    }

    fn open_sessions(&self) -> u64 {
        self.open_sessions.load(Ordering::SeqCst)
    }

    fn cancelled_sessions(&self) -> u64 {
        self.cancelled_sessions.load(Ordering::SeqCst)
    }

    async fn hold(&self) {
        let mut state = self.state.lock().await;
        state.hold_snapshot = true;
        state.hold_registration = true;
    }

    async fn hold_registration(&self) {
        self.state.lock().await.hold_registration = true;
    }

    async fn release(&self) {
        let mut state = self.state.lock().await;
        state.hold_snapshot = false;
        state.hold_registration = false;
    }

    async fn disconnect(&self) {
        let mut state = self.state.lock().await;
        for subscription in state.subscriptions.drain(..) {
            let _ = subscription.events.send(FakeWatchEvent::Fault(
                "connection reset by peer".to_string(),
            ));
        }
    }

    async fn compact(&self) {
        let mut state = self.state.lock().await;
        state.compact_revision = state.revision;
        state.history.clear();
        let compact_revision = state.compact_revision;
        for subscription in state.subscriptions.drain(..) {
            let _ = subscription.events.send(FakeWatchEvent::Fault(format!(
                "required revision has been compacted below {compact_revision}"
            )));
        }
    }

    async fn write(&self, key: String, value: Option<Vec<u8>>) {
        let mut state = self.state.lock().await;
        state.revision += 1;
        let revision = state.revision;
        match value.clone() {
            Some(bytes) => {
                state.values.insert(key.clone(), bytes);
            }
            None => {
                state.values.remove(&key);
            }
        }
        state.history.push(FakeRevision {
            revision,
            key: key.clone(),
            value: value.clone(),
        });
        state.subscriptions.retain(|subscription| {
            subscription.key != key
                || subscription
                    .events
                    .send(FakeWatchEvent::Value(value.clone()))
                    .is_ok()
        });
    }

    async fn await_open_sessions(&self, expected: u64) {
        self.await_counter(&self.open_sessions, expected, "open sessions")
            .await;
    }

    async fn await_cancelled_sessions(&self, expected: u64) {
        self.await_counter(&self.cancelled_sessions, expected, "cancelled sessions")
            .await;
    }

    async fn await_counter(&self, counter: &AtomicU64, expected: u64, label: &str) {
        for _ in 0..2_000 {
            if counter.load(Ordering::SeqCst) == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!(
            "{label} never reached {expected}; observed {}",
            counter.load(Ordering::SeqCst)
        );
    }

    async fn await_release(&self, registration: bool) {
        loop {
            {
                let state = self.state.lock().await;
                let held = if registration {
                    state.hold_registration
                } else {
                    state.hold_snapshot
                };
                if !held {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

#[async_trait]
impl EtcdWatchBackend for InMemoryEtcdConfigClient {
    type Session = FakeWatchSession;

    async fn snapshot(&self, key: &str) -> Result<EtcdSnapshot, ConfigStoreError> {
        self.await_release(false).await;
        let state = self.state.lock().await;
        Ok(EtcdSnapshot {
            value: state.values.get(key).cloned(),
            revision: state.revision,
        })
    }

    async fn watch_from(
        &self,
        key: &str,
        start_revision: i64,
    ) -> Result<Self::Session, ConfigStoreError> {
        self.await_release(true).await;
        let mut state = self.state.lock().await;
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        if start_revision <= state.compact_revision {
            let _ = events_tx.send(FakeWatchEvent::Fault(format!(
                "required revision {start_revision} has been compacted"
            )));
        } else {
            for entry in &state.history {
                if entry.revision >= start_revision && entry.key == key {
                    let _ = events_tx.send(FakeWatchEvent::Value(entry.value.clone()));
                }
            }
            state.subscriptions.push(FakeSubscription {
                key: key.to_string(),
                events: events_tx,
            });
        }
        self.open_sessions.fetch_add(1, Ordering::SeqCst);
        Ok(FakeWatchSession {
            backend: self.clone(),
            events: events_rx,
            cancelled: false,
        })
    }
}

#[async_trait]
impl EtcdConfigClient for InMemoryEtcdConfigClient {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigStoreError> {
        Ok(EtcdWatchBackend::snapshot(self, key).await?.value)
    }

    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), ConfigStoreError> {
        self.write(key, Some(value)).await;
        Ok(())
    }

    async fn watch(&self, key: &str) -> Result<RawConfigWatch, ConfigStoreError> {
        let snapshot = EtcdWatchBackend::snapshot(self, key).await?;
        Ok(spawn_config_watch(
            self.clone(),
            key.to_string(),
            snapshot,
            test_policy(),
        ))
    }
}

struct FakeWatchSession {
    backend: InMemoryEtcdConfigClient,
    events: mpsc::UnboundedReceiver<FakeWatchEvent>,
    cancelled: bool,
}

#[async_trait]
impl EtcdWatchSession for FakeWatchSession {
    async fn next(&mut self) -> Result<Vec<Option<Vec<u8>>>, ConfigStoreError> {
        match self.events.recv().await {
            Some(FakeWatchEvent::Value(value)) => Ok(vec![value]),
            Some(FakeWatchEvent::Fault(message)) => Err(ConfigStoreError::Backend { message }),
            None => Err(ConfigStoreError::Backend {
                message: "fake etcd closed the watch stream".to_string(),
            }),
        }
    }

    async fn cancel(&mut self) {
        if self.cancelled {
            return;
        }
        self.cancelled = true;
        self.backend
            .cancelled_sessions
            .fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for FakeWatchSession {
    fn drop(&mut self) {
        self.backend.open_sessions.fetch_sub(1, Ordering::SeqCst);
    }
}
