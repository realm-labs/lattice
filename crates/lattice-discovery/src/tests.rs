use std::{
    collections::VecDeque,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::{Stream, StreamExt};
use lattice_config::store::{ConfigStore, ConfigStoreError, ConfigWatch, LocalConfigStore};
use lattice_core::{actor_ref::NodeAddress, coordinator::CoordinatorScope};
use serde_json::json;
use tokio::sync::{Mutex, watch::Sender};

use crate::{
    aggregate::AggregateDiscovery,
    config_store::ConfigStoreDiscovery,
    dns::{DnsDiscovery, DnsDiscoveryConfig, DnsLookup, DnsMode, DnsResolver, SrvRecord},
    provider::{
        CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
        DiscoverySource, DiscoveryTarget,
    },
    static_provider::{StaticDiscovery, StaticEndpoint},
};

#[tokio::test]
async fn static_discovery_emits_one_validated_snapshot() {
    let discovery = StaticDiscovery::new(
        scope(),
        "test",
        vec![static_endpoint("a", 7447, Some("node-a"), 10)],
    )
    .unwrap();
    let snapshots = discovery.snapshots().collect::<Vec<_>>().await;

    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].as_ref().unwrap().generation, 1);
    assert!(
        StaticDiscovery::new(
            scope(),
            "test",
            vec![
                static_endpoint("a", 7447, None, 10),
                static_endpoint("a", 7447, None, 20),
            ],
        )
        .is_err()
    );
}

#[tokio::test]
async fn config_store_closes_get_watch_race_and_retains_last_valid_document() {
    let first = document(1, vec![("a", 7447, "node-a", 10)]);
    let second = document(2, vec![("b", 7448, "node-b", 20)]);
    let store = RacingStore::new(first, second);
    let discovery = ConfigStoreDiscovery::with_reconnect_delay(
        scope(),
        store.clone(),
        "/discovery",
        Duration::from_millis(1),
    )
    .unwrap();
    let mut snapshots = discovery.snapshots();

    let initial = snapshots.next().await.unwrap().unwrap();
    let raced = snapshots.next().await.unwrap().unwrap();
    assert_eq!(initial.targets[0].address.host(), "a");
    assert_eq!(raced.targets[0].address.host(), "b");

    store
        .tx
        .send_replace(Some(document(1, vec![("stale", 7449, "stale", 0)])));
    assert!(matches!(
        snapshots.next().await.unwrap(),
        Err(DiscoveryError::Provider { .. })
    ));
    store.tx.send_replace(Some(json!({ "broken": true })));
    assert!(matches!(
        snapshots.next().await.unwrap(),
        Err(DiscoveryError::Provider { .. })
    ));
    store
        .tx
        .send_replace(Some(document(3, vec![("c", 7450, "node-c", 5)])));
    let recovered = snapshots.next().await.unwrap().unwrap();
    assert_eq!(recovered.generation, 3);
    assert_eq!(recovered.targets[0].address.host(), "c");
}

#[tokio::test]
async fn config_store_absent_key_starts_empty_and_watch_populates() {
    let store = LocalConfigStore::default();
    let discovery = ConfigStoreDiscovery::new(scope(), store.clone(), "/discovery").unwrap();
    let mut snapshots = discovery.snapshots();

    assert!(snapshots.next().await.unwrap().unwrap().targets.is_empty());
    store
        .put(
            "/discovery".to_string(),
            document(1, vec![("node-a", 7447, "node-a", 10)]),
        )
        .await
        .unwrap();
    assert_eq!(
        snapshots.next().await.unwrap().unwrap().targets[0]
            .expected_node_id
            .as_deref(),
        Some("node-a")
    );
}

#[tokio::test]
async fn config_store_reconnects_after_closed_watch() {
    let store = ReconnectingStore::new(document(1, vec![("node-a", 7447, "node-a", 10)]));
    let discovery = ConfigStoreDiscovery::with_reconnect_delay(
        scope(),
        store.clone(),
        "/discovery",
        Duration::from_millis(1),
    )
    .unwrap();
    let mut snapshots = discovery.snapshots();

    assert_eq!(
        snapshots.next().await.unwrap().unwrap().targets[0]
            .address
            .host(),
        "node-a"
    );
    assert!(matches!(
        snapshots.next().await.unwrap(),
        Err(DiscoveryError::Provider { .. })
    ));
    store
        .tx
        .send_replace(Some(document(2, vec![("node-b", 7447, "node-b", 10)])));
    assert_eq!(
        snapshots.next().await.unwrap().unwrap().targets[0]
            .address
            .host(),
        "node-b"
    );
    assert!(store.get_calls.load(Ordering::SeqCst) >= 2);
}

#[tokio::test(start_paused = true)]
async fn dns_host_refreshes_on_clamped_ttl_and_retains_targets_on_failure() {
    let resolver = FakeDnsResolver::new(
        vec![
            Ok(DnsLookup {
                records: vec![
                    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                    IpAddr::V6(Ipv6Addr::LOCALHOST),
                ],
                ttl: Duration::from_secs(1),
            }),
            Err("NXDOMAIN".to_string()),
            Ok(DnsLookup {
                records: vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))],
                ttl: Duration::from_secs(60),
            }),
        ],
        Vec::new(),
    );
    let discovery = DnsDiscovery::with_resolver(
        dns_config(DnsMode::Host {
            hostname: "nodes.example".to_string(),
            port: 7447,
        }),
        Arc::new(resolver),
    )
    .unwrap();
    let mut snapshots = discovery.snapshots();

    let first = snapshots.next().await.unwrap().unwrap();
    assert_eq!(first.targets.len(), 2);
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(matches!(
        snapshots.next().await.unwrap(),
        Err(DiscoveryError::Provider { .. })
    ));
    tokio::time::advance(Duration::from_secs(3)).await;
    let recovered = snapshots.next().await.unwrap().unwrap();
    assert_eq!(recovered.targets[0].address.host(), "10.0.0.2");
}

#[tokio::test]
async fn dns_srv_preserves_priority_weight_and_tls_server_name() {
    let resolver = FakeDnsResolver::new(
        vec![Ok(DnsLookup {
            records: vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7))],
            ttl: Duration::from_secs(30),
        })],
        vec![Ok(DnsLookup {
            records: vec![SrvRecord {
                target: "node-a.example".to_string(),
                port: 7447,
                priority: 12,
                weight: 9,
            }],
            ttl: Duration::from_secs(20),
        })],
    );
    let discovery = DnsDiscovery::with_resolver(
        dns_config(DnsMode::Srv {
            service: "_lattice._tcp.example".to_string(),
        }),
        Arc::new(resolver),
    )
    .unwrap();
    let snapshot = discovery.snapshots().next().await.unwrap().unwrap();

    assert_eq!(snapshot.targets[0].priority, 12);
    assert!(snapshot.targets[0].source.origins().any(|origin| matches!(
        origin,
        DiscoveryOrigin::Dns { server_name, weight: 9, .. } if server_name == "node-a.example"
    )));
}

#[tokio::test]
async fn aggregate_deduplicates_merges_sources_and_rotates_equal_priority() {
    let first = Arc::new(SequenceDiscovery::new(vec![Ok(
        CoordinatorDirectorySnapshot {
            scope: scope(),
            generation: 1,
            targets: vec![target("a", 7447, None, 10), target("b", 7447, None, 10)],
        },
    )]));
    let second = Arc::new(SequenceDiscovery::new(vec![
        Ok(CoordinatorDirectorySnapshot {
            scope: scope(),
            generation: 1,
            targets: vec![config_target("a", 7447, Some("node-a"), 5)],
        }),
        Ok(CoordinatorDirectorySnapshot {
            scope: scope(),
            generation: 2,
            targets: vec![
                config_target("a", 7447, Some("node-a"), 5),
                config_target("c", 7447, None, 10),
            ],
        }),
    ]));
    let aggregate = AggregateDiscovery::new(vec![first, second]).unwrap();
    let values = aggregate
        .snapshots()
        .filter_map(|item| async { item.ok() })
        .take(2)
        .collect::<Vec<_>>()
        .await;

    let a = values[0]
        .targets
        .iter()
        .find(|target| target.address.host() == "a")
        .unwrap();
    assert_eq!(a.priority, 5);
    assert_eq!(a.expected_node_id.as_deref(), Some("node-a"));
    assert_eq!(a.source.origins().len(), 2);
    let equal_priority_first = values[0]
        .targets
        .iter()
        .filter(|target| target.priority == 10)
        .map(|target| target.address.host())
        .collect::<Vec<_>>();
    let equal_priority_second = values[1]
        .targets
        .iter()
        .filter(|target| target.priority == 10)
        .map(|target| target.address.host())
        .collect::<Vec<_>>();
    assert_ne!(equal_priority_first, equal_priority_second);
}

#[derive(Clone)]
struct RacingStore {
    first: serde_json::Value,
    tx: Sender<Option<serde_json::Value>>,
}

impl RacingStore {
    fn new(first: serde_json::Value, raced: serde_json::Value) -> Self {
        let (tx, _rx) = tokio::sync::watch::channel(Some(raced));
        Self { first, tx }
    }
}

#[async_trait]
impl ConfigStore for RacingStore {
    async fn get(&self, _key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
        Ok(Some(self.first.clone()))
    }

    async fn put(&self, _key: String, value: serde_json::Value) -> Result<(), ConfigStoreError> {
        self.tx.send_replace(Some(value));
        Ok(())
    }

    async fn watch(&self, _key: &str) -> Result<ConfigWatch, ConfigStoreError> {
        Ok(ConfigWatch::from_receiver(self.tx.subscribe()))
    }
}

#[derive(Clone)]
struct ReconnectingStore {
    tx: Sender<Option<serde_json::Value>>,
    watch_calls: Arc<AtomicUsize>,
    get_calls: Arc<AtomicUsize>,
}

impl ReconnectingStore {
    fn new(initial: serde_json::Value) -> Self {
        let (tx, _rx) = tokio::sync::watch::channel(Some(initial));
        Self {
            tx,
            watch_calls: Arc::new(AtomicUsize::new(0)),
            get_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl ConfigStore for ReconnectingStore {
    async fn get(&self, _key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.tx.borrow().clone())
    }

    async fn put(&self, _key: String, value: serde_json::Value) -> Result<(), ConfigStoreError> {
        self.tx.send_replace(Some(value));
        Ok(())
    }

    async fn watch(&self, _key: &str) -> Result<ConfigWatch, ConfigStoreError> {
        if self.watch_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(ConfigWatch::channel(self.tx.borrow().clone()).1);
        }
        Ok(ConfigWatch::from_receiver(self.tx.subscribe()))
    }
}

type IpLookupQueue = Arc<Mutex<VecDeque<Result<DnsLookup<IpAddr>, String>>>>;
type SrvLookupQueue = Arc<Mutex<VecDeque<Result<DnsLookup<SrvRecord>, String>>>>;

#[derive(Clone)]
struct FakeDnsResolver {
    ips: IpLookupQueue,
    srvs: SrvLookupQueue,
}

impl FakeDnsResolver {
    fn new(
        ips: Vec<Result<DnsLookup<IpAddr>, String>>,
        srvs: Vec<Result<DnsLookup<SrvRecord>, String>>,
    ) -> Self {
        Self {
            ips: Arc::new(Mutex::new(ips.into())),
            srvs: Arc::new(Mutex::new(srvs.into())),
        }
    }
}

#[async_trait]
impl DnsResolver for FakeDnsResolver {
    async fn lookup_ip(&self, _hostname: &str) -> Result<DnsLookup<IpAddr>, String> {
        self.ips.lock().await.pop_front().unwrap()
    }

    async fn lookup_srv(&self, _service: &str) -> Result<DnsLookup<SrvRecord>, String> {
        self.srvs.lock().await.pop_front().unwrap()
    }
}

#[derive(Clone)]
struct SequenceDiscovery {
    scope: CoordinatorScope,
    items: Vec<Result<CoordinatorDirectorySnapshot, DiscoveryError>>,
}

impl SequenceDiscovery {
    fn new(items: Vec<Result<CoordinatorDirectorySnapshot, DiscoveryError>>) -> Self {
        Self {
            scope: scope(),
            items,
        }
    }
}

impl CoordinatorDiscovery for SequenceDiscovery {
    fn scope(&self) -> &CoordinatorScope {
        &self.scope
    }

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        Box::pin(futures_util::stream::iter(self.items.clone()))
    }
}

fn dns_config(mode: DnsMode) -> DnsDiscoveryConfig {
    DnsDiscoveryConfig {
        scope: scope(),
        mode,
        min_refresh: Duration::from_secs(5),
        max_refresh: Duration::from_secs(30),
        retry_delay: Duration::from_secs(3),
    }
}

fn scope() -> CoordinatorScope {
    CoordinatorScope::Membership
}

fn document(generation: u64, endpoints: Vec<(&str, u16, &str, u16)>) -> serde_json::Value {
    json!({
        "schema_version": 1,
        "generation": generation,
        "endpoints": endpoints.into_iter().map(|(host, port, node_id, priority)| json!({
            "host": host,
            "port": port,
            "node_id": node_id,
            "priority": priority,
        })).collect::<Vec<_>>()
    })
}

fn target(host: &str, port: u16, node_id: Option<&str>, priority: u16) -> DiscoveryTarget {
    DiscoveryTarget {
        address: NodeAddress::new(host, port).unwrap(),
        expected_node_id: node_id.map(str::to_string),
        source: DiscoverySource::single(DiscoveryOrigin::Static {
            name: "test".to_string(),
        }),
        priority,
    }
}

fn static_endpoint(host: &str, port: u16, node_id: Option<&str>, priority: u16) -> StaticEndpoint {
    StaticEndpoint {
        address: NodeAddress::new(host, port).unwrap(),
        expected_node_id: node_id.map(str::to_string),
        priority,
    }
}

fn config_target(host: &str, port: u16, node_id: Option<&str>, priority: u16) -> DiscoveryTarget {
    DiscoveryTarget {
        address: NodeAddress::new(host, port).unwrap(),
        expected_node_id: node_id.map(str::to_string),
        source: DiscoverySource::single(DiscoveryOrigin::ConfigStore {
            key: "/discovery".to_string(),
        }),
        priority,
    }
}
