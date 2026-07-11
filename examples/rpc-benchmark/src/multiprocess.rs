use std::path::Path;
use std::process::Child;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::service_kind;
use lattice_placement::authority::TonicPlacementAuthority;
use lattice_placement::endpoint::EndpointPool;
use lattice_placement::registry::InstanceState;
use lattice_placement::routing::cache::RouteCacheConfig;
use lattice_placement::routing::placement::PlacementRouteResolver;
use lattice_placement::routing::placement::PlacementWatchStarter;
use lattice_placement::routing::resolver::BoxRouteResolver;
use lattice_placement::routing::rpc::ResolvingRpcCore;
use lattice_placement::storage::etcd::client::RealEtcdClient;
use lattice_placement::storage::etcd::{EtcdPlacementStore, EtcdPlacementStoreConfig};
use lattice_placement::storage::{PlacementPrefix, PlacementStore};
use lattice_rpc::client::TonicEndpointChannelPoolConfig;
use lattice_rpc::metadata::RpcClientContextFactory;
use lattice_rpc::security::RpcTransportSecurity;
use tokio::fs;
use tokio::time::sleep;
use tonic::transport::Channel;

use crate::BENCH_SERVICE;
use crate::error::{BenchmarkError, BenchmarkResult};
use crate::generated::{GeneratedTonicEndpointTransport, bench_rpc};
use crate::topology::{BenchClient, BenchmarkConfig};

pub type EtcdBenchmarkPlacementStore = EtcdPlacementStore<RealEtcdClient>;

pub const BENCH_DRIVER_SERVICE: ServiceKind = service_kind!("BenchDriver");

#[derive(Debug, Clone)]
pub struct EtcdBenchmarkConfig {
    pub endpoints: Vec<String>,
    pub key_prefix: String,
    pub coordinator_endpoint: String,
    pub instance_lease_ttl_secs: i64,
    pub activation_lock_ttl_secs: i64,
}

impl EtcdBenchmarkConfig {
    pub fn new(endpoints: Vec<String>, key_prefix: impl Into<String>) -> Self {
        Self {
            endpoints,
            key_prefix: key_prefix.into(),
            coordinator_endpoint: "http://127.0.0.1:50080".to_string(),
            instance_lease_ttl_secs: 30,
            activation_lock_ttl_secs: 10,
        }
    }

    pub fn with_coordinator_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.coordinator_endpoint = endpoint.into();
        self
    }

    pub async fn connect_store(&self) -> BenchmarkResult<EtcdBenchmarkPlacementStore> {
        EtcdPlacementStore::dangerously_connect_unauthenticated(EtcdPlacementStoreConfig {
            key_prefix: self.key_prefix.clone(),
            endpoints: self.endpoints.clone(),
            instance_lease_ttl_secs: self.instance_lease_ttl_secs,
            activation_lock_ttl_secs: self.activation_lock_ttl_secs,
        })
        .await
        .map_err(BenchmarkError::from)
    }
}

pub async fn build_bench_client_from_etcd(
    config: &BenchmarkConfig,
    etcd: &EtcdBenchmarkConfig,
    instance_id: InstanceId,
) -> BenchmarkResult<(
    Arc<BenchClient>,
    lattice_placement::routing::placement::PlacementWatchTask,
)> {
    let store = etcd.connect_store().await?;
    let authority = placement_authority(&etcd.coordinator_endpoint)?;
    let resolver = PlacementRouteResolver::new(
        BENCH_SERVICE,
        store,
        Arc::new(authority),
        RouteCacheConfig::default(),
    );
    let watch = resolver.start_placement_watch().await?;
    let context_factory = RpcClientContextFactory::new(BENCH_DRIVER_SERVICE, instance_id);
    let transport = GeneratedTonicEndpointTransport::with_transport_config(
        RpcTransportSecurity::plaintext(),
        TonicEndpointChannelPoolConfig::try_new(config.channel_stripes)
            .expect("benchmark channel stripe count is clamped to at least one"),
    );
    let core = ResolvingRpcCore::new(
        BENCH_SERVICE,
        BoxRouteResolver::new(resolver),
        EndpointPool::new(),
        context_factory,
        transport,
    )
    .with_retry_policy(config.rpc_retry_policy());
    Ok((Arc::new(bench_rpc::Client::new(core)), watch))
}

pub fn placement_authority(endpoint: &str) -> BenchmarkResult<TonicPlacementAuthority> {
    let channel = Channel::from_shared(endpoint.to_string())
        .map_err(|error| BenchmarkError::InvalidPlacementAuthorityEndpoint {
            message: error.to_string(),
        })?
        .connect_lazy();
    Ok(TonicPlacementAuthority::new(channel))
}

pub async fn wait_for_ready_files(
    ready_files: &[impl AsRef<Path>],
    children: &mut [Child],
    timeout: Duration,
) -> BenchmarkResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut ready = 0;
        for ready_file in ready_files {
            if fs::try_exists(ready_file).await? {
                ready += 1;
            }
        }
        if ready == ready_files.len() {
            return Ok(());
        }
        fail_if_any_child_exited(children)?;
        if Instant::now() >= deadline {
            return Err(BenchmarkError::Timeout {
                operation: "waiting for benchmark node ready files",
                timeout,
            });
        }
        sleep(Duration::from_millis(25)).await;
    }
}

pub async fn wait_for_ready_instances(
    store: &EtcdBenchmarkPlacementStore,
    expected: usize,
    timeout: Duration,
) -> BenchmarkResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let ready = store
            .list_instances(&BENCH_SERVICE)
            .await?
            .into_iter()
            .filter(|record| record.state == InstanceState::Ready)
            .count();
        if ready >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchmarkError::Timeout {
                operation: "waiting for ready Bench instances in placement store",
                timeout,
            });
        }
        sleep(Duration::from_millis(25)).await;
    }
}

pub fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub fn benchmark_placement_prefix(prefix: impl Into<String>) -> PlacementPrefix {
    PlacementPrefix::new(prefix)
}

fn fail_if_any_child_exited(children: &mut [Child]) -> BenchmarkResult<()> {
    for child in children {
        if let Some(status) = child.try_wait()? {
            return Err(BenchmarkError::ChildExited {
                status: status.to_string(),
            });
        }
    }
    Ok(())
}
