use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use lattice_core::instance::InstanceId;
use lattice_core::service_context::ServiceContext;
use lattice_placement::routing::rpc::RpcRetryPolicy;
use lattice_placement::storage::PlacementPrefix;
use lattice_placement::storage::memory::InMemoryPlacementStore;
use lattice_rpc::client::TonicEndpointChannelPoolConfig;
use lattice_service::actor::ActorRegistration;
use lattice_service::error::LatticeServiceError;
use lattice_service::service::LatticeService;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

use crate::actors::{
    BenchActor, BenchActorFactory, ChainActor, ChainActorFactory, WorkerActor, WorkerActorFactory,
};
use crate::error::{BenchmarkError, BenchmarkResult};
use crate::generated::{bench_rpc, chain_rpc, worker_rpc};
use crate::{BENCH_ACTOR, BENCH_SERVICE, CHAIN_ACTOR, CHAIN_SERVICE, WORKER_ACTOR, WORKER_SERVICE};

pub type BenchClient = bench_rpc::Client<bench_rpc::DefaultClientCore>;
pub type ChainClient = chain_rpc::Client<chain_rpc::DefaultClientCore>;

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub nodes: usize,
    pub actors: u64,
    pub concurrency: usize,
    pub requests: usize,
    pub channel_stripes: usize,
    pub rpc_retry: bool,
    pub request_dedup: bool,
    pub payload_bytes: usize,
}

impl BenchmarkConfig {
    pub fn from_env() -> Self {
        Self {
            nodes: env_usize("LATTICE_BENCH_NODES", 2).max(1),
            actors: env_u64("LATTICE_BENCH_ACTORS", 256).max(1),
            concurrency: env_usize("LATTICE_BENCH_CONCURRENCY", 64).max(1),
            requests: env_usize("LATTICE_BENCH_REQUESTS", 10_000).max(1),
            channel_stripes: env_usize("LATTICE_BENCH_CHANNEL_STRIPES", 4).max(1),
            rpc_retry: env_bool("LATTICE_BENCH_RPC_RETRY", true),
            request_dedup: env_bool("LATTICE_BENCH_REQUEST_DEDUP", true),
            payload_bytes: env_usize("LATTICE_BENCH_PAYLOAD_BYTES", 0),
        }
    }

    pub fn test_default() -> Self {
        Self {
            nodes: 2,
            actors: 8,
            concurrency: 4,
            requests: 32,
            channel_stripes: 4,
            rpc_retry: true,
            request_dedup: true,
            payload_bytes: 0,
        }
    }

    fn rpc_client_transport(&self) -> TonicEndpointChannelPoolConfig {
        TonicEndpointChannelPoolConfig::try_new(self.channel_stripes)
            .expect("benchmark channel stripe count is clamped to at least one")
    }

    pub fn rpc_retry_policy(&self) -> RpcRetryPolicy {
        if self.rpc_retry {
            RpcRetryPolicy::RouteCorrection
        } else {
            RpcRetryPolicy::Disabled
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkTopology {
    inner: Arc<BenchmarkTopologyInner>,
}

#[derive(Debug)]
struct BenchmarkTopologyInner {
    bench_client: Arc<BenchClient>,
    chain_client: Arc<ChainClient>,
    shutdowns: Mutex<Vec<oneshot::Sender<()>>>,
    tasks: Mutex<Vec<JoinHandle<Result<(), LatticeServiceError>>>>,
}

impl BenchmarkTopology {
    pub async fn start(config: &BenchmarkConfig) -> BenchmarkResult<Self> {
        let placement_store = InMemoryPlacementStore::new(PlacementPrefix::new(format!(
            "/lattice/rpc-benchmark/{}",
            run_id()
        )));
        let mut shutdowns = Vec::new();
        let mut tasks = Vec::new();
        let mut bench_context = None;
        let mut chain_context = None;

        for index in 0..config.nodes {
            let node = start_worker_node(index, placement_store.clone(), config).await?;
            shutdowns.push(node.shutdown);
            tasks.push(node.task);
        }
        for index in 0..config.nodes {
            let node = start_chain_node(index, placement_store.clone(), config).await?;
            if chain_context.is_none() {
                chain_context = Some(node.context.clone());
            }
            shutdowns.push(node.shutdown);
            tasks.push(node.task);
        }
        for index in 0..config.nodes {
            let node = start_bench_node(index, placement_store.clone(), config).await?;
            if bench_context.is_none() {
                bench_context = Some(node.context.clone());
            }
            shutdowns.push(node.shutdown);
            tasks.push(node.task);
        }

        let bench_client = client_from_context::<BenchClient>(
            bench_context.as_ref().expect("bench context is set"),
        )?;
        let chain_client = client_from_context::<ChainClient>(
            chain_context.as_ref().expect("chain context is set"),
        )?;

        Ok(Self {
            inner: Arc::new(BenchmarkTopologyInner {
                bench_client,
                chain_client,
                shutdowns: Mutex::new(shutdowns),
                tasks: Mutex::new(tasks),
            }),
        })
    }

    pub fn bench_client(&self) -> Arc<BenchClient> {
        self.inner.bench_client.clone()
    }

    pub fn chain_client(&self) -> Arc<ChainClient> {
        self.inner.chain_client.clone()
    }

    pub async fn shutdown(&self) -> BenchmarkResult<()> {
        let shutdowns = std::mem::take(&mut *self.inner.shutdowns.lock().await);
        for shutdown in shutdowns {
            let _ = shutdown.send(());
        }

        let tasks = std::mem::take(&mut *self.inner.tasks.lock().await);
        for task in tasks {
            task.await??;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct StartedNode {
    context: ServiceContext,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), LatticeServiceError>>,
}

async fn start_bench_node(
    index: usize,
    placement_store: InMemoryPlacementStore,
    config: &BenchmarkConfig,
) -> BenchmarkResult<StartedNode> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = LatticeService::builder(BENCH_SERVICE)
        .instance_id(InstanceId::new(format!("bench-{index}")))
        .listen(listener)
        .ready_signal(ready_tx)
        .rpc_client_transport(config.rpc_client_transport())
        .rpc_retry_policy(config.rpc_retry_policy())
        .placement_store::<InMemoryPlacementStore, _>(placement_store)
        .register_client::<bench_rpc::Binding>()
        .register_actor(
            ActorRegistration::builder(BENCH_ACTOR)
                .factory(BenchActorFactory)
                .build(),
        )
        .register_sharded_rpc(
            bench_rpc::Binding::for_actor::<BenchActor>(BENCH_ACTOR)
                .request_dedup(config.request_dedup),
        )
        .build()
        .await?;
    start_service(service, ready_rx, shutdown_tx, shutdown_rx).await
}

async fn start_chain_node(
    index: usize,
    placement_store: InMemoryPlacementStore,
    config: &BenchmarkConfig,
) -> BenchmarkResult<StartedNode> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = LatticeService::builder(CHAIN_SERVICE)
        .instance_id(InstanceId::new(format!("chain-{index}")))
        .listen(listener)
        .ready_signal(ready_tx)
        .rpc_client_transport(config.rpc_client_transport())
        .rpc_retry_policy(config.rpc_retry_policy())
        .placement_store::<InMemoryPlacementStore, _>(placement_store)
        .register_client::<chain_rpc::Binding>()
        .register_client::<worker_rpc::Binding>()
        .register_actor(
            ActorRegistration::builder(CHAIN_ACTOR)
                .factory(ChainActorFactory)
                .build(),
        )
        .register_sharded_rpc(
            chain_rpc::Binding::for_actor::<ChainActor>(CHAIN_ACTOR)
                .request_dedup(config.request_dedup),
        )
        .build()
        .await?;
    start_service(service, ready_rx, shutdown_tx, shutdown_rx).await
}

async fn start_worker_node(
    index: usize,
    placement_store: InMemoryPlacementStore,
    config: &BenchmarkConfig,
) -> BenchmarkResult<StartedNode> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = LatticeService::builder(WORKER_SERVICE)
        .instance_id(InstanceId::new(format!("worker-{index}")))
        .listen(listener)
        .ready_signal(ready_tx)
        .rpc_client_transport(config.rpc_client_transport())
        .rpc_retry_policy(config.rpc_retry_policy())
        .placement_store::<InMemoryPlacementStore, _>(placement_store)
        .register_actor(
            ActorRegistration::builder(WORKER_ACTOR)
                .factory(WorkerActorFactory)
                .build(),
        )
        .register_sharded_rpc(
            worker_rpc::Binding::for_actor::<WorkerActor>(WORKER_ACTOR)
                .request_dedup(config.request_dedup),
        )
        .build()
        .await?;
    start_service(service, ready_rx, shutdown_tx, shutdown_rx).await
}

async fn start_service(
    service: LatticeService,
    ready_rx: oneshot::Receiver<std::net::SocketAddr>,
    shutdown: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
) -> BenchmarkResult<StartedNode> {
    let context = service.context().clone();
    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    ready_rx.await.map_err(|_| BenchmarkError::ReadyDropped)?;
    Ok(StartedNode {
        context,
        shutdown,
        task,
    })
}

fn client_from_context<T>(context: &ServiceContext) -> BenchmarkResult<Arc<T>>
where
    T: Send + Sync + 'static,
{
    context
        .extension::<T>()
        .ok_or(BenchmarkError::MissingClient {
            client_type: std::any::type_name::<T>(),
        })
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn run_id() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}
