use std::path::PathBuf;

use clap::Parser;
use lattice_core::instance::InstanceId;
use lattice_placement::routing::rpc::RpcRetryPolicy;
use lattice_placement::storage::etcd::client::RealEtcdClient;
use lattice_placement::storage::etcd::{EtcdPlacementStore, EtcdPlacementStoreConfig};
use lattice_rpc::client::TonicEndpointChannelPoolConfig;
use lattice_service::actors::registration::ActorRegistration;
use lattice_service::config::InstanceConfig;
use lattice_service::runtime::service::LatticeService;
use rpc_benchmark::actors::{BenchActor, BenchActorFactory};
use rpc_benchmark::error::BenchmarkResult;
use rpc_benchmark::generated::bench_rpc;
use rpc_benchmark::multiprocess::{parse_csv, placement_authority};
use rpc_benchmark::{BENCH_ACTOR, BENCH_SERVICE};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[derive(Debug, Parser)]
#[command(about = "Run one rpc-benchmark service node")]
struct Args {
    #[arg(long)]
    index: usize,
    #[arg(long)]
    key_prefix: String,
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    etcd_endpoints: String,
    #[arg(long, default_value = "http://127.0.0.1:50080")]
    coordinator_endpoint: String,
    #[arg(long, default_value_t = 4)]
    channel_stripes: usize,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    rpc_retry: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    request_dedup: bool,
    #[arg(long)]
    ready_file: PathBuf,
}

#[tokio::main]
async fn main() -> BenchmarkResult<()> {
    let args = Args::parse();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let placement_store =
        EtcdPlacementStore::<RealEtcdClient>::dangerously_connect_unauthenticated(
            EtcdPlacementStoreConfig {
                key_prefix: args.key_prefix,
                endpoints: parse_csv(&args.etcd_endpoints),
                instance_lease_ttl_secs: 30,
                activation_lock_ttl_secs: 10,
            },
        )
        .await?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let placement_authority = placement_authority(&args.coordinator_endpoint)?;
    let mut builder = LatticeService::builder(BENCH_SERVICE)
        .instance(InstanceConfig::new(InstanceId::new(format!(
            "bench-{}",
            args.index
        ))))
        .listen(listener)
        .ready_signal(ready_tx)
        .rpc_client_transport(
            TonicEndpointChannelPoolConfig::try_new(args.channel_stripes)
                .expect("channel_stripes is provided by benchmark driver"),
        )
        .placement_store::<EtcdPlacementStore<RealEtcdClient>, _>(placement_store)
        .placement_authority(placement_authority)
        .register_actor(
            ActorRegistration::builder(BENCH_ACTOR)
                .factory(BenchActorFactory)
                .build(),
        )
        .register_sharded_rpc(
            bench_rpc::Binding::for_explicit_actor::<BenchActor>(BENCH_ACTOR)
                .request_dedup(args.request_dedup),
        );
    if !args.rpc_retry {
        builder = builder.rpc_retry_policy(RpcRetryPolicy::Disabled);
    }
    let service = builder.build().await?;
    let task = tokio::spawn(service.run_until_shutdown());
    let local_addr = ready_rx
        .await
        .map_err(|_| rpc_benchmark::error::BenchmarkError::ReadyDropped)?;
    tokio::fs::write(&args.ready_file, local_addr.to_string()).await?;
    task.await??;
    Ok(())
}
