use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use clap::Parser;
use lattice_core::instance::InstanceId;
use rpc_benchmark::error::BenchmarkResult;
use rpc_benchmark::multiprocess::{
    EtcdBenchmarkConfig, build_bench_client_from_etcd, parse_csv, wait_for_ready_files,
    wait_for_ready_instances,
};
use rpc_benchmark::topology::BenchmarkConfig;
use rpc_benchmark::workload::{WorkloadConfig, run_routed_rpc_fanout_with_client};

#[derive(Debug, Parser)]
#[command(about = "Run a local multi-process lattice RPC benchmark")]
struct Args {
    #[arg(long, default_value_t = 2)]
    nodes: usize,
    #[arg(long, default_value_t = 256)]
    actors: u64,
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    #[arg(long, default_value_t = 10_000)]
    requests: usize,
    #[arg(long, default_value_t = 4)]
    channel_stripes: usize,
    #[arg(long, default_value_t = 0)]
    payload_bytes: usize,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    rpc_retry: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    request_dedup: bool,
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    etcd_endpoints: String,
    #[arg(long, default_value = "http://127.0.0.1:50080")]
    coordinator_endpoint: String,
    /// Placement namespace shared with the separately started coordinator.
    #[arg(long)]
    key_prefix: String,
    #[arg(long)]
    node_exe: Option<PathBuf>,
    #[arg(long, default_value_t = 15)]
    startup_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> BenchmarkResult<()> {
    let args = Args::parse();
    let config = BenchmarkConfig {
        nodes: args.nodes.max(1),
        actors: args.actors.max(1),
        concurrency: args.concurrency.max(1),
        requests: args.requests.max(1),
        channel_stripes: args.channel_stripes.max(1),
        rpc_retry: args.rpc_retry,
        request_dedup: args.request_dedup,
        payload_bytes: args.payload_bytes,
    };
    let etcd = EtcdBenchmarkConfig::new(parse_csv(&args.etcd_endpoints), args.key_prefix)
        .with_coordinator_endpoint(args.coordinator_endpoint);
    let node_exe = args.node_exe.unwrap_or_else(default_node_exe);
    let ready_dir =
        std::env::temp_dir().join(format!("lattice-rpc-benchmark-{}", std::process::id()));
    tokio::fs::create_dir_all(&ready_dir).await?;

    let mut children = Vec::new();
    let mut ready_files = Vec::new();
    for index in 0..config.nodes {
        let ready_file = ready_dir.join(format!("bench-{index}.ready"));
        let child = spawn_node(&node_exe, &ready_file, &etcd, &config, index)?;
        children.push(child);
        ready_files.push(ready_file);
    }
    let mut children = ChildGuard::new(children);
    let startup_timeout = Duration::from_secs(args.startup_timeout_secs);
    wait_for_ready_files(&ready_files, children.as_mut_slice(), startup_timeout).await?;

    let store = etcd.connect_store().await?;
    wait_for_ready_instances(&store, config.nodes, startup_timeout).await?;

    let (client, _watch) =
        build_bench_client_from_etcd(&config, &etcd, InstanceId::new("bench-driver")).await?;
    let workload = WorkloadConfig::from(&config);
    let warmup = WorkloadConfig {
        actors: workload.actors,
        concurrency: workload.concurrency.clamp(1, 16),
        requests: workload.actors as usize,
        payload_bytes: workload.payload_bytes,
    };
    run_routed_rpc_fanout_with_client(
        "routed_rpc_fanout_multiprocess_warmup",
        client.clone(),
        &warmup,
    )
    .await?;
    let report = run_routed_rpc_fanout_with_client(
        "routed_rpc_fanout_multiprocess_warm_cache",
        client,
        &workload,
    )
    .await?;
    println!("{report}");
    children.shutdown();
    let _ = tokio::fs::remove_dir_all(ready_dir).await;
    Ok(())
}

fn spawn_node(
    node_exe: &Path,
    ready_file: &Path,
    etcd: &EtcdBenchmarkConfig,
    config: &BenchmarkConfig,
    index: usize,
) -> BenchmarkResult<Child> {
    let mut command = Command::new(node_exe);
    command
        .arg("--index")
        .arg(index.to_string())
        .arg("--key-prefix")
        .arg(&etcd.key_prefix)
        .arg("--etcd-endpoints")
        .arg(etcd.endpoints.join(","))
        .arg("--coordinator-endpoint")
        .arg(&etcd.coordinator_endpoint)
        .arg("--channel-stripes")
        .arg(config.channel_stripes.to_string())
        .arg("--rpc-retry")
        .arg(config.rpc_retry.to_string())
        .arg("--request-dedup")
        .arg(config.request_dedup.to_string())
        .arg("--ready-file")
        .arg(ready_file)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    Ok(command.spawn()?)
}

fn default_node_exe() -> PathBuf {
    let current = std::env::current_exe().expect("current executable path");
    current.with_file_name(format!(
        "rpc-benchmark-node{}",
        std::env::consts::EXE_SUFFIX
    ))
}

#[derive(Debug)]
struct ChildGuard {
    children: Vec<Child>,
}

impl ChildGuard {
    fn new(children: Vec<Child>) -> Self {
        Self { children }
    }

    fn as_mut_slice(&mut self) -> &mut [Child] {
        &mut self.children
    }

    fn shutdown(mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.children.clear();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
