use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;

use crate::bench::{ChainPingRequest, PingRequest};
use crate::error::{BenchmarkError, BenchmarkResult};
use crate::metrics::WorkloadReport;
use crate::topology::{BenchmarkConfig, BenchmarkTopology};

#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub actors: u64,
    pub concurrency: usize,
    pub requests: usize,
    pub payload_bytes: usize,
}

impl From<&BenchmarkConfig> for WorkloadConfig {
    fn from(config: &BenchmarkConfig) -> Self {
        Self {
            actors: config.actors,
            concurrency: config.concurrency,
            requests: config.requests,
            payload_bytes: config.payload_bytes,
        }
    }
}

pub async fn warm_up(topology: &BenchmarkTopology, config: &WorkloadConfig) -> BenchmarkResult<()> {
    let warmup = WorkloadConfig {
        actors: config.actors.clamp(1, 32),
        concurrency: config.concurrency.clamp(1, 16),
        requests: (config.actors.min(32) as usize).max(1),
        payload_bytes: config.payload_bytes,
    };
    run_routed_rpc_fanout(topology, &warmup).await?;
    run_cross_service_chain(topology, &warmup).await?;
    Ok(())
}

pub async fn run_routed_rpc_fanout(
    topology: &BenchmarkTopology,
    config: &WorkloadConfig,
) -> BenchmarkResult<WorkloadReport> {
    let client = topology.bench_client();
    let mut tasks = JoinSet::new();
    let started = Instant::now();
    let worker_count = worker_count(config);

    for worker_id in 0..worker_count {
        let client = client.clone();
        let config = config.clone();
        tasks.spawn(async move {
            let mut report = WorkerReport::with_capacity(requests_for_worker(
                worker_id,
                worker_count,
                config.requests,
            ));
            let payload = vec![0_u8; config.payload_bytes];
            let mut sequence = worker_id;
            while sequence < config.requests {
                let actor_id = actor_id_for(sequence, config.actors);
                let request_started = Instant::now();
                match client
                    .ping(PingRequest {
                        actor_id,
                        sequence: sequence as u64,
                        payload: payload.clone(),
                    })
                    .await
                {
                    Ok(reply) => report.record_success(request_started.elapsed(), reply.actor_id),
                    Err(_) => report.record_error(),
                }
                sequence += worker_count;
            }
            Ok::<_, BenchmarkError>(report)
        });
    }

    collect_report(
        "routed_rpc_fanout_warm_cache",
        config.requests,
        started,
        tasks,
    )
    .await
}

pub async fn run_cross_service_chain(
    topology: &BenchmarkTopology,
    config: &WorkloadConfig,
) -> BenchmarkResult<WorkloadReport> {
    let client = topology.chain_client();
    let mut tasks = JoinSet::new();
    let started = Instant::now();
    let worker_count = worker_count(config);

    for worker_id in 0..worker_count {
        let client = client.clone();
        let config = config.clone();
        tasks.spawn(async move {
            let mut report = WorkerReport::with_capacity(requests_for_worker(
                worker_id,
                worker_count,
                config.requests,
            ));
            let mut sequence = worker_id;
            while sequence < config.requests {
                let actor_id = actor_id_for(sequence, config.actors);
                let worker_actor_id = actor_id_for(sequence + 17, config.actors);
                let request_started = Instant::now();
                match client
                    .chain_ping(ChainPingRequest {
                        actor_id,
                        worker_actor_id,
                        sequence: sequence as u64,
                    })
                    .await
                {
                    Ok(reply) => report.record_success(request_started.elapsed(), reply.actor_id),
                    Err(_) => report.record_error(),
                }
                sequence += worker_count;
            }
            Ok::<_, BenchmarkError>(report)
        });
    }

    collect_report(
        "cross_service_chain_warm_cache",
        config.requests,
        started,
        tasks,
    )
    .await
}

async fn collect_report(
    name: &'static str,
    requests: usize,
    started: Instant,
    mut tasks: JoinSet<BenchmarkResult<WorkerReport>>,
) -> BenchmarkResult<WorkloadReport> {
    let mut latencies = Vec::with_capacity(requests);
    let mut observed_actor_ids = BTreeSet::new();
    let mut errors = 0;

    while let Some(result) = tasks.join_next().await {
        let worker = result??;
        latencies.extend(worker.latencies);
        observed_actor_ids.extend(worker.observed_actor_ids);
        errors += worker.errors;
    }

    Ok(WorkloadReport {
        name,
        requests,
        successes: latencies.len(),
        errors,
        elapsed: started.elapsed(),
        latencies,
        observed_actor_ids,
    })
}

#[derive(Debug)]
struct WorkerReport {
    latencies: Vec<Duration>,
    observed_actor_ids: BTreeSet<u64>,
    errors: usize,
}

impl WorkerReport {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            latencies: Vec::with_capacity(capacity),
            observed_actor_ids: BTreeSet::new(),
            errors: 0,
        }
    }

    fn record_success(&mut self, latency: Duration, actor_id: u64) {
        self.latencies.push(latency);
        self.observed_actor_ids.insert(actor_id);
    }

    fn record_error(&mut self) {
        self.errors += 1;
    }
}

fn worker_count(config: &WorkloadConfig) -> usize {
    config.concurrency.max(1).min(config.requests.max(1))
}

fn requests_for_worker(worker_id: usize, worker_count: usize, requests: usize) -> usize {
    if worker_id >= requests {
        return 0;
    }
    ((requests - 1 - worker_id) / worker_count) + 1
}

fn actor_id_for(sequence: usize, actors: u64) -> u64 {
    (sequence as u64 % actors.max(1)) + 1
}
