use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
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
}

impl From<&BenchmarkConfig> for WorkloadConfig {
    fn from(config: &BenchmarkConfig) -> Self {
        Self {
            actors: config.actors,
            concurrency: config.concurrency,
            requests: config.requests,
        }
    }
}

pub async fn warm_up(topology: &BenchmarkTopology, config: &WorkloadConfig) -> BenchmarkResult<()> {
    let warmup = WorkloadConfig {
        actors: config.actors.clamp(1, 32),
        concurrency: config.concurrency.clamp(1, 16),
        requests: (config.actors.min(32) as usize).max(1),
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
    let semaphore = Arc::new(Semaphore::new(config.concurrency.max(1)));
    let mut tasks = JoinSet::new();
    let started = Instant::now();

    for sequence in 0..config.requests {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore open");
        let client = client.clone();
        let actor_id = actor_id_for(sequence, config.actors);
        tasks.spawn(async move {
            let _permit = permit;
            let request_started = Instant::now();
            let reply = client
                .ping(PingRequest {
                    actor_id,
                    sequence: sequence as u64,
                    payload: Vec::new(),
                })
                .await
                .map_err(BenchmarkError::from)?;
            Ok::<_, BenchmarkError>((request_started.elapsed(), reply.actor_id))
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
    let semaphore = Arc::new(Semaphore::new(config.concurrency.max(1)));
    let mut tasks = JoinSet::new();
    let started = Instant::now();

    for sequence in 0..config.requests {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore open");
        let client = client.clone();
        let actor_id = actor_id_for(sequence, config.actors);
        let worker_actor_id = actor_id_for(sequence + 17, config.actors);
        tasks.spawn(async move {
            let _permit = permit;
            let request_started = Instant::now();
            let reply = client
                .chain_ping(ChainPingRequest {
                    actor_id,
                    worker_actor_id,
                    sequence: sequence as u64,
                })
                .await
                .map_err(BenchmarkError::from)?;
            Ok::<_, BenchmarkError>((request_started.elapsed(), reply.actor_id))
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
    mut tasks: JoinSet<BenchmarkResult<(Duration, u64)>>,
) -> BenchmarkResult<WorkloadReport> {
    let mut latencies = Vec::with_capacity(requests);
    let mut observed_actor_ids = BTreeSet::new();
    let mut errors = 0;

    while let Some(result) = tasks.join_next().await {
        match result? {
            Ok((latency, actor_id)) => {
                latencies.push(latency);
                observed_actor_ids.insert(actor_id);
            }
            Err(_) => {
                errors += 1;
            }
        }
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

fn actor_id_for(sequence: usize, actors: u64) -> u64 {
    (sequence as u64 % actors.max(1)) + 1
}
