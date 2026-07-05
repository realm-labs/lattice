use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rpc_benchmark::error::BenchmarkResult;
use rpc_benchmark::metrics::WorkloadReport;
use rpc_benchmark::topology::{BenchmarkConfig, BenchmarkTopology};
use rpc_benchmark::workload::{
    WorkloadConfig, run_cross_service_chain, run_routed_rpc_fanout, warm_up,
};
use tokio::runtime::Runtime;

fn rpc_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let config = BenchmarkConfig::from_env();
    let topology = runtime
        .block_on(BenchmarkTopology::start(&config))
        .expect("benchmark topology starts");
    let workload = WorkloadConfig::from(&config);
    runtime
        .block_on(warm_up(&topology, &workload))
        .expect("benchmark warmup succeeds");

    let mut group = c.benchmark_group("multi_node_rpc");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    group.bench_with_input(
        BenchmarkId::new("routed_rpc_fanout_warm_cache", config.requests),
        &workload,
        |bench, workload| {
            bench.to_async(&runtime).iter_custom(|iterations| {
                let topology = topology.clone();
                let mut workload = workload.clone();
                workload.requests *= iterations as usize;
                async move {
                    report_or_panic(run_routed_rpc_fanout(&topology, &workload).await).elapsed
                }
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("cross_service_chain_warm_cache", config.requests),
        &workload,
        |bench, workload| {
            bench.to_async(&runtime).iter_custom(|iterations| {
                let topology = topology.clone();
                let mut workload = workload.clone();
                workload.requests *= iterations as usize;
                async move {
                    report_or_panic(run_cross_service_chain(&topology, &workload).await).elapsed
                }
            });
        },
    );

    group.finish();
    runtime
        .block_on(topology.shutdown())
        .expect("benchmark topology shuts down");
}

fn report_or_panic(result: BenchmarkResult<WorkloadReport>) -> WorkloadReport {
    match result {
        Ok(report) => {
            if report.errors != 0 {
                panic!("benchmark workload reported errors: {report}");
            }
            eprintln!("{report}");
            report
        }
        Err(error) => panic!("benchmark workload failed: {error}"),
    }
}

criterion_group!(benches, rpc_benchmark);
criterion_main!(benches);
