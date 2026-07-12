#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use remoting_benchmark::{BenchmarkConfig, RemotingTopology};
use tokio::runtime::Runtime;

fn remoting_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let config = BenchmarkConfig::from_env();
    let topology = runtime
        .block_on(async { RemotingTopology::start(&config) })
        .expect("remoting topology");
    let mut group = c.benchmark_group("remoting_association");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.bench_with_input(
        BenchmarkId::new("bulk_tell_admission", config.payload_bytes),
        &config,
        |bench, config| {
            let topology = &topology;
            let requests = config.requests;
            let payload_bytes = config.payload_bytes;
            bench
                .to_async(&runtime)
                .iter_custom(move |iterations| async move {
                    topology
                        .run_bulk_tell(requests * iterations as usize, payload_bytes)
                        .await
                        .expect("bulk tell workload")
                        .elapsed
                });
        },
    );
    group.finish();
    runtime.block_on(topology.shutdown());
}

criterion_group!(benches, remoting_benchmark);
criterion_main!(benches);
