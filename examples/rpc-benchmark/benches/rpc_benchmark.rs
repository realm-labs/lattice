#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lattice_remoting::{
    transport::FramedWriter,
    wire::{Frame, FrameCodec, FrameKind},
};
use remoting_benchmark::{
    BenchmarkConfig, RemotingTopology, actor_completion::ActorCompletionTopology,
    end_to_end::RemoteActorTopology,
};
use tokio::runtime::Runtime;

#[derive(Clone, PartialEq, prost::Message)]
struct WirePayload {
    #[prost(bytes = "bytes", tag = "1")]
    payload: Bytes,
}

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
        BenchmarkId::new("prepared_bulk_tell_admission", config.payload_bytes),
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
    group.bench_with_input(
        BenchmarkId::new("unprepared_bulk_tell_admission", config.payload_bytes),
        &config,
        |bench, config| {
            let topology = &topology;
            let requests = config.requests;
            let payload_bytes = config.payload_bytes;
            bench
                .to_async(&runtime)
                .iter_custom(move |iterations| async move {
                    topology
                        .run_unprepared_bulk_tell(requests * iterations as usize, payload_bytes)
                        .await
                        .expect("unprepared bulk tell workload")
                        .elapsed
                });
        },
    );
    group.finish();
    runtime.block_on(topology.shutdown());
}

fn actor_completion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let config = BenchmarkConfig::from_env();
    let topology = runtime
        .block_on(ActorCompletionTopology::start_timing(1024))
        .expect("actor completion topology");
    let mut group = c.benchmark_group("actor_completion");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(config.requests as u64));
    group.bench_with_input(
        BenchmarkId::new("bounded_mailbox", config.payload_bytes),
        &config,
        |bench, config| {
            let topology = &topology;
            let requests = config.requests;
            let payload_bytes = config.payload_bytes;
            bench
                .to_async(&runtime)
                .iter_custom(move |iterations| async move {
                    topology
                        .run(requests.saturating_mul(iterations as usize), payload_bytes)
                        .await
                        .expect("actor completion workload")
                        .workload
                        .elapsed
                });
        },
    );
    group.finish();
    runtime
        .block_on(topology.shutdown())
        .expect("actor completion shutdown");
}

fn remote_actor_round_trip_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let config = BenchmarkConfig::from_env();
    let topology = runtime
        .block_on(RemoteActorTopology::start(config.bulk_stripes))
        .expect("remote actor topology");
    let mut group = c.benchmark_group("remote_actor_end_to_end");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(config.round_trip_requests as u64));
    group.bench_with_input(
        BenchmarkId::new("tcp_ask_round_trip", config.payload_bytes),
        &config,
        |bench, config| {
            let topology = &topology;
            let requests = config.round_trip_requests;
            let payload_bytes = config.payload_bytes;
            bench
                .to_async(&runtime)
                .iter_custom(move |iterations| async move {
                    topology
                        .run_timing(requests.saturating_mul(iterations as usize), payload_bytes)
                        .await
                        .expect("remote actor round-trip workload")
                });
        },
    );
    group.finish();
    runtime
        .block_on(topology.shutdown())
        .expect("remote actor topology shutdown");
}

fn wire_codec_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let config = BenchmarkConfig::from_env();
    let codec = FrameCodec::new(256 * 1024).expect("valid frame limit");
    let ready = Frame::encode_message(
        FrameKind::Tell,
        &WirePayload {
            payload: Bytes::from(vec![0_u8; config.payload_bytes]),
        },
    );
    let mut group = c.benchmark_group("remoting_frame_write");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("vectored_writer", |bench| {
        bench.to_async(&runtime).iter_custom(|iterations| {
            let codec = codec.clone();
            let ready = &ready;
            async move {
                let mut writer = FramedWriter::new(tokio::io::sink(), codec);
                let started = Instant::now();
                for _ in 0..iterations {
                    writer
                        .write_frame(black_box(ready))
                        .await
                        .expect("write vectored frame");
                }
                started.elapsed()
            }
        });
    });
    group.bench_function("coalescing_codec", |bench| {
        bench.iter(|| {
            codec
                .encode(black_box(&ready))
                .expect("coalesce frame for writing")
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    remoting_benchmark,
    actor_completion_benchmark,
    remote_actor_round_trip_benchmark,
    wire_codec_benchmark
);
criterion_main!(benches);
