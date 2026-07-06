use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lattice_core::{
    BackpressurePolicy, CoalesceKey, DirectLinkEndpoint, DirectLinkMessageId, LinkId, LinkSequence,
};
use lattice_direct_link::{
    BackpressureQueue, DirectLinkConnection, DirectLinkFrame, DirectLinkFrameCodec,
    DirectLinkListenConfig, DirectLinkTransport, TcpDirectLinkTransport,
};
use tokio::runtime::Runtime;

fn direct_link_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");

    let mut tcp = c.benchmark_group("direct_link_tcp_single_process");
    tcp.sample_size(10);
    tcp.measurement_time(Duration::from_secs(5));
    for payload_size in [128_usize, 4096] {
        tcp.bench_with_input(
            BenchmarkId::new("loopback_write_read", payload_size),
            &payload_size,
            |bench, payload_size| {
                bench
                    .to_async(&runtime)
                    .iter_custom(|iterations| tcp_write_read(iterations, *payload_size, false));
            },
        );
    }
    tcp.finish();

    let mut local = c.benchmark_group("direct_link_local_multi_process_shape");
    local.sample_size(10);
    local.measurement_time(Duration::from_secs(5));
    for payload_size in [128_usize, 4096] {
        local.bench_with_input(
            BenchmarkId::new("independent_transports_loopback", payload_size),
            &payload_size,
            |bench, payload_size| {
                bench
                    .to_async(&runtime)
                    .iter_custom(|iterations| tcp_write_read(iterations, *payload_size, true));
            },
        );
    }
    local.finish();

    let mut matrix = c.benchmark_group("direct_link_payload_backpressure_matrix");
    for payload_size in [128_usize, 4096, 65_536] {
        matrix.bench_with_input(
            BenchmarkId::new("frame_codec_roundtrip", payload_size),
            &payload_size,
            |bench, payload_size| {
                let codec = DirectLinkFrameCodec::new(0);
                let frame = DirectLinkFrame::message(
                    LinkId::new("bench-link"),
                    LinkSequence(1),
                    DirectLinkMessageId(7),
                    vec![0; *payload_size],
                );
                bench.iter(|| {
                    let encoded = codec.encode(&frame).expect("encode frame");
                    let decoded = codec.decode(&encoded).expect("decode frame");
                    criterion::black_box(decoded);
                });
            },
        );
    }
    for policy in backpressure_policies() {
        matrix.bench_with_input(
            BenchmarkId::new("backpressure_enqueue", policy_name(&policy)),
            &policy,
            |bench, policy| {
                bench.iter(|| {
                    let mut queue = BackpressureQueue::new(policy.clone());
                    for id in 0..2048 {
                        criterion::black_box(queue.try_enqueue(DirectLinkMessageId(id)));
                    }
                    criterion::black_box(queue.snapshot());
                });
            },
        );
    }
    matrix.finish();
}

async fn tcp_write_read(
    iterations: u64,
    payload_size: usize,
    independent_transports: bool,
) -> Duration {
    let server_transport = TcpDirectLinkTransport::new();
    let listener = server_transport
        .bind(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new("tcp://127.0.0.1:0".parse().unwrap()),
            max_frame_size: 0,
        })
        .await
        .expect("bind direct-link listener");
    let endpoint = listener.local_endpoint();
    let server = tokio::spawn(async move {
        let mut connection = listener.accept().await.expect("accept direct-link");
        for _ in 0..iterations {
            criterion::black_box(
                connection
                    .read_frame()
                    .await
                    .expect("read direct-link frame"),
            );
        }
    });

    let client_transport = if independent_transports {
        TcpDirectLinkTransport::new()
    } else {
        server_transport
    };
    let mut client = client_transport
        .connect_physical(endpoint)
        .await
        .expect("connect direct-link");
    let payload = vec![0; payload_size];
    let start = Instant::now();
    for sequence in 0..iterations {
        client
            .write_frame(DirectLinkFrame::message(
                LinkId::new("bench-link"),
                LinkSequence(sequence + 1),
                DirectLinkMessageId(7),
                payload.clone(),
            ))
            .await
            .expect("write direct-link frame");
    }
    client.close().await.expect("close direct-link client");
    server.await.expect("server task");
    start.elapsed()
}

fn backpressure_policies() -> Vec<BackpressurePolicy> {
    vec![
        BackpressurePolicy::Block { max_pending: 64 },
        BackpressurePolicy::FailFast { max_pending: 64 },
        BackpressurePolicy::DropNewest { max_pending: 64 },
        BackpressurePolicy::DropOldest { max_pending: 64 },
        BackpressurePolicy::Coalesce {
            max_pending: 64,
            key: CoalesceKey::new("bench"),
        },
        BackpressurePolicy::Disconnect { max_pending: 64 },
    ]
}

fn policy_name(policy: &BackpressurePolicy) -> &'static str {
    match policy {
        BackpressurePolicy::Block { .. } => "block",
        BackpressurePolicy::FailFast { .. } => "fail_fast",
        BackpressurePolicy::DropNewest { .. } => "drop_newest",
        BackpressurePolicy::DropOldest { .. } => "drop_oldest",
        BackpressurePolicy::Coalesce { .. } => "coalesce",
        BackpressurePolicy::Disconnect { .. } => "disconnect",
    }
}

criterion_group!(benches, direct_link_benchmark);
criterion_main!(benches);
