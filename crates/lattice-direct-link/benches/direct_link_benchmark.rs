use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lattice_core::{
    ActorId, ActorKind, ActorRef, BackpressurePolicy, CoalesceKey, DirectLinkEndpoint,
    DirectLinkMessageDescriptor, DirectLinkMessageId, DirectLinkMode, DirectLinkOpenRequest,
    DirectLinkOptions, DirectLinkStreamDescriptor, InstanceId, LinkDirection, LinkId,
    LinkMessageFlags, LinkSequence, LinkTarget, OutboundDirectLinkMessage, ServiceKind,
    TraceContext,
};
use lattice_direct_link::{
    BackpressureQueue, DIRECT_LINK_PROTOCOL_VERSION, DirectLinkConnection, DirectLinkEndpointPool,
    DirectLinkEndpointPoolConfig, DirectLinkFrame, DirectLinkFrameCodec, DirectLinkFrameKind,
    DirectLinkListenConfig, DirectLinkTransport, NegotiatedDirection, OpenLinkAck,
    OpenLinkDirection, OpenLinkRequest, PooledDirectLinkEndpointPool, TcpDirectLinkListener,
    TcpDirectLinkTransport,
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

    let mut pooling = c.benchmark_group("direct_link_pooling_comparison");
    pooling.sample_size(10);
    pooling.measurement_time(Duration::from_secs(5));
    for link_count in [16_usize, 64] {
        pooling.bench_with_input(
            BenchmarkId::new("one_tcp_connection_per_link", link_count),
            &link_count,
            |bench, link_count| {
                bench.to_async(&runtime).iter_custom(|iterations| {
                    one_connection_per_link(iterations.min(16), *link_count, 128)
                });
            },
        );
        pooling.bench_with_input(
            BenchmarkId::new("pooled_striped_tcp_connections", link_count),
            &link_count,
            |bench, link_count| {
                bench.to_async(&runtime).iter_custom(|iterations| {
                    pooled_striped_links(iterations.min(16), *link_count, 128, 4)
                });
            },
        );
    }
    pooling.finish();
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

async fn one_connection_per_link(
    iterations: u64,
    link_count: usize,
    payload_size: usize,
) -> Duration {
    let transport = TcpDirectLinkTransport::new();
    let listener = bind_listener(&transport).await;
    let endpoint = listener.local_endpoint();
    let expected_messages = iterations as usize * link_count;
    let server = tokio::spawn(handle_link_server(listener, expected_messages));
    let payload = vec![0; payload_size];
    let start = Instant::now();
    for iteration in 0..iterations {
        for link_index in 0..link_count {
            let link_id = LinkId::new(format!("raw-{iteration}-{link_index}"));
            let mut connection = transport
                .connect_physical(endpoint.clone())
                .await
                .expect("connect direct-link");
            connection
                .write_frame(
                    DirectLinkFrame::open_link(&wire_open_link_request(link_id.clone()))
                        .expect("encode open-link"),
                )
                .await
                .expect("write open-link");
            let response = connection.read_frame().await.expect("read open-link ack");
            assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkAck);
            connection
                .write_frame(DirectLinkFrame::message(
                    link_id,
                    LinkSequence(1),
                    DirectLinkMessageId(7),
                    payload.clone(),
                ))
                .await
                .expect("write direct-link frame");
            connection.close().await.expect("close direct-link client");
        }
    }
    server.await.expect("server task");
    start.elapsed()
}

async fn pooled_striped_links(
    iterations: u64,
    link_count: usize,
    payload_size: usize,
    connections_per_endpoint: usize,
) -> Duration {
    let transport = TcpDirectLinkTransport::new();
    let listener = bind_listener(&transport).await;
    let endpoint = listener.local_endpoint();
    let expected_messages = iterations as usize * link_count;
    let server = tokio::spawn(handle_link_server(listener, expected_messages));
    let pool_config = DirectLinkEndpointPoolConfig {
        connections_per_endpoint: std::num::NonZeroUsize::new(connections_per_endpoint).unwrap(),
        ..DirectLinkEndpointPoolConfig::default()
    };
    let pool = PooledDirectLinkEndpointPool::new(transport, pool_config.clone());
    let link_ids = striped_link_ids(&pool_config, link_count);
    let payload = vec![0; payload_size];
    let start = Instant::now();
    for iteration in 0..iterations {
        for link_id in &link_ids {
            let link_id = LinkId::new(format!("{iteration}-{link_id}"));
            let session = pool
                .open_link(open_link_request(link_id.clone(), endpoint.clone()))
                .await
                .expect("open pooled direct-link");
            session
                .session
                .sender
                .tell(OutboundDirectLinkMessage {
                    link_id,
                    direction: LinkDirection::SourceToTarget,
                    message_id: DirectLinkMessageId(7),
                    proto_full_name: "bench.Payload",
                    payload: payload.clone(),
                    flags: LinkMessageFlags::EMPTY,
                })
                .await
                .expect("write pooled direct-link frame");
        }
    }
    criterion::black_box(pool.metrics_snapshot().physical_connections_opened);
    criterion::black_box(pool.metrics_snapshot().links_per_connection);
    server.await.expect("server task");
    start.elapsed()
}

async fn bind_listener(transport: &TcpDirectLinkTransport) -> TcpDirectLinkListener {
    transport
        .bind(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new("tcp://127.0.0.1:0".parse().unwrap()),
            max_frame_size: 0,
        })
        .await
        .expect("bind direct-link listener")
}

async fn handle_link_server(listener: TcpDirectLinkListener, expected_messages: usize) {
    let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);
    loop {
        tokio::select! {
            _ = done_rx.recv() => break,
            accepted = listener.accept() => {
                let mut connection = accepted.expect("accept direct-link");
                let received = received.clone();
                let done_tx = done_tx.clone();
                tokio::spawn(async move {
                    while let Ok(frame) = connection.read_frame().await {
                        match frame.kind {
                            DirectLinkFrameKind::OpenLink => {
                                let request = frame.decode_open_link().expect("decode open-link");
                                connection
                                    .write_frame(DirectLinkFrame::open_link_ack(&OpenLinkAck {
                                        link_id: request.link_id.clone(),
                                        source_to_target: NegotiatedDirection {
                                            direction: LinkDirection::SourceToTarget,
                                            stream_name: request.source_to_target.stream_name,
                                            accepted_message_type_ids: request
                                                .source_to_target
                                                .supported_message_type_ids,
                                            next_receive_sequence: LinkSequence(1),
                                            backpressure: request.options.backpressure,
                                            closed: false,
                                        },
                                        target_to_source: None,
                                    }).expect("encode open-link ack"))
                                    .await
                                    .expect("write open-link ack");
                            }
                            DirectLinkFrameKind::Message
                                if received.fetch_add(
                                    1,
                                    std::sync::atomic::Ordering::Relaxed,
                                ) + 1
                                    >= expected_messages =>
                            {
                                let _ = done_tx.try_send(());
                            }
                            _ => {}
                        }
                    }
                });
            }
        }
    }
}

fn open_link_request(link_id: LinkId, endpoint: DirectLinkEndpoint) -> DirectLinkOpenRequest {
    DirectLinkOpenRequest {
        link_id: link_id.clone(),
        source: bench_actor_ref("Gateway", 1),
        target: LinkTarget::Endpoint {
            endpoint,
            target: bench_actor_ref("World", 2),
        },
        mode: DirectLinkMode::Unidirectional,
        source_to_target: bench_stream(),
        target_to_source: None,
        options: DirectLinkOptions::default(),
        trace: TraceContext::default(),
    }
}

fn wire_open_link_request(link_id: LinkId) -> OpenLinkRequest {
    OpenLinkRequest {
        protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
        link_id: link_id.clone(),
        source: bench_actor_ref("Gateway", 1),
        target: bench_actor_ref("World", 2),
        mode: DirectLinkMode::Unidirectional,
        source_to_target: OpenLinkDirection::from_stream(link_id, &bench_stream()),
        target_to_source: None,
        options: DirectLinkOptions::default(),
    }
}

fn bench_actor_ref(kind: &'static str, id: u64) -> ActorRef {
    ActorRef::direct(
        ServiceKind::new(kind),
        ActorKind::new(kind),
        ActorId::U64(id),
        InstanceId::new(format!("{kind}-{id}")),
        "http://127.0.0.1:18080".parse().unwrap(),
        None,
    )
}

fn bench_stream() -> DirectLinkStreamDescriptor {
    DirectLinkStreamDescriptor {
        stream_name: "bench".to_string(),
        messages: vec![DirectLinkMessageDescriptor {
            message_id: DirectLinkMessageId(7),
            proto_full_name: "bench.Payload".to_string(),
            rust_type_name: "Payload".to_string(),
        }],
    }
}

fn striped_link_ids(config: &DirectLinkEndpointPoolConfig, link_count: usize) -> Vec<String> {
    let mut by_stripe = vec![Vec::<String>::new(); config.connections_per_endpoint.get()];
    let mut candidate = 0;
    while by_stripe.iter().map(Vec::len).sum::<usize>() < link_count {
        let link_id = LinkId::new(format!("pooled-{candidate}"));
        by_stripe[config.stripe_index_for_link(&link_id)].push(link_id.to_string());
        candidate += 1;
    }
    by_stripe.into_iter().flatten().take(link_count).collect()
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
