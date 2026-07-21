#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    error::Error,
    hint::black_box,
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use lattice_remoting::{
    transport::FramedWriter,
    wire::{Frame, FrameCodec, FrameKind},
};
use remoting_benchmark::{
    BenchmarkConfig, RemotingTopology,
    actor_completion::ActorCompletionTopology,
    end_to_end::RemoteActorTopology,
    matrix::{local_actor_admission, placement_matrix},
};

struct CountingAllocator;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, PartialEq, prost::Message)]
struct WirePayload {
    #[prost(bytes = "bytes", tag = "1")]
    payload: Bytes,
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = BenchmarkConfig::from_env();
    let frame_write_allocations =
        measure_frame_write(config.requests, config.payload_bytes).await?;
    let local = local_actor_admission(config.requests).await?;
    let raw_completion_topology = ActorCompletionTopology::start_timing(1024).await?;
    let before_raw_allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let before_raw_deallocations = DEALLOCATIONS.load(Ordering::Relaxed);
    let raw_local_completion = raw_completion_topology
        .run_raw(config.requests, config.payload_bytes)
        .await?;
    let raw_completion_allocations = ALLOCATIONS.load(Ordering::Relaxed) - before_raw_allocations;
    let raw_completion_deallocations =
        DEALLOCATIONS.load(Ordering::Relaxed) - before_raw_deallocations;
    raw_completion_topology.shutdown().await?;
    let completion_topology = ActorCompletionTopology::start(1024).await?;
    let local_completion = completion_topology
        .run(config.requests, config.payload_bytes)
        .await?;
    completion_topology.shutdown().await?;
    let remote_actor = RemoteActorTopology::start(config.bulk_stripes).await?;
    let remote_round_trip = remote_actor
        .run(config.round_trip_requests, config.payload_bytes)
        .await?;
    remote_actor.shutdown().await?;
    let topology = RemotingTopology::start(&config)?;
    let before_allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let before_deallocations = DEALLOCATIONS.load(Ordering::Relaxed);
    let before_fds = open_file_descriptors();
    let report = topology
        .run_bulk_tell(config.requests, config.payload_bytes)
        .await?;
    let remote_allocations = ALLOCATIONS.load(Ordering::Relaxed) - before_allocations;
    let remote_deallocations = DEALLOCATIONS.load(Ordering::Relaxed) - before_deallocations;
    let after_remote_fds = open_file_descriptors();
    topology.shutdown().await;
    let mut matrix = vec![serde_json::json!({
        "name": local.name,
        "operations": local.operations,
        "elapsed_nanos": local.elapsed.as_nanos(),
        "throughput_per_second": local.throughput_per_second(),
    })];
    matrix.push(serde_json::json!({
        "name": report.name,
        "operations": report.requests,
        "elapsed_nanos": report.elapsed.as_nanos(),
        "throughput_per_second": report.throughput_per_second(),
    }));
    matrix.extend(
        placement_matrix(config.requests, config.payload_bytes)?
            .into_iter()
            .map(|measurement| {
                serde_json::json!({
                    "name": measurement.name,
                    "operations": measurement.operations,
                    "elapsed_nanos": measurement.elapsed.as_nanos(),
                    "throughput_per_second": measurement.throughput_per_second(),
                })
            }),
    );
    let result = serde_json::json!({
        "requests": report.requests,
        "payload_bytes": config.payload_bytes,
        "bulk_stripes": config.bulk_stripes,
        "throughput_per_second": report.throughput_per_second(),
        "elapsed_nanos": report.elapsed.as_nanos(),
        "allocations": remote_allocations,
        "deallocations": remote_deallocations,
        "open_fds_before": before_fds,
        "open_fds_after": after_remote_fds,
        "physical_connections": 2 + config.bulk_stripes,
        "frame_write_allocations": frame_write_allocations,
        "local_actor_completion": {
            "raw_throughput_per_second": raw_local_completion.throughput_per_second(),
            "raw_elapsed_nanos": raw_local_completion.elapsed.as_nanos(),
            "raw_mailbox_full_retries": raw_local_completion.mailbox_full_retries,
            "raw_allocations": raw_completion_allocations,
            "raw_deallocations": raw_completion_deallocations,
            "throughput_per_second": local_completion.workload.throughput_per_second(),
            "elapsed_nanos": local_completion.workload.elapsed.as_nanos(),
            "latency_p50_nanos": local_completion.workload.percentile_latency(0.50).as_nanos(),
            "latency_p95_nanos": local_completion.workload.percentile_latency(0.95).as_nanos(),
            "latency_p99_nanos": local_completion.workload.percentile_latency(0.99).as_nanos(),
            "processing_p99_nanos": local_completion.processing_percentile(0.99).as_nanos(),
            "mailbox_full_retries": local_completion.mailbox_full_retries,
            "maximum_queue_depth": local_completion.maximum_queue_depth,
        },
        "remote_actor_tcp_round_trip": {
            "requests": remote_round_trip.requests,
            "throughput_per_second": remote_round_trip.throughput_per_second(),
            "elapsed_nanos": remote_round_trip.elapsed.as_nanos(),
            "latency_p50_nanos": remote_round_trip.percentile_latency(0.50).as_nanos(),
            "latency_p95_nanos": remote_round_trip.percentile_latency(0.95).as_nanos(),
            "latency_p99_nanos": remote_round_trip.percentile_latency(0.99).as_nanos(),
        },
        "matrix": matrix,
    });
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn measure_frame_write(
    requests: usize,
    payload_bytes: usize,
) -> Result<serde_json::Value, Box<dyn Error>> {
    let codec = FrameCodec::new(256 * 1024)?;
    let frame = Frame::encode_message(
        FrameKind::Tell,
        &WirePayload {
            payload: Bytes::from(vec![0_u8; payload_bytes]),
        },
    );
    let (vectored_allocations, vectored_deallocations) =
        measure_writer_allocations(codec.clone(), &frame, requests).await?;
    let before_allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let before_deallocations = DEALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..requests {
        black_box(codec.encode(black_box(&frame))?);
    }
    let coalescing_allocations = ALLOCATIONS.load(Ordering::Relaxed) - before_allocations;
    let coalescing_deallocations = DEALLOCATIONS.load(Ordering::Relaxed) - before_deallocations;
    Ok(serde_json::json!({
        "vectored_writer": {
            "allocations": vectored_allocations,
            "deallocations": vectored_deallocations,
        },
        "coalescing_codec": {
            "allocations": coalescing_allocations,
            "deallocations": coalescing_deallocations,
        },
    }))
}

async fn measure_writer_allocations(
    codec: FrameCodec,
    frame: &Frame,
    requests: usize,
) -> Result<(u64, u64), Box<dyn Error>> {
    let mut writer = FramedWriter::new(tokio::io::sink(), codec);
    let before_allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let before_deallocations = DEALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..requests {
        black_box(writer.write_frame(black_box(frame)).await?);
    }
    Ok((
        ALLOCATIONS.load(Ordering::Relaxed) - before_allocations,
        DEALLOCATIONS.load(Ordering::Relaxed) - before_deallocations,
    ))
}

fn open_file_descriptors() -> Option<usize> {
    ["/proc/self/fd", "/dev/fd"]
        .into_iter()
        .find_map(|path| std::fs::read_dir(path).ok().map(|entries| entries.count()))
}
