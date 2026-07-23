#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::{error::Error, hint::black_box};

use bytes::Bytes;
use lattice_remoting::{
    association::metrics::AssociationMetricsSnapshot,
    transport::FramedWriter,
    wire::{Frame, FrameCodec, FrameKind},
};
use remoting_benchmark::{
    BenchmarkConfig, RemotingTopology,
    actor_completion::ActorCompletionTopology,
    end_to_end::RemoteActorTopology,
    matrix::{local_actor_admission, placement_matrix},
    measurement::{AllocationDelta, AllocationSnapshot, CountingAllocator},
};

#[derive(Clone, PartialEq, prost::Message)]
struct WirePayload {
    #[prost(bytes = "bytes", tag = "1")]
    payload: Bytes,
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
    let before_raw_allocations = AllocationSnapshot::now();
    let raw_local_completion = raw_completion_topology
        .run_raw(config.requests, config.payload_bytes)
        .await?;
    let raw_completion_allocations = AllocationSnapshot::now().delta_since(before_raw_allocations);
    raw_completion_topology.shutdown().await?;
    let completion_topology = ActorCompletionTopology::start(1024).await?;
    let local_completion = completion_topology
        .run(config.requests, config.payload_bytes)
        .await?;
    completion_topology.shutdown().await?;
    let remote_actor = RemoteActorTopology::start(config.bulk_stripes).await?;
    let remote_tell = remote_actor
        .run_tell(config.requests, config.payload_bytes)
        .await?;
    let remote_tell_sender_transport = remote_actor.association_metrics();
    let remote_tell_receiver_transport = remote_actor.inbound_association_metrics();
    let remote_round_trip = remote_actor
        .run(config.round_trip_requests, config.payload_bytes)
        .await?;
    let remote_actor_sender_transport = remote_actor.association_metrics();
    let remote_actor_receiver_transport = remote_actor.inbound_association_metrics();
    remote_actor.shutdown().await?;
    let topology = RemotingTopology::start(&config)?;
    let before_allocations = AllocationSnapshot::now();
    let before_fds = open_file_descriptors();
    let report = topology
        .run_bulk_tell(config.requests, config.payload_bytes)
        .await?;
    let remote_allocations = AllocationSnapshot::now().delta_since(before_allocations);
    let after_remote_fds = open_file_descriptors();
    let remoting_transport = topology.association_metrics();
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
        "allocations": remote_allocations.allocations,
        "deallocations": remote_allocations.deallocations,
        "allocation_metrics": allocation_delta_value(remote_allocations),
        "open_fds_before": before_fds,
        "open_fds_after": after_remote_fds,
        "physical_connections": 2 + config.bulk_stripes,
        "frame_write_allocations": frame_write_allocations,
        "association_transport": association_metrics_value(remoting_transport),
        "local_actor_completion": {
            "raw_throughput_per_second": raw_local_completion.throughput_per_second(),
            "raw_elapsed_nanos": raw_local_completion.elapsed.as_nanos(),
            "raw_mailbox_full_retries": raw_local_completion.mailbox_full_retries,
            "raw_allocations": raw_completion_allocations.allocations,
            "raw_deallocations": raw_completion_allocations.deallocations,
            "raw_allocation_metrics": allocation_delta_value(raw_completion_allocations),
            "throughput_per_second": local_completion.workload.throughput_per_second(),
            "elapsed_nanos": local_completion.workload.elapsed.as_nanos(),
            "latency_p50_nanos": local_completion.workload.percentile_latency(0.50).as_nanos(),
            "latency_p95_nanos": local_completion.workload.percentile_latency(0.95).as_nanos(),
            "latency_p99_nanos": local_completion.workload.percentile_latency(0.99).as_nanos(),
            "latency_p999_nanos": local_completion.workload.percentile_latency(0.999).as_nanos(),
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
            "latency_p999_nanos": remote_round_trip.percentile_latency(0.999).as_nanos(),
            "sender_association_transport": association_metrics_value(remote_actor_sender_transport),
            "receiver_association_transport": association_metrics_value(remote_actor_receiver_transport),
        },
        "remote_actor_tcp_tell": {
            "requests": remote_tell.requests,
            "throughput_per_second": remote_tell.throughput_per_second(),
            "elapsed_nanos": remote_tell.elapsed.as_nanos(),
            "sender_association_transport": association_metrics_value(remote_tell_sender_transport),
            "receiver_association_transport": association_metrics_value(remote_tell_receiver_transport),
        },
        "matrix": matrix,
    });
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn association_metrics_value(metrics: AssociationMetricsSnapshot) -> serde_json::Value {
    serde_json::json!({
        "outbound_queue_rejections": metrics.outbound_queue_rejections,
        "association_byte_budget_rejections": metrics.association_byte_budget_rejections,
        "node_byte_budget_rejections": metrics.node_byte_budget_rejections,
        "outbound_write_batches": metrics.outbound_write_batches,
        "outbound_written_frames": metrics.outbound_written_frames,
        "outbound_socket_writes": metrics.outbound_socket_writes,
        "average_outbound_batch_frames": metrics.average_outbound_batch_frames(),
        "average_socket_writes_per_batch": metrics.average_socket_writes_per_batch(),
        "exact_target_cache_hits": metrics.exact_target_cache_hits,
        "exact_target_cache_misses": metrics.exact_target_cache_misses,
    })
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
    let vectored_allocations = measure_writer_allocations(codec.clone(), &frame, requests).await?;
    let before_allocations = AllocationSnapshot::now();
    for _ in 0..requests {
        black_box(codec.encode(black_box(&frame))?);
    }
    let coalescing_allocations = AllocationSnapshot::now().delta_since(before_allocations);
    Ok(serde_json::json!({
        "vectored_writer": allocation_delta_value(vectored_allocations),
        "coalescing_codec": allocation_delta_value(coalescing_allocations),
    }))
}

async fn measure_writer_allocations(
    codec: FrameCodec,
    frame: &Frame,
    requests: usize,
) -> Result<AllocationDelta, Box<dyn Error>> {
    let mut writer = FramedWriter::new(tokio::io::sink(), codec);
    let before_allocations = AllocationSnapshot::now();
    for _ in 0..requests {
        black_box(writer.write_frame(black_box(frame)).await?);
    }
    Ok(AllocationSnapshot::now().delta_since(before_allocations))
}

fn allocation_delta_value(delta: AllocationDelta) -> serde_json::Value {
    serde_json::json!({
        "allocations": delta.allocations,
        "deallocations": delta.deallocations,
        "reallocations": delta.reallocations,
        "allocated_bytes": delta.allocated_bytes,
        "deallocated_bytes": delta.deallocated_bytes,
        "live_bytes_change": delta.live_bytes_change,
        "process_peak_live_bytes": delta.peak_live_bytes,
    })
}

fn open_file_descriptors() -> Option<usize> {
    ["/proc/self/fd", "/dev/fd"]
        .into_iter()
        .find_map(|path| std::fs::read_dir(path).ok().map(|entries| entries.count()))
}
