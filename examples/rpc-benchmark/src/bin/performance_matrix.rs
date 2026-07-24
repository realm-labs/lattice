#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::{collections::BTreeSet, error::Error};

use lattice_remoting::association::metrics::AssociationMetricsSnapshot;
use remoting_benchmark::{
    actor_completion::ActorCompletionTopology,
    end_to_end::RemoteActorTopology,
    measurement::{CountingAllocator, ResourceDelta, ResourceSnapshot},
    metrics::WorkloadReport,
    saturation::{SaturationReport, SaturationTopology},
    scaling::{ActorScaleTopology, MailboxContentionReport, ScaleReport},
    suite::PerformanceSuiteConfig,
};
use serde_json::{Value, json};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = PerformanceSuiteConfig::from_env();
    let mut workloads = Vec::new();

    eprintln!("measuring local tell/ask payload and window matrix");
    let local = ActorCompletionTopology::start_timing(config.mailbox_capacity).await?;
    local
        .run_raw(
            config.tell_requests.min(1_000),
            config.primary_payload_bytes(),
        )
        .await?;
    let mut calibration_peaks = Vec::with_capacity(config.calibration_rounds);
    for _ in 0..config.calibration_rounds {
        calibration_peaks.push(
            local
                .run_raw(config.calibration_requests, config.primary_payload_bytes())
                .await?
                .throughput_per_second(),
        );
    }
    calibration_peaks.sort_by(f64::total_cmp);
    let local_peak = calibration_peaks[calibration_peaks.len() / 2];
    for &payload_bytes in &config.payload_bytes {
        let before = ResourceSnapshot::now();
        let report = local.run_raw(config.tell_requests, payload_bytes).await?;
        let resources = ResourceSnapshot::now().delta_since(before);
        workloads.push(raw_tell_value(
            "local_tell_completion",
            payload_bytes,
            &report,
            resources,
        ));

        for &window in &config.ask_windows {
            let before = ResourceSnapshot::now();
            let report = local
                .run_ask(config.ask_requests, payload_bytes, window)
                .await?;
            let resources = ResourceSnapshot::now().delta_since(before);
            workloads.push(workload_value(
                "local_ask",
                payload_bytes,
                Some(window),
                &report,
                resources,
                None,
            ));
        }
    }
    local.shutdown().await?;

    eprintln!("measuring remote tell/ask payload and window matrix");
    let remote = RemoteActorTopology::start(config.bulk_stripes).await?;
    remote
        .run_tell(
            config.tell_requests.min(1_000),
            config.primary_payload_bytes(),
        )
        .await?;
    for &payload_bytes in &config.payload_bytes {
        let transport_before = remote.association_metrics();
        let before = ResourceSnapshot::now();
        let report = remote.run_tell(config.tell_requests, payload_bytes).await?;
        let resources = ResourceSnapshot::now().delta_since(before);
        let transport = transport_delta(remote.association_metrics(), transport_before);
        workloads.push(workload_value(
            "remote_tell_completion",
            payload_bytes,
            None,
            &report,
            resources,
            Some(transport),
        ));

        for &window in &config.ask_windows {
            let transport_before = remote.association_metrics();
            let before = ResourceSnapshot::now();
            let report = remote
                .run_windowed(config.ask_requests, payload_bytes, window)
                .await?;
            let resources = ResourceSnapshot::now().delta_since(before);
            let transport = transport_delta(remote.association_metrics(), transport_before);
            workloads.push(workload_value(
                "remote_ask",
                payload_bytes,
                Some(window),
                &report,
                resources,
                Some(transport),
            ));
        }
    }
    remote.shutdown().await?;

    eprintln!("measuring single-actor mailbox producer contention without capacity pressure");
    let contention_topology =
        ActorScaleTopology::start_contention(config.mailbox_contention_requests).await?;
    contention_topology
        .run_contention(
            config.mailbox_contention_requests.min(1_000),
            config.primary_payload_bytes(),
            1,
            1,
        )
        .await?;
    let mut mailbox_contention = Vec::new();
    for &producer_count in &config.producer_counts {
        let before = ResourceSnapshot::now();
        let report = contention_topology
            .run_contention(
                config.mailbox_contention_requests,
                config.primary_payload_bytes(),
                producer_count,
                config.mailbox_contention_rounds,
            )
            .await?;
        let resources = ResourceSnapshot::now().delta_since(before);
        mailbox_contention.push(contention_value(
            config.primary_payload_bytes(),
            &report,
            resources,
        ));
    }
    contention_topology.shutdown().await?;

    eprintln!("measuring producer/actor scaling matrix");
    let scale_payload = config.primary_payload_bytes();
    let mut scaling = Vec::new();
    for &actor_count in &config.actor_counts {
        let topology = ActorScaleTopology::start(actor_count, config.mailbox_capacity).await?;
        topology
            .run(config.scaling_requests.min(1_000), scale_payload, 1)
            .await?;
        for &producer_count in &config.producer_counts {
            let before = ResourceSnapshot::now();
            let report = topology
                .run(config.scaling_requests, scale_payload, producer_count)
                .await?;
            let resources = ResourceSnapshot::now().delta_since(before);
            scaling.push(scale_value(scale_payload, &report, resources));
        }
        topology.shutdown().await?;
    }

    eprintln!("measuring open-loop saturation curve");
    let saturation_topology = SaturationTopology::start(config.mailbox_capacity).await?;
    let mut saturation_calibration_peaks = Vec::with_capacity(config.calibration_rounds);
    for _ in 0..config.calibration_rounds {
        saturation_calibration_peaks.push(
            saturation_topology
                .calibrate(
                    config.calibration_requests,
                    scale_payload,
                    config.saturation_producers,
                )
                .await?
                .throughput_per_second(),
        );
    }
    saturation_calibration_peaks.sort_by(f64::total_cmp);
    let saturation_peak = saturation_calibration_peaks[saturation_calibration_peaks.len() / 2];
    let mut saturation = Vec::new();
    let mut measured_rates = BTreeSet::new();
    for &fraction in &config.saturation_fractions {
        let target_rate = (saturation_peak * fraction).round().max(1.0) as u64;
        if !measured_rates.insert(target_rate) {
            continue;
        }
        let before = ResourceSnapshot::now();
        let report = saturation_topology
            .run(
                target_rate,
                config.saturation_duration,
                config.saturation_burst_horizon,
                scale_payload,
                config.saturation_sample_every,
                config.saturation_producers,
            )
            .await?;
        let resources = ResourceSnapshot::now().delta_since(before);
        saturation.push(saturation_value(
            fraction,
            scale_payload,
            &report,
            resources,
        ));
    }
    saturation_topology.shutdown().await?;

    let output = json!({
        "schema_version": 1,
        "machine": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "available_parallelism": std::thread::available_parallelism()?.get(),
            "debug_assertions": cfg!(debug_assertions),
        },
        "config": {
            "tell_requests": config.tell_requests,
            "ask_requests": config.ask_requests,
            "scaling_requests": config.scaling_requests,
            "mailbox_contention_requests": config.mailbox_contention_requests,
            "mailbox_contention_rounds": config.mailbox_contention_rounds,
            "calibration_requests": config.calibration_requests,
            "calibration_rounds": config.calibration_rounds,
            "calibration_throughput_samples": calibration_peaks,
            "payload_bytes": config.payload_bytes,
            "ask_windows": config.ask_windows,
            "producer_counts": config.producer_counts,
            "actor_counts": config.actor_counts,
            "mailbox_capacity": config.mailbox_capacity,
            "bulk_stripes": config.bulk_stripes,
            "saturation_duration_nanos": config.saturation_duration.as_nanos(),
            "saturation_fractions": config.saturation_fractions,
            "saturation_sample_every": config.saturation_sample_every,
            "saturation_producers": config.saturation_producers,
            "saturation_burst_horizon_nanos": config.saturation_burst_horizon.as_nanos(),
            "calibrated_local_peak_per_second": local_peak,
            "saturation_calibration_throughput_samples": saturation_calibration_peaks,
            "calibrated_saturation_peak_per_second": saturation_peak,
        },
        "workloads": workloads,
        "mailbox_contention": mailbox_contention,
        "scaling": scaling,
        "saturation": saturation,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn contention_value(
    payload_bytes: usize,
    report: &MailboxContentionReport,
    resources: ResourceDelta,
) -> Value {
    json!({
        "name": "local_mailbox_mpsc_contention",
        "payload_bytes": payload_bytes,
        "requests": report.requests,
        "producer_count": report.producer_count,
        "rounds": report.rounds,
        "admission_elapsed_nanos": report.admission_elapsed.as_nanos(),
        "completion_elapsed_nanos": report.completion_elapsed.as_nanos(),
        "admission_throughput_per_second": report.admission_throughput_per_second(),
        "completion_throughput_per_second": report.completion_throughput_per_second(),
        "resources": resource_value(resources, report.requests),
    })
}

fn workload_value(
    name: &'static str,
    payload_bytes: usize,
    window: Option<usize>,
    report: &WorkloadReport,
    resources: ResourceDelta,
    transport: Option<Value>,
) -> Value {
    json!({
        "name": name,
        "payload_bytes": payload_bytes,
        "window": window,
        "requests": report.requests,
        "successes": report.successes,
        "errors": report.errors,
        "elapsed_nanos": report.elapsed.as_nanos(),
        "throughput_per_second": report.throughput_per_second(),
        "latency_samples": report.latency_sample_count(),
        "latency_p50_nanos": report.percentile_latency(0.50).as_nanos(),
        "latency_p95_nanos": report.percentile_latency(0.95).as_nanos(),
        "latency_p99_nanos": report.percentile_latency(0.99).as_nanos(),
        "latency_p999_nanos": report.percentile_latency(0.999).as_nanos(),
        "resources": resource_value(resources, report.requests),
        "transport": transport,
    })
}

fn raw_tell_value(
    name: &'static str,
    payload_bytes: usize,
    report: &remoting_benchmark::actor_completion::RawActorCompletionReport,
    resources: ResourceDelta,
) -> Value {
    json!({
        "name": name,
        "payload_bytes": payload_bytes,
        "requests": report.requests,
        "successes": report.requests,
        "errors": 0,
        "elapsed_nanos": report.elapsed.as_nanos(),
        "throughput_per_second": report.throughput_per_second(),
        "mailbox_full_retries": report.mailbox_full_retries,
        "resources": resource_value(resources, report.requests),
    })
}

fn scale_value(payload_bytes: usize, report: &ScaleReport, resources: ResourceDelta) -> Value {
    json!({
        "name": "local_tell_scaling",
        "payload_bytes": payload_bytes,
        "requests": report.requests,
        "actor_count": report.actor_count,
        "producer_count": report.producer_count,
        "mailbox_full_retries": report.mailbox_full_retries,
        "elapsed_nanos": report.elapsed.as_nanos(),
        "throughput_per_second": report.throughput_per_second(),
        "resources": resource_value(resources, report.requests),
    })
}

fn saturation_value(
    fraction: f64,
    payload_bytes: usize,
    report: &SaturationReport,
    resources: ResourceDelta,
) -> Value {
    json!({
        "name": "local_tell_open_loop",
        "calibrated_peak_fraction": fraction,
        "payload_bytes": payload_bytes,
        "target_rate_per_second": report.target_rate_per_second,
        "producer_count": report.producer_count,
        "offered": report.offered,
        "admitted": report.admitted,
        "rejected": report.rejected,
        "rejection_ratio": report.rejection_ratio(),
        "offered_per_second": report.offered_per_second(),
        "completed_per_second": report.completed_per_second(),
        "offer_elapsed_nanos": report.offer_elapsed.as_nanos(),
        "completion_elapsed_nanos": report.completion_elapsed.as_nanos(),
        "generator_missed": report.generator_missed,
        "generator_miss_ratio": if report.offered + report.generator_missed == 0 {
            0.0
        } else {
            report.generator_missed as f64
                / (report.offered + report.generator_missed) as f64
        },
        "maximum_schedule_lag_nanos": report.maximum_schedule_lag.as_nanos(),
        "latency_samples": report.sampled_latencies.len(),
        "latency_p50_nanos": report.percentile_latency(0.50).as_nanos(),
        "latency_p95_nanos": report.percentile_latency(0.95).as_nanos(),
        "latency_p99_nanos": report.percentile_latency(0.99).as_nanos(),
        "latency_p999_nanos": report.percentile_latency(0.999).as_nanos(),
        "resources": resource_value(resources, report.offered),
    })
}

fn resource_value(resources: ResourceDelta, operations: usize) -> Value {
    let operations = operations.max(1) as f64;
    let cpu = resources.cpu.map(|cpu| {
        json!({
            "user_nanos": cpu.user.as_nanos(),
            "system_nanos": cpu.system.as_nanos(),
            "total_nanos": cpu.total().as_nanos(),
            "nanos_per_operation": cpu.total().as_nanos() as f64 / operations,
        })
    });
    json!({
        "cpu": cpu,
        "allocations": resources.allocations.allocations,
        "deallocations": resources.allocations.deallocations,
        "reallocations": resources.allocations.reallocations,
        "allocated_bytes": resources.allocations.allocated_bytes,
        "deallocated_bytes": resources.allocations.deallocated_bytes,
        "allocations_per_operation": resources.allocations.allocations as f64 / operations,
        "allocated_bytes_per_operation": resources.allocations.allocated_bytes as f64 / operations,
        "live_bytes_change": resources.allocations.live_bytes_change,
        "process_peak_live_bytes": resources.allocations.peak_live_bytes,
    })
}

fn transport_delta(
    current: AssociationMetricsSnapshot,
    earlier: AssociationMetricsSnapshot,
) -> Value {
    let write_batches = current
        .outbound_write_batches
        .saturating_sub(earlier.outbound_write_batches);
    let written_frames = current
        .outbound_written_frames
        .saturating_sub(earlier.outbound_written_frames);
    let socket_writes = current
        .outbound_socket_writes
        .saturating_sub(earlier.outbound_socket_writes);
    json!({
        "outbound_queue_rejections": current
            .outbound_queue_rejections
            .saturating_sub(earlier.outbound_queue_rejections),
        "association_byte_budget_rejections": current
            .association_byte_budget_rejections
            .saturating_sub(earlier.association_byte_budget_rejections),
        "node_byte_budget_rejections": current
            .node_byte_budget_rejections
            .saturating_sub(earlier.node_byte_budget_rejections),
        "outbound_write_batches": write_batches,
        "outbound_written_frames": written_frames,
        "outbound_socket_writes": socket_writes,
        "average_outbound_batch_frames": if write_batches == 0 {
            0.0
        } else {
            written_frames as f64 / write_batches as f64
        },
        "average_socket_writes_per_batch": if write_batches == 0 {
            0.0
        } else {
            socket_writes as f64 / write_batches as f64
        },
        "exact_target_cache_hits": current
            .exact_target_cache_hits
            .saturating_sub(earlier.exact_target_cache_hits),
        "exact_target_cache_misses": current
            .exact_target_cache_misses
            .saturating_sub(earlier.exact_target_cache_misses),
    })
}
