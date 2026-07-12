#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

use remoting_benchmark::matrix::{local_actor_admission, placement_matrix};
use remoting_benchmark::{BenchmarkConfig, RemotingTopology};

struct CountingAllocator;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchmarkConfig::from_env();
    let local = local_actor_admission(config.requests).await?;
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
        "matrix": matrix,
    });
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn open_file_descriptors() -> Option<usize> {
    ["/proc/self/fd", "/dev/fd"]
        .into_iter()
        .find_map(|path| std::fs::read_dir(path).ok().map(|entries| entries.count()))
}
