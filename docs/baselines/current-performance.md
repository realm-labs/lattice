# Current Performance Baseline

This document is the current reproducible performance baseline for Lattice. It records measurement
boundaries, commands, workload configuration, results, and operating guidance. It is not a
chronological optimization log.

Last captured on 2026-07-23.

## Measurement Boundaries

The results below use several deliberately separate boundaries:

- **Local tell completion** ends only after the target Actor processes the trailing barrier. It is
  not mailbox admission throughput.
- **Ask completion** ends after every reply is received.
- **Remote tell completion** ends after the remote Actor processes the trailing marker over plain
  loopback TCP.
- **Admission** benchmarks stop after accepting work into a bounded queue. They are useful for
  profiling individual hot paths but must not be compared with completion throughput.
- **CPU time** is process user plus system CPU, including the generator, runtime, client, and server.
- **Allocation bytes** measure allocation traffic, not retained memory.
- **Saturation** uses the exact four-producer topology used for its closed-loop calibration.
- Criterion is the timing baseline. The counting allocator is used only for allocation and file
  descriptor evidence because it changes timing.

Remote results in this document are single-host loopback measurements without TLS. They do not claim
two-host network capacity or production tail latency.

## Reproduction

### Capture Environment

| Property | Value |
|---|---|
| Host | Apple M1 Max |
| Operating system | macOS 26.5.2, build 25F84 |
| Architecture | aarch64 |
| Logical CPUs available | 10 |
| Rust | rustc 1.97.1 (8bab26f4f 2026-07-14) |
| Cargo | 1.97.1 |
| Build profile | release |

Unless a table says otherwise, the Actor/remoting workloads use a bounded mailbox capacity of 1,024,
one bulk stripe, and a 128-byte payload.

### Commands

```text
cargo bench -p remoting-benchmark --bench remoting_benchmark
cargo run --release -p remoting-benchmark --bin measure
cargo run --release -p remoting-benchmark --bin performance_matrix
cargo bench -p lattice-store-mongodb --bench persistence
```

The default performance matrix uses:

- 100,000 tell messages and 10,000 ask messages;
- 1,000,000 messages for producer/Actor scaling;
- payload sizes of 0, 128, 1,024, and 16,384 bytes;
- ask windows of 1, 64, and 256;
- producer counts of 1, 4, and 16;
- Actor counts of 1, 16, and 256;
- ten seconds per saturation point;
- three 1,000,000-message closed-loop saturation calibration samples.

Environment overrides are documented in
[`examples/rpc-benchmark/README.md`](../../examples/rpc-benchmark/README.md).

## Actor and Remoting Results

### 128-Byte Completion Matrix

| Workload | Window | Throughput | Latency p50 / p99 / p99.9 | CPU ns/op | Allocations/op | Allocated bytes/op |
|---|---:|---:|---:|---:|---:|---:|
| Local tell completion | — | 8.065M/s | not sampled | 248 | ~0 | ~0 |
| Local ask | 1 | 152.4K/s | 5.4 / 12.9 / 33.2 us | 7,100 | 5.00 | 1,024 |
| Local ask | 64 | 678.9K/s | 53.8 / 89.7 / 97.5 us | 2,942 | 5.00 | 1,024 |
| Local ask | 256 | 700.2K/s | 205.7 / 320.0 / 381.7 us | 2,847 | 5.00 | 1,024 |
| Remote tell completion | — | 2.619M/s | not sampled | 872 | 1.02 | 353 |
| Remote ask | 1 | 12.11K/s | 79.5 / 133.5 / 176.3 us | 65,736 | 16.00 | 3,821 |
| Remote ask | 64 | 215.1K/s | 273.2 / 393.0 / 445.3 us | 9,844 | 12.36 | 9,128 |
| Remote ask | 256 | 258.5K/s | 875.8 / 1,339.3 / 2,063.7 us | 11,021 | 12.16 | 5,923 |

Increasing the ask window from 64 to 256 improves local throughput by 3.1% and remote throughput by
20.2%, while increasing p99 latency by roughly 3.6x and 3.4x respectively. A window of 64 is the
balanced default for throughput-sensitive workloads that still care about tail latency.

The interactive lane polls concurrent inbound asks directly instead of creating one Tokio task per
request, and outbound asks share one deadline driver. A single small frame is copied out of an
oversized socket read-ahead slab so a short-lived payload cannot pin 64 KiB of backing capacity. The
copy is deliberately limited to frames no larger than half of the available slab; batched frames
remain zero-copy.

### Remote Tell Payload Curve

Every row completed all 100,000 messages. A queue or byte-budget wait records the transition from
immediate admission to event-driven capacity waiting; it is not a dropped message or a busy retry.

| Payload | Throughput | CPU ns/op | Allocated bytes/op | Queue waits | Byte-budget waits | Frames/batch |
|---:|---:|---:|---:|---:|---:|---:|
| 0 B | 2.933M/s | 696 | 70 | 172 | 0 | 255.10 |
| 128 B | 2.619M/s | 872 | 353 | 107 | 0 | 255.75 |
| 1 KiB | 1.195M/s | 1,716 | 2,423 | 347 | 0 | 255.10 |
| 16 KiB | 121.7K/s | 13,358 | 40,237 | 0 | 387 | 253.81 |

The event-driven byte-budget path parks a sender instead of spinning when a large frame exhausts
capacity. The remaining large-payload cost is primarily data copying, socket work, and remote
dispatch rather than sender-side budget retries.

### Producer and Actor Scaling

The following results use 1,000,000 local tell completions per cell.

| Actors | 1 producer | 4 producers | 16 producers |
|---:|---:|---:|---:|
| 1 | 8.899M/s | 3.384M/s | 1.577M/s |
| 16 | 3.781M/s | 4.272M/s | 4.062M/s |
| 256 | 2.258M/s | 4.210M/s | 2.897M/s |

A single hot Actor peaks with one producer. Distributed work reaches approximately 4.2M/s with four
producers. Sixteen producers add enough contention to reduce throughput, especially when they target
one Actor.

### Sustained Saturation

The closed-loop four-producer calibration samples were 2.870M/s, 2.984M/s, and 3.010M/s. The median
2.984M/s result defines the percentages below. Generator misses report work that could not be offered
on schedule, so offered throughput must be read together with completion throughput.

| Target | Target rate | Offered | Completed | Rejected | Schedule miss | p99 / p99.9 |
|---:|---:|---:|---:|---:|---:|---:|
| 25% | 746K/s | 744K/s | 744K/s | 0.04% | 0.21% | 64 / 754 us |
| 50% | 1.492M/s | 1.490M/s | 1.489M/s | 0.07% | 0.12% | 60 / 523 us |
| 75% | 2.238M/s | 2.223M/s | 2.216M/s | 0.33% | 0.63% | 104 / 569 us |
| 90% | 2.685M/s | 2.673M/s | 2.669M/s | 0.15% | 0.45% | 199 / 530 us |
| 100% | 2.984M/s | 2.922M/s | 2.845M/s | 2.64% | 2.07% | 440 / 1,341 us |
| 110% | 3.282M/s | 3.054M/s | 2.850M/s | 6.67% | 6.96% | 491 / 679 us |

The saturation knee lies between 90% and 100% of closed-loop peak. The 2.984M/s calibration is not a
safe sustained operating target: rejection, schedule miss, and tail latency rise sharply at 100%.

## Cross-Framework Reference

These captures use the dedicated comparison harness and are reference data, not the Lattice
regression gate. They use the same payload and completion boundary within each comparison, but the
historical JVM captures were not rerun in the same final low-load interval as the current matrix.

### Local Tell

The workload uses one producer, one consumer, 500,000 messages per round, and a 128-byte payload.
Lattice uses its bounded 1,024-entry mailbox and retries a returned message when full.

| Framework | Completion throughput |
|---|---:|
| Lattice | 8.675M/s |
| Akka 2.6.21 | 8.850M/s |

Lattice is 2.0% below Akka in this exact single-producer/single-consumer topology.

### Loopback Remote Tell

| Framework | Completion throughput |
|---|---:|
| Lattice | 2.299M/s median |
| Akka 2.6.21 | 505.5K/s |
| Pekko 1.4.0 | 521.5K/s |

The three Lattice captures were 2.299M/s, 2.297M/s, and 2.299M/s. This is 4.55x the Akka result and
4.41x the Pekko result for this loopback harness. It does not establish a corresponding advantage
under TLS, cross-host networking, packet loss, or multi-node contention.

## MongoDB Persistence Results

The persistence benchmark uses an in-memory acknowledger and therefore isolates coordinator,
preparation, diff, and completion work. It excludes MongoDB server execution and network I/O. Each
document mutates one scalar and one entry in a two-entry map.

| Documents | Pipeline | Pipeline throughput | Drain | Drain throughput |
|---:|---:|---:|---:|---:|
| 1 | 4.265–4.285 us | 233–234K docs/s | 4.347–4.370 us | 229–230K docs/s |
| 100 | 417.31–418.25 us | 239–240K docs/s | 423.79–446.48 us | 224–236K docs/s |
| 1,000 | 5.046–5.213 ms | 192–198K docs/s | 4.979–5.368 ms | 186–201K docs/s |

These numbers must not be used as a real database throughput claim. Larger fields, partial failures,
multi-pass drains, indexes, write concern, and server/network behavior require separate acceptance
measurements.

## Regression Guidance

- Compare completion workloads only with the same completion boundary, payload, concurrency, and
  topology.
- Treat admission benchmarks as component profiles, never as delivered-message throughput.
- Keep timing and counting-allocator captures separate.
- Investigate local tell regressions against the 128-byte completion matrix and scaling table.
- Investigate remote regressions across both the 128-byte completion result and payload curve.
- Use saturation results to detect a changed knee; do not gate solely on peak closed-loop throughput.
- Refresh this document when the harness, runtime behavior, capture host, or measurement boundary
  changes materially.

## Historical Context

The legacy transport baseline before the hard switch remains in
[`pre-hard-switch.md`](pre-hard-switch.md). Intermediate optimization captures previously stored in
the post-hard-switch log remain available in Git history; they are intentionally excluded here so
obsolete measurement boundaries and current release evidence cannot be confused.
