# Remoting Benchmark

The `remoting-benchmark` package measures local actor admission, concrete remote bulk-tell admission,
stable and unknown shard routing, allocation evaluation, rebalance planning, full handoff reduction,
and reliable-control reconnect replay. The remoting topology attaches control, interactive, and
configurable bulk lanes, installs a bounded protocol catalogue, uses stable sender/target striping,
and continuously drains admitted frames while maintaining the Association byte budget.

Criterion records both paths: `prepared_bulk_tell_admission` binds the stable Association, exact
target, sender, protocol decision, and bulk stripe once before the timed loop;
`unprepared_bulk_tell_admission` exercises the one-shot convenience API. The allocator-instrumented
`measure` binary uses the prepared route and excludes route construction from its measurement window.
Admission workloads intentionally publish no latency samples because queue admission is not message
delivery.

`actor_completion/raw_bounded_mailbox` uses one batch-tail barrier to wait until every tell has run
through the local Actor handler. It avoids per-message completion channels and benchmark timestamps,
so it is the runtime throughput baseline. `actor_completion/per_message_latency` retains those
per-message measurements to quantify their cost. The `measure` binary additionally runs an
observer-enabled workload and records end-to-end p50/p95/p99, Actor-observed queue and processing
percentiles, peak queue depth, and mailbox-full retries.

`remote_actor_end_to_end/tcp_ask_round_trip` crosses a real loopback TCP Association, decodes and
dispatches the request into a remote Actor, encodes its reply, and returns it through the client
Endpoint. Criterion measures the uninstrumented sequential round-trip path; `measure` records its
p50/p95/p99 latency distribution. This is separate from bulk-tell admission and therefore should not
be compared as if the two workloads had the same completion boundary.
`remoting_frame_write` compares the production stack-header/vectored-write path with the contiguous
coalescing codec fallback. Its sink target isolates framing overhead; it is not a network-latency
benchmark. The `measure` output also records allocator calls for both write paths.

Run:

```bash
cargo bench -p remoting-benchmark --bench remoting_benchmark
cargo run --release -p remoting-benchmark --bin measure
cargo run --release -p remoting-benchmark --bin performance_matrix
```

Configuration:

```text
LATTICE_BENCH_REQUESTS=10000
LATTICE_BENCH_ROUND_TRIPS=1000
LATTICE_BENCH_PAYLOAD_BYTES=128
LATTICE_BENCH_BULK_STRIPES=1  # 1..4
```

## Performance matrix

`performance_matrix` emits one machine-readable JSON document covering five distinct questions:

- local and loopback-TCP tell/ask completion across payload sizes;
- local and remote ask throughput and latency across in-flight windows;
- single-Actor MPSC admission contention without mailbox capacity pressure;
- local tell scaling across producer and Actor counts;
- local tell behavior at fixed offered rates around the calibrated completion peak.

Tell throughput ends when the destination Actor processes a trailing barrier/marker. Ask throughput
ends when every reply is received. Neither is an admission-only measurement. Per-request ask
latencies and sampled saturation latencies include queueing and handler completion, and report
p50/p95/p99/p99.9. The saturation producer is open-loop: mailbox-full attempts are counted as
rejections instead of being retried. It runs on dedicated producer threads and reports skipped
scheduled arrivals plus maximum schedule lag, so an underpowered load generator is visible instead
of silently turning delayed arrivals into an unbounded burst. Catch-up bursts are bounded by both the
configured burst horizon and one quarter of the mailbox capacity.

The MPSC contention workload is the deliberate exception to the completion-only rule: its mailbox
can hold the entire round, so it never exercises mailbox-full retry behavior. It records both the
time for all producers to finish admission and the time for the Actor to process a trailing barrier.
This separates shared-queue contention from bounded-mailbox backpressure and scheduler yielding.

Every row also records process user/system CPU time, allocation/deallocation/reallocation calls,
allocated/deallocated bytes, and per-operation CPU/allocation costs. Process CPU includes all runtime,
client, server, and transport tasks active during the row. Run the binary in release mode; the output
records whether debug assertions were enabled to prevent accidental debug/release comparisons.
The JSON keeps two peak calibrations: the normal local tell completion path and the dedicated-producer
path used by the saturation test. Fixed offered rates are derived from the latter, so producer
contention and pacing cost are not hidden behind a faster, structurally different calibration.

Matrix configuration:

```text
LATTICE_BENCH_TELL_REQUESTS=100000
LATTICE_BENCH_ASK_REQUESTS=10000
LATTICE_BENCH_SCALING_REQUESTS=1000000
LATTICE_BENCH_CONTENTION_REQUESTS=250000
LATTICE_BENCH_CONTENTION_ROUNDS=3
LATTICE_BENCH_CALIBRATION_REQUESTS=1000000
LATTICE_BENCH_CALIBRATION_ROUNDS=3
LATTICE_BENCH_PAYLOAD_MATRIX=0,128,1024,16384
LATTICE_BENCH_ASK_WINDOWS=1,64,256
LATTICE_BENCH_PRODUCERS=1,4,16
LATTICE_BENCH_ACTORS=1,16,256
LATTICE_BENCH_MAILBOX_CAPACITY=1024
LATTICE_BENCH_SATURATION_MILLIS=10000
LATTICE_BENCH_SATURATION_FRACTIONS=0.25,0.5,0.75,0.9,1.0,1.1
LATTICE_BENCH_SATURATION_SAMPLE_EVERY=1024
LATTICE_BENCH_SATURATION_BURST_MICROS=1000
LATTICE_BENCH_SATURATION_PRODUCERS=4
```

For a longer multicore scaling capture, increase `LATTICE_BENCH_SCALING_REQUESTS` and include `4096`
in `LATTICE_BENCH_ACTORS`. Producer/Actor scaling is intentionally kept separate from the
per-message latency path so timestamp collection does not determine its throughput.

The legacy pooled transport snapshot is retained in `docs/baselines/pre-hard-switch.md`. Current
completion, scaling, saturation, allocator, and persistence results are maintained in
`docs/baselines/current-performance.md`. Real TCP/TLS acceptance remains separate so queue admission
is not confused with end-to-end delivery.
