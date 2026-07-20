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

`actor_completion/bounded_mailbox` waits until every tell has run through the local Actor handler,
so its throughput includes mailbox scheduling and handler completion. The `measure` binary additionally
records end-to-end p50/p95/p99, Actor-observed queue and processing percentiles, peak queue depth, and
mailbox-full retries. Criterion disables the detailed observer to keep the timing baseline free from
per-message metric collection overhead.

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
```

Configuration:

```text
LATTICE_BENCH_REQUESTS=10000
LATTICE_BENCH_ROUND_TRIPS=1000
LATTICE_BENCH_PAYLOAD_BYTES=128
LATTICE_BENCH_BULK_STRIPES=1  # 1..4
```

This benchmark is compared with the captured pooled transport baseline in
`docs/baselines/pre-hard-switch.md`; observed post-switch budgets and allocator/FD measurements are in
`docs/baselines/post-hard-switch.md`. Real TCP/TLS acceptance remains separate so queue admission is
not confused with end-to-end delivery.
