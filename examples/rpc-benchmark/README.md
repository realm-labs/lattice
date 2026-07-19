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
LATTICE_BENCH_PAYLOAD_BYTES=128
LATTICE_BENCH_BULK_STRIPES=1  # 1..4
```

This benchmark is compared with the captured pooled transport baseline in
`docs/baselines/pre-hard-switch.md`; observed post-switch budgets and allocator/FD measurements are in
`docs/baselines/post-hard-switch.md`. Real TCP/TLS acceptance remains separate so queue admission is
not confused with end-to-end delivery.
