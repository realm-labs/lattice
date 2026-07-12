# Remoting Benchmark

The `remoting-benchmark` package measures bounded bulk-tell admission through one exact active
Association. The topology attaches control, interactive, and configurable bulk lanes, installs a
bounded protocol catalogue, uses stable sender/target striping, and continuously drains admitted
frames while maintaining the Association byte budget.

Run:

```bash
cargo bench -p remoting-benchmark --bench remoting_benchmark
```

Configuration:

```text
LATTICE_BENCH_REQUESTS=10000
LATTICE_BENCH_PAYLOAD_BYTES=128
LATTICE_BENCH_BULK_STRIPES=1  # 1..4
```

This benchmark is compared with the captured pooled transport baseline in
`docs/baselines/pre-hard-switch.md`. Real TCP/TLS, reconnect, allocation, handoff, and resource
measurements are separate acceptance benchmarks so queue admission is not confused with end-to-end
delivery.
