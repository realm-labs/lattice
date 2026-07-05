# RPC Benchmark

`rpc-benchmark` measures lattice internal RPC across multiple in-process
`LatticeService` nodes. It is intended as a stable baseline for framework RPC
overhead, placement routing, generated clients, actor activation, and actor
handler dispatch.

Run:

```bash
cargo bench -p rpc-benchmark --bench rpc_benchmark
```

Default topology:

```text
2 Bench nodes
2 Chain nodes
2 Worker nodes
256 actors
64 concurrent requests
10,000 requests per Criterion iteration
```

Environment overrides:

```bash
LATTICE_BENCH_NODES=4 \
LATTICE_BENCH_ACTORS=1024 \
LATTICE_BENCH_CONCURRENCY=128 \
LATTICE_BENCH_REQUESTS=50000 \
cargo bench -p rpc-benchmark --bench rpc_benchmark
```

Benchmarks:

```text
routed_rpc_fanout_warm_cache
cross_service_chain_warm_cache
```

Each run prints throughput, average latency, p50, p95, p99, error count, and
the number of actors observed in replies. The topology warms actor activation
and route caches before Criterion measures the workload.

This benchmark is intentionally single-process multi-node. A true multi-process
driver can be added later after this baseline is stable.
