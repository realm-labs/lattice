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
4 tonic channels per endpoint
0-byte single-hop payload
RPC route-correction retry enabled
server request dedup enabled
```

Environment overrides:

```bash
LATTICE_BENCH_NODES=4 \
LATTICE_BENCH_ACTORS=1024 \
LATTICE_BENCH_CONCURRENCY=128 \
LATTICE_BENCH_REQUESTS=50000 \
LATTICE_BENCH_CHANNEL_STRIPES=4 \
LATTICE_BENCH_PAYLOAD_BYTES=0 \
LATTICE_BENCH_RPC_RETRY=true \
LATTICE_BENCH_REQUEST_DEDUP=true \
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

Generated clients use per-endpoint tonic channel striping by default. Set
`LATTICE_BENCH_CHANNEL_STRIPES=1` to compare against the previous
single-channel-per-endpoint behavior.

Set `LATTICE_BENCH_PAYLOAD_BYTES` to a larger value, for example `16384`, to
make request body copy/encode costs visible in the single-hop benchmark.

Set `LATTICE_BENCH_RPC_RETRY=false` to measure the hot path without
route-correction retry buffering, and `LATTICE_BENCH_REQUEST_DEDUP=false` to
measure without server-side request id reservation.

The Criterion benchmark is intentionally single-process multi-node for stable
framework measurements. Use the local multi-process driver below when you need
process-boundary data.

## Local Multi-Process Benchmark

Use the multi-process driver to compare against the in-process Criterion
baseline. It starts multiple `rpc-benchmark-node` child processes on
`127.0.0.1:0`, uses etcd as the shared placement store, then runs the same
single-hop routed RPC workload from the driver process.

Start a disposable local etcd and a coordinator that uses the same explicit
placement namespace as the driver. The dangerous unauthenticated setting is
accepted only for loopback development:

```bash
cargo build -p rpc-benchmark --bins --release
cargo build -p lattice-placement --bin lattice-coordinator --release

export LATTICE_BENCH_PREFIX=/lattice/rpc-benchmark/manual

LATTICE_CLUSTER_PREFIX="$LATTICE_BENCH_PREFIX" \
LATTICE_ETCD_ENDPOINTS=http://127.0.0.1:2379 \
LATTICE_COORDINATOR_ADDR=127.0.0.1:50080 \
LATTICE_DANGEROUSLY_ALLOW_UNAUTHENTICATED_ETCD=true \
target/release/lattice-coordinator &
COORDINATOR_PID=$!

target/release/rpc-benchmark-driver \
  --etcd-endpoints http://127.0.0.1:2379 \
  --coordinator-endpoint http://127.0.0.1:50080 \
  --key-prefix "$LATTICE_BENCH_PREFIX" \
  --nodes 2 \
  --actors 256 \
  --concurrency 64 \
  --requests 10000 \
  --channel-stripes 4

kill "$COORDINATOR_PID"
```

`--key-prefix` is required because the driver, nodes, and coordinator must use
exactly the same placement namespace. The driver does not start or configure a
coordinator implicitly. The current multi-process driver covers the single-hop
`BenchRpc.Ping` path; the in-process Criterion target still covers both
single-hop and chained RPC scenarios.
