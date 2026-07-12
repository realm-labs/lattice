# Pre-Hard-Switch Baseline

Captured on 2026-07-12 before macro batch 1 from commit
`d66f56831de832fab71d7357476b9b237cbba5d5` on `main`.

## Environment

```text
OS: Darwin 25.5.0 arm64
CPU: Apple M1 Max
Memory: 32 GiB
rustc: 1.97.0 (2d8144b78 2026-07-07)
cargo: 1.97.0 (c980f4866 2026-06-30)
```

## Workspace Verification

`cargo test --workspace --all-targets` passed. This included the legacy Direct Link
benchmark smoke run and the RPC benchmark smoke run. Eight real-etcd tests were ignored
because no external endpoint was configured; all other tests passed.

The RPC benchmark smoke results were:

| Workload | Throughput | Average | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| Routed RPC fanout, 10,000 requests | 15,283.35/s | 4.088 ms | 3.817 ms | 6.532 ms | 14.195 ms |
| Cross-service chain, 10,000 requests | 8,776.23/s | 7.198 ms | 6.676 ms | 9.883 ms | 29.068 ms |

## Direct Link Benchmark

Command:

```text
cargo bench -p lattice-direct-link --bench direct_link_benchmark -- --save-baseline pre-hard-switch
```

Criterion samples remain under `target/criterion/**/pre-hard-switch` in the capture
worktree. The benchmark configuration used four pooled TCP connections, 64 concurrent
senders, a 128-byte steady-state payload, and either 16 or 64 logical links.

| Case | 95% estimate interval |
|---|---:|
| TCP loopback, 128 B | 16.683-24.028 us/message |
| TCP loopback, 4 KiB | 18.946-21.238 us/message |
| Independent transports, 128 B | 19.415-33.111 us/message |
| Independent transports, 4 KiB | 14.777-16.973 us/message |
| Frame codec round trip, 128 B | 109.12-109.41 ns |
| Frame codec round trip, 4 KiB | 229.58-230.64 ns |
| Frame codec round trip, 64 KiB | 3.2248-3.2967 us |
| Pooled striped steady state, 16 links | 5.8158-6.1901 us/message |
| Pooled striped steady state, 64 links | 6.6597-7.1589 us/message |

Backpressure enqueue intervals for 2,048 attempted entries were 2.9442-2.9634 us
(`block`), 2.6359-2.6428 us (`fail_fast`), 4.4991-4.5383 us (`drop_newest`),
6.6401-6.6575 us (`drop_oldest`), 4.5304-4.6125 us (`coalesce`), and
3.6319-3.6769 us (`disconnect`).

The legacy benchmark did not instrument allocator calls or sample process file
descriptors. Its connection count is fixed by the four-stripe configuration, but no
honest allocation or observed-FD numeric baseline exists. The remoting benchmark must
add those measurements and retain this absence explicitly when defining its regression
budget.

## Legacy Surface Inventory

Before deletion, the workspace contained:

- 64 files referring to Direct Link names or APIs;
- 96 files referring to lattice RPC or tonic;
- 39 files referring to per-actor placement records, epoch floors, or activation locks;
- 152 public declarations or re-exports across the legacy Direct Link and RPC source trees.

The old storage generation used these incompatible namespaces below the configured
placement prefix:

```text
/logic/instances/
/logic/actors/
/logic/virtual_shards/
/logic/singletons/
/logic/epoch_floors/actors/
/logic/epoch_floors/virtual_shards/
/logic/epoch_floors/singletons/
/logic/activation_locks/
/logic/singleton_locks/
/coordinator/leader
```

## Permitted Hard-Switch Break Set

Macro batch 1 may intentionally break all of the following without adapters, dual
writes, fallbacks, or production feature flags:

- crate graph: remove `lattice-rpc` and `lattice-direct-link`; add internal
  `lattice-remoting`;
- public API: remove generated tonic clients/services, RPC wrappers, endpoint routing,
  Direct Link managers/sessions/streams/open calls, `Linked<M>`, and link lifecycle
  messages;
- actor identity: replace endpoint/owner/epoch references with cluster, exact node
  incarnation, canonical actor path, activation ID, and protocol ID;
- wire protocol: reject JSON OpenLink and all old gRPC services; accept only the new
  binary Association generation;
- storage: remove per-actor placement records, actor epoch floors, actor activation
  locks, per-actor tombstones, dynamic singleton scopes, and old virtual-shard records;
- service assembly and deployment: replace tonic listeners/channels and manual link
  endpoints with one remoting listener and bounded Association manager;
- generated code, examples, tests, and benchmarks: replace RPC/Direct Link declarations
  and call sites with actor protocols and ActorRef/EntityRef/SingletonRef messaging.

Mixed old/new wire or storage generations are forbidden. The named acceptance scenarios
in section 7 of `production-hardening-plan.md` are the authoritative test inventory for
the replacement.
