# 06. Appendix

> Defaults, design tradeoffs, and framework/business boundary.
> Back to: [architecture index](README.md)

---

## 1. Recommended Starting Defaults

```text
normal actor mailbox: 4096 messages
maximum remoting frame: 256 KiB
physical connections per active Association: 1 control + 1 interactive + 1 bulk stripe
configurable bulk stripes per Association: 1..4, increase only from benchmark evidence
maximum active Associations per node: 256, configurable from the process FD budget
maximum outbound bytes: 16 MiB per Association and 256 MiB per node
reliable control outbox: 1024 frames per Association, bounded by the Association byte limit
association handshake timeout: 3 s
idle data-connection timeout: 60 s when no asks, watches, routes, or pending sends depend on it
heartbeat interval: 2 s
suspect after: 3 missed heartbeats
maximum ask deadline: 30 s
Coordinator snapshot chunk: 192 KiB; maximum assembled snapshot: 64 MiB / 5 s
claim grant: 15 s TTL, renew every 5 s, 2 s local safety margin
Coordinator leader recovery objective: less than claim TTL minus safety margin
placement capacity: positive configured units per eligible shard-host node; default 100
entity passivation: 10 min, configured per type
ShardRegion buffer: 1024 messages per shard, 10000 messages / 64 MiB per region
rebalance: 10 s evaluation, 5 s load report, 20 s load-sample max age, 10% minimum relative improvement
rebalance stability: 2 min minimum shard residence, 30 s node-join stability, 30 s cooldown
rebalance limits: 4 moves per round, 8 cluster-wide, 4 per entity type, 2 per source/target node
drain deadline: 30-120 s by service class
admin inspect timeout: 1-3 s per node
```

These are guardrails, not performance promises. Tune them from measured payload size, mailbox latency, reconnect rate, and handoff duration.

## 2. Why Three Reference Types

One universal “remote ref” would hide incompatible identity semantics.

| Reference | Identity | Follows relocation or stop-and-respawn | Route |
|---|---|---:|---|
| `ActorRef<A>` | exact node incarnation, path, activation | No; an in-place supervision restart preserves the activation | direct association |
| `EntityRef<A>` | entity type and entity ID | Yes | local ShardRegion |
| `SingletonRef<A>` | configured singleton kind | Yes | local SingletonProxy |

Keeping these types distinct makes stale-reference behavior, buffering, watch behavior, and operational ownership visible in the API.

## 3. Why Typed Messages over Format-Neutral Frames

Wire payloads are necessarily bytes, but business code should not handle opaque buffers. A registered actor protocol provides stable IDs, codec selection, size validation, typed decode, handler coverage checks, typed ask replies, and schema fingerprints. This preserves Rust type safety without coupling all messages to Protobuf or a giant framework enum.

## 4. Delivery Reality

`tell` is at-most-once. `ask` adds correlation and a deadline, not transactional certainty. A timeout or broken association can leave the caller unable to know whether the handler ran. Automatic retries would turn that uncertainty into duplicate state changes, so retry and deduplication are explicit business protocol decisions.

Placement fencing prevents two claim generations from legitimately serving the same shard or singleton; it does not turn network delivery into exactly-once processing.

State-bearing control commands are different from business messages. The Association control stream may retransmit a bounded sequenced command and the receiver applies it idempotently; this mechanism never retransmits an uncertain tell/ask frame.

## 5. Why There Is No Direct Link API

The old Direct Link model existed to bypass the gRPC command path for high-throughput fire-and-forget traffic. Unified remoting already provides persistent TCP/TLS associations, binary frames, independent bounded lanes, batching opportunities, fixed bounded connection groups/stripes, heartbeat, reconnect, and backpressure.

Keeping a separate public link model would force business code to choose transport, open and close sessions, handle reconnect, register a second protocol, and reconcile different lifecycle/error semantics. lattice therefore keeps the reusable transport machinery inside `lattice-remoting` and exposes only typed recipient operations.

Large files, media, voice, and UDP realtime synchronization may use an external purpose-built service. They are not Actor mailbox traffic and do not justify a second lattice actor transport.

## 6. Framework and Business Boundary

Framework owns:

```text
actor runtime, mailboxes, supervision, lifecycle, DeathWatch
reference identity and remoting associations
tell/ask correlation, deadlines, bounded buffering, codecs
Coordinator protocol, membership, shards, claims, singletons, drain
shared placement-slot authority and reliable control delivery
allocation strategies, bounded load view, persisted rebalance plans and move limits
EventBus, scheduler, config, Gateway adapters, inspection and telemetry abstractions
```

Business owns:

```text
actor state and repositories
transaction, durability, save/load, idempotency, and compensation rules
message and reply types plus codec/schema evolution
authentication claims and domain authorization
entity keys, passivation policy, and singleton definitions
external client protocols and business workflows
```

The framework does not require a business database and does not persist actor state automatically.

## 7. Summary

Use concrete `ActorRef` for one live activation, `EntityRef` for a movable/passivated sharded identity, and `SingletonRef` for a failover-capable singleton. All three use one actor messaging and remoting runtime. Shard and Singleton keep separate public semantics while sharing placement authority; state-bearing protocols share reliable control delivery without replaying business traffic. etcd and the Coordinator establish control-plane truth; they stay out of healthy known message paths.
