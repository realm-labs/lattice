# lattice Unified Actor Remoting Execution Plan

> Status: architecture reset in progress
> Decision: remove framework gRPC and reuse/refactor Direct Link transport internals as `lattice-remoting`
> Model: ActorRef + EntityRef/ShardRegion + SingletonRef/SingletonProxy, inspired by Akka/Pekko
> Coordination: etcd-backed Coordinator without Gossip

---

## 0. Goal Prompt

```text
Your goal is to fully execute docs/production-hardening-plan.md.

Read the document completely before changing code. Treat it as the primary execution plan and use
docs/architecture/ as the architecture reference. The local Pekko source tree recorded below is a
behavioral reference only; do not copy its source.

Start from the Current Execution Pointer and execute the current hard-switch macro batch. Phases are
dependency and acceptance groupings, not commit boundaries. Prefer one large cross-crate change over
compatibility scaffolding or a sequence of temporarily clean abstractions.

Do not preserve framework gRPC, Explicit Placement, public Direct Link, per-actor placement records,
per-actor epoch floors, or per-actor placement tombstones behind compatibility flags. Do not create
temporary compatibility adapters, dual writes, fallback routing, or old/new production feature modes.

This is an intentional hard switch. Intermediate worktrees and commits may fail to compile, test, or
run while APIs, crates, generated code, and call sites are replaced across the workspace. Do not spend
time restoring compilation between mechanical breakage and its planned replacement. Record known
breakage in the tracker and continue through the macro batch.

The highest-priority invariant is that every remote message is delivered only to the exact node and
actor activation named by ActorRef, or through the currently authorized Shard/Singleton owner named
by EntityRef/SingletonRef. A reused node address or actor path must never receive a stale message.

Implement the complete target architecture rather than replacing it with a minimal v1 that requires a
later structural rewrite. Multi-lane Associations, bounded protocol catalogues, revisioned snapshots,
per-slot claims, handoff barriers, logical watch_current, and the full Singleton model remain required.
Control complexity by sharing internal mechanisms and narrowing fault domains, not by deleting target
capabilities or deferring them outside this plan.

Aim to finish in roughly three large English conventional commits: foundation/remoting hard switch,
distributed runtime, then hardening/acceptance. A fourth commit is acceptable only for a genuinely
independent migration or generated-code boundary. Update this tracker after each macro batch. Run any
checks that are meaningful during broken intermediate states, but require full fmt/clippy/test only at
the final acceptance boundary.
```

### 0.1 Hard-Switch Execution Policy

```text
Compilation between macro commits: optional; failure is allowed and expected.
Compilation at final completion: mandatory.
Commit size: intentionally large and cross-crate.
Target commit count: 3, exceptionally 4.
Compatibility layers: forbidden.
Old/new dual routing or dual writes: forbidden.
Phase boundaries: checklist/dependency markers, not commit boundaries.
Tracker updates: after each macro batch and whenever the known broken frontier changes materially.
```

Intermediate commits must still be coherent enough to review: they state the intended broken frontier,
do not contain unrelated work, and do not claim passing verification that was not run. A red commit is
acceptable; an ambiguous compatibility state is not.

---

## 1. Architecture Decision

### 1.1 One Internal Transport

lattice will use one internal cluster transport:

```text
ActorRef / EntityRef / SingletonRef
        -> lattice-remoting association
        -> TCP or TLS-TCP
        -> remote actor / ShardRegion / SingletonManager
```

- Remove lattice-owned gRPC services, clients, tonic adapters, and RPC code generation.
- Allow `etcd-client` to retain its transitive gRPC implementation dependency.
- Refactor `lattice-direct-link` into internal `lattice-remoting`; business code no longer opens links.
- Preserve the pooled Direct Link benchmark as the performance baseline for remoting bulk tell; throughput
  optimization stays inside the association and never recreates a public link/session model.
- Coordinator, actor messaging, ShardRegion, Singleton and DeathWatch share the same remoting stack.
- External Gateway/HTTP protocols remain edge interfaces and are not service-to-service transports.
- No Gossip, persistent actors, event sourcing, remembered entities, Akka Streams, or exactly-once delivery.

### 1.2 Three Reference Types

#### Concrete ActorRef

Every live Actor, including child actors, may expose a serializable concrete reference:

```rust
ActorRef<A> {
    cluster_id,
    node_address,
    node_incarnation,
    actor_path,
    activation_id,
    protocol_id,
}
```

- `ActorRef` targets one exact activation and routes directly to its node association.
- Reusing a node address, actor path, or local numeric ID creates a new incarnation/activation ID.
- An in-place supervision restart replaces the actor instance inside the same actor cell and preserves
  ActivationId; only full stop followed by a new spawn at the path creates a new activation.
- A stale reference goes to dead letters or returns `Terminated`; it never retargets a replacement actor.
- User and child actor paths are supported. Framework `/system` paths are reserved and cannot be
  constructed from untrusted input.
- Wildcard `ActorSelection`, remote deployment, and arbitrary path pattern lookup are not in v1.

#### EntityRef

```rust
EntityRef<A> {
    cluster_id,
    entity_type,
    entity_id,
    protocol_id,
    entity_config_fingerprint,
}
```

- Location-transparent and routed through the local ShardRegion/proxy.
- Entity ID maps deterministically to one fixed shard.
- The local Shard creates the concrete actor activation and is the only production owner allowed
  to load that sharded entity.

#### SingletonRef

```rust
SingletonRef<A> {
    cluster_id,
    singleton_kind,
    protocol_id,
    singleton_config_fingerprint,
}
```

- Routes through SingletonProxy to the fixed registered SingletonManager owner.
- Dynamic singleton scopes are removed.

All three references implement a shared recipient interface for tell/ask. DeathWatch remains activation-scoped: ActorRef uses `watch`, while EntityRef/SingletonRef use resolve-without-activate `watch_current`.

### 1.3 Delivery Semantics

- `tell`: Akka-style at-most-once. It returns after admission to the local bounded remoting/region
  queue and does not wait for handler completion or a remote acknowledgement.
- `ask`: boot-unique correlation-based request/reply with a caller-local monotonic deadline. The wire carries only remaining timeout budget; Association loss after socket-write commitment produces `UnknownResult`.
- No automatic resend of business messages after connection failure.
- Caller cancellation does not roll back remote actor effects.
- Tell ordering is preserved for one stable sender/target bulk stripe. It is not promised across tell/ask lanes, reconnect, crash failover, or handoff.
- Unknown or moving shards may buffer only within configured count, byte, and age limits.

### 1.4 Watch Semantics

Watch observes a concrete activation, not a logical identity forever.

- `ActorRef.watch` observes the activation encoded in the reference.
- `EntityRef.watch_current` resolves the current activation but does not create one; inactive returns `NotActive`.
- `SingletonRef.watch_current` resolves the current activation but does not allocate one; unavailable returns `Unavailable`.
- Passivation, explicit stop, handoff, claim loss, or node incarnation removal produces one `Terminated`.
- A later activation requires a new watch.
- Temporary association loss is not termination. Reconnect re-registers the same activation watch;
  absent or changed activation returns `Terminated`.

### 1.5 Security and Compatibility

- Internal transport supports plaintext TCP and TLS-TCP. TLS is optional.
- Plaintext mode assumes a trusted private network; handshake identity is a claim, not authentication.
- TLS mode verifies peer certificate identity against handshake cluster/node ID/node incarnation; advertised roles and protocol eligibility are authorized separately.
- This is a breaking, full-stop migration. Old and new cluster wire/storage generations cannot mix.

### 1.6 Complete Model, Shared Internal Primitives

- Shard and Singleton retain distinct public references, routers, activation rules, and lifecycle semantics, but reuse one internal placement-slot authority engine for assignment generation, claim, drain, fencing, and replacement eligibility.
- DeathWatch, Coordinator state, claims, handoff, drain, and Singleton control reuse one bounded Association-level reliable control stream with sequence, cumulative acknowledgement, replay, and idempotent application.
- Transport handshake compatibility and business `ProtocolId` compatibility are separate fault domains. A mismatched business protocol disables that protocol and dependent hosting eligibility without closing unrelated traffic on a valid Association.
- The handoff revision barrier is complete for live Region sessions subscribed to the affected entity type. Unrelated nodes do not join the barrier; new subscriptions install the Handoff snapshot slice before routing.
- `StopFailed` may block a voluntary release while authority remains valid, but it is observable and bounded by retry/operator policy. External claim loss always fences admission and cannot be extended by lifecycle cleanup.

---

## 2. Reference Model

Behavior was checked against local Apache Pekko source:

```text
Repository: ~/IdeaProjects/pekko
Commit: cc22e586d32db1ba54d223486151cd0e29217aa4
```

Relevant files:

- `remote/.../artery/Association.scala`, `Handshake.scala`, `Control.scala`, and `OutboundEnvelope.scala`;
- `remote/src/main/resources/reference.conf` for lanes, queue bounds, frame bounds and transport modes;
- `cluster-sharding/.../ShardCoordinator.scala`, `ShardRegion.scala`, and `Shard.scala`;
- `cluster-sharding/.../internal/DDataRememberEntitiesShardStore.scala` only as evidence that
  remembered entities are separate from ownership; lattice does not implement it in v1.

lattice adapts the association, concrete activation reference, DeathWatch, ShardRegion handoff and
Singleton concepts to Rust/Tokio and an external etcd-backed Coordinator. It does not implement Pekko
membership Gossip, remote deployment, Streams, persistence, or source-compatible APIs.

---

## 3. Public APIs and Wire Contracts

### 3.1 Actor Identity

```rust
struct NodeAddress { host, port }
struct NodeIncarnation(u128);
struct ActorPath { segments: Vec<ActorPathSegment> }
struct ActivationId { node_incarnation, local_sequence }

enum RecipientRef<A> {
    Actor(ActorRef<A>),
    Entity(EntityRef<A>),
    Singleton(SingletonRef<A>),
}
```

Actor paths are canonical, bounded, UTF-8 validated, and reject empty, `.`, `..`, separators inside
segments, reserved `/system` construction, excess depth, and excess encoded length. Runtime-created
system refs may still serialize through the trusted internal protocol.

### 3.2 Messaging API

```rust
recipient.tell(message).await
recipient.ask(message, deadline).await
ctx.watch(&actor_ref).await
ctx.watch_current(&entity_ref).await
ctx.watch_current(&singleton_ref).await
ctx.unwatch(watch_id).await
```

Concrete ActorRef delivery validates node incarnation, exact path, and ActivationId before mailbox
lookup. EntityRef and SingletonRef resolve to concrete refs internally but never expose endpoint or
owner fields as durable identity.

### 3.3 Codec Registry

```rust
trait WireCodec<T>: Send + Sync + 'static {
    fn encode(&self, value: &T, output: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode(&self, input: &[u8]) -> Result<T, CodecError>;
}
```

Each actor protocol explicitly registers:

```text
explicit protocol_id: u64
message_id: u64
tell/ask mode
request codec
reply codec
codec and schema IDs/versions
erased typed-dispatch closure
maximum encoded payload
```

- `actor_protocol!` is the canonical type-checked source and generates the registrar, canonical descriptor and BLAKE3 fingerprint. External business catalogues may generate this Rust declaration.
- Message IDs are explicit and collisions fail service construction.
- `tell` bindings require `Reply = ()`.
- An ask pending entry knows the reply codec; reply frames need no global reply type ID.
- Business payloads may use protobuf, bincode, postcard, JSON, or another codec.
- Remoting core does not depend on a business serializer.
- Framework control messages use internal prost schemas without gRPC service generation.
- The transport handshake establishes the Association first; a later bounded reliable-control exchange installs `(ProtocolId, ProtocolFingerprint)` catalogues. References carry ProtocolId and frames carry protocol/message IDs. A mismatch rejects only that ProtocolId and dependent hosting eligibility.

### 3.4 Association Protocol

One logical association is keyed by:

```text
cluster_id + local node incarnation + remote canonical address + remote node incarnation
```

Handshake fields:

```text
protocol major/minor
cluster ID
local/remote node ID and incarnation
local/remote canonical node address
AssociationId for lane attachment after the control handshake
lane kind and index
maximum frame size
feature bits
```

Initial transport defaults:

```text
1 logical Association per exact local/remote NodeIncarnation pair
1 control TCP/TLS connection per Association
1 interactive TCP/TLS connection per Association
1 bulk TCP/TLS stripe per Association; configurable 1..4 after benchmark evidence
256 active Associations per node, validated against the derived physical-connection/FD budget
256 KiB maximum frame; no v1 fragmentation
3 s connect timeout
100 ms..5 s reconnect backoff
2 s heartbeat; suspect after 3 missed heartbeats
1024 queued control frames per association
4096 queued interactive frames per association
8192 queued bulk frames per stripe
16 MiB queued outbound bytes per association and 256 MiB per node
4096 pending asks per service instance
60 s idle data-connection timeout
```

All limits are validated, configurable, observable, and never accept zero/unbounded production values.

Frame kinds:

```text
Handshake / HandshakeAck
Heartbeat / HeartbeatAck
Tell
Ask / Reply / Failure
Watch / WatchAck / Unwatch / Terminated
CoordinatorRequest / CoordinatorReply / CoordinatorEvent
Backpressure
Close / ProtocolError
```

State-bearing control frames are wrapped in `ControlEnvelope(association_epoch, control_sequence, command_id, payload)` and acknowledged by cumulative `ControlAck`. The bounded outbox replays only to the same peer incarnation after reconnect; receivers deduplicate and apply commands idempotently. Heartbeat/backpressure hints are ephemeral, and uncertain business tell/ask frames are never replayed by this mechanism. An unrecoverable control gap triggers the owning reconciliation path, such as a fresh Coordinator snapshot or complete watch-set reinstall.

Simultaneous dials use stable node ordering to keep one logical Association. Every physical connection
attaches with AssociationId, exact incarnations, lane kind and stripe index; duplicate attachments are
closed deterministically. A connection from an old node incarnation is rejected and quarantined.
AssociationId is also the in-memory control epoch: it survives bounded physical-lane reconnect while
reconciliation state is retained, but changes when the logical Association/incarnation/state is replaced.
Control and interactive traffic use physical connections separate from bulk traffic so a bulk TCP
stream cannot head-of-line block heartbeat, watch, handoff, ask/reply, or shutdown messages.
Every tell uses a stable bulk stripe; ask/reply/failure uses interactive. V1 exposes no business option
to move a tell between lanes, and it promises no ordering between tell and ask lanes.
Transport feature incompatibility may close the Association. Business protocol catalogue incompatibility
is isolated per ProtocolId so an unrelated actor protocol cannot take down the peer relationship.

---

## 4. Coordinator, Shards, Singleton and Watch

### 4.1 Coordinator and etcd

- Coordinator instances elect a leader through etcd.
- Only Coordinator has placement/membership namespace read/write/watch capability.
- Ordinary instances perform one minimal read-only leader lookup for bootstrap, then use remoting.
- The leader record contains endpoint, instance, incarnation, protocol generation, and leadership term.
- Services register and heartbeat through remoting; Coordinator creates and renews their etcd leases.
- New leaders renew a service only after its current incarnation reconnects and re-registers.
- Coordinator sends a bounded `SnapshotBegin/Chunk/End` stream with count/byte/deadline limits and BLAKE3 validation, atomically followed by ordered generation/revision deltas. Snapshot slices are filtered by the node's hosted/proxied entity-type and used-singleton subscriptions; a new subscription installs its slice before becoming Ready. Gaps require resnapshot.
- Snapshot pages, deltas, claims, handoff, drain, readiness, and watch lifecycle use the shared reliable control stream. Coordinator revision supplies authoritative state ordering while Association control sequence supplies bounded reconnect replay; applying either layer is idempotent.
- Known homes continue serving while their locally tracked claim deadline is valid during a Coordinator
  outage. Claim grants carry term/generation/sequence plus TTL; nodes derive monotonic deadlines with a safety margin. New placement and handoff stop. Claim expiry fences local admission before actor shutdown.
- This continuation is deliberately bounded rather than indefinite. Production defaults and HA validation must satisfy `Coordinator leader recovery objective < claim TTL - safety margin`; increasing TTL also increases worst-case crash-failover delay.
- Normal ActorRef, EntityRef and SingletonRef delivery never reads etcd and never hops through Coordinator.

### 4.2 ShardRegion

```text
entity_id -> stable shard_id -> ShardHomeRecord
```

EntityId is a maximum 256-byte canonical business-key encoding. V1 shard mapping is
`xxh3_64_with_seed(bytes, 0x4c41_5454_4943_4531) % shard_count`; Rust DefaultHasher and implicit
type/order inputs are forbidden.

There is one persistent, normally non-deleted record per configured shard:

```text
key and config fingerprint
home instance/incarnation
assignment epoch
Coordinator term/generation
Unallocated | Allocating | Running | BeginHandoff | Stopping
```

Shard and Singleton records are distinct `PlacementSlot` keys handled by one authority engine. That engine owns persistence, term/generation checks, claim grant/renew/loss, local deadline fencing, drain, and replacement eligibility. ShardRegion/Shard and SingletonProxy/SingletonManager keep their different routing and activation behavior.

ShardRegion responsibilities:

- host or proxy-only mode;
- bounded home cache and ordered Coordinator events;
- local Shard tasks and claim validity;
- single-flight unknown-home resolution;
- bounded handoff/unknown-home buffering;
- only local Shard may load or invoke a sharded entity;
- passivation changes no placement metadata and creates no tombstone.

Handoff order:

```text
persist BeginHandoff
-> freeze the live subscribed Region-session barrier set for the affected entity type and publish StateDelta(handoff revision)
-> subscribed host/proxy Regions invalidate home, buffer, and AppliedRevision; unrelated nodes do not participate
-> later joiners or new entity-type subscriptions install the Handoff snapshot slice before routing
-> dead barrier members leave only through membership/lease fencing, never handoff timeout alone
-> DrainShard; old Shard stops admission and terminates entities
-> ShardDrained or independently proven old claim/instance expiry
-> Coordinator persists next-generation claim and sends ClaimGranted(term, generation, sequence, TTL)
-> destination derives local monotonic deadline, starts Shard, and sends ShardReady
-> persist/publish Active StateDelta
-> Regions flush bounded buffers
```

Timeout alone is never proof that the old owner stopped.
Stopping failure blocks voluntary drain/release but never overrides external fencing. Grant expiry first
stops admission; failed cleanup raises StateLossPossible. A new owner may start only after old claim
invalidation, so business persistence must be crash-safe when state recovery is required.

### 4.3 Singleton

- Singleton types are fixed during service construction.
- One persistent record and one short-lived claim per `(service, singleton_kind)`.
- SingletonManager owns activation; SingletonProxy routes tell/ask/watch_current.
- Singleton delegates assignment generation, claim, drain, fencing, and replacement eligibility to the shared placement-slot authority engine rather than duplicating shard ownership logic.
- No dynamic scope, persistent actor state, or transparent state transfer.
- Claim loss fences delivery and stops the activation before reassignment.

### 4.4 DeathWatch

Remote watch registry is bounded on watcher and target sides.

```text
Watch(exact ActorRef activation)
-> WatchAck(watch_id, activation_id)
-> Terminated(watch_id, activation_id, reason)
```

Watch/Unwatch/Terminated commands use the shared reliable control sequence. Reconnect replays or reconciles the complete bounded desired watch set for the same peer incarnation; duplicate commands cannot create duplicate watches or duplicate terminal delivery.

- Concrete actors may watch any concrete ActorRef path.
- EntityRef/SingletonRef first resolve a current activation.
- Reconnect re-registers the exact ActivationId.
- Target absence or changed ActivationId terminates the old watch.
- Coordinator node-removal events synthesize `NodeDown` termination for every affected concrete ref.

---

## 5. Current Migration Baseline

Reusable from current Direct Link:

- length-prefixed TCP framing and pre-allocation frame limit;
- listener/connector abstraction;
- connection pooling and connection stripes;
- heartbeat, reconnect backoff, frame metrics, and partial backpressure machinery.

Must be replaced:

- JSON OpenLink handshake and link/session/stream business APIs;
- endpoint-targeted `ActorRef` and manual link opening;
- default unbounded link limits;
- owner+epoch remote-watch prototype and unbounded watch channels;
- generated gRPC services/clients, tonic security adapters, and route resolvers;
- Explicit Placement records, activation locks, actor floors, and tombstones.

Reusable elsewhere:

- actor mailbox and typed `Handler<M>` model;
- local call/tell and termination notifications;
- service instance boot incarnation;
- etcd leader/storage adapters and several bounded watch primitives;
- virtual-shard mapper concepts, after migration to the new EntityRef/ShardRegion contract.

---

## 6. Current Progress Tracker

### Hard-Switch Macro Batches

The target is three large commits. Checklist items from several phases may be completed together.

1. `refactor(remoting)!: hard-switch actor identity and transport`
   - capture the old benchmark/storage/API baseline;
   - move reusable Direct Link transport internals into `lattice-remoting`;
   - immediately delete framework gRPC, public Direct Link, Explicit Placement, per-actor floors and tombstones;
   - replace references, paths, protocol registration, local dispatch, Association, reliable control delivery, remote tell/ask, and all directly affected call sites;
   - compilation may remain broken at this commit while distributed owners and service assembly still target removed APIs.
2. `feat(cluster)!: add coordinator sharding watch and singleton runtime`
   - complete Coordinator remoting, filtered snapshots, per-slot grants, subscribed-Region handoff barrier, DeathWatch, shared placement authority, ShardRegion, Singleton and service/Gateway assembly;
   - remove the remaining old control/storage assumptions while fixing the workspace against the new public API;
   - targeted tests should run where practical, but full workspace green is not yet a commit requirement.
3. `test(remoting)!: complete hard-switch acceptance and migration`
   - finish storage-generation cutover, admin/ops, bounded-resource hardening, multi-process coverage and benchmarks;
   - delete every remaining compatibility residue and update all examples/docs;
   - finish only when full fmt, clippy, tests and global acceptance pass.

Do not split a macro batch merely to obtain a green commit. Split only when the resulting commit is an
independently reviewable migration boundary and the total remains at most four.

### Phase 0: Architecture Reset

Status: `[ ]` in progress.

Current Execution Pointer:

```text
Capture the pre-change workspace/benchmark/storage/API baseline, record the permitted break set, then
start hard-switch macro batch 1. Do not create a standalone documentation commit and do not wait for
Phase 0 to become a green build boundary.
```

- [x] Decide to remove framework gRPC and public Direct Link.
- [x] Choose at-most-once tell/ask, concrete-activation watch, explicit u64 message IDs, optional TLS, Coordinator-only etcd ownership, fixed Singleton types, and arbitrary concrete Actor paths.
- [x] Record the local Pekko reference baseline and current reusable Direct Link components.
- [x] Update every architecture/API/example document to the unified remoting model.
- [ ] Record the pre-change workspace and benchmark baseline.
  - [ ] Preserve pooled Direct Link throughput, allocation, connection/FD, payload-size and backpressure results as the remoting comparison baseline.
- [ ] Define named acceptance tests for every invariant.
- [ ] Record all permitted API, wire, storage, crate and deployment breaks.
- [ ] Phase 0 evidence recorded and folded into hard-switch macro commit 1; no standalone commit required.

### Phase 1: Hard Switch, Actor Identity, Paths, and Codec Registry

Status: `[ ]` not started.

- [ ] Move reusable `lattice-direct-link` transport internals into `lattice-remoting`, then delete the public Direct Link crate/API surface and manual endpoint/session/stream model.
- [ ] Delete `lattice-rpc`, tonic service/client adapters, RPC codegen and framework direct tonic dependencies immediately after capturing the baseline.
- [ ] Remove gRPC service definitions from `control.proto`; retain/move only internal non-gRPC control schemas needed by later macro batches.
- [ ] Delete Explicit Placement types, records, locks, floors, per-actor tombstones/reclamation and their authority/watch APIs.
- [ ] Add bounded canonical `ActorPath`, `NodeIncarnation`, and boot-unique `ActivationId`.
- [ ] Give every local and child actor an exact path/activation registry entry.
- [ ] Replace endpoint/owner-based ActorRef with typed concrete `ActorRef<A>`.
- [ ] Add logical `EntityRef<A>` and `SingletonRef<A>` plus shared tell/ask recipient API and separate activation-scoped watch_current surface.
- [ ] Add erased codec registry with explicit ProtocolId/message ID, request/reply codec/schema versions and dispatch closure.
- [ ] Add canonical `actor_protocol!` generation of registrar/descriptor/BLAKE3 fingerprint plus the equivalent low-level ActorProtocol builder; allow external business tools to generate the Rust declaration.
- [ ] Separate transport compatibility from bounded post-handshake business protocol catalogue negotiation; mismatch must disable only the affected ProtocolId and dependent hosting eligibility.
- [ ] Reject duplicate IDs, unsupported messages, oversized payloads, reserved paths and stale activations.
- [ ] Adapt local tell/ask/watch/watch_current to the new public reference API before adding networking.
- [ ] Add path reuse, stale ref, codec diversity, ProtocolId/fingerprint/schema-version collision and local dispatch tests.
- [ ] Phase 1 checklist and available directional evidence complete within macro batch 1; compilation is not required at this boundary.

### Phase 2: Unified Remoting Association

Status: `[ ]` not started.

- [ ] Complete the moved `lattice-remoting` transport, frame and association modules without retaining a compatibility facade.
- [ ] Replace OpenLink JSON protocol with versioned binary association handshake.
- [ ] Implement exactly one logical Association per local/remote NodeIncarnation pair with lazy single-flight establishment and a bounded node-level Association registry.
- [ ] Give each Association one fixed control connection, one fixed interactive connection and configurable 1..4 bulk stripes, defaulting to one bulk stripe.
- [ ] Bind every physical connection to AssociationId, exact peer incarnations, lane kind and stripe index; resolve simultaneous dials and duplicate attachments deterministically.
- [ ] Define actor sender identity as path+ActivationId and non-actor sender identity as boot-scoped SenderId; add stable sender/recipient bulk striping, bounded per-lane queues, frame batching/vectored-write opportunities and no business-visible stream/session API.
- [ ] Add plaintext TCP and optional TLS-TCP with complete configuration validation.
- [ ] Add heartbeat, reconnect, incarnation quarantine, idle data-connection close, frame/payload bounds and graceful supervised join; control failure immediately stops all new data admission and no v1 data lane continues independently.
- [ ] Add one Association-level reliable control stream with association epoch, control sequence, command ID, cumulative Ack, bounded replay, idempotent receive, incarnation reset and reconciliation fallback; never replay uncertain business frames.
- [ ] Remove public link/session/stream/open APIs from the new crate.
- [ ] Add real socket tests for handshake, old incarnation, simultaneous connection, duplicate lane, stable stripe ordering, control non-starvation under bulk load, reliable-control replay/dedup/gap recovery, per-ProtocolId mismatch isolation, partial-lane failure, association cap, overload, malformed frame, heartbeat timeout and TLS.
- [ ] Phase 2 checklist and available directional evidence complete within macro batch 1; compilation is not required at this boundary.

### Phase 3: Remote ActorRef Tell and Ask

Status: `[ ]` not started.

- [ ] Implement Tell, Ask, Reply and Failure frames using opaque codec payloads.
- [ ] Route concrete ActorRef directly to its node association and exact path/ActivationId.
- [ ] Add boot-unique bounded pending ask correlation, monotonic caller deadlines, wire timeout budgets, caller cancellation and `UnknownResult` at the first socket-write boundary.
- [ ] Check ask expiry before admission, Region buffering, socket write, remote mailbox admission and Handler start; never claim Handler cancellation rolls back effects.
- [ ] Keep expected business errors inside typed Reply and map runtime failures to stable bounded/redacted `RemoteFailureCode` frames.
- [ ] Map mailbox full/closed, unknown message, decode failure, authorization failure and stale activation to structured failures.
- [ ] Preserve per-sender/target tell ordering only on its stable bulk stripe within one Association/home epoch; document and test the lack of ordering across tell/ask lanes.
- [ ] Prove tell returns after local admission and never waits for handler completion.
- [ ] Add disconnect-before/after-dispatch, stale ref, queue full, deadline and codec failure tests.
- [ ] Phase 3 checklist and available directional evidence complete within macro batch 1; compilation is not required at this boundary.

### Phase 4: Coordinator Over Remoting

Status: `[ ]` not started.

- [ ] Replace tonic `PlacementCoordinator`/`LogicControl` services with internal remoting control messages.
- [ ] Implement etcd leader bootstrap, protocol generation and current leader reconnect.
- [ ] Implement instance registration/heartbeat and Coordinator-owned etcd leases.
- [ ] Implement subscription-filtered bounded SnapshotBegin/Chunk/End staging with chunk/count/byte/deadline limits, BLAKE3 validation, atomic install, ordered revision deltas and resync over reliable control delivery.
- [ ] Bind every mutation to current leadership term and exact node incarnation.
- [ ] Implement term/generation/sequence/TTL claim grants, renewal cadence, local monotonic deadlines and safety margin so Coordinator outage continues known homes only until expiry.
- [ ] Add leader failover, stale leader, event gap, lease expiry and reconnect tests with real etcd.
- [ ] Phase 4 checklist and available directional evidence complete within macro batch 2; full workspace compilation is not required at this boundary.

### Phase 5: Shard and ShardRegion

Status: `[ ]` not started.

- [ ] Add `ShardedActor::Key`, bounded canonical EntityId encoding and immutable entity config with explicit Xxh3V1 seed/hash, stable fingerprint and validated shard count.
- [ ] Add persistent bounded shard-home records for memory and etcd backends.
- [ ] Implement Region host/proxy, home cache, single-flight lookup and bounded buffers.
- [ ] Make local Shard the only component allowed to load/deliver sharded entities.
- [ ] Implement Coordinator-backed claim grant installation/renewal/loss and immediate local fencing.
- [ ] Implement the frozen subscribed Region-session revision barrier for the affected entity type plus StateDelta/AppliedRevision, DrainShard/ShardDrained, ClaimGranted and ShardReady handoff; exclude unrelated nodes and handle concurrent join/subscription/leave through snapshot and membership fencing.
- [ ] Implement one internal PlacementSlot authority engine shared by Shard and Singleton for assignment generation, claims, drain, fencing, and replacement eligibility while preserving their distinct routing/lifecycle behavior.
- [ ] Make StopFailed block voluntary drain but never override claim fencing; surface StateLossPossible and test crash-safe owner replacement.
- [ ] Add local passivation with no Coordinator/etcd mutation.
- [ ] Add routing, buffering, migration, crash, claim loss, drain and unauthorized-load tests.
- [ ] Phase 5 checklist and available directional evidence complete within macro batch 2; full workspace compilation is not required at this boundary.

### Phase 6: Remote DeathWatch

Status: `[ ]` not started.

- [ ] Implement bounded Watch/WatchAck/Unwatch/Terminated control protocol.
- [ ] Support arbitrary concrete ActorRef paths and exact ActivationId validation.
- [ ] Implement activation-scoped ActorRef `watch` and resolve-without-activate EntityRef/SingletonRef `watch_current`, returning NotActive/Unavailable and requiring a new watch after replacement.
- [ ] Re-register watches after association reconnect and terminate changed activations.
- [ ] Synthesize node-down terminations from Coordinator incarnation removal.
- [ ] Delete owner+epoch and unbounded-channel remote watch implementation.
- [ ] Add local/remote stop, passivation, handoff, reconnect, path reuse, node loss and overflow tests.
- [ ] Phase 6 checklist and available directional evidence complete within macro batch 2; full workspace compilation is not required at this boundary.

### Phase 7: Cluster Singleton

Status: `[ ]` not started.

- [ ] Add fixed Singleton registration and duplicate configuration validation.
- [ ] Add Coordinator-backed owner record, activation epoch and claim.
- [ ] Implement SingletonManager/Proxy over the unified remoting association.
- [ ] Delegate Singleton assignment/claim/drain/fencing to the shared PlacementSlot authority engine; do not build a second ownership algorithm.
- [ ] Support tell/ask/watch_current and bounded unavailable-owner buffering.
- [ ] Fence and stop on claim loss before publishing a replacement.
- [ ] Remove dynamic scoped singleton and direct singleton registry RPC.
- [ ] Add election, owner crash, leader failover, claim loss, buffering and watch tests.
- [ ] Phase 7 checklist and available directional evidence complete within macro batch 2; full workspace compilation is not required at this boundary.

### Phase 8: Legacy Absence Verification and Storage Cutover

Status: `[ ]` not started; completes in macro batch 3 after source/API deletion in macro batch 1.

Legacy source/API deletion is intentionally front-loaded into macro batch 1 even though it breaks the
workspace. Phase 8 does not perform that deletion again; it proves absence, migrates storage/deployment,
and removes any residue discovered while macro batch 2 restored the distributed runtime.

- [ ] Verify `lattice-rpc`, framework tonic adapters, public Direct Link, Explicit Placement, per-actor floors/tombstones and old control services are absent from source, Cargo features, generated code and public docs.
- [ ] Replace admin placement views with nodes, associations, paths, entity types, shards, singletons and watches.
- [ ] Add schema-generation preflight and refuse mixed old/new processes.
- [ ] Perform full-stop legacy key cleanup only after old credentials are revoked.
- [ ] Add compile-fail/API absence tests and update all examples/benchmarks.
- [ ] Phase 8 storage cutover and absence evidence complete within final macro batch 3.

### Phase 9: Production Hardening and End-to-End Acceptance

Status: `[ ]` not started.

- [ ] Supervise every listener, association, lane, Coordinator subscription, claim, Shard, Singleton and actor task.
- [ ] Bound all connections, frames, queues, pending asks, watches, buffers, maps and shutdown joins.
- [ ] Make CorrelationId boot-unique and pending-ask state bounded; keep business idempotency keys/dedup business-owned and never imply cancellation rollback.
- [ ] Add live/partial admin inspection and low-cardinality metrics.
- [ ] Complete Gateway failure isolation and edge admission control.
- [ ] Add multi-process Gateway -> EntityRef ask -> remote Shard -> Actor -> Reply coverage.
- [ ] Add arbitrary child ActorRef serialization/tell/ask/watch across nodes.
- [ ] Add Coordinator outage, handoff, crash, reconnect, TLS/plaintext and abuse scenarios.
- [ ] Benchmark local actor, concrete remote ref, stable shard, unknown home, handoff and reconnect.
- [ ] Compare remoting bulk-tell throughput, allocations and connection/FD usage against the preserved pooled Direct Link baseline and document the accepted regression budget.
- [ ] Prove remote hot paths do not access etcd or Coordinator.
- [ ] Run full workspace verification and audit every checked item.
- [ ] Phase 9 verification and final conventional commit complete.

---

## 7. Required Acceptance Scenarios

```text
concrete_actor_ref_targets_exact_path_and_activation
reused_path_or_node_address_rejects_stale_ref
arbitrary_child_actor_ref_round_trips_across_nodes
tell_returns_after_bounded_local_admission
ask_disconnect_after_dispatch_returns_unknown_result
codec_registry_supports_multiple_serializers_and_rejects_id_collisions
protocol_catalog_rejects_fingerprint_and_schema_version_mismatch
protocol_mismatch_disables_only_affected_protocol_not_association
association_rejects_old_incarnation_and_duplicate_connection
control_connection_loss_stops_all_new_data_admission
reliable_control_replays_only_same_incarnation_and_deduplicates_commands
reliable_control_gap_triggers_bounded_reconciliation
ask_deadline_is_checked_before_remote_handler_start
snapshot_chunks_install_atomically_or_are_discarded
coordinator_outage_serves_known_claims_until_deadline_then_fences
claim_ttl_uses_local_monotonic_deadline_and_rejects_stale_term_or_sequence
entity_key_and_xxh3_v1_have_cross_process_golden_vectors
shard_handoff_invalidates_regions_before_old_actor_stop
handoff_revision_barrier_handles_concurrent_join_and_fenced_leave
handoff_barrier_contains_only_subscribed_regions_for_entity_type
stop_failed_blocks_voluntary_handoff_but_not_external_fencing
unauthorized_shard_never_reaches_loader_or_handler
passivation_writes_no_placement_metadata
remote_watch_observes_exact_activation_and_reconnects_safely
watch_current_inactive_does_not_activate_and_returns_not_active
singleton_replacement_waits_for_old_claim_fencing
framework_contains_no_gRPC_service_or_public_direct_link_api
remote_hot_path_performs_no_etcd_read_or_coordinator_hop
```

Tests for wire, identity and fencing claims must use real TCP/TLS connections. Coordinator storage
acceptance must include a disposable real etcd server. Fake transports remain useful directional tests
but are not final evidence.

---

## 8. Engineering Constraints

```text
Do not add a second internal transport.
Do not expose raw remoting links to business code.
Do not route concrete ActorRef through ShardRegion.
Do not bypass ShardRegion for EntityRef or SingletonProxy for SingletonRef.
Do not retarget a stale ActorRef when node address or actor path is reused.
Do not perform per-message etcd reads or Coordinator hops.
Do not automatically replay business messages after disconnect.
Do not use timeout alone as proof that an old shard/singleton stopped.
Do not make watch_current activate an inactive entity or singleton.
Do not implement separate Shard and Singleton ownership algorithms; share PlacementSlot authority while preserving distinct public semantics.
Do not let one business ProtocolId mismatch close an otherwise transport-compatible Association.
Do not replay tell/ask business frames through reliable control delivery.
Do not include nodes unrelated to an entity type in that type's handoff revision barrier.
Do not introduce persistent actors, Gossip, Streams, remote deployment or ActorSelection in v1.
```

---

## 9. Verification Commands

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets

cargo test -p lattice-remoting
cargo test -p lattice-actor remote
cargo test -p lattice-placement coordinator
cargo test -p lattice-service shard_region
cargo test -p lattice-service singleton
```

Target names are introduced as phases land. Real-etcd commands require a dedicated disposable endpoint
and fail explicitly when invoked without it.

---

## 10. Global Completion Criteria

- [ ] Arbitrary live Actor paths can produce serializable exact-activation ActorRef values.
- [ ] Concrete ActorRef, EntityRef and SingletonRef support the documented tell/ask plus activation-scoped watch/watch_current semantics.
- [ ] Reused node addresses and actor paths cannot receive stale messages or watches.
- [ ] One bounded TCP/TLS remoting system carries every internal cluster message.
- [ ] State-bearing control commands survive reconnect through bounded sequenced replay/idempotent reconciliation without replaying uncertain business messages.
- [ ] Framework-owned gRPC and public Direct Link APIs are deleted.
- [ ] Coordinator is the only placement/membership etcd authority.
- [ ] Shard and Singleton ownership is bounded, lease-fenced and leadership-generation-fenced.
- [ ] Shard and Singleton reuse one placement-slot authority engine while retaining distinct reference, routing and lifecycle semantics.
- [ ] Explicit Placement, per-actor floors and placement tombstones are deleted.
- [ ] Passivation writes no placement metadata and framework recovery claims no actor-state persistence.
- [ ] Association loss, queue pressure, handoff and Coordinator outage have bounded explicit outcomes.
- [ ] Every runtime task and collection is supervised, bounded and deadline-controlled.
- [ ] Hot-path remote messaging performs no etcd read and no Coordinator hop.
- [ ] Architecture docs, examples and benchmarks match the unified remoting system.
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes.
- [ ] `cargo test --workspace --all-targets` passes.
- [ ] The hard switch is delivered in approximately three (at most four) large English conventional commits; intermediate red commits document their broken frontier, and final behavior has audited executable coverage.
