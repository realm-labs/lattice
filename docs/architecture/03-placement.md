# 03. Coordinator, Sharding, and Singleton Placement

> Control-plane state and data-plane behavior for logical actor references.
> Back to: [architecture index](README.md)

---

## 1. Control-Plane Boundary

The elected Coordinator is the sole writer of membership decisions, shard assignments, handoff state, and singleton assignments. Runtime nodes learn decisions through Coordinator messages and watch streams; they do not perform arbitrary etcd placement reads or writes.

A node may read the minimal Coordinator bootstrap record needed to find the current leader. After association, all control traffic uses actor remoting.

Normal `ActorRef`, `EntityRef`, and `SingletonRef` messages never go through etcd.

## 2. etcd Metadata

Recommended logical keys:

```text
/lattice/<cluster>/schema_generation
/lattice/<cluster>/coordinator/leader
/lattice/<cluster>/members/<node_id>
/lattice/<cluster>/entity_types/<entity_type>
/lattice/<cluster>/shards/<entity_type>/<shard_id>
/lattice/<cluster>/shard_claims/<entity_type>/<shard_id>
/lattice/<cluster>/singletons/<singleton_kind>
/lattice/<cluster>/singleton_claims/<singleton_kind>
/lattice/<cluster>/rebalances/<plan_id>
```

There are no per-entity placement keys and no concrete actor-path keys. Concrete `ActorRef` identity lives in remoting/runtime state; logical entity activation is local to its shard owner.

All records carry schema generation and revision information. An incompatible schema generation prevents startup rather than guessing compatibility.

Shard and Singleton remain different public/runtime concepts, but their distributed authority is implemented by one internal placement-slot engine:

```text
PlacementSlotKey = Shard(entity_type, shard_id) | Singleton(singleton_kind)

PlacementSlot {
  owner_node_incarnation,
  assignment_generation,
  state,
  optional_target,
  optional_active_move: (plan_id, move_id),
  claim,
}
```

The shared engine owns assignment persistence, term/generation validation, claim grant/renew/loss, local deadline fencing, drain, and replacement eligibility. `ShardRegion/Shard` and `SingletonProxy/SingletonManager` retain their distinct routing, activation, passivation, and public-reference semantics; they do not implement a second ownership algorithm.

## 3. Membership and Coordinator Leadership

Nodes register through their remoting Coordinator session. `NodeHello` is a bounded session
advertisement; persisted generation-4 membership is a `MemberRecord` containing the exact `NodeKey`,
the validated hello, `Joining | Up | Leaving`, `StateVersion { term, revision }`, and member lease ID. Removed is
an ordered event with the exact node incarnation and graceful, failure, force, or incarnation-replaced
reason; it is not an active stored status. The Coordinator leader record is also lease-backed and
fenced by an election term.

Registration persists `Joining` and sends a full snapshot. The node replies with
`JoinReady(snapshot_version)` from the same Association. Only an exact term and revision match may CAS the
record to `Up`; the resulting revisioned member delta and exact `MemberUp` acknowledgement open
service admission. Replayed hello/join-ready commands from that session are idempotent. Allocation,
handoff barriers, and singleton selection exclude every member not in `Up`.

The full snapshot includes all active `MemberRecord` values. A logic session validates the complete
digest, decodes members and placement slices, and emits one atomic member-snapshot effect before
`JoinReady`. Service routing installs that replacement snapshot and then accepts only contiguous
`MemberEvent` deltas from the same term. A higher term requires a new snapshot, and lower-term
snapshots, events, barriers, and acknowledgements are rejected; discovery candidates never enter
this directory.

Graceful drain CASes `Up -> Leaving`, moves placement through the normal persisted handoff path, and
sends `DrainReady` only when no slot remains owned. `DrainComplete` deletes the exact record and
revokes its lease. Heartbeat expiry and administrative force removal use the same exact-record CAS
removal path; force removal includes an operation ID and expected incarnation so it cannot remove a
replacement process.

A new leader:

1. obtains a higher election term;
2. reconstructs members, assignments, claims, active RebalancePlans, and in-progress handoffs from etcd;
3. establishes remoting associations to live members;
4. reconciles observed claim holders before issuing new assignments;
5. resumes required recovery/drain work, then allocation and automatic rebalancing only after reconciliation and fresh-input checks.

Every authoritative mutation is a named domain transaction. Its storage predicate contains the
exact serialized, lease-backed `LeaderRecord` and exact term, plus every record/counter comparison
needed by the operation. Initial allocation and target installation put slot plus lease-attached
claim atomically. Move reservation and completion update slot plus plan atomically. A false leader
predicate is `LeadershipLost`, even when every record-local comparison still matches. Runtime
effects, deltas, grants, and admin replies occur only after commit.

Election begins with read-only bounded inventory. Reconciliation then adopts a matching older-term
claim without changing owner or generation, fences active records whose claim is absent, and resumes
persisted handoff phases. Periodic reconciliation uses bounded pages/work and exposes cursor backlog,
oldest work, last success, and quarantine details. Claim lease expiry is an external fencing event:
it may temporarily leave an active record claimless, but recovery must fence it and may reinstall
only the same owner until previous authority invalidity is proven.

Coordinator leadership does not by itself grant permission to serve a shard or singleton. The leader persists a generation claim in etcd and sends the selected owner a bounded `ClaimGranted` control message. The owner may serve only while the matching grant remains valid according to its local monotonic deadline. Runtime nodes do not acquire or renew claims by connecting to etcd directly. Claim authority remains per placement slot; transport may batch renewals for many slots owned by one node without weakening per-slot generation or expiry checks.

### 3.1 Revisioned State Snapshot

The Coordinator session is node-level. `NodeHello` advertises the node incarnation, roles, capacity, hosted/proxied entity types, singleton eligibility/usage, and protocol catalogue. The snapshot contains cluster membership plus only the shard/singleton slices that the node is eligible to host or has subscribed to route. A node adding a Region or SingletonProxy subscription must install the corresponding snapshot slice before that component becomes Ready. The initial state may exceed the 256 KiB remoting frame, so it uses a bounded chunk protocol:

```text
SnapshotBegin(snapshot_id, state_version, chunk_count, total_bytes, blake3_digest)
SnapshotChunk(snapshot_id, index, bytes)  // at most 192 KiB payload
SnapshotEnd(snapshot_id)
AppliedRevision(state_version)
```

The receiver stages chunks outside the live routing table, rejects duplicate/out-of-range chunks, and enforces configured chunk count, total bytes, and assembly deadline. It atomically installs the snapshot only after every chunk and the BLAKE3 digest validate. A disconnect, timeout, digest mismatch, or revision gap discards staging and requests a fresh snapshot. Deltas received while staging are buffered only within a small bound or trigger resnapshot.

Snapshot pages, deltas, and acknowledgements use the Association reliable control stream. Coordinator revision provides state ordering and gap detection; Association control sequence provides bounded retransmission across a connection loss. Applying either a replayed control envelope or a replayed state revision is idempotent.

## 4. Sharded Entities

Each entity type declares a stable configuration:

```rust
EntityType::builder::<PlayerActor>("player")
    .shards(256)
    .hash_version(ShardHashVersion::Xxh3V1)
    .max_entities_per_shard(1_024)
    .max_buffered_messages_per_region(10_000)
    .max_buffered_bytes(64 * 1024 * 1024)
    .passivate_after(Duration::from_minutes(10))
    .rebalance(
        RebalancePolicy::weighted_least_load()
            .interval(Duration::from_secs(10))
            .min_relative_improvement(0.10)
            .min_shard_residence(Duration::from_minutes(2))
            .node_join_stability(Duration::from_secs(30))
            .cooldown(Duration::from_secs(30))
            .max_moves_per_round(4)
            .max_concurrent_moves(8),
    )
    .build()?;
```

The strategy ID/version and hard eligibility constraints are part of the entity-type configuration fingerprint. Operational thresholds and concurrency limits may be updated only through validated revisioned Coordinator configuration; an update affects future proposals and never rewrites an active plan or bypasses its recorded policy version.

The actor type fixes its business key:

```rust
impl ShardedActor for PlayerActor {
    type Key = PlayerId;
}

impl EntityKey for PlayerId {
    fn to_entity_id(&self) -> EntityId {
        EntityId::from_bounded_bytes(self.0.to_be_bytes())
    }

    fn try_from_entity_id(entity_id: &EntityId) -> Result<Self, EntityKeyDecodeError> {
        let bytes: [u8; 8] = entity_id
            .as_bytes()
            .try_into()
            .map_err(|_| EntityKeyDecodeError::invalid_length(8, entity_id.len()))?;
        Ok(PlayerId::new(u64::from_be_bytes(bytes)))
    }
}
```

`EntityId` is at most 256 canonical bytes. `ShardHashVersion::Xxh3V1` is exactly `xxh3_64_with_seed(entity_id_bytes, 0x4c41_5454_4943_4531)`, followed by modulo configured shard count. Rust `Hash`, `DefaultHasher`, platform endianness, type names, and declaration order are forbidden inputs. Shard count, canonical key encoding, hash version, and seed are persistent compatibility decisions; changing any requires an explicit full-stop shard migration.

A shard record contains the owner node/incarnation, assignment generation, state (`unassigned`, `starting`, `active`, `handoff`, or `stopped`), and optional target. Records are retained rather than deleted during ordinary movement so generations remain monotonic.

## 5. ShardRegion Data Plane

Every node using or hosting an entity type has one local ShardRegion. It runs proxy-only without the host role and may own local Shards only when eligible. It:

1. computes the shard ID;
2. uses the latest Coordinator-supplied shard table;
3. delivers locally or forwards over remoting;
4. activates the entity on first delivery at the owning shard;
5. buffers bounded messages while assignment or handoff is unresolved;
6. rejects overflow and expired asks explicitly.

Initial guardrails are 1,024 buffered messages per shard, 10,000 per region, 64 MiB per region, and a message residence limit of `min(message deadline, 30 s)`.

## 6. Allocation and Rebalancing

Allocation decides the owner of an unassigned shard. Rebalancing decides whether an already active shard should move; it never changes ownership directly. Both produce validated placement decisions, while every actual move uses the handoff/claim state machine in section 7.

### 6.1 Strategy Contract

The Coordinator owns one strategy instance per entity type. Strategy code is pure with respect to cluster state: it receives an immutable bounded view, performs no etcd/network I/O, and returns a proposal that the Coordinator must validate before persistence.

```rust
pub trait ShardAllocationStrategy: Send + Sync + 'static {
    fn allocate(
        &self,
        request: &AllocationRequest,
        view: &PlacementView,
    ) -> Result<AllocationDecision, AllocationError>;

    fn rebalance(
        &self,
        trigger: RebalanceTrigger,
        view: &PlacementView,
        limits: &RebalanceLimits,
    ) -> Result<RebalanceProposal, AllocationError>;
}
```

```text
PlacementView {
  coordinator_term and state_revision
  eligible nodes with incarnation, roles, zone, protocol support and configured capacity
  latest bounded NodeLoad and per-shard ShardLoad samples with sequence and age
  current shard owner/generation/residence time
  active claims, handoffs, drains and rebalance moves
}

RebalanceProposal {
  reason
  base_revision
  moves: [shard, expected_generation, source, target, estimated_improvement]
}
```

For the same `PlacementView`, policy version, trigger, and limits, a strategy must return the same ordered proposal. The Coordinator revalidates that every source/generation still matches, every target is Ready and eligible, no shard is already moving, projected target capacity including pending reservations is available, limits remain available, and the proposal was calculated from an acceptable revision. A strategy failure or invalid proposal skips the round and emits telemetry; it never partially mutates placement.

### 6.2 Load and Capacity Model

`NodeHello` supplies relatively stable hard inputs: roles, zone/failure-domain attributes, supported protocols/entity types, and positive capacity units. Ready nodes periodically send bounded latest-value `NodeLoadReport` control messages containing a boot-scoped sample sequence, active shard/entity counts, mailbox pressure, and processing/CPU load summaries. Shards may additionally report a bounded EWMA weight; absent shard measurements use weight `1`. Load reports are ephemeral and are not replayed by reliable control delivery; reconnect or leader change requires a fresh higher-sequence sample.

Load reports are advisory, not authority. They are kept in Coordinator memory, bounded by live nodes/shards, and are not written to etcd on every sample. A new leader waits for fresh samples before automatic optimization. Stale/missing load excludes a node as an automatic rebalance target unless the strategy explicitly falls back to configured capacity for necessary allocation/recovery. Reports cannot override role, protocol, drain, claim, or health eligibility.

The built-in `WeightedLeastLoad` strategy minimizes normalized load:

```text
normalized_node_load = sum(hosted_shard_weights) / configured_capacity_units
```

It uses deterministic node/shard ordering for ties, considers the post-move source and target scores, and proposes a move only when the configured absolute/relative improvement threshold is met. Custom strategies may add zone affinity, pinning, data locality, or business costs, but return the same validated proposal type.

### 6.3 Triggers and Priority

Placement work has explicit reasons and priority:

```text
1. Recovery: dead, fenced, or ineligible owner; restore availability after old authority is invalid.
2. Drain: evacuate a draining node.
3. Manual: authenticated operator request, optionally constrained to source/target/shards.
4. Automatic: periodic tick, stable node join, or material capacity/load change.
```

Recovery and drain bypass balance improvement and shard-residence thresholds but still obey fencing and bounded concurrency. Manual moves bypass improvement only when the command explicitly requests it. Automatic rebalancing requires a healthy reconciled Coordinator, fresh enough inputs, minimum shard residence, node-join stability delay, cluster cooldown, and improvement hysteresis. Coordinator degradation, leadership reconciliation, incompatible schema/protocols, or an active entity-type barrier pauses new automatic plans.

### 6.4 Persisted Plan and Limits

The Coordinator converts an accepted proposal into a persisted, term/revision-fenced plan before starting a move:

```text
RebalancePlan {
  plan_id
  entity_type
  reason
  coordinator_term
  base_revision
  policy_id and policy_version
  status: Planned | Running | Completed | Cancelled | Failed
  moves: [
    shard_id
    expected_generation
    source_incarnation
    target_incarnation
    status: Pending | Handoff | Completed | Cancelled | Failed
  ]
}
```

Only one move may be active for a shard, and at most one automatic plan may be active per entity type. Pending moves reserve their estimated weight against target capacity so concurrent plans cannot overcommit a node. Limits apply independently to proposals and execution: maximum moves per round, and maximum concurrent moves per cluster, entity type, source node, and target node. The current leader measures cooldown and minimum residence with local monotonic timers derived from reconciled transition state, never from caller clocks. After leader failover, any interval whose elapsed duration cannot be proven is conservatively restarted from reconciliation time. Active plan count and retained completed-plan history are bounded by `maximum_completed_plan_history`. Recovery and each terminal transition compact only Completed/Cancelled/Failed records, ordered by authoritative base revision and plan ID. Deletion is revision-conditional in memory and etcd; active or handoff-linked plans are never retention candidates.

Higher-priority recovery/drain work may cancel or preempt lower-priority moves only while they remain `Pending`; their reservations are then released idempotently. A move that has entered `Handoff` owns the slot's `active_move` marker and must complete or recover forward before another plan may target that shard.

A new Coordinator leader reconstructs plans and slot handoffs from etcd after claim reconciliation. It cancels stale `Pending` moves whose base assumptions no longer hold and idempotently resumes already persisted handoffs from their authoritative slot state. Cancellation stops only moves that have not crossed into handoff; once source invalidation/drain begins, the move is completed or recovered forward rather than rolled back to an ambiguous owner.

### 6.5 Singleton Boundary

Singletons reuse the shared placement move, drain, claim, and fencing machinery but do not participate in periodic load balancing. They move only for owner failure/ineligibility, node drain, configuration change, or an authenticated manual relocation, using singleton-specific eligibility and lifecycle rules.

## 7. Handoff

A controlled shard handoff uses the same node-level revision stream as normal assignment updates. Its barrier is complete for the affected entity type, not cluster-global: it contains the live node sessions whose `NodeHello` subscription says they host or proxy that entity type and therefore may cache its shard home. Nodes unrelated to the entity type cannot block the handoff.

```text
Coordinator transactionally links optional plan/move ID and persists BeginHandoff(next generation, target)
  -> publishes StateDelta(handoff revision)
  -> every subscribed Region in the frozen barrier set invalidates home, buffers, and AppliedRevision(revision)
  -> a node joining later receives the Handoff state in its snapshot before becoming Ready
  -> a node adding the entity-type subscription during handoff installs that snapshot slice before routing
  -> a failed barrier member leaves only through membership/lease fencing, not by handoff timeout alone
  -> Coordinator sends DrainShard to source
  -> source stops admission, drains handlers, and stops entities
  -> source sends ShardDrained; Coordinator revokes or independently proves expiry of old claim
  -> Coordinator persists next-generation claim and sends ClaimGranted to target
  -> target installs the grant, starts Shard, and sends ShardReady
  -> Coordinator persists Active and publishes the next StateDelta
  -> Regions apply the revision and flush unexpired bounded buffers
```

The first version does not transfer in-memory actor state. Reactivated entities load state through business hooks when applicable. Stateful migration therefore needs business-level save/load correctness.

`Actor::stopping` failure blocks voluntary `ShardDrained` and therefore blocks graceful handoff while the old claim is valid. It is observable and follows a bounded retry/operator policy rather than becoming an invisible permanent state. It never overrides fencing: explicit claim loss or local grant expiry first stops all new mailbox admission, then performs best-effort shutdown and raises `StateLossPossible` if cleanup/save fails. After the old claim is independently invalid, single-owner safety permits a new owner even when the crashed/fenced actor could not save; business persistence must be crash-safe if that recovery is required.

## 8. Claims, Failure, and Coordinator Outage

A shard owner may serve only while its locally installed grant matches the Coordinator's lease-backed claim, leadership term, assignment generation, grant sequence, and node incarnation.

```text
ClaimGranted {
  coordinator_term,
  assignment_generation,
  grant_sequence,
  ttl,
}
```

The wire carries a duration, never a remote wall-clock timestamp. On receipt, the owner computes `local_deadline = monotonic_now + ttl - safety_margin`. Initial defaults are 15 s TTL, renewal every 5 s, and 2 s safety margin. A renewal must have the same generation, a nondecreasing grant sequence, and the current or a higher reconciled Coordinator term. A higher term supersedes the old grant only after the new leader reconciles the etcd claim. Process suspension or delayed renewal that crosses the local deadline fences admission immediately before shutdown.

During temporary Coordinator unavailability:

- known active shard routes continue while the destination association and claim remain valid;
- local owners continue serving already claimed shards;
- no new assignment, relocation, or singleton failover is invented;
- no automatic rebalance plan is created or advanced before leadership/claim reconciliation;
- unknown shards and exhausted buffers fail with `CoordinatorUnavailable` or `ShardUnavailable`;
- recovery reconciles claims before buffered traffic resumes.

This keeps a short control-plane outage from stopping healthy known data paths without permitting split ownership. It is deliberately bounded: a known route stops when its local grant deadline expires even if the data connection is healthy. Production configuration must therefore satisfy `Coordinator leader recovery objective < claim TTL - safety margin`; increasing the TTL extends outage tolerance but also increases the worst-case crash-failover delay.

## 9. Passivation

Entity passivation is local lifecycle management, not a placement change. The shard keeps ownership; only the entity activation stops. The next message may activate a new instance.

Passivation does not write etcd or increment shard generation. `EntityRef` remains usable, while every `watch_current(EntityRef)` bound to the old activation receives `Terminated`; observing a later activation requires a new watch.

## 10. Cluster Singletons

Singleton kinds are declared at service startup. A SingletonProxy tracks the Coordinator assignment and forwards through remoting. Singleton routing and lifecycle remain separate from sharding, while ownership delegates to the shared placement-slot engine and uses the same term/generation/sequence/TTL grant and local monotonic fencing rules as shard ownership.

On failure, the Coordinator waits until the old claim is invalid, selects a compatible node, publishes the next generation, and activates the new singleton. The first version supports fixed singleton kinds only; it does not retain the old Explicit Placement model.

## 11. Drain and Shutdown

Graceful drain proceeds in this order:

1. mark node draining, stop external admission, and reject new placements;
2. hand off owned shards;
3. relocate singleton ownership;
4. drain actor mailboxes and pending asks within a deadline;
5. terminate concrete actors and notify watchers;
6. close remoting associations and revoke membership lease.

Forced shutdown relies on lease expiry and claim fencing. Operators must be able to observe which shards or singletons still block a drain.

## 12. Migration Constraint

The old generated gRPC, Explicit Placement, and Direct Link protocols are not wire-compatible with this design. Cutover is full-stop: stop old nodes, migrate/clear incompatible framework metadata under an explicit schema-generation procedure, then start only new-protocol nodes. Mixed clusters are unsupported.
