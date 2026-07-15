# 03. Membership, Placement Domains, Sharding, and Singletons

> Control-plane state and data-plane behavior for logical actor references.
> Back to: [architecture index](README.md)

---

## 1. Control-Plane Boundary

The membership leader is the sole writer of global exact-node lifecycle. Each `PlacementDomainId`
has one independent lease-backed placement leader that is the sole writer of that domain's
participants, configuration, shard/singleton assignments, claims, handoffs, plans, and admin
operations. A supervised `CoordinatorHost` may campaign for several scopes; losing one scope does
not stop another.

A node discovers one membership scope and one placement scope for every domain it hosts or proxies.
After authenticated association, all control traffic uses actor remoting.

Normal `ActorRef`, `EntityRef`, and `SingletonRef` messages never go through etcd.

## 2. etcd Metadata

Recommended logical keys:

```text
/lattice/<cluster>/schema_generation
/lattice/<cluster>/membership/{leader,term,state_revision}
/lattice/<cluster>/membership/members/<node_id>
/lattice/<cluster>/domains/<domain>/{leader,term,state_revision}
/lattice/<cluster>/domains/<domain>/members/<node_id>
/lattice/<cluster>/domains/<domain>/entity_types/<entity_type>
/lattice/<cluster>/domains/<domain>/shards/<entity_type>/<shard_id>
/lattice/<cluster>/domains/<domain>/shard_claims/<entity_type>/<shard_id>
/lattice/<cluster>/domains/<domain>/singletons/<singleton_kind>
/lattice/<cluster>/domains/<domain>/singleton_claims/<singleton_kind>
/lattice/<cluster>/domains/<domain>/rebalances/<plan_id>
/lattice/<cluster>/domains/<domain>/admin/<operation_id>
```

There are no per-entity placement keys and no concrete actor-path keys. Concrete `ActorRef` identity lives in remoting/runtime state; logical entity activation is local to its shard owner.

Generation 5 is the only runtime schema. `MembershipVersion` orders membership; domain-qualified
`PlacementVersion` orders each placement stream. Generation 4, `migrating-to-5`, or a different
durable-limit record prevents startup rather than guessing compatibility.

Shard and Singleton remain different public/runtime concepts, but their distributed authority is implemented by one internal placement-slot engine:

```text
PlacementSlotKey = Shard(domain, entity_type, shard_id) | Singleton(domain, singleton_kind)

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

## 3. Membership and Placement-Domain Leadership

Nodes first register through a membership session with bounded `MemberHello` (exact `NodeKey`, roles,
failure-domain attributes, protocol catalogue, and remoting capabilities). Persisted `MemberRecord`
contains that hello, `Joining | Up | Leaving`, `MembershipVersion`, and lease ID. Removed is an
ordered exact-incarnation event, not a stored status.

Registration persists `Joining` and sends a membership snapshot. `JoinReady(snapshot_version)` from
the same Association performs the only `Joining -> Up` transition. A Joining snapshot never opens
readiness; the local reducer must install its exact `Up` delta. Replayed hello/join-ready commands
are idempotent.

After global `Up`, each explicit domain session sends bounded `PlacementDomainHello`: domain/config
fingerprint, positive quota, hosted configurations, proxy subscriptions, and constraints. The
domain leader persists `DomainMemberRecord` and allocates only when both exact global and domain
records are `Up`. A mismatch rejects only that domain.

Membership and placement snapshots/deltas use distinct reducers and error families. Scope/domain
mismatch is rejected before mutation. A higher term requires a full snapshot for only that scope;
lower-term snapshots, events, barriers, and acknowledgements are rejected.

A new placement-domain leader:

1. obtains a higher election term;
2. reconstructs only its domain members/configuration/assignments/claims/plans/handoffs;
3. verifies exact authoritative global `Up` records;
4. reconciles claim holders before issuing mutations;
5. resumes required recovery/drain work, then allocation and automatic rebalancing only after reconciliation and fresh-input checks.

Every authoritative mutation is a named domain transaction comparing an exact
`PlacementLeaderGuard`, domain revision, global member, domain member/config, and operation-specific
slot/claim/plan predicates. Typed guards make cross-plane use unrepresentable; scoped keys and
contract tests reject cross-domain mutation. Runtime effects occur only after commit.

Election begins with read-only bounded inventory. Reconciliation then adopts a matching older-term
claim without changing owner or generation, fences active records whose claim is absent, and resumes
persisted handoff phases. Periodic reconciliation uses bounded pages/work and exposes cursor backlog,
oldest work, last success, and quarantine details. Claim lease expiry is an external fencing event:
it may temporarily leave an active record claimless, but recovery must fence it and may reinstall
only the same owner until previous authority invalidity is proven.

Placement leadership does not itself grant serving authority. The domain leader persists a claim and
sends bounded `ClaimGranted`; owners validate domain, slot, generation, sequence, and monotonic TTL.
Runtime nodes never acquire claims directly from etcd.

### 3.1 Revisioned State Snapshot

Membership and every placement domain have separate bounded snapshot streams. A domain snapshot
contains only that domain's configurations, participants, slots, claims, and plans. Adding a Region
or SingletonProxy installs its domain slice before Ready. Large snapshots use:

```text
SnapshotBegin(snapshot_id, MembershipVersion|PlacementVersion, chunk_count, total_bytes, blake3_digest)
SnapshotChunk(snapshot_id, index, bytes)  // at most 192 KiB payload
SnapshotEnd(snapshot_id)
AppliedRevision(PlacementVersion)
```

The receiver stages chunks outside the live routing table, rejects duplicate/out-of-range chunks, and enforces configured chunk count, total bytes, and assembly deadline. It atomically installs the snapshot only after every chunk and the BLAKE3 digest validate. A disconnect, timeout, digest mismatch, or revision gap discards staging and requests a fresh snapshot. Deltas received while staging are buffered only within a small bound or trigger resnapshot.

Snapshot pages, deltas, and acknowledgements use reliable Association control. Scope version provides
ordering/gap detection; Association sequence provides bounded retransmission. Replay is idempotent.

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

For the same domain `PlacementView`, policy version, trigger, and limits, a strategy must return the same ordered proposal. The placement-domain leader revalidates sources, generations, eligible targets, pending domain capacity, concurrency limits, and base revision. A failure skips the round and never partially mutates placement.

### 6.2 Load and Capacity Model

`MemberHello` supplies global roles, failure-domain attributes, protocols, and remoting capabilities.
Each `PlacementDomainHello` supplies that domain's hosted/proxied types, constraints, and explicit
positive capacity units. Ready domain participants send bounded latest-value load reports. Two
domains never consume one unspecified global capacity pool.

Load reports are advisory, not authority. They are bounded in domain-leader memory and are not
persisted on every sample. They cannot override global/domain membership, protocol, drain, claim,
or health eligibility.

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

Recovery and drain bypass balance improvement and residence thresholds but still obey fencing and
bounded concurrency. Automatic rebalancing requires a healthy reconciled domain leader and fresh
inputs. Failure or reconciliation in another domain does not pause this domain.

### 6.4 Persisted Plan and Limits

The placement-domain leader converts an accepted proposal into a persisted, domain/term/revision-fenced plan before starting a move:

```text
RebalancePlan {
  plan_id
  entity_type
  reason
  domain and coordinator_term
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

Only one move may be active for a shard, and at most one automatic plan may be active per entity
type. Pending moves reserve domain capacity. Limits apply per domain/entity/source/target plus an
optional CoordinatorHost-wide cap; revisions and write sets are never shared across domains.
Cooldown, history, and deletion remain bounded and revision-conditional.

Higher-priority recovery/drain work may cancel or preempt lower-priority moves only while they remain `Pending`; their reservations are then released idempotently. A move that has entered `Handoff` owns the slot's `active_move` marker and must complete or recover forward before another plan may target that shard.

A new domain leader reconstructs only its plans and handoffs after claim reconciliation. It cancels
stale `Pending` moves and resumes persisted handoffs forward; another domain's state is neither read
nor changed.

### 6.5 Singleton Boundary

Singletons reuse the shared placement move, drain, claim, and fencing machinery but do not participate in periodic load balancing. They move only for owner failure/ineligibility, node drain, configuration change, or an authenticated manual relocation, using singleton-specific eligibility and lifecycle rules.

## 7. Handoff

A controlled shard handoff uses its domain's revision stream. Its barrier contains only live sessions
whose `PlacementDomainHello` subscribes to the affected entity type. Unrelated domains and types
cannot block it.

```text
Domain leader transactionally links optional plan/move ID and persists BeginHandoff(next generation, target)
  -> publishes domain delta(handoff revision)
  -> every subscribed Region in the frozen barrier set invalidates home, buffers, and AppliedRevision(revision)
  -> a node joining later receives the Handoff state in its snapshot before becoming Ready
  -> a node adding the entity-type subscription during handoff installs that snapshot slice before routing
  -> a failed barrier member leaves only through membership/lease fencing, not by handoff timeout alone
  -> domain leader sends DrainShard to source
  -> source stops admission, drains handlers, and stops entities
  -> source sends ShardDrained; domain leader revokes or proves expiry of old claim
  -> domain leader persists next-generation claim and sends ClaimGranted to target
  -> target installs the grant, starts Shard, and sends ShardReady
  -> domain leader persists Active and publishes the next domain delta
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

This keeps a short control-plane outage from stopping healthy known data paths without permitting
split ownership. It is deliberately bounded: a known route stops at its local grant deadline.
Production configuration must satisfy `placement-domain leader recovery objective < claim TTL -
safety margin`; increasing TTL extends outage tolerance and worst-case crash-failover delay.

## 9. Passivation

Entity passivation is local lifecycle management, not a placement change. The shard keeps ownership; only the entity activation stops. The next message may activate a new instance.

Passivation does not write etcd or increment shard generation. `EntityRef` remains usable, while every `watch_current(EntityRef)` bound to the old activation receives `Terminated`; observing a later activation requires a new watch.

## 10. Cluster Singletons

Singleton kinds require an explicit domain. A SingletonProxy tracks that domain's assignment and
forwards through remoting. Ownership uses the shared domain placement-slot engine while public
singleton semantics remain distinct from sharding.

On failure, the domain leader waits until the old claim is invalid, selects a compatible domain
member, publishes the next generation, and activates the new singleton.

## 11. Drain and Shutdown

Graceful drain aggregates all joined domains:

1. stop new admission and begin every domain drain concurrently within bounds;
2. independently hand off/fence shards and singletons in each domain;
3. acknowledge each domain completion without undoing completed domains;
4. stop activations and finish bounded work;
5. remove global membership only after every required domain completion;
6. at deadline, fence unfinished domains independently and terminate.

Forced shutdown relies on lease expiry and claim fencing. Operators must be able to observe which shards or singletons still block a drain.

## 12. Migration Constraint

Generation 4 is not wire/storage compatible with generation 5. Cutover is full-stop: stop old
processes, require a complete explicit type-to-domain mapping, run the resumable offline migration,
verify fenced ownership/scoped counters, then start only generation-5 processes. Mixed clusters and
automatic startup migration are unsupported.
