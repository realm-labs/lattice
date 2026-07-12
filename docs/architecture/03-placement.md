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
  claim,
}
```

The shared engine owns assignment persistence, term/generation validation, claim grant/renew/loss, local deadline fencing, drain, and replacement eligibility. `ShardRegion/Shard` and `SingletonProxy/SingletonManager` retain their distinct routing, activation, passivation, and public-reference semantics; they do not implement a second ownership algorithm.

## 3. Membership and Coordinator Leadership

Nodes register through their remoting Coordinator session. The Coordinator writes and renews lease-backed membership containing node ID, incarnation, remoting endpoint, roles, capacity, protocol capabilities, and drain state. The Coordinator leader record is also lease-backed and fenced by an election term.

A new leader:

1. obtains a higher election term;
2. reconstructs members, assignments, claims, and in-progress handoffs from etcd;
3. establishes remoting associations to live members;
4. reconciles observed claim holders before issuing new assignments;
5. resumes allocation only after reconciliation.

Coordinator leadership does not by itself grant permission to serve a shard or singleton. The leader persists a generation claim in etcd and sends the selected owner a bounded `ClaimGranted` control message. The owner may serve only while the matching grant remains valid according to its local monotonic deadline. Runtime nodes do not acquire or renew claims by connecting to etcd directly. Claim authority remains per placement slot; transport may batch renewals for many slots owned by one node without weakening per-slot generation or expiry checks.

### 3.1 Revisioned State Snapshot

The Coordinator session is node-level. `NodeHello` advertises the node incarnation, roles, capacity, hosted/proxied entity types, singleton eligibility/usage, and protocol catalogue. The snapshot contains cluster membership plus only the shard/singleton slices that the node is eligible to host or has subscribed to route. A node adding a Region or SingletonProxy subscription must install the corresponding snapshot slice before that component becomes Ready. The initial state may exceed the 256 KiB remoting frame, so it uses a bounded chunk protocol:

```text
SnapshotBegin(snapshot_id, revision, chunk_count, total_bytes, blake3_digest)
SnapshotChunk(snapshot_id, index, bytes)  // at most 192 KiB payload
SnapshotEnd(snapshot_id)
AppliedRevision(revision)
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
    .build()?;
```

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

## 6. Handoff

A controlled shard handoff uses the same node-level revision stream as normal assignment updates. Its barrier is complete for the affected entity type, not cluster-global: it contains the live node sessions whose `NodeHello` subscription says they host or proxy that entity type and therefore may cache its shard home. Nodes unrelated to the entity type cannot block the handoff.

```text
Coordinator freezes the subscribed Region-session barrier set and persists BeginHandoff(next generation, target)
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

## 7. Claims, Failure, and Coordinator Outage

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
- unknown shards and exhausted buffers fail with `CoordinatorUnavailable` or `ShardUnavailable`;
- recovery reconciles claims before buffered traffic resumes.

This keeps a short control-plane outage from stopping healthy known data paths without permitting split ownership. It is deliberately bounded: a known route stops when its local grant deadline expires even if the data connection is healthy. Production configuration must therefore satisfy `Coordinator leader recovery objective < claim TTL - safety margin`; increasing the TTL extends outage tolerance but also increases the worst-case crash-failover delay.

## 8. Passivation

Entity passivation is local lifecycle management, not a placement change. The shard keeps ownership; only the entity activation stops. The next message may activate a new instance.

Passivation does not write etcd or increment shard generation. `EntityRef` remains usable, while every `watch_current(EntityRef)` bound to the old activation receives `Terminated`; observing a later activation requires a new watch.

## 9. Cluster Singletons

Singleton kinds are declared at service startup. A SingletonProxy tracks the Coordinator assignment and forwards through remoting. Singleton routing and lifecycle remain separate from sharding, while ownership delegates to the shared placement-slot engine and uses the same term/generation/sequence/TTL grant and local monotonic fencing rules as shard ownership.

On failure, the Coordinator waits until the old claim is invalid, selects a compatible node, publishes the next generation, and activates the new singleton. The first version supports fixed singleton kinds only; it does not retain the old Explicit Placement model.

## 10. Drain and Shutdown

Graceful drain proceeds in this order:

1. mark node draining, stop external admission, and reject new placements;
2. hand off owned shards;
3. relocate singleton ownership;
4. drain actor mailboxes and pending asks within a deadline;
5. terminate concrete actors and notify watchers;
6. close remoting associations and revoke membership lease.

Forced shutdown relies on lease expiry and claim fencing. Operators must be able to observe which shards or singletons still block a drain.

## 11. Migration Constraint

The old generated gRPC, Explicit Placement, and Direct Link protocols are not wire-compatible with this design. Cutover is full-stop: stop old nodes, migrate/clear incompatible framework metadata under an explicit schema-generation procedure, then start only new-protocol nodes. Mixed clusters are unsupported.
