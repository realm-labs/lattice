# 05. Gateway and Operations

> External protocol adaptation, remoting security, observability, configuration, and common flows.
> Back to: [architecture index](README.md)

---

## 1. Gateway Boundary

Gateway receives business-specific client frames and turns them into typed actor messages.

```text
read external frame
  -> decode msg_id and payload
  -> authenticate session and apply rate limits
  -> build typed message and select ActorRef / EntityRef / SingletonRef
  -> tell or ask through the local actor runtime
  -> encode an optional reply to the external protocol
```

The Gateway never forwards opaque client bytes into an actor for a second decode. It does not expose the internal remoting protocol to clients or own domain state.

```rust
#[async_trait::async_trait]
pub trait ClientCodec: Send + Sync + 'static {
    async fn decode(&self, bytes: Bytes) -> Result<ClientFrame, GatewayError>;
    async fn encode(&self, frame: ServerFrame) -> Result<Bytes, GatewayError>;
}
```

Large route tables should be generated from a business-owned schema or loaded from validated configuration. A route binding selects a recipient kind, extracts its ID/path, chooses tell or ask, and declares a rate class.

## 2. Client Push

A Gateway connection is represented internally by a concrete actor reference to a session actor:

```rust
pub type GatewaySessionRef = ActorRef<GatewaySessionActor>;
```

Logic actors may store and use that serializable reference. Its node incarnation and activation ID make stale connections safe: reconnecting at the same logical session does not revive an old reference. Watching it reports connection termination through normal DeathWatch.

## 3. Security

Internal security is an association policy, not RPC middleware.

```rust
RemotingConfig::builder()
    .listen("0.0.0.0:25520".parse()?)
    .advertise("world-0.world:25520".parse()?)
    .security(RemotingSecurity::tls(
        TlsIdentity::from_pem_files("node.pem", "node-key.pem")?,
        TrustRoots::from_pem_file("cluster-ca.pem")?,
    ))
    .authorize(ClusterAuthorization::roles(["logic", "gateway"]))
    .build()?;
```

Plain TCP is allowed only inside an explicitly trusted isolated network. In both profiles the handshake validates cluster ID, node incarnation, protocol version, frame bounds, and peer authorization.

External client authentication produces an `AuthContext` carried in the actor envelope where required. It must not be confused with node identity.

## 4. Observability

Operators need structured inspection for:

- members, node incarnations, roles, and drain state;
- associations, reconnects, heartbeat state, lane depth, and dropped/rejected frames;
- actor paths, activation counts, mailbox depth, supervision, and termination;
- ShardRegions, ownership generations, claims, handoffs, buffers, and passivation;
- allocation strategy/version, node capacity/load sample age, normalized load, active RebalancePlans, move limits, cooldown, and last decision reason;
- singleton assignments, claims, and proxy buffers;
- pending asks, deadlines, `UnknownResult`, and remote failures;
- remote watches, orphan cleanup, and termination delivery.

Metrics must use low-cardinality labels. Actor paths, entity IDs, correlation IDs, and session IDs belong in sampled/redacted traces or bounded inspection responses, not metric labels.

Trace context propagates through Gateway bindings, actor envelopes, remoting frames, EventBus headers, and scheduler envelopes. A remote receive span links to the send context and records recipient kind, message type ID, association, queue latency, and handler outcome.

## 5. Admin Surface

`lattice-ops` exposes Rust inspector/command traits. An authenticated HTTP adapter may provide health, readiness, metrics, cluster summary, members, associations, shards, singletons, mailboxes, watches, allocation/rebalance plans, drain, and handoff commands.

Mutating cluster operations are sent to the Coordinator actor through remoting. The HTTP adapter must not mutate etcd directly. Operations are authorized, audited, bounded by deadlines, and safe to retry only when their command protocol declares idempotency.

Inspection responses expose machine-readable lifecycle state, Coordinator term/revision, node incarnation, assignment generation, active plan/move IDs and partial/stale markers. The Docker distributed-test harness waits on these bounded predicates; fixed sleeps and human-log matching are not correctness oracles.

Rebalance operations include inspect/explain, pause/resume automatic planning, trigger an immediate evaluation, submit an idempotent manual relocation, and cancel only plan moves that have not entered handoff. The API exposes why a proposal was accepted/rejected and which eligibility, freshness, hysteresis, cooldown, or concurrency rule applied; it never allows an operator to bypass claim fencing.

Operation IDs, status, typed result, `StateVersion`, and expiry metadata are stored durably with a
bounded fingerprint. Pause settings and manual-plan/member/plan mutations are committed atomically
with their operation result. Repeating any mutating operation during the configured count/age
retention window returns the original typed result; reusing an ID for different arguments is an
idempotency conflict. After compaction, clients must use a new operation ID.
Manual relocation persists a normal generation-conditional plan and enters the same handoff path as
automatic, drain, and recovery movement. Completed/cancelled/failed plan history is inspectable but
bounded by the Coordinator retention policy.

Inspection includes durable pause state, retained operation count, durable cardinality limits,
current `StateVersion`, reconciliation backlog/quarantine, and aggregated leadership-loss,
conflict, unknown-outcome, and capacity counts. Labels remain operation-family level and never carry
record IDs.

## 6. Configuration Example

```toml
[node]
cluster_id = "prod-game"
node_id = "world-0"
roles = ["logic", "world"]

[node.placement]
capacity_units = 100
zone = "cn-east-1a"

[remoting]
listen = "0.0.0.0:25520"
advertise = "world-0.world:25520"
max_frame_bytes = 262144
handshake_timeout_ms = 3000
heartbeat_interval_ms = 2000
bulk_stripes_per_association = 1
max_active_associations = 256
association_idle_timeout_secs = 60
max_total_outbound_bytes = 268435456

[remoting.queues]
control_messages = 1024
interactive_messages = 4096
bulk_messages_per_stripe = 8192
max_outbound_bytes_per_association = 16777216

[remoting.tls]
enabled = true
certificate_file = "/run/secrets/node.pem"
private_key_file = "/run/secrets/node-key.pem"
ca_file = "/run/secrets/cluster-ca.pem"

[coordinator]
bootstrap_etcd = ["https://etcd.internal:2379"]
key_prefix = "/lattice/prod-game"
snapshot_chunk_bytes = 196608
snapshot_max_bytes = 67108864
snapshot_assembly_timeout_secs = 5
claim_ttl_secs = 15
claim_renew_interval_secs = 5
claim_safety_margin_secs = 2

[placement.rebalance]
strategy = "weighted-least-load/v1"
interval_secs = 10
load_report_interval_secs = 5
load_sample_max_age_secs = 20
min_relative_improvement = 0.10
min_shard_residence_secs = 120
node_join_stability_secs = 30
cooldown_secs = 30
max_moves_per_round = 4
max_concurrent_cluster = 8
max_concurrent_entity_type = 4
max_concurrent_source = 2
max_concurrent_target = 2

[sharding.player]
shards = 256
hash_version = "xxh3-v1"
max_buffered_messages_per_shard = 1024
passivate_after_secs = 600

[event_bus.nats]
url = "nats://nats:4222"

[admin_http]
bind = "0.0.0.0:19090"
```

Only the Coordinator consumes general placement-store configuration. Other nodes use bootstrap access solely to locate and authenticate the Coordinator leader.

Cluster nodes configure candidate providers through `cluster_discovery` and a bounded
`ClusterJoinConfig`. Static, ConfigStore, DNS and Kubernetes EndpointSlice records are reachability
hints only. The authenticated Coordinator snapshot and revisioned member deltas populate the local
member directory; discovery and direct store reads are never used for business routing.

`LatticeService` exposes lifecycle and member snapshots plus bounded watches. `leave(deadline)`
closes admission, coordinates placement drain, completes exact-incarnation membership removal and
then stops remoting. `shutdown()` spends the configured leave budget and force-stops on expiry;
`force_shutdown()` immediately fences local work. The low-level `connect_peer(NodeIdentity)` API is
diagnostic transport access and cannot admit a member or make a service Ready.

## 7. Common Call Flows

### Exact actor activation

```text
actor holds remote ActorRef<SessionActor>
  -> association to referenced node incarnation
  -> resolve exact actor path + activation ID
  -> mailbox

stale node or activation -> StaleActivation/Terminated; never reroute by path
```

### First sharded entity message

```text
EntityRef<PlayerActor>.ask(GetProfile)
  -> local Player ShardRegion computes shard
  -> known owner or Coordinator assignment
  -> owning region activates PlayerActor if absent
  -> mailbox handles GetProfile and replies
```

### Shard handoff

```text
Coordinator starts handoff
  -> regions buffer within limits
  -> old owner drains and releases claim
  -> new owner acquires next generation
  -> regions update route and flush unexpired messages
```

### Automatic shard rebalance

```text
eligible hosts report bounded latest-value load summaries
  -> Coordinator builds immutable PlacementView
  -> entity allocation strategy proposes generation-conditional moves
  -> Coordinator validates freshness, eligibility, improvement and limits
  -> persists RebalancePlan
  -> each admitted move executes the normal shard handoff
  -> plan progress remains inspectable and recoverable after leader failover
```

### Singleton failover

```text
SingletonRef<Matchmaker>.tell(command)
  -> local proxy
  -> current claim holder
  -> on failure proxy buffers within limits
  -> Coordinator publishes next generation after old claim expires
  -> proxy flushes
```

### Coordinator temporarily unavailable

Known routes and valid claim holders continue. New placement, failover, and unknown routes wait within bounded buffers, then fail explicitly. No runtime node manufactures an owner.

## 8. Forbidden Patterns

```text
Do not expose remoting frames as a business API.
Do not create a second gRPC path for internal actor commands.
Do not address a replacement actor through a stale concrete ActorRef.
Do not query or mutate etcd for every message.
Do not let a load report or allocation strategy grant authority or bypass Coordinator validation.
Do not start automatic rebalance while the Coordinator is degraded, unreconciled, or using stale required load inputs.
Do not route normal data traffic through the Coordinator.
Do not use EventBus for single-owner commands or immediate replies.
Do not use unbounded association, proxy, shard, or mailbox buffers.
Do not automatically retry state-changing asks after UnknownResult.
Do not expose control actors to unauthenticated external clients.
Do not assume exactly-once delivery.
```
