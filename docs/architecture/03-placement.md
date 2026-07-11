# 03. Placement and HA

> Placement model, etcd metadata, RouteResolver, Coordinator, activation, singleton, migration, drain, dynamic scaling, graceful shutdown, crash handling, and watch.  
> Back to: [architecture index](README.md)

---

## 11. Placement Model

lattice supports three placement modes.

### 11.1 Virtual Shard Placement

Virtual shards are used for large numbers of lightweight actors such as player, guild, inventory, or session-like actors.

```text
actor_id -> virtual_shard_id: stable hash
virtual_shard_id -> owner instance: Coordinator assignment
```

Properties:

```text
actor_id to shard is stable.
shard to instance can change during rebalance.
owner changes increment epoch.
scale-out does not force-migrate Running actors by default.
only idle or eligible shards move automatically unless policy says otherwise.
```

Assignment is not a fixed enum. It is implemented through a trait with default strategies:

```rust
#[async_trait::async_trait]
pub trait VirtualShardAssigner: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    async fn plan(
        &self,
        input: VirtualShardAssignInput,
    ) -> Result<VirtualShardAssignPlan, PlacementError>;
}
```

Default implementations may include rendezvous hashing, bounded-load rendezvous hashing, static range assignment, and gradual rebalance.

### 11.2 Explicit Actor Placement

Explicit placement is used for heavy actors such as world, room, zone, or any actor where owner activation needs a control-plane decision.

```text
(actor_kind, actor_id) -> owner instance + epoch + lease
```

If no owner exists, the RouteResolver asks the Coordinator to activate one.

### 11.3 Singleton Placement

Cluster singleton placement guarantees exactly one active owner per singleton scope.

```text
(singleton_kind, scope) -> owner instance + owner boot incarnation + epoch + lease
```

Singletons are for low-frequency control-plane or global workflow actors. They should not be used as high-frequency player request entry points.

---

## 12. etcd Metadata

etcd is an independent placement store. It is not the business database.

Recommended prefix:

```text
/lattice/{cluster}/
```

Runtime keys:

```text
/logic/instances/{service_kind}/{instance_id}
/logic/vshards/{service_kind}/{actor_kind}/{shard_id}
/logic/actors/{service_kind}/{actor_kind}/{actor_id}
/logic/activation_locks/{service_kind}/{actor_kind}/{actor_id}
/logic/singletons/{service_kind}/{singleton_kind}/{scope}
/logic/singleton_locks/{service_kind}/{singleton_kind}/{scope}
/authority/epoch_floors/v1/actors/{service_kind}/{actor_kind}/{actor_id}
/authority/epoch_floors/v1/vshards/{service_kind}/{actor_kind}/{shard_id}
/authority/epoch_floors/v1/singletons/{service_kind}/{singleton_kind}/{scope}
```

### 12.0 Deployment Bootstrap

A minimal deployment does not manually prewrite `/logic` placement keys. It only needs:

```text
etcd cluster endpoints and credentials
cluster prefix
service runtime config
optional static bootstrap config
```

Instances register themselves at startup. Coordinators create and update runtime placement keys.

Authenticated production connections keep secret bytes out of the bootstrap tree. The source-compatible `EtcdPlacementStoreConfig` still contains only the prefix, endpoints, and TTLs; `EtcdConnectionOptions` separately selects an `EtcdPasswordAuthentication` whose password comes from an absolute regular file. The file read is timeout- and size-bounded, accepts one trailing LF or CRLF, and rejects empty, overlong, invalid-UTF-8, or NUL-bearing values. Connection sections reject unknown fields, and store/connection/client `Debug` output never includes endpoints, usernames, password paths, passwords, or tokens. Endpoint URL userinfo is rejected.

```toml
[placement_store]
key_prefix = "/lattice/prod"
endpoints = ["https://etcd.internal.example:2379"]
instance_lease_ttl_secs = 30
activation_lock_ttl_secs = 30

[placement_store.connection]
token_refresh_interval_secs = 30
# Optional for a private CA; platform roots remain enabled.
ca_file = "/run/secrets/lattice-etcd-ca.pem"

[placement_store.connection.authentication]
username = "placement-authority"
password_file = "/run/secrets/lattice-etcd-password"
```

Password authentication requires explicit HTTPS endpoints and the platform trust roots. An optional absolute-path CA file is read with the same timeout and regular-file rules and a 1 MiB limit. `from_config()` requires the authenticated connection section and fails startup when it is missing, partial, or contains unknown fields. Plaintext authentication exists only through a conspicuously named programmatic escape restricted to explicit loopback URLs for disposable integration tests; it cannot be enabled by the serialized connection section. The separately named `dangerously_connect_unauthenticated` API is likewise only for deliberate local development.

Production etcd must use its signed JWT token mode; the server's stateful `simple` token is suitable only for disposable development. The configured refresh interval must remain shorter than the server JWT TTL with enough allowance for clock skew and the 10-second authentication deadline; the client cannot introspect or enforce a remote server's TTL setting. Authenticated clients refresh one shared connection token at a configurable 1–240 second interval (30 seconds by default), single-flight concurrent refreshes, retain a canceled or failed attempt behind a separate one-second retry backoff, and attach the token to KV, watch, lease-grant, lease-keepalive, and background ownership-proof RPCs. Endpoint connect and Authenticate are each deadline-bounded. The disposable TLS test uses JWT tokens and proves refresh after token expiry for a live ownership watch.

The standalone coordinator accepts `LATTICE_ETCD_USERNAME` and `LATTICE_ETCD_PASSWORD_FILE` together, plus optional `LATTICE_ETCD_CA_FILE` and `LATTICE_ETCD_TOKEN_REFRESH_INTERVAL_SECS`. It otherwise fails closed unless `LATTICE_DANGEROUSLY_ALLOW_UNAUTHENTICATED_ETCD=true` is supplied exactly. That explicitly dangerous mode accepts loopback HTTP endpoints only.

The coordinator RPC listener has an independent fail-closed mTLS bootstrap. Production requires all of `LATTICE_COORDINATOR_TLS_CERT_FILE`, `LATTICE_COORDINATOR_TLS_KEY_FILE`, `LATTICE_COORDINATOR_TLS_CLIENT_CA_FILE`, and `LATTICE_COORDINATOR_TRUST_DOMAIN`. Each PEM path must be absolute and name a regular, nonempty file no larger than 1 MiB; reads have a five-second deadline and errors do not expose paths or material. TLS configuration is parsed before the process connects to etcd or campaigns for leadership. The only plaintext escape is the exact `LATTICE_DANGEROUSLY_ALLOW_UNAUTHENTICATED_COORDINATOR=true` setting with no TLS fields and a loopback bind.

Every `LatticeService` build must now receive both an explicit placement store and an explicit semantic `PlacementAuthority`. There is no private in-memory fallback; omitting either side fails startup. The operator remains responsible for configuring the read store against the same placement namespace used by the authority. Production code supplies a `TonicPlacementAuthority` built from its configured channel. The convenience `dangerously_use_in_process_placement` and its `DevelopmentInProcessPlacementAuthority` are intentionally named development escapes for single-process examples and tests; they hand a writable store to the service process and are not a production topology.

The semantic authority surface contains instance registration, keepalive, and state transition plus `ActivateActor`, `ActivateSingleton`, and `DrainInstance`. After tonic's bounded protobuf decode, the production service authenticates the verified leaf certificate before inspecting request fields or entering any placement mutation. It accepts exactly one canonical URI SAN of the form `spiffe://<trust-domain>/svc/<service>/instance/<instance>/incarnation/<incarnation>`; caller metadata and manually installed `PeerIdentity` extensions are not authentication. Registration requires the exact certificate identity, a bounded record, `Starting` state, and an absent instance key; the authority allocates the lease and revokes it if the atomic create loses. Existing-instance methods perform an exact service/instance lookup and admit only a certificate incarnation matching the current nonzero-leased record; actor/singleton activation additionally requires `Ready`. Thus a still-valid certificate from an earlier boot cannot renew, transition, activate, or drain after the instance ID is reused. Actor and singleton activation remain available to current authenticated workloads in the configured trust domain because cross-service resolution is expected. Liveness and drain are self-only, and drain still requires the expected current lease atomically. A separate operator/admin principal is not implemented, so remote peer-instance lifecycle operations are deliberately denied.

Authority calls have a five-second default deadline and reject zero or greater-than-sixty-second configured deadlines. Replies are accepted only when every requested identity is echoed exactly, owner/epoch/lease fields are valid, activation state is `Running`, and drain counts fit the local platform. Normal shutdown drain carries the runtime's expected instance incarnation and lease; the authority atomically changes state only if service, instance, incarnation, and lease still match, so a stale process cannot drain a replacement that reused its instance ID. The same four-part backend CAS guards `Ready` and `Stopping`; incarnation is independent of the numeric lease and still fences if a broken or restored backend reuses that lease value. State CAS permits idempotent repeats and only forward lifecycle edges (`Starting -> Ready`, `Ready -> Draining/Stopping`, `Draining -> Stopping`, `Stopping -> Dead`), preventing a current credential from resurrecting a stopping instance. The service performs no blind pre-drain state write. Admin self-drain first reads the current incarnation and lease and submits the same fenced operation. Missing targets are RPC errors, while a valid drain with no replacement is an explicit identity-, incarnation-, lease-, and count-validated successful-reply outcome. Generic transport or proxy `RESOURCE_EXHAUSTED` errors remain failures and are never interpreted as graceful shutdown. Explicit-actor route resolution then additionally requires a `Ready` owner whose instance lease exactly matches the placement record. Direct Link target resolution enforces the same state and lease lineage. Singleton records use a separate owner lease and also persist the selected instance's boot incarnation. Routing and the local ownership gate require a `Running` record whose owner ID and incarnation match the current `Ready` instance; a replacement process therefore cannot inherit an old singleton merely by reusing its instance ID. Renewal crosses authenticated semantic authority in batches of at most 4,096 complete record claims. The authority validates the current `Ready` boot, exact stored record equality, running state, and unique key/lease for the entire batch before any keepalive, then echoes an exact count. Real-etcd expiry and allocation-failure cleanup remain part of the dedicated lifecycle work.

This API boundary is necessary but not yet a complete reclamation boundary. Real tonic coverage proves create-only registration, current-incarnation instance and singleton renewal, lifecycle transition, accepted activation/drain identity, all-before-renew duplicate rejection, and rejection of missing certificates, untrusted issuers, wrong server trust/host, plaintext, malformed or wrong-domain SPIFFE IDs, a different workload spoofing a target, and a stale certificate for the same reusable service/instance without mutating placement. Certificate reload/revocation is not implemented and coordinator-to-runtime `LogicControl` remains plaintext. More importantly, the runtime's direct etcd read/watch connection can still issue Lease RPCs even though production service lifecycle call sites no longer use them. Until reads/watches cross a semantic proxy or equivalent protocol enforcement boundary, authenticated authority RPC is not sufficient evidence for tombstone reclamation.

The required deployment RBAC split must be provisioned in etcd; a caller-supplied profile label is not an authorization boundary. The authority identity receives `READWRITE` only for its cluster prefix. An ordinary runtime identity receives `READ` for the cluster prefix (the current ownership adapter watches the whole `/logic/` range) and, while direct liveness remains, `WRITE` for exactly its own `/logic/instances/{service_kind}/{instance_id}` key rather than an instances prefix. A static exact-key credential still does not prove process incarnation. Moreover, etcd does not key-authorize either `LeaseGrant` or `LeaseKeepAlive`: real-server coverage proves a runtime identity can allocate leases without a key write and can renew a known foreign lease even though it cannot mutate the attached key. Merely narrowing the Rust placement trait or routing framework call sites through a liveness RPC is therefore insufficient while the runtime retains a direct etcd connection. Runtime reads/watches must ultimately use a semantic proxy, or an equivalent network/L7 enforcement boundary that makes Lease RPCs unavailable. Runtime identities receive no placement, lock, leader, floor, retirement, generation, or seal write range. The repository's disposable real-etcd test provisions these roles and proves runtime reads and watches, exact-key liveness, denial of peer-instance/actor/floor mutations, atomic denial of a mixed actor/floor transaction, anonymous and bad-credential rejection, immediate denial after an already-connected legacy writer's range is revoked, success/failure/cancellation-bounded token refresh, and TLS rejection for an untrusted CA or wrong host. Production role provisioning and credential lifecycle remain deployment responsibilities. Authority `WRITE` still includes deletion, so its credential must stay outside service processes behind the semantic placement API; RBAC alone does not authorize tombstone reclamation.

### 12.1 Instance Registry

Each service process registers an `InstanceRecord`:

```rust
pub struct InstanceRecord {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub incarnation: InstanceIncarnation,
    pub advertised_endpoint: Uri,
    pub control_endpoint: Uri,
    pub version: String,
    pub state: InstanceState,
    pub capacity: InstanceCapacity,
    pub labels: BTreeMap<String, String>,
    pub lease_id: LeaseId,
}
```

`incarnation` is generated per service boot and must match the workload certificate. It is required persisted data: legacy records without it fail decoding rather than being treated as current. `LocalOwnershipSnapshot` is configured for that same value and fences if it observes a replacement incarnation, even if a backend incorrectly reuses the numeric lease ID.

States:

```text
Starting
Ready
Draining
Stopping
Dead
```

### 12.2 Explicit Placement Record

```rust
pub struct ActorPlacementRecord {
    pub actor_kind: ActorKind,
    pub actor_id: ActorId,
    pub owner: InstanceId,
    pub epoch: Epoch,
    pub lease_id: LeaseId,
    pub state: PlacementState,
}
```

### 12.3 Activation Lock

Activation lock prevents concurrent owners:

```text
try create activation lock with short lease
select target instance
reserve the next durable epoch with CAS while checking the lock
ask target LogicControl.ActivateActor with the reserved epoch
target starts actor and confirms the reserved epoch
atomically commit the reservation and placement record while checking the lock
release activation lock
```

### 12.4 Epoch

Epoch is the fencing token. Every owner change increments it. Old owners must reject writes once they observe a newer epoch or lose their lease.

Each actor, virtual-shard, and singleton identity has a durable, non-leased epoch-floor record under `/authority/epoch_floors/v1`. A placement-record lease may delete the current owner record, but it must not delete or lower this floor. A new owner first reserves an epoch above both the current record and the durable floor with compare-and-swap, then commits that exact reservation and the placement record in one transaction. The placement record remains the ownership authority throughout reservation; a failed activation or commit may burn an epoch, but it never publishes an owner. Gaps are therefore valid and epochs must never be reused after deletion, lease expiry, or service-process restart.

`PlacementVersion` is an opaque per-write modification-revision token. It must not be implemented as an etcd key `version`, because key versions restart after deletion and permit an old pre-delete CAS token to match a recreated key.

Before advancing a floor, every hardened reservation and compatibility compare-and-put path validates the current record/floor lineage. A live record with no floor fails closed. When their epochs are equal, both keys must have the same modification revision because a hardened commit writes them in one transaction. When the floor is ahead after a burned reservation, its epoch and modification revision must both be newer than the record's. A lower floor or any other token relationship is corrupt or unproven and must not be repaired implicitly by a later write.

Ownership views carry an opaque proof bound to the complete placement record, including its owner, lease, state, epoch, and modification token. The snapshot transaction establishes revision `R`, then point-reads only the bounded live records' exact floor keys at `R`; it never scans the permanent floor namespace. Each watch revision `r` is held back until every selected placement upsert or delete has a floor read at `r`. Deletes must also match the view's cached key, previous modification token, and full `prev_kv`, and a floor modified by the delete transaction is invalid. The cache and an entire same-revision batch are staged atomically before publication. Before value decoding or capacity accounting, the etcd watch selects only the exact local-instance key and the requested service's actor, virtual-shard, and singleton ranges. Selected work in one revision is bounded by `2 * max_entries + 1`: at most the old and new live-key sets plus the local-instance key. Duplicate selected keys fail closed, while retained live-record capacity is checked only after the whole batch is staged, so a full-capacity delete-and-insert replacement is independent of event order.

The real-etcd handoff registers its `R+1` watch and waits for Created before declaring the snapshot usable. A following linearizable revision read is a no-gap barrier when etcd is still at `R`: no historical event exists, and the registered watch buffers every later mutation. If etcd has advanced, startup instead replays and proves the bounded historical stream until a progress response reaches at least that linearizable read revision; an older queued progress response is not readiness. Dropping the public ownership view cancels its adapter bridge and underlying server-watch task.

Real-etcd floor reads are split into bounded transaction chunks below the default etcd transaction-operation limit. Compaction or any missing, malformed, leased, miskeyed, or lineage-inconsistent proof terminates and fences the view without publishing a partial batch; a backend must never retry such a proof at the latest revision. Memory proof failures likewise close the affected stream, and deterministic views prune dead watchers and stop matching-key collection at `max_entries + 1`.

The local view records store observation separately from authorization: a complete record is retained as local-present, remote-present, or lifecycle-fenced, while absence records whether it came from an exact proof-bearing delete or a coherent resync snapshot. Instance loss or reincarnation changes only local authorization and never fabricates record absence. An absent identity can reappear only at a higher epoch and, during resync, with a record modification revision newer than the absence observation. Every absent observation remains capacity-accounted for now.

Backend final-cardinality accounting does not by itself authorize the local view to forget history. Physical reclamation additionally requires an opaque capability tied to a protected sealed cutover generation. That seal must prove exclusive hardened writers, complete atomic backfill, and permanent non-leased floor retention; it cannot be replaced by a configuration boolean. Etcd grants put and delete through the same `WRITE` permission, so strict floor nondeletion ultimately requires an enforcing placement-authority or proxy API that never exposes floor deletion to ordinary runtime credentials.

The lattice public API must not itself be a raw-etcd escape hatch. The arbitrary-key transport trait, key/value codec (including the `EpochFloor` variant), ownership adapter records, epoch transaction request types, and generic client-retaining store constructor are crate-private. External crates receive the typed placement-store surface, and compile-fail coverage proves that a `RealEtcdClient` cannot be used to call raw `delete` or construct a store while retaining the client. Integration fault injection uses a narrow hidden test client that can schedule typed mutation failures but exposes no key put/delete API. Ordinary-service local coordinators have been removed from activation and drain paths, but this type boundary is still not sufficient: a process that retains a direct etcd credential can use another etcd library. The remaining direct liveness credential must lose unrestricted Lease RPC access through the required credential/RBAC and semantic-proxy separation before a reclamation capability can be issued.

Service assembly, placement route resolvers, and the placement view installed in `ServiceContext` depend on the read/watch-only `PlacementReadStore`. `ReadOnlyPlacementStore` deliberately does not implement the writable `PlacementStore`, and executable lifecycle coverage runs a service through that restricted capability while a separate authority owns mutation. The public view exposes no lease grant/keepalive, instance upsert, activation, drain, compare-and-put, or lock methods; compile-fail tests cover that boundary. The private erased store handle likewise exposes no lease mutation: it only discovers a bounded set of current-boot singleton records, while instance and singleton renewal cross `PlacementAuthority`. This is a Rust capability boundary, not a network authorization boundary: the wrapper's concrete etcd read/watch connection nevertheless retains raw Lease protocol capability. Removing that direct connection through the read/watch proxy is required before reclamation authority can be issued.

This guarantee begins only after an identity has a floor written by a hardened writer or an upgrade backfill. Before rollout, stop every writer that does not maintain floors and prevent its credentials or protocol from writing the hardened namespace. Atomically CAS-backfill each live placement record and its floor at one modification revision, and reject any floor attached to a lease. An identity whose record was already deleted has no reconstructable last epoch; deployment must seed it from an authoritative source or prohibit its reuse. Floors cannot later be deleted as ordinary cleanup. Mixed old/new placement writers are not a supported rolling-upgrade mode.

### 12.5 Singleton Owner

Singleton owner records follow the same lease and epoch rules as explicit actors, but the key is `(singleton_kind, scope)`. Each record also stores `owner_incarnation`; this field is required persisted data and legacy records without it fail closed. A change of incarnation is an authority change even if owner ID and lease are unchanged, so it requires an epoch advance. Route resolution, local ingress authorization, and renewal all require it to match the current instance record.

---

## 13. PlacementStore Trait

```rust
#[async_trait::async_trait]
pub trait PlacementStore: Clone + Send + Sync + 'static {
    async fn get_instance(&self, key: InstanceKey) -> Result<Option<InstanceRecord>, PlacementError>;
    async fn list_instances(&self, service: ServiceKind) -> Result<Vec<InstanceRecord>, PlacementError>;
    async fn get_actor(&self, key: ActorPlacementKey) -> Result<Option<ActorPlacementRecord>, PlacementError>;
    async fn compare_and_put_actor(
        &self,
        key: ActorPlacementKey,
        expected: Option<PlacementVersion>,
        value: ActorPlacementRecord,
    ) -> Result<(), PlacementError>;
    async fn watch(&self, prefix: PlacementPrefix) -> Result<PlacementWatchStream, PlacementError>;
}
```

The first production adapter is etcd. Tests should use an in-memory fake.

---

## 14. RouteResolver

RouteResolver turns `(service_kind, actor_kind, route_key)` into `RouteTarget`.

```rust
#[async_trait::async_trait]
pub trait RouteResolver: Clone + Send + Sync + 'static {
    async fn resolve(&self, request: ResolveRequest) -> Result<RouteTarget, ResolveError>;
    async fn invalidate(&self, key: RouteCacheKey, reason: InvalidateReason);
}
```

Cache rules:

```text
route cache is local and best-effort.
cache hit must not contact etcd.
hard ttl bounds stale entries.
NOT_OWNER and FENCED invalidate immediately.
watch updates may refresh cache.
retry keeps the same request_id.
```

Resolve flows:

```text
explicit actor:
  cache -> placement record -> if missing call PlacementAuthority.ActivateActor
  -> require Running record + Ready instance + matching lease -> cache target

virtual shard:
  hash actor_id to shard -> shard assignment -> instance record -> cache target

singleton:
  singleton owner record -> if missing call PlacementAuthority.ActivateSingleton
  -> require Running record + Ready instance -> cache target
```

---

## 15. PlacementCoordinator

The Coordinator is deployed as a small replicated service with leader election. Only the leader writes placement decisions.

Coordinator APIs:

```text
ActivateActor
ActivateSingleton
DrainInstance
MigrateActor
RebalanceVirtualShards
InspectPlacement
```

`DrainInstance` requires the expected current instance lease. Administrative
callers resolve that lease immediately before the request; the backend state
transition is compare-and-set, not a read followed by a blind upsert.

Instance selection should consider:

```text
service kind
state == Ready
version compatibility
capacity and current load
labels and region affinity
anti-affinity
drain state
```

Coordinator is not in the normal business RPC path.

Ordinary services do not construct a local coordinator for route misses, Direct Link opens, admin drain, or shutdown drain. Those mutations cross the semantic authority interface. Only the explicitly named development adapter restores the old in-process topology.

---

## 16. LogicControl API

Each logic service exposes an internal control-plane RPC server:

```text
ActivateActor
PrepareMigrateOut
CommitMigrateOut
AbortMigrateOut
DrainLocalActors
WatchActor
UnwatchActor
NotifyOwnershipLost
InspectLocalState
```

This endpoint must use internal authentication and must not be exposed to Gateway or clients.

---

## 17. Actor Activation

Logic service startup:

```text
load BootstrapConfig
create placement/eventbus/config/telemetry/admin components
start business RPC server
start LogicControl RPC server
register InstanceRecord with lease
start placement watch
mark instance Ready after warmup
```

Activation on target:

```text
validate activation request and epoch
check local registry
run ActorFactory / ActorLoader
insert actor into registry only after successful creation
start mailbox loop
return success to Coordinator
```

If create/load fails, the runtime must not leave a zombie actor. Waiters are woken with an error and later activation may retry.

---

## 18. Cluster Singleton

Singleton activation:

```text
resolve singleton owner
if missing, Coordinator acquires activation lock
select target instance
target activates singleton actor
write owner record with lease and epoch
generated singleton client calls the owner directly
```

Failover:

```text
owner lease expires or node is declared down
Coordinator increments epoch
new owner activates
old owner is fenced if it comes back
```

Singleton handlers must check fencing before committing externally visible writes.

---

## 19. Migration and Drain

### 19.1 Instance Drain

Drain is used for scale-in, rolling update, and graceful shutdown.

```text
mark readiness=false
mark instance Draining
stop new activation and new service tasks
cancel or pause event subscriptions as configured
migrate or passivate actors
move singleton ownership
release placement leases after safe stop
close RPC servers
exit process
```

During drain, the instance keeps its lease until actors are safely handled. Otherwise other nodes may interpret the drain as a crash.

### 19.2 Actor Migration

Migration is a control-plane operation:

```text
PrepareMigrateOut on old owner
block or redirect new writes
save business state through business hook
activate on new owner
CAS placement to new owner with incremented epoch
CommitMigrateOut old owner
```

RPC behavior during migration:

```text
old owner may return NOT_OWNER or Migrating.
client invalidates route cache and retries if idempotent.
new owner must reject stale epoch.
```

### 19.3 Dynamic Scale-Out

Scale-out flow:

```text
new pod starts and registers InstanceRecord
Coordinator observes Ready instance
new activations may target the new instance
virtual shard assigner may gradually move eligible shards
Running actors are not force-migrated by default
```

### 19.4 Dynamic Scale-In

Scale-in flow:

```text
mark instance Draining
stop assigning new owners to it
drain or migrate actors and singleton ownership
wait for completion or operator policy
terminate pod after safe drain
```

### 19.5 Node Graceful Shutdown

SIGTERM, Kubernetes preStop, or admin shutdown should run the same graceful flow:

```text
readiness=false
enter Draining
keep lease alive during drain
stop new work
finish or reject in-flight work according to policy
stop actors through lifecycle hooks
release owner leases after successful stop
shutdown servers
```

### 19.6 Node Crash and Failover

On crash, `Actor::stopping` is not called. Recovery relies on:

```text
instance lease expiry
owner lease expiry
epoch increment
new owner activation
business state reload from business database
request_id/idempotency handling
event subscriber idempotency
```

This is why business state persistence belongs to business code, not lattice persistence.

### 19.7 Autoscaling Metrics

Useful metrics:

```text
actor count by kind and instance
mailbox depth and latency
activation rate and failure rate
route cache hit rate
NOT_OWNER count
shard load
gateway session count
rate-limit rejects
CPU, memory, and RPC latency
```

### 19.8 Correctness

```text
No two owners may commit writes for the same actor epoch.
All owner changes increment epoch.
Old owners must be fenced.
Route cache is advisory.
Drain must not release leases before safe stop.
Crash recovery must not depend on actor stop hooks.
```
