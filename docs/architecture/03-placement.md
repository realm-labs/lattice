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
(singleton_kind, scope) -> owner instance + epoch + lease
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
/logic/actors/{actor_kind}/{actor_id}
/logic/activation_locks/{actor_kind}/{actor_id}
/logic/singletons/{singleton_kind}/{scope}
/logic/epochs/{actor_kind}/{actor_id}
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

### 12.1 Instance Registry

Each service process registers an `InstanceRecord`:

```rust
pub struct InstanceRecord {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub advertised_endpoint: Uri,
    pub control_endpoint: Uri,
    pub version: String,
    pub state: InstanceState,
    pub capacity: InstanceCapacity,
    pub labels: BTreeMap<String, String>,
    pub lease_id: LeaseId,
}
```

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
ask target LogicControl.ActivateActor
target starts actor and confirms epoch
write placement record with CAS
release activation lock
```

### 12.4 Epoch

Epoch is the fencing token. Every owner change increments it. Old owners must reject writes once they observe a newer epoch or lose their lease.

### 12.5 Singleton Owner

Singleton owner records follow the same lease and epoch rules as explicit actors, but the key is `(singleton_kind, scope)`.

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
  cache -> placement record -> if missing call Coordinator.ActivateActor -> cache target

virtual shard:
  hash actor_id to shard -> shard assignment -> instance record -> cache target

singleton:
  singleton owner record -> if missing call ActivateSingleton -> cache target
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
