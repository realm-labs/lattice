# lattice Implementation Plan

> Version: v0.1  
> Based on: [architecture/README.md](architecture/README.md) and the architecture chapters.  
> Goal: split the lattice architecture into phases that can be implemented, tested, and accepted incrementally.

---

## 0. Phase Principles

```text
Single-node semantics before distributed semantics.
Typed actor/RPC programming model before automated placement.
Static routing before etcd/Coordinator dynamic routing.
Owner/fencing correctness before migration, drain, and production ops.
Workspace crate boundaries before feature implementation.
Every phase must keep the main branch runnable.
Every phase must include examples and automated tests.
```

Incomplete capabilities should be exposed through explicit feature flags, fake stores, or static config. Do not hide half-finished behavior in the production path.

---

## 1. Test Requirements

### 1.1 Unit Tests

```text
Handler<M> compile-time bounds
ActorHandle call/tell
system mailbox priority
mailbox full
local timer
local actor watch
local child actor lifecycle
passivation policy
supervision decision
ConfigSource / BootstrapConfig parsing
```

### 1.2 Integration Tests

```text
fake PlacementStore
fake RouteResolver
fake tonic transport
actor activation race
NOT_OWNER retry
FENCED retry
request_id dedup
virtual shard routing
explicit actor activation
singleton failover
instance drain
migration
gateway rate limit
eventbus local/cluster publish subscribe
node graceful shutdown
```

### 1.3 Chaos Tests

```text
target service succeeds but response is lost
timeout followed by retry
old owner recovers after lease expiry
Coordinator leader switch
temporary etcd outage
partial placement write failure
new request arrives while actor is passivating
singleton fails over while running a long business job
rolling update with mixed versions
route cache hits stale owner
EventBus subscriber duplicate delivery
NodeInspect partial result
```

---

## 2. Phase Roadmap

```text
Phase 1 Actor Runtime
  -> Phase 2 Typed RPC + Codegen MVP
  -> Phase 3 Route Cache + Static Placement
  -> Phase 4 Virtual Shard + Lazy Activation
  -> Phase 5 Explicit Placement + Coordinator
  -> Phase 6 Cluster Singleton
  -> Phase 7 Ops Production Features
```

### 2.1 Current Progress Tracker

This tracker is the source of truth for goal progress. When a slice is implemented, tested, and committed, update the matching item from `[ ]` to `[x]`. Mark a phase `[x]` only after every required item in that phase is checked and the phase acceptance tests pass.

#### Phase 1: Single-Node Actor Runtime

Status: `[x]` complete.

- [x] Workspace crate split exists and framework code is not concentrated in one root crate.
- [x] Core ids, `ActorKind`, `ServiceKind`, `ActorId`, `RouteKey`, `Epoch`, `RequestId`, and const helper macros exist.
- [x] `ConfigSource`, `ConfigFormat`, and `BootstrapConfig` support TOML/YAML/JSON/env/inline/composite configuration.
- [x] `Actor`, `Message`, `Handler<M>`, `ActorContext`, `ActorRuntime`, `ActorHandle.call/tell`, and typed replies exist.
- [x] Mailbox has normal/system lanes, bounded capacity behavior, and system priority tests.
- [x] Actor timers, scoped tasks, stop/passivation requests, and lifecycle cleanup exist.
- [x] Actor registry prevents duplicate local activation and handles activation waiters, timeout, failure, and retry.
- [x] Local actor watch/unwatch and local child actor lifecycle exist.
- [x] `ActorExecutionPolicy::TaskPerActor`, `KeyedWorkerPool`, and `DedicatedThreadPool` have real implementations and tests.
- [x] Business error propagation and actor error hook exist.

#### Phase 2: Typed RPC + Codegen MVP

Status: `[x]` complete.

- [x] `lattice_codegen::configure()` build script API exists.
- [x] `proto/lattice/options.proto` exists with service and method options.
- [x] Descriptor-backed proto option parsing and validation exist.
- [x] Generated `RoutedRequest` and `RpcRequest` implementations exist.
- [x] Generated typed client wrapper exists.
- [x] Generated actor server adapter and registry-backed server adapter exist.
- [x] Generated gateway route bindings and dispatcher exist.
- [x] gRPC metadata carries framework RPC context instead of business request fields.
- [x] Codegen rejects missing service/actor/route metadata, unsupported route key types, optional/repeated route keys, and duplicate gateway ids.
- [x] Multiple generated gRPC services can be registered on one endpoint.
- [x] `LatticeService::register_client::<Binding>()` must construct and expose generated typed clients through service/actor context instead of only recording the service kind.
- [x] `examples/minimal-world` should use the final generated client access path, not ad hoc client/core construction.
- [x] Generated transport should avoid unnecessary encode/decode when the concrete request type is already known, or document why that cost is acceptable.
  - Documented tradeoff: the generated endpoint transport currently crosses the object-safe `EndpointRpcTransport::unary<Req>` boundary and dispatches by `Req::METHOD`, so it re-encodes the generic request into the concrete generated tonic request and decodes the concrete reply back into `Req::Reply`. This keeps one generated transport usable by `ResolvingRpcCore`, gateway dispatch, and fake transports while Phase 5 placement-backed client construction is still pending. The extra encode/decode happens only on the client transport boundary, after gateway payload decode and before tonic encode, and is acceptable for the Phase 2 MVP. A later specialized typed transport can remove it without changing business handlers or generated client APIs.

#### Phase 3: Route Cache + Static Placement

Status: `[x]` complete for static placement.

- [x] `RouteResolver` abstraction exists.
- [x] `EndpointPool` and tonic endpoint channel reuse exist.
- [x] Local route cache exists with stale/hard-expiry behavior.
- [x] Static route resolver exists.
- [x] `ResolvingRpcCore` performs owner resolution and direct owner calls.
- [x] NOT_OWNER/FENCED invalidation and retry exist.
- [x] Retry preserves the request id.
- [x] Static multi-instance routing is covered by tests/example code.

#### Phase 4: Virtual Shard + Lazy Activation

Status: `[x]` complete.

- [x] Virtual shard id mapping exists.
- [x] Virtual shard assignment model exists.
- [x] `VirtualShardAssigner` trait and default assignment strategies exist.
- [x] `VirtualShardAssignerRegistry` exists.
- [x] Gradual rebalance logic exists and increments epochs in tests.
- [x] Registry-backed lazy actor activation exists.
- [x] Concurrent local lazy activation starts one actor and shares waiters.
- [x] Loader/factory failure wakes waiters and remains retryable.
- [x] Virtual shard ownership is not yet persisted through a production `PlacementStore` keyspace.
- [x] `LatticeService` does not yet start a placement watch to refresh local shard/owner caches.
- [x] Scale-out does not yet make new service instances automatically participate in shard assignment.
- [x] Scale-in does not yet drain/rebalance shard ownership before termination.
- [x] Running actor migration/passivation policy is not yet connected to shard rebalance decisions.

#### Phase 5: Explicit Placement + Coordinator

Status: `[ ]` incomplete and currently the highest-priority gap.

- [x] `PlacementStore` trait exists.
- [x] In-memory placement store exists.
- [x] etcd placement store adapter exists and uses a cluster key prefix.
- [x] Actor activation lock exists.
- [x] Actor placement records include owner, epoch, lease id, and state.
- [x] `PlacementCoordinator` library type can activate/move/drain/fail over actors in tests.
- [x] `LatticeService` writes an `InstanceRecord` at startup.
- [x] `InstanceRecord` does not yet include a real liveness lease/keepalive contract.
- [x] `LatticeService` does not write `Starting -> Ready -> Draining -> Stopping` state transitions as a lifecycle.
- [x] `LatticeService` does not keep an instance lease alive or remove/expire records on crash.
- [x] etcd instance registration is ordinary KV, not lease-backed liveness.
- [x] `LogicControl` is only a Rust trait; no tonic control-plane RPC service is exposed by logic services.
- [x] Coordinator is only an in-process library type; there is no runnable coordinator service/binary.
- [x] Coordinator leader election is not implemented.
- [x] Store-backed `PlacementRouteResolver` is missing: cache miss should read placement, call Coordinator activation if absent, and cache the target.
- [ ] Explicit actor activation is not wired into generated clients or `LatticeService`.
- [ ] `register_client` does not build a resolver/core from the configured placement store.
- [ ] Placement watch is not wired into route cache invalidation in running services.
- [ ] Deployment still requires examples to hand-build static resolvers for real RPC calls.

#### Phase 6: Cluster Singleton

Status: `[ ]` incomplete.

- [x] In-memory singleton placement model and activation race tests exist.
- [x] Singleton owner record has owner, epoch, lease id, and state in the current model.
- [ ] Singleton ownership is not stored through the production `PlacementStore`/etcd keyspace.
- [ ] `ActivateSingleton` control-plane API is not implemented as a service endpoint.
- [ ] Generated singleton client/adapter is missing.
- [ ] Singleton owner lease/keepalive/failover is not connected to service lifecycle.
- [ ] Old singleton owner fencing is not enforced in the runtime path.

#### Phase 7: Ops Production Features

Status: `[ ]` incomplete.

- [x] Local passivation, supervision, scoped task cleanup, and stop-failed behavior exist.
- [x] Admin/ops helper modules and inspector models exist.
- [x] Config source and config store abstractions exist, including local and etcd config adapters.
- [x] LocalEventBus and NATS event bus adapters exist.
- [x] Typed event publisher and current `ServiceEvents::subscribe_actor` bridge exist.
- [x] Gateway binary frame codec, generated gateway binding, push validation, keyed Governor rate limiter, and tower-style pipeline tests exist.
- [x] OpenTelemetry/fmt telemetry setup exists.
- [x] Actor scheduler and service scheduler exist as non-durable lifecycle-bound schedulers.
- [x] Direct/routed `ActorRef` messaging exists.
- [x] Cross-node remote watch model exists in framework code/tests.
- [ ] `subscribe_actor` is not yet the final typed API from `ctx.service().cluster_events()/local_events()` to actor handlers; it still requires manual event-to-RPC mapping.
- [ ] Event subscriptions are not yet owned and cancelled by `LatticeService` shutdown/drain.
- [ ] Service scheduler is not yet exposed through `ServiceContext`.
- [ ] Admin HTTP is not wired into `LatticeService` startup as a managed listener.
- [ ] Node graceful shutdown is not wired into `LatticeService::run_until_shutdown`.
- [ ] Drain/migration are not connected to runtime actor registries, placement leases, or RPC readiness.
- [ ] Gateway startup is still mostly example-specific and not represented as a framework service API.
- [ ] Cluster/node inspection does not query live services through LogicControl/Admin APIs.
- [ ] Security/mTLS integration is partial and not connected to service builders by default.
- [ ] Full chaos test suite is not implemented.

### Phase 1: Single-Node Actor Runtime

Goal: implement the local actor programming model so business code can write `Actor + Handler<M>` and verify mailbox, timers, and registry semantics.

Deliverables:

```text
Cargo workspace layout
lattice-core ids, Epoch, RequestId, errors
ServiceKind / ActorKind / actor_kind! / service_kind!
InstanceId / InstanceConfig
ConfigSource / ConfigFormat / BootstrapConfig
lattice-actor Actor / Message / Handler<M>
ActorRuntime / ActorScheduler abstraction
ActorExecutionPolicy API with TaskPerActor as the Phase 1 default
ActorRuntimeConfig / ActorSpawnOptions API
ActorHandle.call/tell
type-erased Envelope
system/normal mailbox
ActorContext
local timer
ActorContext scoped task
ActorContext request_passivation/request_stop
local ActorContext watch/unwatch
local child actor spawn/stop
ActorRegistry
bounded activation waiters; no business-visible unbounded stash
actor state-machine example
```

Acceptance:

```text
Repository is a Cargo workspace with dedicated framework crates, not a single root crate containing all modules.
The root lattice crate is only a facade/prelude crate if it remains.
Business code can write WorldActor with Handler<M>.
Business code can define ActorKind/ServiceKind as reusable constants.
BootstrapConfig supports TOML/YAML/JSON/env/composite sources.
Actor execution is spawned and managed through lattice ActorRuntime, even if the backing executor is the process Tokio runtime.
Tokio runtime is not exposed as the actor scheduling model.
TaskPerActor is the default Phase 1 execution policy and is selected through ActorRuntimeConfig / ActorSpawnOptions.
Invalid execution policy configuration returns explicit errors instead of silently falling back.
ActorHandle does not expose or depend on Tokio JoinHandle.
Mailbox and Handler<M> semantics are independent from execution policy.
Local timer can drive WorldTick.
Tasks created by ActorContext are cancelled or isolated on stop/passivation.
Business handlers can request passivation and return the current response before stop starts.
Local watch sends typed termination notifications.
Parent actors can spawn local child actors and stop them with the parent lifecycle.
call/tell return results and propagate errors.
System mailbox priority works.
Mailbox full has explicit error or backpressure behavior.
Activation/loading waiters have capacity and timeout.
Activation failure wakes waiters and allows retry.
Business actors can model internal state machines with enum state and typed messages.
No giant framework enum.
```

Suggested tests:

```text
workspace package graph check
Handler<M> compile-time bounds
actor_kind/service_kind const macro tests
bootstrap config format/merge/env override tests
ActorRuntime spawn policy test
ActorExecutionPolicy default TaskPerActor test
invalid execution policy returns explicit error test
ActorHandle does not expose JoinHandle compile test
ActorHandle call/tell
system mailbox priority
mailbox full
activation waiter timeout/capacity
activation failure wakes waiters and allows retry
actor state machine transition/timer/pending queue example
local timer
scoped task cancellation
business self passivation after handler returns
local actor watch terminated notification
watcher stop auto-unwatch
local child actor lifecycle
child supervisor restart/stop-parent
ActorRegistry duplicate-start prevention
```

### Phase 2: Typed RPC + Codegen MVP

Goal: build the generated glue between proto typed RPC and actor handlers. Placement may still be local or static.

Deliverables:

```text
proto/lattice/options.proto with service-level service_kind/actor_kind/default_route_key and method-level route_key override/gateway_msg_id options
lattice_codegen::configure() build.rs API
programmatic gateway_route_ids build.rs API
file-based gateway_routes build.rs API
tonic-prost-build descriptor pipeline
descriptor-backed lattice option parsing and validation
RoutedRequest generation
RpcRequest generation
generated typed client wrapper
generated server adapter
Gateway route table generation
Gateway ClientCodec abstraction
Gateway ClientMessageBinding decode/forward generation
RpcMetadata injection/extraction
RpcError
RouteTarget.advertised_endpoint
multiple gRPC services on one advertised_endpoint
examples/minimal-world build.rs and proto-driven generated bindings
```

Acceptance:

```text
Proto-driven codegen uses build.rs only; proc macros are not part of Phase 2 RPC generation.
Custom proto options are the source of truth; no parallel TOML route config is required for generated RPC.
Gateway msg_id mapping may come from proto options, gateway_route_ids, or gateway_routes files.
Large business protocols should prefer business-owned msg_id tables over embedding ids in RPC proto methods.
Every generated lattice RPC service validates service_kind and actor_kind; every generated method validates default_route_key or route_key override, request type, and reply type.
Route key fields are validated against request messages and support only uint64, int64, string, and bytes in Phase 2.
Route key fields must generate non-Option Rust fields: proto3 ordinary scalar fields and proto2 required scalar fields are allowed; optional, repeated, and oneof route keys are rejected during codegen.
Duplicate gateway_msg_id values and unknown gateway route methods are rejected during codegen.
PlayerService can call WorldActor through generated WorldClient.
Missing Handler<Rpc<Request>> fails at compile time.
Generated adapter converts tonic request into actor.call.
Generated client injects request_id, route_epoch, source_service, source_instance, and TraceContext into gRPC metadata.
One logic service endpoint can register multiple generated tonic services.
Business proto requests do not define framework meta fields.
Gateway route table is generated from protocol/codegen metadata, avoiding handwritten hundreds of msg_id rules.
Gateway decodes msg_id/payload from binary frames and decodes the concrete proto request by msg_id.
Logic services handle typed gRPC requests, not opaque client bytes.
```

Suggested tests:

```text
generated output snapshot tests
proto option parsing tests
missing service_kind, actor_kind, and route_key validation tests
unsupported route key field type validation tests
optional/repeated/oneof route key rejection tests
duplicate gateway_msg_id validation tests
programmatic gateway_route_ids tests
file-based gateway_routes tests
lattice_codegen::configure().compile_protos integration test
generated client API compile tests
missing handler compile-fail tests
fake tonic transport round trip
gateway client codec frame decode
gateway generated binding decode_and_forward
logic adapter receives typed request
multi rpc service same endpoint
grpc metadata injection/extraction
gateway route table duplicate/binding validation
```

### Phase 3: Route Cache + Static Placement

Goal: implement owner direct calls, route cache, and NOT_OWNER retry before introducing etcd and Coordinator.

Deliverables:

```text
RouteResolver
EndpointPool
local route cache
static placement config
NOT_OWNER retry
advertised_endpoint channel reuse across proto services
```

Acceptance:

```text
Multiple world-service instances can statically own different world_id ranges.
Route cache hits do not access external stores.
Owner change triggers NOT_OWNER, cache invalidation, and one retry.
Retry reuses the same request_id.
Connection pool is keyed by instance_id / advertised_endpoint, not actor_id.
```

Suggested tests:

```text
cache hit/miss
cache hard ttl/soft ttl
NOT_OWNER invalidate
request_id retry invariance
endpoint pool reuse
```

### Phase 4: Virtual Shard + Lazy Activation

Goal: support large numbers of lightweight actors by routing through virtual shards and lazy-activating actors on owner instances.

Deliverables:

```text
virtual shard assignment
VirtualShardAssigner trait + default assigners
VirtualShardAssignerRegistry by stable name
virtual shard gradual rebalance
ActorExecutionPolicy::KeyedWorkerPool implementation
lightweight actor lazy activation
ActorFactory/ActorLoader lifecycle
in-memory/static instance registry until etcd phase
```

Acceptance:

```text
actor_id routes to a shard owner.
KeyedWorkerPool execution policy can run multiple lightweight actors on a bounded worker set.
KeyedWorkerPool maps scheduler_key deterministically to a worker.
KeyedWorkerPool preserves the same mailbox and Handler<M> semantics as TaskPerActor.
Target instance lazy-loads actor on registry miss.
Concurrent lazy activation starts only one local actor.
Business ActorLoader failure has explicit error behavior.
scale out/in can gradually rebalance virtual shards and increments epoch on every owner change.
scale out moves only idle/eligible shards by default and does not force-migrate Running actors.
```

Suggested tests:

```text
virtual shard hash consistency
virtual shard owner lookup
virtual shard assigner trait/default implementation
assigner deterministic plan
virtual shard gradual rebalance
KeyedWorkerPool deterministic actor-to-worker mapping test
KeyedWorkerPool mailbox ordering and system priority test
local lazy activation race
business actor loader/saver lifecycle
stop/save failure enters StopFailed and blocks passivation/drain
admin retry-stop / force-stop lifecycle
passivation policy smoke test
```

### Phase 5: Explicit Placement + Coordinator

Goal: introduce independent etcd and PlacementCoordinator so heavy actors can activate, move owners, and use epoch/fencing correctly.

Deliverables:

```text
independent etcd PlacementStore
etcd key_prefix / cluster prefix
InstanceConfig -> InstanceRecord registration
Coordinator.ActivateActor
activation lock
epoch
owner lease
placement watch
LogicControl.ActivateActor
Coordinator leader election
```

Acceptance:

```text
actor without owner activates automatically.
Concurrent activation creates only one owner.
Owner changes always increment epoch.
Old owner returns NOT_OWNER/FENCED.
Coordinator does not forward normal business RPC.
Deployment does not require prewriting /logic placement keys.
Runtime reads/writes only under the current cluster prefix.
```

Suggested tests:

```text
fake PlacementStore unit tests
etcd integration tests
etcd key prefix isolation
instance record lease cleanup
actor activation race
lease expired failover path
placement watch cache refresh
FENCED retry behavior
```

### Phase 6: Cluster Singleton

Goal: support cluster singleton actors, ensuring one active owner per scope with failover and fencing.

Deliverables:

```text
singleton placement
ActivateSingleton
singleton owner lease
epoch fencing
singleton generated client/adapter
```

Acceptance:

```text
SeasonManager has only one Running owner in the cluster.
Owner crash triggers automatic failover.
Failover increments epoch.
Old owner cannot commit writes.
Singleton is not used as a high-frequency player request entry point.
```

Suggested tests:

```text
singleton activation race
owner lease expiry
old owner fenced
singleton business job failover
route refresh after NOT_OWNER
```

### Phase 7: Ops Production Features

Goal: complete production operations so lattice supports rolling update, debugging, migration, drain, reliable background work, and secure access.

Deliverables:

```text
passivation
supervision
drain
migration
dynamic scale out/in integration
admin API
ClusterInspector / NodeInspector
default axum admin adapter
BootstrapConfig typed section API
from_config component builders
ConfigSource file/env/inline/composite adapters
metrics/tracing
OpenTelemetry exporter integration
tracing-subscriber + lattice-telemetry-otlp adapter crate
TraceContext propagation across RPC/EventBus/scheduler/actor mailbox
GatewaySessionRef + GatewayPush RPC
Gateway tower pipeline + Governor keyed rate limiter
gateway route table config/codegen validation
EventBus abstraction + NATS adapter
LocalEventBus / NodeEvents in-memory adapter
typed EventPublisher / ServiceEvents subscriber API
EventBus subscribe_actor to owner mailbox API
ConfigStore abstraction in lattice-config + LocalConfigStore
optional lattice-config-etcd adapter crate
actor scheduler + service scheduler
cross-node actor watch/unwatch
ActorExecutionPolicy::DedicatedThreadPool implementation
business saga / pending operation example
transactional outbox guidance example
security/mTLS integration
chaos tests
```

Acceptance:

```text
Rolling update is supported.
Scale-out lets new instances participate in activation/shard assignment.
Scale-in safely drains actors, shards, and singletons before termination.
SIGTERM/preStop/admin shutdown enters graceful shutdown instead of immediately releasing leases.
Node crash is handled through lease expiry, epoch/fencing, and reload.
Actors can be queried and migrated.
Cluster summary, instances, placement, vshards, singletons, mailboxes, schedulers, and event subscriptions can be inspected.
Bootstrap config supports TOML/YAML/JSON/env override and validates component from_config at build time.
Trace spans cover cross-node RPC, EventBus fan-out, scheduler timers, and actor handlers.
New requests during drain have clear routing behavior.
NOT_OWNER, activation, and timeout issues are diagnosable.
Actors can push to current client connections through GatewaySessionRef.
Gateway push validates session_id and connection_epoch.
Gateway supports per-principal/session rate limit by rate_class.
State-changing RPCs have request_id dedup.
Cross actor/service workflows model pending/operation_id/retry/compensation/manual_required.
Typed EventPublisher fills metadata automatically.
LocalEventBus/NodeEvents handle same-node events in memory.
NATS JetStream subscribers can consume idempotently by event_id.
Service stop/drain/shutdown cancels runtime-managed event subscriptions.
subscribe_actor delivers events to actor handlers through owner routing/fencing.
Actors can watch remote actor incarnations and receive notifications for stop/passivation/migrate/fence/node down.
ConfigStore supports low-frequency watch/reload and custom backend implementations.
Actor scheduler is bound to actor lifecycle and cancelled on stop/passivation.
Service scheduler is bound to service instance lifecycle and lost after restart.
DedicatedThreadPool execution policy isolates configured actor families from normal Tokio worker threads.
All ActorExecutionPolicy variants are implemented by the end of Phase 7; none remain stub-only or unsupported in the completed framework.
```

Suggested tests:

```text
instance drain integration
node graceful shutdown signal/preStop
node crash lease expiry failover
cluster/node inspector aggregation
admin API auth/pagination/partial result
actor migration protocol
scale out new instance assignment
scale in drain before pod termination
virtual shard rebalance throttling
request_id dedup
business saga partial failure/idempotent retry example
transactional outbox guidance example
gateway session reconnect fencing
gateway push stale session drop
gateway governor keyed limiter
gateway tower load_shed/concurrency
eventbus publish/subscribe integration
subscriber consumer group
config store watch/reload
bootstrap config format/composite/from_config
actor scheduler cancellation
service scheduler shutdown
DedicatedThreadPool isolation and shutdown test
metrics labels smoke
trace fields smoke
trace context propagation across rpc/eventbus/scheduler
eventbus consumer span links producer context
cross-node actor watch notification
watch target owner crash synthesized notification
chaos test suite
```

---

## 3. Minimal Runnable Example Shape

Framework-level API sketches are in [architecture/07-api-examples.md](architecture/07-api-examples.md). Implementation should first make `examples/minimal-world` run with that API shape, then refine runtime/codegen/placement internals.

```text
examples/minimal-world/
  proto/
    world.proto
    player.proto

  crates/
    world-service/
      src/main.rs
      src/world_actor.rs
      src/world_registry.rs

    player-service/
      src/main.rs
      src/player_actor.rs

    placement-coordinator/
      src/main.rs

  config/
    world-service.toml
    player-service.toml
    coordinator.toml
```

### 3.1 Business IDs

`WorldId` and `PlayerId` are defined by the minimal-world business example. They are not built into lattice.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorldId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlayerId(pub u64);

impl ActorKey for WorldId {
    fn to_route_key(&self) -> RouteKey {
        RouteKey::U64(self.0)
    }

    fn to_actor_id(&self) -> ActorId {
        ActorId::U64(self.0)
    }

    fn try_from_actor_id(actor_id: &ActorId) -> Result<Self, ActorKeyDecodeError> {
        match actor_id {
            ActorId::U64(value) => Ok(WorldId(*value)),
            _ => Err(ActorKeyDecodeError {
                reason: "expected u64 actor id for WorldId".to_string(),
            }),
        }
    }
}
```

### 3.2 WorldActor

```rust
pub struct WorldActor {
    pub world_id: WorldId,
    pub epoch: Epoch,
    pub players: HashMap<PlayerId, PlayerRuntimeState>,
}

#[async_trait]
impl Actor for WorldActor {
    async fn started(&mut self, ctx: &mut ActorContext<Self>) {
        ctx.notify_interval(
            std::time::Duration::from_millis(50),
            || WorldTick { delta_ms: 50 },
        );
    }

    async fn stopping(
        &mut self,
        ctx: &mut ActorContext<Self>,
        reason: StopReason,
    ) -> Result<(), ActorStopError> {
        self.save_to_business_db(reason).await?;
        ctx.cancel_all_tasks();
        Ok(())
    }
}
```

### 3.3 Handler

```rust
#[async_trait]
impl Handler<Rpc<EnterWorldRequest>> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<EnterWorldRequest>,
    ) -> Result<EnterWorldReply, WorldError> {
        let player_id = PlayerId(msg.req.player_id);
        self.players.insert(player_id, PlayerRuntimeState::default());

        Ok(EnterWorldReply { ok: true })
    }
}
```

### 3.4 Service Registration

```rust
pub const WORLD_SERVICE: ServiceKind = service_kind!("World");
pub const WORLD_ACTOR: ActorKind = actor_kind!("World");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = AppDeps::from_env().await?;

    let service = LatticeService::builder(WORLD_SERVICE)
        .instance(InstanceConfig::from_env()?)
        .config(ConfigSource::file("config/world-service.toml"))
        .placement_store(EtcdPlacementStore::from_config())
        .cluster_event_bus(NatsEventBus::from_config())
        .telemetry(TelemetryConfig::from_config())
        .admin_http(AdminHttpConfig::from_config())
        .register_actor(
            ActorRegistration::builder(WORLD_ACTOR)
                .factory(WorldActorFactory::new(app.clone()))
                .mailbox(MailboxConfig::bounded(4096))
                .passivation(PassivationPolicy::IdleTimeout(Duration::from_secs(300)))
                .build(),
        )
        .register_sharded_rpc(WorldRpcBinding::for_actor(WORLD_ACTOR))
        .register_client::<PlayerRpcBinding>()
        .build()
        .await?;

    service.run_until_shutdown().await
}
```

---

## 4. Global Acceptance Checklist

Pre-implementation checks:

```text
[ ] Repository is organized as a Cargo workspace with the planned framework crates.
[ ] The root lattice crate does not become a dumping ground; it only re-exports deliberate public facade APIs if needed.
[ ] Actor state has a unique owner.
[ ] Owner changes increment epoch.
[ ] State-changing requests carry request_id.
[ ] Cross-service workflows have operation_id, pending state, retry/query for unknown results, compensation, or manual repair path.
[ ] Generated adapter/runtime checks route_epoch before entering Handler.
[ ] RPC client encapsulates route/cache/retry.
[ ] One advertised_endpoint can register multiple generated gRPC services and reuse connections by instance_id.
[ ] Business code does not use raw tonic clients directly.
[ ] Gateway uses codegen binding to decode payload into typed proto request instead of forwarding opaque bytes to Logic.
[ ] Cross-process references use ActorRef/GatewaySessionRef, not ActorHandle.
[ ] Gateway push validates session_id and connection_epoch.
[ ] Gateway per-principal/session rate limit uses keyed limiter, not only instance-wide RateLimit.
[ ] EventBus is not used to replace typed RPC that needs owner/fencing/return value.
[ ] Same-node events prefer LocalEventBus to avoid unnecessary cluster EventBus dependency.
[ ] Business event publishing defaults to typed EventPublisher instead of hand-filled EventEnvelope.
[ ] Subscribers are idempotent by event_id.
[ ] Event subscriptions are owned by service runtime and cancelled on shutdown.
[ ] subscribe_actor routes through ActorRef/ActorKey to owner mailbox and never holds cross-process ActorHandle.
[ ] Framework layer depends only on ConfigStore/EventBus/PlacementStore, not business databases.
[ ] BootstrapConfig / from_config parse and validate only during build and support TOML/YAML/JSON/env override.
[ ] Coordinator is not on the business hot path.
[ ] etcd deployment prewrites only cluster prefix/auth/config, not /logic placement keys.
[ ] route cache has NOT_OWNER invalidation.
[ ] New Ready instances participate in activation/shard assignment after scale-out.
[ ] scale-out rebalance does not force-migrate Running actors by default.
[ ] scale-in drains before pod termination.
[ ] node shutdown sets readiness=false, drains, and keeps instance lease until drain finishes.
[ ] node crash recovery does not depend on Actor::stopping and uses lease expiry + reload.
[ ] Cluster state is queried through Admin/Ops API, not direct etcd scans.
[ ] NodeInspect list APIs are paginated and support partial result/unreachable instance.
[ ] Actor business state inspect is optional, read-only, redacted, and timeout bounded.
[ ] TraceContext propagates through gRPC metadata, EventBus headers, actor mailbox envelope, and scheduler envelope.
[ ] Framework emits tracing spans for RPC server, EventBus publish/consume, and placement resolve.
[ ] lattice-telemetry-otlp can install fmt-only telemetry and optional OTLP tracing export.
[ ] EventBus fan-out uses span links rather than a fake single parent-child chain.
[ ] metrics labels avoid actor_id/request_id/event_id/session_id high-cardinality fields.
[ ] actor activation has a distributed lock.
[ ] actor registry prevents duplicate local activation.
[ ] singleton has lease + epoch + durable owner record.
[ ] passivation calls business stop/save hook and cleans mailbox/tasks.
[ ] business request_passivation/request_stop takes effect after the current handler returns.
[ ] stop/save failure enters StopFailed, blocks unload and owner release, and waits for retry/operator intervention.
[ ] mailbox has capacity and priority.
[ ] there is no business-visible unbounded stash; activation waiters have capacity and timeout.
[ ] slow I/O does not block realtime actors.
[ ] Handler does not use raw tokio::spawn for actor-owned tasks; it uses ActorContext.
[ ] actor stop/passivation/lost ownership cancels or isolates managed tasks.
[ ] actor watch is bound to current owner epoch and auto-unwatches when watcher stops/passivates.
[ ] cross-node watch uses ActorRef/LogicControl and does not hold remote ActorHandle.
[ ] local child actor does not write placement and stops with parent lifecycle.
[ ] actor/service scheduler is explicitly non-durable and lost on restart.
[ ] metrics/trace/admin APIs are sufficient for production diagnosis.
```

---

## 5. Codex Goal Execution Protocol

This section defines how a Codex goal should execute this plan. The standalone prompt only points to this file; the loop, checklist, and exit criteria are defined here.

### 5.1 Execution Loop

```text
1. Read docs/implementation-plan.md and start from "2.1 Current Progress Tracker".
2. Identify the earliest phase whose phase status is still [ ].
3. Within that phase, identify the earliest unchecked checklist item that is not blocked by a later-phase dependency.
4. Read the detailed phase deliverables, acceptance items, and suggested tests below the tracker.
5. Read the docs/architecture/* chapters relevant to that unchecked item.
6. Select a small slice: one checklist item, or a few tightly related checklist items that can be completed end to end.
7. Inspect the current codebase and classify what is done, missing, or inconsistent with the architecture for that slice.
8. Implement the missing capability, keeping public APIs aligned with docs/architecture/07-api-examples.md where applicable.
9. Add tests for the new capability, using this file's test scope.
10. Run the required verification commands for the slice.
11. Update "2.1 Current Progress Tracker":
    - mark completed checklist items [x];
    - add newly discovered missing work as [ ] items;
    - mark the phase status [x] only when all required items in that phase are [x].
12. If implementation proves architecture or plan text stale, update both docs and this plan. Do not use doc edits as a substitute for implementation.
13. Commit the completed slice with an English conventional commit message.
14. Summarize completed work, remaining work, verification results, and commit id/message.
15. If the current phase checklist is fully satisfied, move to the next phase. Otherwise choose the next small slice in the same phase.
```

### 5.2 Per-Phase Checklist Template

```text
Phase: <phase name>

Architecture references:
- [ ] <relevant architecture docs read>

Deliverables:
- [ ] <copied from this phase's Deliverables section>

Acceptance:
- [ ] <copied from this phase's Acceptance section>

Tests:
- [ ] <copied from this phase's Suggested tests section>

Examples:
- [ ] examples/minimal-world reflects this phase where applicable
- [ ] public API shape matches docs/architecture/07-api-examples.md where applicable

Workspace:
- [ ] implementation lives in dedicated workspace crates
- [ ] root crate is only a deliberate facade/prelude if present
- [ ] no framework area is implemented as a large single-crate internal module tree when it has its own planned crate

Verification:
- [ ] cargo fmt
- [ ] cargo clippy
- [ ] cargo test
- [ ] phase-specific integration/chaos tests

Commit:
- [ ] slice is committed with an English conventional commit message

Exit decision:
- [ ] All deliverables are implemented
- [ ] All acceptance items are satisfied
- [ ] Tests pass
- [ ] No architecture item in this phase remains documentation-only
```

### 5.3 Phase Exit Rules

Each phase can exit only when all items are true:

```text
[ ] All deliverables in this phase are implemented.
[ ] All acceptance items in this phase are satisfied.
[ ] All suggested tests in this phase are implemented, or covered by explicit equivalent tests.
[ ] examples/minimal-world or a matching example demonstrates the key capability.
[ ] Relevant API sketches in docs/architecture/07-api-examples.md are covered by compile tests, examples, or implementation.
[ ] The crate split for this phase matches the planned Cargo workspace boundaries.
[ ] cargo fmt passes.
[ ] cargo clippy passes.
[ ] cargo test passes.
[ ] Every completed slice has an English conventional commit.
[ ] No framework capability in this phase remains documentation-only.
```

### 5.4 Goal Completion Criteria

The whole goal can be marked complete only when:

```text
[ ] Phase 1 through Phase 7 are complete.
[ ] This file's global acceptance checklist is fully satisfied.
[ ] architecture/00-overview.md system boundaries and module responsibilities are implemented.
[ ] architecture/01-actor-runtime.md actor runtime capabilities are implemented and tested.
[ ] All ActorExecutionPolicy variants are implemented and tested: TaskPerActor, KeyedWorkerPool, and DedicatedThreadPool.
[ ] UnsupportedExecutionPolicy is used only for invalid configuration, not for planned policies in the completed framework.
[ ] architecture/02-rpc.md typed RPC, metadata, codegen, and gateway decode/forward are implemented and tested.
[ ] architecture/03-placement.md placement, scale, drain, shutdown, crash, and watch are implemented and tested.
[ ] architecture/04-eventbus-scheduler-config.md event bus, scheduler, and config are implemented and tested.
[ ] architecture/05-gateway-ops.md gateway, rate limit, admin, telemetry, and inspection are implemented and tested.
[ ] Valid constraints in architecture/06-appendix.md are not violated.
[ ] API sketches in architecture/07-api-examples.md are covered by examples or compile tests.
[ ] The implementation uses the planned Cargo workspace crate split; the root crate is not a monolithic implementation crate.
[ ] examples/minimal-world runs as an end-to-end example.
[ ] cargo fmt passes.
[ ] cargo clippy passes.
[ ] cargo test passes.
[ ] All completed implementation slices are committed with English conventional commit messages.
[ ] Production paths have no unexplained TODO / FIXME / unimplemented! / todo!.
```

### 5.5 Working Constraints

```text
Work on the earliest unfinished phase first; avoid unrelated refactors.
Implement the planned Cargo workspace crate split before adding more framework features.
Do not implement lattice as one root crate with many internal modules.
Use dedicated crates for framework areas: lattice-core, lattice-actor, lattice-rpc, lattice-codegen, lattice-placement, lattice-coordinator, lattice-eventbus, lattice-scheduler, lattice-config, lattice-gateway, and lattice-ops.
The root lattice crate may exist as a small facade/prelude crate only.
Within a phase, progress by one or a few small checklist items at a time.
Each slice must be implemented, tested, verified, and committed before continuing.
Use English conventional commit messages, for example "feat(actor): add bounded mailbox" or "test(rpc): cover metadata extraction".
Keep public APIs aligned with architecture/07-api-examples.md.
If implementation proves an architecture API flawed, update architecture docs and this plan together.
Do not put business database responsibilities into the framework layer; depend at most on ConfigStore/EventBus/PlacementStore.
Do not hardcode business types in the framework layer; World/Player/Guild may appear only in examples or business-facing tests.
ActorHandle is local-only. Across processes, use ActorRef, GatewaySessionRef, or generated clients.
Async tasks must be managed by actor/service runtime; avoid leaking raw tokio::spawn tasks.
Scheduler is actor/service-level and non-durable; scheduled tasks are lost on restart.
EventBus broadcast does not replace typed RPC when return values, owner routing, or fencing are required.
Every state-mutating cross-service workflow must consider request_id, operation_id, idempotency, compensation, or manual_required.
Do not use super::super imports; prefer crate:: paths or local module paths that stay readable.
Avoid pub use unless it is part of a deliberate public facade or prelude.
Do not pile unrelated logic into one file; split code into coherent modules.
No single file may exceed 1200 lines of code without a documented reason in the module or plan.
```

---

## 6. Version Split

```text
v0.1: Phase 1 + Phase 2, minimal-world can run locally
v0.2: Phase 3, multi-instance static placement
v0.3: Phase 4, virtual shard and lazy activation
v0.4: Phase 5, etcd PlacementStore and Coordinator
v0.5: Phase 6, Cluster Singleton
v0.6: Phase 7, production ops features
```

Before every version release, update:

```text
examples/minimal-world
config examples
architecture checklist
known limitations
migration notes
```
