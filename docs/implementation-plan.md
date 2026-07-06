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
request_id duplicate guard
virtual shard routing
explicit actor activation
singleton failover
instance drain
migration
gateway rate limit
eventbus local/cluster publish subscribe
node graceful shutdown
direct-link open/close
direct-link unidirectional delivery
direct-link bidirectional delivery
direct-link backpressure
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
Direct Link peer disconnect during send
Direct Link unsupported message after open
Direct Link target actor passivation during stream
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
  -> Phase 8 Direct Actor Link
  -> Phase 9 Direct Link Multiplexed Transport Runtime
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
- [x] `LatticeService::register_client::<Binding>()` constructs and exposes generated typed clients through service/actor context.
- [x] `examples/minimal-world` uses the final generated client access path, not ad hoc client/core construction.
- [x] Generated transport avoids unnecessary encode/decode when the concrete request type is already known.
  - The generated endpoint transport now dispatches by `Req::METHOD`, verifies the concrete generated request/reply types with `Any` downcasts, and calls the generated tonic client directly. This keeps the object-safe `EndpointRpcTransport::unary<Req>` boundary while removing the previous protobuf re-encode/re-decode on the client transport boundary.

#### Phase 3: Route Cache + Static Placement

Status: `[x]` complete for static placement.

- [x] `RouteResolver` abstraction exists.
- [x] `EndpointPool` and tonic endpoint channel reuse exist.
- [x] Local route cache exists with stale/hard-expiry behavior.
- [x] Static route resolver exists.
- [x] `ResolvingRpcCore` performs owner resolution and direct owner calls.
- [x] NOT_OWNER/FENCED invalidation and retry exist.
- [x] Retry preserves the request id.
- [x] Placement-backed RPC retry is configurable; disabled retry sends once without retaining a retry copy of the request body.
- [x] Ambiguous transport/status failures are normalized to `RpcError::UnknownResult` and are not transparently retried.
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
- [x] Virtual shard ownership is persisted through the `PlacementStore` keyspace, including the etcd adapter.
- [x] `LatticeService` can start placement watches for service-owned route caches.
- [x] Scale-out makes new Ready service instances automatically participate in shard assignment through a coordinator placement watch that triggers configured virtual-shard rebalance plans without changing the existing running-actor movement policy.
- [x] Scale-in service shutdown drains placement ownership and migrates owned actors/virtual shards when a replacement instance exists.
- [x] Running actor migration/passivation policy is connected to shard rebalance decisions through LogicControl shard-preparation RPC, registry-backed `ShardMigrationPolicy`, and coordinator `prepare_and_rebalance_virtual_shards` coverage.

#### Phase 5: Explicit Placement + Coordinator

Status: `[x]` complete.

- [x] `PlacementStore` trait exists.
- [x] In-memory placement store exists.
- [x] etcd placement store adapter exists and uses a cluster key prefix.
- [x] Actor activation lock exists.
- [x] Actor placement records include owner, epoch, lease id, and state.
- [x] `PlacementCoordinator` library type can activate/move/drain/fail over actors in tests.
- [x] `LatticeService` writes an `InstanceRecord` at startup.
- [x] `InstanceRecord` includes a liveness lease/keepalive contract.
- [x] `LatticeService` writes `Starting -> Ready -> Draining -> Stopping` state transitions as a lifecycle.
- [x] `LatticeService` keeps an instance lease alive while running.
- [x] etcd instance registration is lease-backed.
- [x] Logic services expose a tonic `LogicControl` endpoint for activation.
- [x] A runnable `lattice-coordinator` binary exists.
- [x] Coordinator leader election exists through `PlacementStore::campaign_coordinator_leader`.
- [x] Store-backed `PlacementRouteResolver` resolves cache misses through placement and coordinator activation.
- [x] Explicit actor activation is wired into generated clients through placement-backed client cores.
- [x] `register_client` builds resolver/core from the configured placement store by default.
- [x] Placement watch is wired into route cache invalidation in running services.
- [x] Crash handling has a lease-expiry reconciliation loop that observes missing/dead instance records and invokes coordinator failover automatically for actors and singletons.
- [x] The runnable coordinator waits and re-campaigns as a standby when leadership is held instead of exiting immediately.
- [x] Examples use placement-backed defaults where appropriate; distributed-login actor-ref messaging no longer constructs an empty static resolver, and minimal-world runs through placement-backed generated clients.

#### Phase 6: Cluster Singleton

Status: `[x]` complete.

- [x] In-memory singleton placement model and activation race tests exist.
- [x] Singleton owner record has owner, epoch, lease id, and state in the current model.
- [x] Singleton ownership is stored through the `PlacementStore`/etcd keyspace.
- [x] `ActivateSingleton` control-plane API is implemented as a service endpoint.
- [x] Generated singleton client/adapter exists through `SingletonBinding`, singleton placement client cores, and `SingletonRegistryService` with executable codegen/service coverage.
- [x] Singleton owner lease/keepalive is connected to service lifecycle for owners held by the running instance.
- [x] Automatic singleton failover after owner crash is covered by the lease-expiry reconciliation loop and failover test coverage.
- [x] Old singleton owner fencing is enforced in the generated singleton runtime path through owner/epoch checks before actor dispatch and FENCED retry status normalization.

#### Phase 7: Ops Production Features

Status: `[x]` complete.

- [x] Local passivation, supervision, scoped task cleanup, and stop-failed behavior exist.
- [x] Admin/ops helper modules and inspector models exist.
- [x] Config source and config store abstractions exist, including local and etcd config adapters.
- [x] LocalEventBus, in-memory NATS-like test adapter, and real async-nats-backed `NatsEventBus` adapter exist with clear naming boundaries.
- [x] Typed event publisher and current `ServiceEvents::subscribe_actor` bridge exist.
- [x] Gateway binary frame codec, generated gateway binding, push validation, keyed Governor rate limiter, and tower-style pipeline tests exist.
- [x] OpenTelemetry/fmt telemetry setup exists.
- [x] Actor scheduler and service scheduler exist as non-durable lifecycle-bound schedulers.
- [x] Direct/routed `ActorRef` messaging exists.
- [x] Cross-node remote watch model exists in framework code/tests.
- [x] `ctx.service().cluster_event_bus()/local_event_bus()` and `cluster_events()/local_events()` accessors exist.
- [x] `subscribe_actor` has typed owner-routed actor delivery semantics through `subscribe_actor_routed`, which builds routed `ActorRef`s from event actor metadata and delivers through `ActorRefRpcCore`.
- [x] Event subscriptions owned by service event buses are cancelled by `LatticeService` shutdown/drain.
- [x] Service scheduler is exposed through `ServiceContext`.
- [x] Admin HTTP is wired into `LatticeService` startup as a managed listener, including authenticated/audited/rate-limited mutating endpoints and service-backed instance drain.
- [x] Node graceful shutdown is wired into `LatticeService::run_until_shutdown`.
- [x] `LatticeService` shutdown drains runtime actor registries with `PassivationReason::Drain`.
- [x] Service drain invokes coordinator-driven placement migration for owned actors before final stop.
- [x] RPC readiness/drain behavior for new requests during shutdown is explicit: `LatticeService` publishes `Draining`, stops accepting RPC before placement/runtime drain, keeps lifecycle drain running after the listener closes, and tests cover listener closure while actor drain is blocked.
- [x] Gateway startup is represented as a framework service API: `GatewayService` owns the TCP accept loop, supports custom connection handlers, manages background tasks and managed tonic service listeners such as push RPC listeners, and distributed-login starts through it with gateway crate coverage.
- [x] Cluster/node inspection queries live services through Admin APIs: services now build admin snapshots from Ready placement records and registered actor kinds at startup, expose node summaries over managed Admin HTTP, and `ClusterInspector`/`HttpNodeInspectorClient` coverage verifies live-node aggregation and partial failures.
- [x] Service identity security is connected to service builders by default: `RpcSecurityPolicy::require_service_identity(...)` derives client auth and SPIFFE-shaped peer identity metadata, generated placement-backed clients receive it through `RpcClientContextFactory`, and servers validate identity from either transport extensions or framework metadata.
- [x] Transport security is connected to service builders by default: `RpcTransportSecurity::tls(...)` configures tonic server TLS/mTLS and is propagated to generated placement-backed clients through `TonicEndpointChannelPool`, without exposing certificate/channel setup to actor handlers.
- [x] Full chaos test suite is implemented across placement, ops, and actor chaos tests.
  - [x] Stale owner recovers after lease expiry and is fenced; route cache invalidates and retries with the same request id.
  - [x] Target service succeeds but response is lost/unknown-result handling is covered by `operation_tracker_models_lost_response_unknown_result_reconciliation`.
  - [x] Timeout followed by retry/reconciliation is covered by `operation_tracker_models_timeout_retry_and_reconciliation`.
  - [x] Coordinator leader switch is covered.
  - [x] Temporary etcd outage is covered.
  - [x] Partial placement write failure is covered.
  - [x] New request arriving while an actor is passivating is covered by `request_arriving_while_actor_is_passivating_is_not_processed_by_old_incarnation`.
  - [x] Singleton failover while a long business job is running is covered.
  - [x] Rolling update with mixed versions is covered.
- [x] EventBus subscriber duplicate delivery is covered by `durable_subscriber_deduplicates_duplicate_event_delivery_by_event_id`.
- [x] `crates/lattice-actor/src/tests.rs` no longer exceeds 1200 LOC (843 lines at audit) and actor coverage is split across focused integration tests for execution policies, lifecycle, lazy activation, registry, passivation chaos, timers, and state machines.
- [x] `crates/lattice-service/src/tests.rs` exceeds 1200 LOC and has a module-level rationale for its crate-private service lifecycle coverage.

#### Phase 8: Direct Actor Link

Status: `[x]` complete.

- [x] Direct Link public API exists: `DirectLinkStream`, `DirectLinkMode::{Unidirectional, Bidirectional}`, `ActorContext::links()`, `connect`, `connect_bidirectional`, `get::<S>`, `close_all`, `tell`, `try_tell`, and directional `close`.
- [x] `lattice-codegen` emits `DirectLinkMessage` metadata for generated protobuf messages without requiring per-message proto options.
- [x] Typed stream binding validates at compile time that the target actor implements `Handler<Linked<T>>` for every message in the stream.
- [x] Direct-link message ids are generated deterministically from stream name plus protobuf full name, with explicit Rust-side manual id override for compatibility.
- [x] `lattice-direct-link` crate exists with transport-independent frame codec, message catalog, stream binding, session manager, and metrics/tracing hooks.
  - [x] Initial `lattice-direct-link` crate, stream message catalog, frame codec, handler-bound actor binding, session registry, and metrics scaffolding exist with focused tests.
  - [x] Concrete session manager drives OpenLink state, negotiated directions, send/receive sequencing, and close transitions instead of only storing sessions.
  - [x] Session-manager metrics/tracing hooks cover open, close, receive validation, protocol errors, drop, coalesce, backpressure, and decode-error surfaces with focused tests.
  - [x] Transport/runtime send and receive task metrics/tracing hooks are emitted from concrete TCP/runtime paths.
- [x] TCP direct-link transport exists as the default implementation behind `DirectLinkTransport` / `DirectLinkConnection`.
- [x] Direct Link listener is managed by `LatticeService`, publishes direct-link endpoint metadata, and shuts down/drains with the service lifecycle.
- [x] OpenLink handshake validates service/actor identity, stream binding, accepted message ids, activation policy, owner epoch, auth, and backpressure policy before creating sessions.
- [x] Invalid OpenLink requests reject with explicit reasons: `NotOwner`, `Fenced`, `ActorUnavailable`, `UnsupportedStream`, `UnsupportedMessageType`, `Unauthorized`, `Overloaded`, or `ProtocolVersionMismatch`.
- [x] Message frame validation rejects unknown link ids, wrong direction, unsupported message ids, decode errors, invalid sequence, and non-activatable target actors before mailbox delivery.
- [x] Unidirectional links deliver fire-and-forget `Linked<T>` messages through actor mailbox without executing handlers on socket tasks.
  - [x] `try_deliver_linked` wraps decoded payloads as `Linked<T>` and uses non-blocking `ActorHandle::try_tell` mailbox enqueue with actor-runtime coverage proving delivery does not wait for handler completion.
  - [x] `DirectLinkActorBinding::try_deliver` dispatches by negotiated message id, decodes the concrete protobuf message type, and enqueues typed `Linked<T>` through the actor mailbox.
  - [x] `DirectLinkInboundRouter` resolves target actor handles, validates message frames through the session manager, and calls the typed mailbox delivery path without executing handlers inline.
  - [x] Managed TCP receive tasks use `DirectLinkInboundRouter` after OpenLink negotiation instead of closing accepted connections.
- [x] Bidirectional links are modeled as two logical unidirectional sessions over one underlying connection, with separate streams, message ids, sequence numbers, and backpressure state.
- [x] The initiator receives the source-to-target send handle from `connect_bidirectional`.
- [x] The target actor receives `LinkOpened` and can obtain the target-to-source send handle through `ctx.links().get::<S>(link_id)`.
- [x] Directional close and whole-link close are implemented and emit `LinkDirectionClosed` / `LinkClosed` exactly once per observed transition.
- [x] Backpressure policies are implemented: `Block`, `FailFast`, `DropNewest`, `DropOldest`, `Coalesce`, and `Disconnect`.
  - [x] Reusable `BackpressureQueue` policy engine implements and tests all six policy decisions with pending/drop/coalesce counters.
  - [x] Outbound direct-link send queues use `BackpressureQueue` and map decisions to send results, drops/coalesces, or disconnect close reasons.
  - [x] Inbound remote mailbox delivery applies negotiated backpressure state before actor mailbox enqueue and emits `LinkBackpressure`.
- [x] Heartbeat, heartbeat timeout, protocol error, node draining, target passivation/migration, and backpressure disconnect close links with structured reasons.
  - [x] Whole-link close transitions and router delivery preserve structured reasons for heartbeat timeout, protocol error, node draining, target passivation, target migration, connection loss, and backpressure disconnect.
  - [x] Backpressure disconnect closes the affected link direction and emits `LinkDirectionClosed` / `LinkClosed` with `BackpressureExceeded`.
  - [x] Heartbeat / heartbeat-ack frame processing refreshes liveness, and idle-timeout scans close stale links with `HeartbeatTimeout`.
  - [x] Managed TCP idle-timeout background task drives liveness scans during service runtime.
  - [x] Managed TCP heartbeat send task writes heartbeat frames on open direct-link connections.
  - [x] Protocol-error frame handling and invalid post-open frames close links with `ProtocolError`.
  - [x] Service node-drain hook closes active links with `NodeDraining` during shutdown before actor registry drain.
  - [x] Actor migration hook closes affected links with `TargetMigrating` before migration passivation.
  - [x] Actor passivation hook closes affected links with `TargetPassivated`.
- [x] Security hooks cover internal bind policy, peer identity/auth, source service/actor authorization, max frame size, connection limit, link limit, and rate limit.
  - [x] Service direct-link bind policy defaults to loopback-only binds and requires explicit `DirectLinkBindPolicy::External` opt-in for wildcard or non-loopback listener endpoints.
  - [x] OpenLink validation covers source service authorization, max frame size, and backpressure pending-limit policy with `Unauthorized` or `Overloaded` rejects.
  - [x] OpenLink peer identity/auth hook rejects missing, trust-domain-mismatched, service-mismatched, or source-instance-mismatched peer identity before creating sessions.
  - [x] Managed TCP OpenLink frames are decoded, validated through the session manager, answered with OpenLinkAck/OpenLinkReject frames, and deliver LinkOpened to the target actor before message delivery.
  - [x] Managed TCP handshake supplies authenticated peer identity to OpenLink validation instead of trusting only declared source metadata.
  - [x] Managed TCP listener enforces a configured connection limit.
  - [x] Session manager enforces a configured active link limit.
  - [x] Direct Link open/data path enforces configured rate limits.
- [x] Observability emits link open/close/send/receive/drop/coalesce/backpressure/decode-error metrics and sampled tracing without per-message spans by default.
  - [x] Session metrics for link open, close, receive validation, drop, coalesce, backpressure, and decode errors are covered by `observability_hooks_increment_metrics`.
  - [x] TCP transport send/receive metrics and trace-level frame events are covered by transport tests; direct-link tracing uses lifecycle/frame events and does not create per-message spans by default.
- [x] Direct Link benchmark exists for TCP single-process, local multi-process, and payload/backpressure matrices.
  - [x] `crates/lattice-direct-link/benches/direct_link_benchmark.rs` covers TCP single-process loopback, local multi-process-shaped independent transport loopback, frame payload sizes, and backpressure policy enqueue matrices.
- [x] Architecture/API examples document when to use gRPC RPC versus Direct Actor Link and show unidirectional and bidirectional flows.
  - [x] `docs/architecture/07-api-examples.md` shows generated RPC, unidirectional direct-link, bidirectional direct-link, target `Linked<T>` / `LinkOpened` handlers, and direct-link service registration. This item is documentation/API guidance only; the referenced APIs are implemented and covered by the preceding Phase 8 code/test items.

#### Phase 9: Direct Link Multiplexed Transport Runtime

Status: `[ ]` not started.

This phase intentionally breaks the current Direct Link transport/runtime shape. Do not preserve compatibility with the one-connection-per-open path or the low-level `DirectLinkTransport::connect(endpoint) -> Connection` API if it blocks endpoint pooling.

- [x] Replace per-link TCP connection assumptions with an instance-to-instance `DirectLinkEndpointPool`.
- [x] Add `DirectLinkEndpointPoolConfig` with `connections_per_endpoint`, `max_links_per_connection`, `max_links_per_endpoint`, idle timeout, connect timeout, and reconnect/backoff settings.
- [x] Replace or narrow `DirectLinkTransport` so business/runtime code opens logical link sessions through the endpoint pool, not raw TCP connections.
- [x] Add pooled connection stripes keyed by target direct-link endpoint and stable link/session hash.
- [x] Multiplex many `link_id` logical sessions over each TCP connection.
- [x] OpenLink frames are routed over a selected endpoint stripe and do not create a dedicated TCP connection per actor pair.
- [x] Outbound `DirectLinkSender` writes frames through a pooled connection writer task.
- [x] Inbound connection task demultiplexes frames by `link_id` and routes them through `DirectLinkInboundRouter`.
- [x] Production `DirectLinkRuntimeHandle` is installed into `ServiceContext` by `LatticeService` so `ctx.links().connect(...)` works without test-only runtime injection.
- [x] Direct Link endpoint resolution uses `ActorRef` / placement instance metadata to find the target instance `direct_link_endpoint`.
- [x] `LinkTarget::Endpoint` remains available for explicit business endpoints, but it also goes through the endpoint pool.
- [x] Endpoint pool enforces connection count, per-connection link count, and per-endpoint link count before OpenLink.
- [x] Node drain closes logical sessions before closing pooled TCP connections.
- [x] Peer connection loss closes all sessions multiplexed on that TCP connection with `ConnectionLost`.
- [ ] Connection-level protocol fatal error closes the connection and every multiplexed session.
- [ ] Link-level protocol error closes only the affected logical link unless the frame corrupts connection state.
- [ ] Metrics distinguish physical connections from logical links: connection open/close, active connections, links per connection, frames per connection, reconnects, and pool queue/backpressure.
- [ ] Direct Link benchmark compares one connection per link versus pooled striped connections and documents the fd/port/throughput impact.

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
service identity security integration
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
Generated service adapters enable configurable lightweight request_id duplicate guards without reply replay caches.
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
lightweight request_id duplicate guard
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

### Phase 8: Direct Actor Link

Goal: add a high-throughput actor-to-actor stream capability for fire-and-forget traffic that does not need owner-routed gRPC command semantics.

Direct Actor Link complements gRPC RPC. It does not replace generated gRPC clients, placement-backed routing, request/reply, request_id dedup, route epoch fencing, or UnknownResult handling.

Deliverables:

```text
lattice-core direct-link ids, modes, options, close reasons, lifecycle messages, Linked<T>, and DirectLinkMessage metadata trait
lattice-codegen DirectLinkMessage metadata generation for protobuf messages
lattice-direct-link crate with frame codec, message descriptors, stream binding, session manager, and transport abstraction
DirectLinkTransport and DirectLinkConnection traits
TCP DirectLinkTransport implementation
DirectLinkStream typed builder using real Rust message types
DirectLinkActorBinding and service registration API
ActorContext::links() manager API
connect for unidirectional links
connect_bidirectional for two directional sessions over one connection
ctx.links().get::<S>(link_id) for target-side reverse handles
DirectLink::tell / try_tell / close
ctx.links().close_all(link_id, reason)
OpenLink / OpenLinkAck / OpenLinkReject handshake
source_to_target and target_to_source directional session negotiation
stream/message validation before session creation
message frame validation before mailbox delivery
LinkOpened / LinkDirectionClosed / LinkClosed / LinkBackpressure / LinkProtocolError handlers
backpressure policies: Block, FailFast, DropNewest, DropOldest, Coalesce, Disconnect
heartbeat and heartbeat timeout
structured close reasons
security policy hooks
metrics and sampled tracing
Direct Link benchmark cases
```

Acceptance:

```text
Business protobuf messages remain ordinary messages and do not need direct-link proto options.
build.rs remains normal proto compilation and does not require string-based direct-link message lists.
DirectLinkStream is composed in application Rust code with real generated Rust message types.
Default direct-link ids are stable across nodes compiled from the same protobuf schema.
Manual Rust-side id override exists for compatibility and cross-language protocols.
Binding a stream to an actor fails to compile if the actor lacks Handler<Linked<T>> for any stream message.
Service build rejects duplicate stream names, duplicate message ids, duplicate actor stream bindings, missing actor registrations, and unsupported message types.
OpenLink rejects wrong actor kind/id, unsupported stream, unsupported message id, unauthorized source, overloaded target, and stale/not-owner targets before creating sessions.
Invalid message frames never reach actor mailboxes and never fall back to dynamic handlers.
Unidirectional link close produces LinkDirectionClosed and then LinkClosed when no direction remains.
Bidirectional link close of one direction keeps the opposite direction usable.
Whole-link close closes every direction and invalidates all send handles.
Target actor can obtain the reverse send handle after LinkOpened for bidirectional links.
Actor passivation, node drain, heartbeat timeout, auth failure, protocol error, and backpressure disconnect close links with clear reasons.
Socket tasks never execute business handlers directly.
Actor stop/passivation cleans up owned direct-link handles and sessions.
TCP is the only required first transport.
Transport adapter boundary is explicit so QUIC/UDP profiles can be added later without changing actor handlers.
UDP-based future profiles must explicitly declare weaker reliability/ordering semantics and cannot silently weaken TCP semantics.
Metrics and tracing provide enough visibility for throughput, close reasons, drops, coalescing, decode errors, and backpressure.
gRPC remains the standard logic-service command path.
```

Suggested tests:

```text
DirectLinkMessage metadata generated for prost messages
DirectLinkStream stable id generation
manual id override
duplicate id rejection
duplicate stream binding rejection
actor missing Handler<Linked<T>> compile-fail test
service build fails for binding without registered actor kind
OpenLink success
OpenLink reject wrong actor kind/id
OpenLink reject UnsupportedStream
OpenLink reject UnsupportedMessageType
OpenLink reject Unauthorized
OpenLink reject Overloaded
unidirectional fire-and-forget delivery to Handler<Linked<T>>
bidirectional connect returns source-to-target handle
target actor obtains target-to-source handle from LinkOpened
same stream bound to two actor types requires both actors to handle all messages
different streams per direction deliver only direction-specific messages
message frame wrong direction closes link with ProtocolError
unknown message id after open closes link with ProtocolError
decode error closes link with ProtocolError
ordering for one directional session
Block backpressure waits
FailFast backpressure returns error
DropNewest drops current message and increments metrics
DropOldest drops old pending message and increments metrics
Coalesce replaces pending message by key
Disconnect closes link on overflow
heartbeat timeout closes whole link
directional close keeps opposite direction open
whole-link close closes both directions
LinkDirectionClosed delivered once
LinkClosed delivered once
actor passivation closes links
service drain closes links before shutdown completes
TCP transport round trip
direct-link benchmark smoke
```

### Phase 9: Direct Link Multiplexed Transport Runtime

Goal: turn Direct Link from a functional link/session implementation into a production transport runtime that reuses a bounded number of instance-to-instance TCP connections and multiplexes many actor links over them.

This phase is a breaking refactor. Remove or replace APIs that expose raw transport connections to the service/runtime hot path. The target runtime shape is endpoint-pooled logical sessions, not one TCP connection per actor pair or per OpenLink.

Current implementation audit:

```text
DirectLinkStream, typed handler validation, session negotiation, frame codec, inbound routing, TCP listener, service-managed listener, lifecycle events, security checks, backpressure, and benchmarks exist.
TcpDirectLinkTransport::connect(endpoint) returns one physical connection.
LatticeService listener accepts physical TCP connections and handles frames on that connection.
Service-side DirectLinkRuntimeHandle is not a production outbound runtime backed by placement resolution and a connection pool.
The implementation does not yet define an endpoint connection pool that multiplexes many link_id sessions over bounded per-endpoint TCP stripes.
```

Deliverables:

```text
DirectLinkEndpointPool
DirectLinkEndpointPoolConfig
DirectLinkEndpointKey
DirectLinkConnectionStripe
DirectLinkConnectionId
pooled TCP connection manager
pooled writer task per physical connection
pooled reader task per physical connection
logical link session table keyed by link_id
link_id to connection stripe mapping
endpoint-level connection limit
endpoint-level logical link limit
per-connection logical link limit
connection idle timeout
connection reconnect/backoff policy
production DirectLinkRuntime implementation
placement-backed target endpoint resolution for ActorRef targets
ServiceContext installs DirectLinkRuntimeHandle from LatticeService build
LinkTarget::Actor opens through placement endpoint resolution and endpoint pool
LinkTarget::Endpoint opens through endpoint pool
OpenLink handshake over pooled connections
CloseDirection / Close frame routing over pooled connections
connection-loss fanout to every logical session on the connection
connection-level and link-level protocol error separation
connection/link metrics separation
benchmark for pooled versus one-connection-per-link behavior
```

Acceptance:

```text
No production path opens one TCP connection per actor pair.
Every outbound Direct Link goes through DirectLinkEndpointPool.
Connections are keyed by target direct_link_endpoint, not actor id.
Default connections_per_endpoint is bounded and configurable.
Multiple logical link_id sessions can share one TCP connection.
Different links distribute across connection stripes using stable hash of link_id or source/target actor ids.
Pool rejects OpenLink before transport connect when max_links_per_endpoint or max_links_per_connection would be exceeded.
Peer connection loss closes all logical sessions on that physical connection with ConnectionLost.
Link-level protocol errors close only the affected link unless connection state is corrupted.
Node draining closes logical sessions before closing physical connections.
Actor passivation closes only links for that actor, not the whole endpoint connection.
Service startup installs a real DirectLinkRuntimeHandle, so ctx.links().connect(...) works in a normal service without test-only extension injection.
ActorRef target resolution reads target direct_link_endpoint from placement instance metadata.
Explicit endpoint target still uses pooling and does not bypass connection limits.
The old raw connect/open path is removed or hidden behind tests; no compatibility shim is required.
Metrics expose physical connection counts separately from logical link counts.
Benchmark demonstrates reduced fd/port usage and documents throughput/latency tradeoffs.
```

Suggested tests:

```text
endpoint pool reuses one TCP connection for multiple links to the same endpoint
endpoint pool honors connections_per_endpoint
endpoint pool stripes links across multiple connections
same link id maps to stable stripe
max_links_per_connection rejects additional OpenLink
max_links_per_endpoint rejects additional OpenLink
connection idle timeout closes unused physical connection
logical link close does not close pooled connection when other links remain
connection loss closes every multiplexed link with ConnectionLost
link-level protocol error closes only that link
connection-level protocol error closes all links on that connection
service build installs DirectLinkRuntimeHandle
ctx.links().connect ActorRef resolves placement direct_link_endpoint and opens through endpoint pool
ctx.links().connect explicit endpoint opens through endpoint pool
node drain closes logical links before physical connections
actor passivation closes only matching actor links
pooled benchmark smoke
fd/connection count regression test with many actor links to one endpoint
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
[x] Repository is organized as a Cargo workspace with the planned framework crates.
[x] The root lattice crate does not become a dumping ground; it only re-exports deliberate public facade APIs if needed.
[x] Actor state has a unique owner.
[x] Owner changes increment epoch.
[x] State-changing requests carry request_id.
[x] Cross-service workflows have operation_id, pending state, retry/query for unknown results, compensation, or manual repair path.
[x] Generated adapter/runtime checks route_epoch before entering Handler.
[x] RPC client encapsulates route/cache/retry.
[x] One advertised_endpoint can register multiple generated gRPC services and reuse connections by instance_id.
[x] Business code does not use raw tonic clients directly.
[x] Gateway uses codegen binding to decode payload into typed proto request instead of forwarding opaque bytes to Logic.
[x] Cross-process references use ActorRef/GatewaySessionRef, not ActorHandle.
[x] Gateway push validates session_id and connection_epoch.
[x] Gateway per-principal/session rate limit uses keyed limiter, not only instance-wide RateLimit.
[x] EventBus is not used to replace typed RPC that needs owner/fencing/return value.
[x] Same-node events prefer LocalEventBus to avoid unnecessary cluster EventBus dependency.
[x] Business event publishing defaults to typed EventPublisher instead of hand-filled EventEnvelope.
[x] Subscribers are idempotent by event_id.
[x] Event subscriptions are owned by service runtime and cancelled on shutdown.
[x] subscribe_actor routes through ActorRef/ActorKey to owner mailbox and never holds cross-process ActorHandle.
[x] Framework layer depends only on ConfigStore/EventBus/PlacementStore, not business databases.
[x] BootstrapConfig / from_config parse and validate only during build and support TOML/YAML/JSON/env override.
[x] Coordinator is not on the business hot path.
[x] etcd deployment prewrites only cluster prefix/auth/config, not /logic placement keys.
[x] route cache has NOT_OWNER invalidation.
[x] New Ready instances participate in activation/shard assignment after scale-out.
[x] scale-out rebalance does not force-migrate Running actors by default.
[x] scale-in drains before pod termination.
[x] node shutdown sets readiness=false, drains, and keeps instance lease until drain finishes.
[x] node crash recovery does not depend on Actor::stopping and uses lease expiry + reload.
[x] Cluster state is queried through Admin/Ops API, not direct etcd scans.
[x] NodeInspect list APIs are paginated and support partial result/unreachable instance.
[x] Actor business state inspect is optional, read-only, redacted, and timeout bounded.
[x] TraceContext propagates through gRPC metadata, EventBus headers, actor mailbox envelope, and scheduler envelope.
[x] Framework emits tracing spans for RPC server, EventBus publish/consume, and placement resolve.
[x] lattice-telemetry-otlp can install fmt-only telemetry and optional OTLP tracing export.
[x] EventBus fan-out uses span links rather than a fake single parent-child chain.
[x] metrics labels avoid actor_id/request_id/event_id/session_id high-cardinality fields.
[x] actor activation has a distributed lock.
[x] actor registry prevents duplicate local activation.
[x] singleton has lease + epoch + durable owner record.
[x] passivation calls business stop/save hook and cleans mailbox/tasks.
[x] business request_passivation/request_stop takes effect after the current handler returns.
[x] stop/save failure enters StopFailed, blocks unload and owner release, and waits for retry/operator intervention.
[x] mailbox has capacity and priority.
[x] there is no business-visible unbounded stash; activation waiters have capacity and timeout.
[x] slow I/O does not block realtime actors.
[x] Handler does not use raw tokio::spawn for actor-owned tasks; it uses ActorContext.
[x] actor stop/passivation/lost ownership cancels or isolates managed tasks.
[x] actor watch is bound to current owner epoch and auto-unwatches when watcher stops/passivates.
[x] cross-node watch uses ActorRef/LogicControl and does not hold remote ActorHandle.
[x] local child actor does not write placement and stops with parent lifecycle.
[x] actor/service scheduler is explicitly non-durable and lost on restart.
[x] metrics/trace/admin APIs are sufficient for production diagnosis.
[x] Direct Actor Link is implemented as a separate high-throughput actor stream capability, not as a gRPC transport replacement.
[x] Direct Actor Link supports TCP unidirectional and bidirectional streams with typed `Linked<T>` actor handlers.
[x] Direct Actor Link stream binding uses real Rust message types and compile-time handler checks.
[x] Direct Actor Link lifecycle, close semantics, backpressure, validation, security, and observability match architecture/02-rpc.md.
[ ] Direct Actor Link production transport multiplexes many logical actor links over bounded instance-to-instance TCP connection pools.
[ ] `ctx.links().connect(...)` works through a production `DirectLinkRuntimeHandle` installed by `LatticeService`, not only through test/runtime injection.
```

---

## 5. Codex Goal Execution Protocol

This section defines how a Codex goal should execute this plan. The standalone prompt only points to this file; the loop, checklist, and exit criteria are defined here.

### 5.1 Execution Loop

```text
1. Read docs/implementation-plan.md and start from "2.1 Current Progress Tracker".
2. Audit checked tracker items before trusting them:
    - validate checked items in the current phase and earlier dependency phases against concrete code;
    - require framework implementation plus executable test or runnable example coverage;
    - accept a checked documentation-only item only when this plan records an explicit rationale for why no code is required;
    - if a checked item is not backed by implementation/coverage, change it back to [ ] or add a precise [ ] missing-work subitem.
3. Identify the earliest phase whose phase status is still [ ].
4. Within that phase, identify the earliest unchecked checklist item that is not blocked by a later-phase dependency.
5. Read the detailed phase deliverables, acceptance items, and suggested tests below the tracker.
6. Read the docs/architecture/* chapters relevant to that unchecked item.
7. Select a small slice: one checklist item, or a few tightly related checklist items that can be completed end to end.
8. Inspect the current codebase and classify what is done, missing, or inconsistent with the architecture for that slice.
9. Implement the missing capability, keeping public APIs aligned with docs/architecture/07-api-examples.md where applicable.
10. Add tests for the new capability, using this file's test scope.
11. Run the required verification commands for the slice.
12. Update "2.1 Current Progress Tracker":
    - mark completed checklist items [x];
    - add newly discovered missing work as [ ] items;
    - mark the phase status [x] only when all required items in that phase are [x].
13. If implementation proves architecture or plan text stale, update both docs and this plan. Do not use doc edits as a substitute for implementation.
14. Commit the completed slice with an English conventional commit message.
15. Summarize completed work, remaining work, verification results, and commit id/message.
16. If the current phase checklist is fully satisfied, move to the next phase. Otherwise choose the next small slice in the same phase.
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
- [ ] Checked tracker items were audited against code, tests, and examples
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
[ ] Checked tracker items in this phase are backed by code plus tests/examples, or an explicit no-code rationale.
```

### 5.4 Goal Completion Criteria

The whole goal can be marked complete only when:

```text
[ ] Phase 1 through Phase 9 are complete.
[ ] This file's global acceptance checklist is fully satisfied.
[x] architecture/00-overview.md system boundaries and module responsibilities are implemented.
[x] architecture/01-actor-runtime.md actor runtime capabilities are implemented and tested.
[x] All ActorExecutionPolicy variants are implemented and tested: TaskPerActor, KeyedWorkerPool, and DedicatedThreadPool.
[x] UnsupportedExecutionPolicy is used only for invalid configuration, not for planned policies in the completed framework.
[ ] architecture/02-rpc.md typed RPC, metadata, codegen, gateway decode/forward, Direct Actor Link, and Direct Link endpoint pooling are implemented and tested.
[x] architecture/03-placement.md placement, scale, drain, shutdown, crash, and watch are implemented and tested.
[x] architecture/04-eventbus-scheduler-config.md event bus, scheduler, and config are implemented and tested.
[x] architecture/05-gateway-ops.md gateway, rate limit, admin, telemetry, and inspection are implemented and tested.
[x] Valid constraints in architecture/06-appendix.md are not violated.
[x] API sketches in architecture/07-api-examples.md are covered by examples or compile tests.
[x] The implementation uses the planned Cargo workspace crate split; the root crate is not a monolithic implementation crate.
[x] examples/minimal-world runs as an end-to-end example.
    It exercises service bootstrap, actor registration, generated RPC, placement-backed client routing, EventBus publishing/subscription, scheduler/config, gateway frame decode/route registration, admin snapshot shape, and telemetry recording/export shape.
[x] cargo fmt passes.
[x] cargo clippy passes.
[x] cargo test passes.
[x] All completed implementation slices are committed with English conventional commit messages.
[x] Production paths have no unexplained TODO / FIXME / unimplemented! / todo!.
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
v0.7: Phase 8, Direct Actor Link
v0.8: Phase 9, Direct Link multiplexed transport runtime
```

Before every version release, update:

```text
examples/minimal-world
config examples
architecture checklist
known limitations
migration notes
```
