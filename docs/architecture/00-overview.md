# 00. Overview and Boundaries

> Design conclusions, goals, terminology, system overview, invariants, and crate split.  
> Back to: [architecture index](README.md)

---

## 0. Design Conclusion

lattice uses a **Sharded Actor + Typed RPC + Placement Coordinator + EventBus** architecture.

```text
Gateway:
  Client msg_id -> RouteSpec -> route_key -> owner instance -> Logic RPC

Logic Service:
  One proto RPC service binds to one actor family or service surface.
  Each actor handles RPC, local events, timers, and system messages through Handler<M>.

RPC:
  Business proto files declare typed RPCs.
  lattice generates sharded typed clients and server adapters.
  Business code does not use raw tonic clients directly.

Placement:
  Virtual shards handle large sets of lightweight actors.
  actor_id -> shard is stable hashing; shard -> instance is assigned by the Coordinator.
  Explicit placement handles heavy actors such as world, room, and zone.
  Singleton placement handles cluster-wide singleton actors.

EventBus:
  Publishes domain events and system events.
  LocalEventBus handles in-process node events.
  NATS Core handles temporary cross-node broadcast.
  NATS JetStream handles durable cross-node pub/sub.
  EventBus does not replace owner RPC.

HA:
  Independent etcd stores placement, leases, and epochs.
  The Coordinator handles activation, migration, failover, and singleton ownership.
  Data-plane RPC connects directly to the owner and does not go through the Coordinator.

Correctness:
  owner epoch and fencing prevent old owners from coming back.
  request_id and dedup support idempotent retry.
  NOT_OWNER repairs stale route caches.
```

In one sentence: lattice is a Rust sharded actor framework for game backends. Business logic is written as typed `Handler<M>` implementations, service-to-service calls use generated sharded RPC clients, asynchronous integration uses EventBus, and the runtime manages actor owners, virtual shards, and cluster singletons with leases, epochs, idempotency, route caching, and NOT_OWNER correction.

---

## 1. Goals and Non-Goals

### 1.1 Goals

1. **Single owner for mutable state**  
   Every mutable state object has one authoritative owner at a time.

2. **Type-safe business code**  
   Business code handles Rust types through `Handler<M>` instead of large enums or hand-written binary codecs.

3. **Typed RPC across services**  
   Logic services call proto-defined typed RPCs. Routing, connection pooling, retries, NOT_OWNER, and epochs are hidden by lattice.

4. **Pub/sub events**  
   Domain and system events are published through EventBus. NATS is the first recommended broker; delivery guarantees come from broker configuration and business idempotency.

5. **Distributed actor placement**  
   Support virtual shards, explicit actor placement, and cluster singletons.

6. **Kubernetes friendly**  
   Kubernetes manages deployment, pod lifecycle, and basic service discovery. Independent etcd manages actor placement.

7. **Operable**  
   Built-in metrics, tracing, admin APIs, drain, migration, singleton failover, and actor inspection.

8. **Rust-native**  
   Use newtypes, traits, associated types, bounded channels, explicit errors, compile-time handler bounds, and minimal shared mutable state.

### 1.2 Non-Goals

```text
No cross-machine realtime combat simulation in the first version.
No distributed-lock-driven per-frame state sync.
No Kubernetes CRD per actor.
No etcd access on every RPC.
No Coordinator hop on every RPC.
No exactly-once guarantee.
No opaque bytes as the main internal service-to-service RPC model.
```

---

## 2. Terminology

| Term | Meaning |
|---|---|
| Logic Service | A process that hosts one or more actor families, such as world-service or player-service |
| Actor | The owner of one piece of state, such as WorldActor or PlayerActor |
| ActorKind | Type identifier for an actor family, such as `World` |
| ActorId | Instance identifier for an actor |
| ServiceKind | Type identifier for a logic service |
| InstanceId | Identifier for one running service process |
| RouteKey | RPC routing key |
| RouteTarget | Resolved target: advertised endpoint, instance id, and epoch |
| Placement | Mapping from actor, shard, or singleton to owner instance |
| Epoch | Fencing token that increases whenever owner changes |
| Lease | etcd lease used for instance and owner liveness |
| Coordinator | Control-plane service for activation, migration, failover, and singleton activation |
| Data plane | Normal business RPC path; must connect directly to the owner |
| Control plane | Placement, activation, migration, drain, and failover path |
| Singleton | Cluster-wide actor with one active owner per scope |
| EventBus | Pub/sub bus for broadcast, asynchronous integration, and cache invalidation |

---

## 3. System Overview

```text
Client
  -> Gateway
    -> Logic Services
      -> generated sharded clients
      -> actor handlers

Control plane:
  Logic Services <-> PlacementCoordinator <-> independent etcd

Async integration:
  Logic Services <-> EventBus / NATS

Deployment:
  Kubernetes handles pods, service DNS, rollout, and lifecycle hooks.
```

### 3.1 Gateway Responsibilities

Gateway owns client connections, authentication/session state, client frame decode/encode, msg_id routing, route key extraction, forwarding to logic owners, push back to clients, and gateway-level rate limiting.

Gateway does not own business state, orchestrate complex business workflows, or write business databases.

### 3.2 Logic Service Responsibilities

Logic services host actor families, maintain local actor registries, expose generated RPC adapters, execute `Handler<M>`, manage local lifecycle/passivation, and call business load/save hooks.

A single process may host multiple actor kinds and multiple generated gRPC services while sharing one advertised endpoint.

### 3.3 PlacementCoordinator Responsibilities

The Coordinator aggregates instance state, assigns virtual shards, activates explicit actors, activates cluster singletons, coordinates migration/drain/failover, and writes placement state to etcd.

It is not in the normal data-plane RPC path.

### 3.4 EventBus Responsibilities

EventBus carries asynchronous domain events, cross-service integration events, cache invalidation, local node events, admin broadcast, and low-frequency fan-out. It must not be used as a substitute for typed owner RPC when a command needs owner routing, fencing, or a synchronous result.

---

## 4. Core Invariants

```text
Each mutable actor state has exactly one authoritative owner.
Every owner change increments epoch.
Every state-changing RPC carries a request_id.
Route cache is an optimization only; NOT_OWNER always invalidates stale entries.
Coordinator handles control plane only.
Data-plane RPC connects directly to the owner.
ActorHandle is local-only.
Cross-process references use ActorRef, GatewaySessionRef, or generated clients.
The framework does not depend on business databases.
Business types are not hardcoded into the framework.
```

---

## 5. Suggested Crate Split

```text
lattice-core
  ids, errors, RouteKey, ActorKind, ServiceKind, InstanceConfig, TraceContext

lattice-actor
  Actor, Handler<M>, ActorContext, mailbox, registry, lifecycle, local watch, child actors

lattice-rpc
  Rpc<T>, RpcContext, generated client runtime, metadata, route cache, EndpointPool

lattice-codegen
  proto options, generated clients, generated adapters, gateway bindings

lattice-placement
  PlacementStore, RouteResolver, virtual shards, explicit placement, singleton placement

lattice-coordinator
  Coordinator service, activation, migration, drain, failover

lattice-eventbus
  EventBus, LocalEventBus, NATS adapter, typed EventPublisher, subscribe_actor

lattice-scheduler
  actor scheduler and service scheduler

lattice-config
  ConfigSource, BootstrapConfig, ConfigStore

lattice-gateway
  client codec, route table, forwarding, session refs, push, rate limit

lattice-ops
  admin APIs, inspectors, telemetry, metrics, tracing
```
