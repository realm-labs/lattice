# 01. Actor Runtime

> Rust core types, actor runtime, mailbox, ActorHandle, lifecycle, watch, and local child actors.  
> Back to: [architecture index](README.md)

---

## 6. Rust Core Types

### 6.1 Newtypes

All framework identifiers are explicit newtypes. Business identifiers such as `WorldId` or `PlayerId` are defined by business crates and are not built into lattice.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceKind(std::borrow::Cow<'static, str>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorKind(std::borrow::Cow<'static, str>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstanceId(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Epoch(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(String);
```

`ActorKind` and `ServiceKind` are opaque framework identifiers. They can be constructed from constants through macros:

```rust
pub const WORLD_SERVICE: ServiceKind = service_kind!("World");
pub const WORLD_ACTOR: ActorKind = actor_kind!("World");
```

The names `World`, `Player`, or `Guild` may appear in examples and business crates, but not as built-in framework variants.

### 6.2 RouteKey and ActorId

`RouteKey` and `ActorId` are abstract values used by routing and placement. They are not business-specific enums.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RouteKey {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ActorId {
    Str(String),
    U64(u64),
    I64(i64),
    Bytes(Vec<u8>),
}

pub trait ActorKey: Clone + Send + Sync + 'static {
    fn to_route_key(&self) -> RouteKey;
    fn to_actor_id(&self) -> ActorId;
    fn try_from_actor_id(actor_id: &ActorId) -> Result<Self, ActorKeyDecodeError>;
}
```

`ActorKey` conversion is used at API boundaries: codegen, route resolution, activation, and event delivery. It should not become a manual step on every business call.

---

## 7. Actor Runtime Design

### 7.1 Principles

```text
Actors are single-threaded state owners.
Business logic is typed through Handler<M>.
The runtime uses type-erased envelopes internally.
The public API does not expose a giant enum.
System messages have priority over normal messages.
Mailbox capacity and activation waiters are bounded.
ActorHandle is local-only.
Cross-process calls use generated clients or ActorRef.
Async tasks are created through ActorContext so they can be cancelled or isolated during stop/passivation.
```

### 7.2 Actor Scheduling Model

The actor scheduling model is part of lattice, not an implementation detail left to each feature. The first implementation runs on the service process's Tokio runtime, but all actor execution must go through `ActorRuntime`.

Required layering:

```text
Tokio runtime
  -> lattice ActorRuntime
    -> ActorExecutor
      -> ActorExecutionPolicy
        -> actor mailbox loop
```

The public scheduling API shape is:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorExecutionPolicy {
    TaskPerActor,
    ShardWorker { worker_count: usize },
    DedicatedThreadPool { worker_count: usize },
    LocalSet,
}

#[derive(Debug, Clone)]
pub struct ActorRuntimeConfig {
    pub default_execution: ActorExecutionPolicy,
}

pub struct ActorRuntime {
    executor: ActorExecutor,
    registry: ActorRegistry,
}

impl ActorRuntime {
    pub fn new(config: ActorRuntimeConfig) -> Self;

    pub async fn spawn_actor<A>(
        &self,
        actor: A,
        options: ActorSpawnOptions,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor;
}

#[derive(Debug, Clone)]
pub struct ActorSpawnOptions {
    pub mailbox: MailboxConfig,
    pub execution: Option<ActorExecutionPolicy>,
}
```

Phase 1 implements only `TaskPerActor`. The other variants are part of the stable design but may return `UnsupportedExecutionPolicy` until their phase is implemented.

Final scheduling semantics:

```text
TaskPerActor:
  One managed Tokio task owns one actor mailbox loop.
  This is the default for explicit actors, local child actors, and early runtime implementation.

ShardWorker:
  A fixed worker set owns many actor mailbox loops.
  Actor identity maps deterministically to a worker.
  This is intended for virtual-shard actors after Phase 4, where task count and locality matter.

DedicatedThreadPool:
  A named pool for actors that must be isolated from normal Tokio worker threads.
  This is for blocking-heavy or CPU-heavy actor families only when they cannot offload work elsewhere.

LocalSet:
  A single-thread Tokio LocalSet for !Send internal tasks or affinity-sensitive local execution.
  Public actor messages still remain Send unless a later phase explicitly relaxes that boundary.
```

Rules:

```text
Actor tasks are spawned by lattice ActorRuntime, not directly by business code.
ActorRuntime owns task naming, lifecycle, cancellation, metrics, tracing, and drain integration.
ActorContext creates scoped tasks through the actor runtime so they can be cancelled or isolated.
ServiceContext creates service-scoped tasks through the service runtime.
CPU-heavy or blocking work must not run directly on Tokio worker threads; use a blocking pool, dedicated worker, or external compute service.
ActorRegistry stores actor ownership independently from the concrete execution policy.
Mailbox semantics are identical across execution policies.
Changing execution policy must not change Handler<M> business code.
```

Forbidden implementation shortcuts:

```text
Do not expose tokio::spawn as the actor spawn API.
Do not make ActorHandle depend on Tokio JoinHandle.
Do not let each actor kind invent its own scheduling path.
Do not encode execution policy into business Handler<M> bounds.
Do not add ShardWorker/DedicatedThreadPool behavior before TaskPerActor semantics are tested.
```

This keeps the first version simple while fixing the final scheduling boundary: lattice owns actor scheduling; Tokio is only the first backing executor.

### 7.3 Core Traits

```rust
#[async_trait::async_trait]
pub trait Actor: Sized + Send + 'static {
    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        Ok(())
    }

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        Ok(())
    }
}

pub trait Message: Send + 'static {
    type Reply: Send + 'static;
}

#[async_trait::async_trait]
pub trait Handler<M>: Actor
where
    M: Message,
{
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        msg: M,
    ) -> Result<M::Reply, ActorError>;
}
```

`Actor::stopping` is where business code normally persists state. If it fails, the actor enters `StopFailed`; the runtime keeps ownership and blocks unload/release until retry or manual intervention.

### 7.4 Rpc Wrapper

RPC metadata is carried by framework metadata, not by business proto request fields.

```rust
#[derive(Debug, Clone)]
pub struct Rpc<T> {
    pub req: T,
    pub ctx: RpcContext,
}
```

`RpcContext` is extracted from gRPC metadata by the generated adapter. Business handlers receive the typed request and context without defining `meta: Option<_>` in every proto message.

### 7.5 Handler Example

```rust
#[async_trait::async_trait]
impl Handler<Rpc<EnterWorldRequest>> for WorldActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        msg: Rpc<EnterWorldRequest>,
    ) -> Result<EnterWorldReply, ActorError> {
        let player_id = PlayerId(msg.req.player_id);
        self.players.insert(player_id, PlayerRuntimeState::default());
        ctx.notify_after(Duration::from_secs(1), WorldTick);
        Ok(EnterWorldReply { ok: true })
    }
}
```

### 7.6 Mailbox

The mailbox has two lanes:

```text
system mailbox:
  stop, passivate, watch notification, ownership lost, supervisor control

normal mailbox:
  RPC messages, local messages, events, timers
```

System messages are prioritized so shutdown, fencing, passivation, and supervision are not starved by gameplay traffic.

Mailbox capacity is explicit. When full, the caller receives a clear backpressure error or timeout. The framework does not expose an unbounded business-visible stash.

### 7.7 ActorHandle

`ActorHandle<A>` is a local typed handle to an already running actor. It is used by local runtime internals, local child actors, tests, and local-only helpers.

```rust
#[derive(Clone)]
pub struct ActorHandle<A: Actor> {
    actor_ref: LocalActorRef,
    _marker: std::marker::PhantomData<A>,
}

impl<A: Actor> ActorHandle<A> {
    pub async fn call<M>(&self, msg: M) -> Result<M::Reply, ActorCallError>
    where
        A: Handler<M>,
        M: Message;

    pub async fn tell<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message<Reply = ()>;
}
```

`ActorHandle` must not cross RPC or EventBus boundaries. Cross-process references use `ActorRef`, `GatewaySessionRef`, or generated clients.

### 7.8 Stash and Deferred Messages

lattice does not expose an arbitrary unbounded stash to business code. During activation/loading, waiters are bounded and have timeouts. If activation fails, all waiters are woken with an error, and a later request may retry activation.

Business state machines should model deferred work explicitly with their own queue or pending operation state.

### 7.9 Slow I/O

Actor handlers should not block realtime actor execution with unbounded slow I/O. Use one of these patterns:

```text
Small bounded I/O in handler when latency is acceptable.
ActorContext scoped task for cancellable background work.
Dedicated service-level worker for heavy or shared I/O.
Business pending state plus retry/compensation for cross-service workflows.
```

Raw `tokio::spawn` is discouraged for actor-owned work because it can leak after actor unload. Use `ActorContext` task APIs.

### 7.10 High-Frequency Input

High-frequency gameplay input should be coalesced, sampled, batched, or pushed through specialized stream handling. It should not create one distributed actor RPC per frame when latency and volume are incompatible with the actor model.

---

## 7.11 Actor Watch

Watch lets one actor observe the termination of another actor's current incarnation.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatchId(String);

#[derive(Debug, Clone)]
pub struct ActorTerminated {
    pub target: ActorRef,
    pub incarnation: ActorIncarnation,
    pub reason: TerminatedReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminatedReason {
    Stopped,
    Passivated,
    Migrated,
    Fenced,
    NodeDown,
}
```

Rules:

```text
Watch is bound to the target's current owner epoch/incarnation.
Local watch can use the local registry directly.
Cross-node watch is registered through ActorRef / LogicControl.
The watcher does not hold a remote ActorHandle.
When the watcher stops or passivates, all watches are removed.
When the target stops, passivates, migrates, is fenced, or the node is declared down, the watcher receives a typed notification.
Notifications are best-effort plus owner/lease semantics; business logic must still be resilient.
```

---

## 7.12 Local Child Actors

A local child actor is spawned by a parent actor inside the same process. It is not placed in etcd and cannot be routed to from other nodes.

Use cases:

```text
Per-world helper actors.
Short-lived workflow helpers.
Local aggregation or throttling workers.
Isolation for slow local tasks without exposing them as distributed actors.
```

Rules:

```text
Children are owned by the parent actor.
Children stop when the parent stops or passivates.
Children are not migrated independently.
Children may be restarted by a parent-defined supervision policy.
Remote code cannot resolve a child actor through placement.
```

Example:

```rust
let child = ctx
    .spawn_child(
        ChildActorKey::new("combat-loop"),
        CombatLoopActor::new(self.world_id),
        ChildActorOptions::default(),
    )
    .await?;
```

---

## 8. Actor Lifecycle

### 8.1 State Machine

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorLifecycleState {
    Empty,
    Activating,
    Loading,
    Running,
    Passivating,
    Stopping,
    StopFailed,
    Stopped,
}
```

Activation is serialized per `(ActorKind, ActorId)`. Concurrent requests wait on a bounded waiter list.

### 8.2 ActorRegistry

The local registry prevents duplicate local activation and maps actor references to mailboxes. It is not a distributed placement store.

### 8.3 Lazy Activation

If a request reaches the owner instance and the local actor is not running, the runtime may ask the registered factory/loader to create it. If creation fails, no zombie actor remains and later requests can retry activation.

### 8.4 Passivation

Passivation stops an idle actor and releases local resources. For placed actors, owner release depends on the placement protocol and stop result.

Rules:

```text
Passivation is requested by policy, admin command, or business code.
The current handler is allowed to finish before stop begins.
New messages during passivation are rejected, redirected, or queued according to the placement state.
Actor::stopping is called for business save/cleanup.
If stopping fails, enter StopFailed and keep ownership until retry or manual intervention.
Scoped tasks and child actors are cancelled or stopped.
```

### 8.5 Business-Initiated Stop

Business code may request its own stop through the context, for example on player logout:

```rust
ctx.request_passivation(PassivationReason::BusinessIdle)?;
```

The request is applied after the current handler returns. This avoids dropping an in-flight reply.

### 8.6 Supervision

Supervision decides what happens when a handler, lifecycle hook, child actor, or scoped task fails.

Recommended first-version decisions:

```text
Handler error: return error to caller; actor remains running unless policy says otherwise.
Panic: stop or restart according to actor policy.
Child failure: restart child, stop child, or stop parent.
stopping failure: enter StopFailed; do not silently drop state.
Repeated failures: surface through metrics/admin API and require operator action when configured.
```
