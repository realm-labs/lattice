# 01. Actor Runtime

> Rust core types, actor runtime, mailbox, ActorHandle, lifecycle, watch, and child actors.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeIncarnation(Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActivationId(Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorPath(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityId(Bytes);
```

`ActorKind` and `ServiceKind` are opaque framework identifiers. They can be constructed from constants through macros:

```rust
pub const WORLD_SERVICE: ServiceKind = service_kind!("World");
pub const WORLD_ACTOR: ActorKind = actor_kind!("World");
```

The names `World`, `Player`, or `Guild` may appear in examples and business crates, but not as built-in framework variants.

### 6.2 Entity Keys

Business identifiers convert to canonical entity bytes only at the `EntityRef` boundary. They are not framework enums and concrete actors do not need entity IDs.

```rust
pub trait EntityKey: Clone + Send + Sync + 'static {
    fn to_entity_id(&self) -> EntityId;
    fn try_from_entity_id(entity_id: &EntityId) -> Result<Self, EntityKeyDecodeError>;
}

pub trait ShardedActor: Actor {
    type Key: EntityKey;
}
```

`EntityId` is a bounded canonical byte string; every `EntityKey` implementation must produce identical bytes across processes and versions. `ActorPath` is canonical, hierarchical, length-bounded, and validated by the runtime. Child names are one escaped path segment. `ActivationId` is newly generated for every actor lifetime and is never derived from the path.

---

## 7. Actor Runtime Design

### 7.1 Principles

```text
Actors are single-threaded state owners.
One-way messages use `Handler<M>`; request/reply messages use `Responder<R>`.
The runtime uses type-erased envelopes internally.
The public API does not expose a giant enum.
System messages have priority over normal messages.
Mailbox capacity and activation waiters are bounded.
ActorHandle is local-only.
Cross-process messages use ActorRef, EntityRef, or SingletonRef through lattice-remoting.
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
    KeyedWorkerPool { worker_count: usize },
    DedicatedThreadPool { worker_count: usize },
}

#[derive(Debug, Clone)]
pub struct ActorRuntimeConfig {
    pub default_execution: ActorExecutionPolicy,
    pub observer: ActorObserverHandle,
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
    pub scheduler_key: Option<SchedulerKey>,
}
```

All `ActorExecutionPolicy` variants are framework-owned scheduling paths. They must not be aliases for each other.

Final scheduling semantics:

```text
TaskPerActor:
  One managed Tokio task owns one actor mailbox loop.
  This is the default for user actors, child actors, and the early runtime implementation.

KeyedWorkerPool:
  A fixed worker set owns many actor mailbox loops on lattice-managed worker runtimes.
  The scheduler_key maps deterministically to a worker.
  If scheduler_key is not provided, the runtime hashes the concrete ActorPath.
  This is useful for stable affinity, cache locality, and predictable distribution without claiming to be a full shard scheduler.

DedicatedThreadPool:
  A named pool for actors that must be isolated from normal Tokio worker threads.
  The pool is scoped by actor Rust type and worker_count.
  Actors of the same type reuse that type's dedicated worker pool.
  Different actor types do not share a dedicated worker pool unless a future explicit named-pool API is added.
  A pool worker can run many actor mailbox loops; this is not one OS thread per actor.
  Actors of the same type are assigned across the pool, currently by round-robin.
  This is for blocking-heavy or CPU-heavy actor families only when they cannot offload work elsewhere.
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
Changing execution policy must not change `Handler<M>` business code.
Sharded entities should pass a stable scheduler_key derived from EntityId when using KeyedWorkerPool.
```

Forbidden implementation shortcuts:

```text
Do not expose tokio::spawn as the actor spawn API.
Do not make ActorHandle depend on Tokio JoinHandle.
Do not let each actor kind invent its own scheduling path.
Do not encode execution policy into business `Handler<M>` bounds.
Do not add KeyedWorkerPool/DedicatedThreadPool behavior before TaskPerActor semantics are tested.
```

This keeps the first version simple while fixing the final scheduling boundary: lattice owns actor scheduling; Tokio is only the first backing executor.

### 7.3 Core Traits

```rust
use std::future::Future;

pub trait Actor: Sized + Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn before_message(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message: MessageView<'_>,
    ) {}

    fn after_message(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        _outcome: MessageOutcome,
    ) {}

    fn on_error<M>(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        _error: &Self::Error,
    ) -> impl Future<Output = ()> + Send
    where
        M: Send + 'static,
    {
        async {}
    }

    fn started(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> impl Future<Output = Result<(), ActorStopError>> + Send {
        async { Ok(()) }
    }
}

pub trait Message: Send + 'static {}

pub trait Request: Send + 'static {
    type Response: Send + 'static;
}

pub trait Responder<R>: Actor
where
    R: Request,
{
    fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: R,
        reply_to: ReplyTo<R::Response>,
    ) -> impl Future<Output = Result<(), ActorError>> + Send;
}

pub trait Handler<M>: Actor
where
    M: Message,
{
    fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        msg: M,
    ) -> impl Future<Output = Result<(), ActorError>> + Send;
}
```

`Actor::stopping` is the business durability hook for a graceful stop. If it fails, the runtime
retains the exact Actor object and its in-memory state, quiesces scoped tasks and children, rejects
business traffic, keeps the admin lane open, and does not emit `ActorTerminated`. Implementations
must make persistence retry-safe with idempotency, operation IDs, or version/CAS semantics. This
does not make process memory crash-safe. External shard/singleton claim loss is stronger: admission
and routing are fenced immediately, and the retained object moves to non-authoritative quarantine
without delaying replacement authority.

#### Message hooks and runtime observation

`before_message` and `after_message` are synchronous actor-local hooks. `MessageView` provides immutable
`Any`-based inspection through `is::<M>()` and `downcast_ref::<M>()`; it cannot consume, mutate,
replace, or suppress typed dispatch. `MessageMetadata` reports the concrete Rust type, tell/request
kind, mailbox lane, submission time, and optional deadline. `after_message` distinguishes successful,
failed, recovered, and dequeued-but-rejected dispatch. Lifecycle commands such as stop remain on the
lifecycle path rather than pretending to be business messages.

Framework-wide telemetry uses one synchronous `ActorObserver`, configured through
`ActorRuntimeConfig::observer` or `ActorRegistry::with_observer`. It observes accepted/rejected
mailbox submissions, queue and processing time, lifecycle outcomes, protocol failures, and
end-to-end request completion. Request completion is emitted once, including for deferred replies,
caller drop, deadline, handler failure, and mailbox rejection. Observer callbacks must remain fast
and must not use actor IDs, entity IDs, or payload values as unbounded metric labels.

### 7.4 Message Envelope Context

Business handlers receive their declared message type directly. A tell sent by an actor exposes that exact activation through `ActorContext`; process-originated tells have no actor sender.

```rust
let sender: Option<&ActorRef<()>> = ctx.sender();
```

The sender is message-scoped and read-only; the runtime replaces it before each tell and clears it after the turn. Clone the `ActorRef` only when the actor intentionally needs to retain the exact sending activation.

`ctx.tell(&actor_ref, message).await` stamps `ctx.self_ref()` as the sender. `ctx.forward(&actor_ref, message).await` preserves the current envelope sender instead, including `None`. Both methods also accept `EntityRef` and `SingletonRef` directly; there is no public binding or bound-recipient type. Local-only `ActorHandle` delivery uses `tell_local` and `forward_local`. Local and remote tells use the same envelope and handler path, and remoting carries an optional exact `ActorRef` after codec dispatch.

Passing `ctx.self_ref().cloned()` in a serializable business message lets another actor retain the reference and send later. `ActorRef<T>` deserializes as ordinary identity data; the receiving context resolves its registered `ProtocolId` when sending, so no bind step is required. Because an `ActorRef` identifies one activation, it becomes stale after stop, restart, passivation, or relocation. Long-lived routing to a sharded or singleton identity should retain an `EntityRef` or `SingletonRef` instead.

An ask does not install a dynamically typed sender in the context. `Responder<R>` receives a typed, single-use `ReplyTo<R::Response>` by value. It may answer immediately or move the token into a continuation message. This keeps reply ownership explicit and prevents tell handlers from accidentally acquiring reply semantics.

### 7.5 Handler Examples

```rust
impl Responder<EnterWorld> for WorldActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: EnterWorld,
        reply_to: ReplyTo<EnterWorldReply>,
    ) -> Result<(), ActorError> {
        let player_id = request.player_id;
        self.players.insert(player_id, PlayerRuntimeState::default());
        ctx.notify_after(Duration::from_secs(1), WorldTick);
        reply_to.send(EnterWorldReply { ok: true })?;
        Ok(())
    }
}

impl Handler<WorldTick> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message: WorldTick,
    ) -> Result<(), ActorError> {
        self.advance_simulation();
        Ok(())
    }
}
```

For asynchronous I/O that is not tied to an ask, any handler can launch bounded work and map its output to a one-way continuation:

```rust
ctx.pipe_to_self(
    async move { db.refresh_profile(player_id).await },
    |result| ProfileRefreshed { result },
)?;
```

When the continuation is private workflow logic rather than a domain message, `continue_with`
resumes directly against the Actor in a later normal-mailbox turn. The callback is synchronous and
may start the next asynchronous step without defining intermediate message types or handlers:

```rust
let db = self.db.clone();
ctx.continue_with(
    async move { db.load_profile(player_id).await },
    |actor, ctx, profile| {
        actor.profile = Some(profile?);

        let db = actor.db.clone();
        let alliance_id = actor.profile.as_ref().unwrap().alliance_id;
        ctx.continue_with(
            async move { db.load_alliance(alliance_id).await },
            |actor, _ctx, alliance| {
                actor.alliance = Some(alliance?);
                Ok(())
            },
        )?;
        Ok(())
    },
)?;
```

The asynchronous futures own their dependencies and cannot borrow Actor state. Other messages may
interleave before either continuation. Direct continuations bypass typed Behavior admission and run
against the then-current Actor state; they remain visible to hooks and runtime observers as
`MessageKind::Continuation`. Use `pipe_to_self` when the result is a meaningful domain message that
should pass through a typed `Handler` and Behavior admission.

When the work belongs to an ask, the responder uses `defer_reply` so the continuation inherits the request deadline and owns its reply capability. The continuation is a later actor turn, so it can combine the query result with current actor state before replying:

```rust
#[derive(lattice_actor::Message)]
struct ProfileLoaded {
    result: Result<DbProfile, DbError>,
    reply_to: ReplyTo<GetPlayerViewResponse>,
}

impl Responder<GetPlayerView> for WorldActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: GetPlayerView,
        reply_to: ReplyTo<GetPlayerViewResponse>,
    ) -> Result<(), ActorError> {
        let db = self.db.clone();
        ctx.defer_reply(
            reply_to,
            async move { db.load_profile(request.player_id).await },
            |result, reply_to| ProfileLoaded { result, reply_to },
        )?;
        Ok(())
    }
}

impl Handler<ProfileLoaded> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        loaded: ProfileLoaded,
    ) -> Result<(), ActorError> {
        match loaded.result {
            Ok(profile) => loaded.reply_to.send(self.build_view(profile))?,
            Err(error) => loaded.reply_to.fail(error)?,
        }
        Ok(())
    }
}
```

`defer_reply` is bounded by the mailbox's deferred capacity, observes the ask deadline, and is cancelled when the actor stops or passivates. Other messages may interleave before the continuation, which is precisely why it observes current rather than captured actor state. For asynchronous work that is not tied to an ask, `pipe_to_self(future, map)` posts the mapped result back as an ordinary one-way message without reply or deadline semantics, while `continue_with(future, callback)` resumes private workflow logic directly against the Actor.

### 7.6 Mailbox

The mailbox has two lanes:

```text
system mailbox:
  stop, passivate, watch notification, ownership lost, supervisor control

normal mailbox:
  actor messages, local events, timers
```

System messages are prioritized so shutdown, fencing, passivation, and supervision are not starved by gameplay traffic.

Mailbox and deferred-operation capacities are explicit. When either is full, the caller receives a clear backpressure error or timeout. The framework does not expose an unbounded business-visible stash.

### 7.7 ActorHandle

`ActorHandle<A>` is a local typed handle to an already running actor. It is used by local runtime internals, local child actors, tests, and local-only helpers.

```rust
#[derive(Clone)]
pub struct ActorHandle<A: Actor> {
    actor_ref: LocalActorRef,
    _marker: std::marker::PhantomData<A>,
}

impl<A: Actor> ActorHandle<A> {
    pub async fn ask<R>(
        &self,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response, ActorCallError>
    where
        A: Responder<R>,
        R: Request;

    pub async fn tell<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        M: Message;

    pub fn try_tell<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        M: Message;
}
```

`tell` waits for bounded mailbox capacity; `try_tell` returns immediately when the mailbox is full.
Every `ActorTellError<M>` variant retains the original message, allowing the caller to retry or
reroute without requiring `M: Clone`. The runtime constructs the type-erased envelope only for the
channel operation and recovers `M` from a rejected envelope.

Every ask has an explicit relative timeout. The runtime converts it to one monotonic deadline before mailbox admission; that deadline covers mailbox waiting, handler execution, and deferred reply delivery. A zero timeout returns `DeadlineExceeded`, and a duration that cannot be represented by `Instant` returns `InvalidTimeout`. There is no public unbounded or absolute-deadline ask API.

Actor code can issue routed asks through `ActorContext`:

```rust
let reply = ctx
    .ask(&entity_ref, request, Duration::from_secs(2))
    .await?;
```

If the current message is itself an ask, `ActorContext` clamps the downstream deadline to the earlier of the parent deadline and the requested timeout. This prevents nested calls from extending the caller's original budget.

`ActorHandle` must not cross remoting or EventBus boundaries. Cross-process messages carry `ActorRef`, `EntityRef`, or `SingletonRef`. A Gateway session is represented by an `ActorRef<GatewaySessionActor>`.

### 7.8 Stash and Deferred Messages

lattice does not expose an arbitrary unbounded stash to business code. During activation/loading, waiters are bounded and have timeouts. If activation fails, all waiters are woken with an error, and a later request may retry activation.

Business state machines should model deferred work explicitly with their own queue or pending operation state.

### 7.9 Slow I/O

Actor handlers should not block realtime actor execution with unbounded slow I/O. Use one of these patterns:

```text
Small bounded I/O in handler when latency is acceptable.
ActorContext `pipe_to_self` for bounded asynchronous work that must return to actor state.
ActorContext `continue_with` for private multi-step asynchronous workflows without intermediate messages.
ActorContext `defer_reply` for request work that must preserve the ask deadline and reply capability.
ActorContext scoped task for cancellable background work that has no caller reply.
Dedicated service-level worker for heavy or shared I/O.
Business pending state plus retry/compensation for cross-service workflows.
```

Raw `tokio::spawn` is discouraged for actor-owned work because it can leak after actor unload. Use `ActorContext` task APIs.

### 7.10 High-Frequency Input

High-frequency gameplay input should be coalesced, sampled, or batched into typed actor messages before delivery. Remote tells use the same remoting association's bounded bulk lane and normal mailbox path; there is no `DirectLinkStream`, `Linked<M>`, or parallel stream transport API. If one message per simulation frame is incompatible with latency or volume limits, change the business message granularity rather than bypassing ActorRef and mailbox semantics.

---

## 7.11 Actor Watch

DeathWatch always observes one concrete activation. Logical references provide `watch_current` only as a resolve-without-activate convenience.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatchId(String);

#[derive(Debug, Clone)]
pub struct Terminated {
    pub subject: WatchedSubject,
    pub reason: TerminatedReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminatedReason {
    Stopped,
    Passivated,
    Handoff,
    StaleActivation,
    ClaimLost,
    NodeDown,
}
```

Rules:

```text
Concrete ActorRef watch is bound to its node incarnation, actor path, and activation ID.
Local concrete watch uses the local registry; remote watch uses lattice-remoting control frames.
Watching a dead concrete activation produces Terminated and never follows a replacement at the same path.
Watching an inactive EntityRef returns NotActive without activating it.
Watching an unavailable SingletonRef returns Unavailable without allocating it.
EntityRef/SingletonRef watch_current resolves and binds the current exact ActivationId.
The watcher does not hold a remote ActorHandle.
When the watcher stops or passivates, all watches are removed.
Concrete Terminated is sent when the activation stops or its node incarnation is declared down.
A passivation, handoff or singleton failover terminates the old activation watch; a later activation requires a new watch.
Business logic must remain resilient to delayed notification and concurrent in-flight messages.
```

---

## 7.12 Local Child Actors

A child actor is spawned by a parent inside the same process and is not independently placed in etcd. Its concrete `ActorRef` is serializable and can be used by remote nodes while that exact activation lives.

Use cases:

```text
Per-world helper actors.
Short-lived workflow helpers.
Local aggregation or throttling workers.
Isolation for slow local tasks while retaining normal concrete-reference semantics.
```

Rules:

```text
Children are owned by the parent actor.
Children stop when the parent stops or passivates.
Children are not migrated independently.
Children may be restarted by a parent-defined supervision policy.
Remote code cannot resolve or create a child through placement or wildcard path selection.
Remote code can send to a child only after receiving its concrete ActorRef.
```

Example:

```rust
let child = ctx
    .spawn_child(
        "combat-loop",
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
    Starting,
    Running,
    Passivating,
    Stopping,
    StopFailed,
    Quarantined,
    Stopped,
}
```

Registry activation is separately observable as
`EntityActivationState::{Absent, Activating, Loading, Active}`. Loading variants never appear in a
live Actor cell.

Entity activation is serialized per `(EntityType, EntityId)` at the owning shard. Concrete spawn is serialized by the parent/path registry. Concurrent activation waiters are bounded and deadline-controlled.

### 8.2 ActorRegistry

The local registry prevents duplicate local activation and maps actor references to mailboxes. It is not a distributed placement store.

Every registry in one service shares a bounded `ActivationDirectory` through `ServiceContext`.
Root and child spawns register the exact `(ActorPath, ActivationId, protocol)` and typed local handle;
remote protocol dispatch resolves through this directory, so heterogeneous child actor types remain
addressable without flattening child paths into root registry keys. Successful stop, passivation,
startup failure, and explicit force stop run one eager exact terminal-cleanup callback before the
single `ActorTerminated` event. `StopFailed` remains reserved but non-routable; quarantine is removed
from current routing and held in a separately bounded recovery map. Capacity exhaustion rejects
registration or quarantine rather than silently dropping retained state.

### 8.3 Lazy Activation

If a request reaches the owner instance and the local actor is not running, the runtime may ask the registered factory/loader to create it. If creation fails, no zombie actor remains and later requests can retry activation.

### 8.4 Passivation

Passivation stops an idle sharded entity activation and releases local resources without changing shard ownership. Concrete user actors normally stop rather than passivate.

Rules:

```text
Passivation is requested by policy, admin command, or business code.
The current handler is allowed to finish before stop begins.
New entity messages during passivation are rejected until the old activation has fully stopped.
Actor::stopping is called for business save/cleanup.
If voluntary stopping fails, enter StopFailed and keep the activation registered until the configured retry/operator policy resolves it while the old claim remains valid.
If an external claim is lost, fence exact/logical admission immediately, quarantine the old object,
and surface StateLossPossible when persistence still fails; replacement authority does not wait.
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
Actor callback panic: terminate the current instance after framework cleanup; do not continue or retry that instance.
Child failure: restart child, stop child, or stop parent.
voluntary stopping failure: enter observable StopFailed and block voluntary release while authority remains valid.
externally fenced stopping failure: never retain authority; raise StateLossPossible for recovery/ops.
Repeated failures: surface through metrics/admin API and require operator action when configured.
```

A panic from `started`, message dispatch and its hooks, `stopping`, or Actor destruction produces the
terminal `Panicked` lifecycle result. Pending asks fail with `ActorPanicked`, DeathWatch observes the
termination exactly once, and the current activation is never resumed. A parent using the
`RestartChild` supervision directive may construct a replacement child, but that replacement has a
new activation identity. Panics in independently spawned scoped or pipe tasks remain isolated from
the Actor and are harvested by the runtime as task failures.
