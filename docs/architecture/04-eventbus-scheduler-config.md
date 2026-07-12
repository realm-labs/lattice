# 04. EventBus, Scheduler, and Configuration

> Asynchronous integration and runtime support outside point-to-point actor messaging.
> Back to: [architecture index](README.md)

---

## 1. EventBus Boundary

EventBus is a typed pub/sub abstraction for broadcast and asynchronous integration.

```text
Use actor tell/ask:
  one destination owns the command
  mailbox ordering matters
  caller needs an immediate typed result
  actor identity, sharding, singleton routing, or DeathWatch matters

Use EventBus:
  zero-to-many consumers
  domain integration or cache invalidation
  producer and consumer lifecycles are decoupled
  broker durability/replay is desired
```

LocalEventBus handles in-process events. A NATS adapter is the recommended first cluster implementation: NATS Core for transient fan-out and JetStream for durable/at-least-once subscriptions.

## 2. Event Types and Envelopes

```rust
pub trait Event: Send + Sync + 'static {
    const TYPE_ID: &'static str;
}

pub struct EventEnvelope<E> {
    pub event_id: EventId,
    pub subject: Subject,
    pub occurred_at: SystemTime,
    pub trace: TraceContext,
    pub payload: E,
}

#[async_trait::async_trait]
pub trait EventBus: Clone + Send + Sync + 'static {
    async fn publish<E: Event>(&self, event: EventEnvelope<E>) -> Result<(), EventBusError>;
    async fn subscribe<E, H>(
        &self,
        filter: SubjectFilter,
        options: DeliveryOptions,
        handler: H,
    ) -> Result<SubscriptionHandle, EventBusError>
    where
        E: Event,
        H: EventHandler<E>;
}
```

Event codecs are registered independently from actor message protocols, though both may use the same `WireCodec<T>` implementations.

## 3. Actor Subscription

An EventBus subscription may translate a typed event into an actor message:

```rust
service.cluster_events()
    .subscribe_entity::<GuildCreated, WorldActor>(
        SubjectFilter::new("game.guild.created"),
        |event| WorldId(event.world_id),
        |event| ApplyGuildCreated(event),
        DeliveryOptions::at_least_once(),
    )
    .await?;
```

The subscription owns no `ActorHandle`. It obtains an `EntityRef` and sends through the local ShardRegion. Broker redelivery means handlers must deduplicate by event ID or a business key. EventBus delivery never inherits `ask` semantics.

Subscriptions are service-scoped, cancelled during drain, and supervised with bounded concurrency and backoff. Durable replay depends on broker configuration and consumer identity.

Recommended subjects:

```text
game.<domain>.<event>
system.<service>.<event>
admin.<scope>.<event>
```

## 4. Actor Scheduler

The actor scheduler delivers ordinary typed messages through a reference. Scheduled tasks are non-durable in the first version.

```rust
ctx.scheduler().tell_after(
    Duration::from_secs(5),
    ctx.self_ref(),
    RetryPendingOperation { operation_id },
);

ctx.scheduler().tell_interval(
    Duration::from_secs(1),
    ctx.self_ref(),
    Tick,
);
```

Actor-owned timers are cancelled when that activation stops. A concrete remote target may die before delivery; the timer reports or records the normal tell failure. Scheduling an `EntityRef` or `SingletonRef` follows the logical reference at delivery time.

Timers use monotonic time for delays and intervals. The scheduler must bound pending tasks and avoid an unbounded catch-up burst after runtime stalls.

## 5. Service Scheduler

Service-level work not owned by one actor uses a service scheduler:

```rust
let handle = service.scheduler().interval(
    Duration::from_secs(30),
    || async { refresh_local_cache().await },
)?;
```

Service tasks are runtime-managed, observable, cancellable, and bounded by explicit concurrency/deadline policy. Business code should not use raw detached tasks for service lifecycle work.

This scheduler is not a durable workflow engine. Work that must survive process loss belongs in business persistence or an external durable scheduler.

## 6. Bootstrap Configuration

Bootstrap configuration constructs immutable or restart-bound runtime components:

```text
node identity and roles
remoting endpoint, protocol limits, TLS, and authorization
Coordinator bootstrap discovery
entity types, shard counts, buffer limits, and passivation
singleton definitions
EventBus, ConfigStore, telemetry, admin HTTP, Gateway, and rate limits
```

```rust
pub enum ConfigSource {
    File(PathBuf),
    FileWithFormat { path: PathBuf, format: ConfigFormat },
    Env { prefix: String },
    Inline(BootstrapConfig),
    Composite(Vec<ConfigSource>),
}
```

Composite sources merge in order; later values override earlier values. Final configuration retains source/version metadata for inspection. Secrets are loaded from mounted files or a secret provider, not embedded in a printable configuration snapshot.

## 7. Coordinator Bootstrap and etcd Credentials

Coordinator-eligible processes receive authenticated etcd configuration and the authority to participate in leader election. Ordinary runtime and Gateway nodes receive only a narrow bootstrap client that can locate the leader and establish an authorized remoting association.

```rust
let service = LatticeService::builder(NodeConfig::from_env()?)
    .config(ConfigSource::file("config/player-service.toml"))
    .remoting(RemotingConfig::from_config()?)
    .coordinator(CoordinatorBootstrap::from_config()?)
    .event_bus(NatsEventBus::from_config()?)
    .config_store(EtcdConfigStore::from_config()?)
    .build()
    .await?;
```

`ConfigStore` credentials are independent from Coordinator placement credentials. Sharing an etcd cluster does not imply sharing identities or write permissions.

Development may use loopback plaintext and an in-memory Coordinator/store through an explicitly dangerous builder option. Production does not silently fall back when Coordinator authentication, remoting TLS policy, or etcd credentials are incomplete.

## 8. Runtime `ConfigStore`

`ConfigStore` is for low-frequency configuration watch/reload:

```rust
#[async_trait::async_trait]
pub trait ConfigStore: Clone + Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Value>, ConfigStoreError>;
    async fn put(&self, key: String, value: Value) -> Result<(), ConfigStoreError>;
    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError>;
}
```

Suitable values include feature flags, rate limits, scheduler switches, and validated operational parameters. It is not for actor state, placement truth, idempotency records, transaction logs, or high-frequency player data.

Protocol fingerprints, actor definitions, shard counts, hash versions, remoting compatibility, and node identity require restart or an explicit cluster migration. Hot reload is allowed only for parameters whose consumers implement validation, atomic application, rollback/failure reporting, and inspection of the active version.

Rebalance strategy ID/version and hard eligibility constraints follow the entity-type compatibility rules. Thresholds, sample-age limits, cooldowns, and concurrency limits may be revisioned operational configuration: a validated update affects only future proposals, while every active `RebalancePlan` retains the policy version and limits under which it was admitted.

## 9. Failure and Backpressure Rules

```text
Event publish failure is visible to the producer.
At-least-once consumers must be idempotent.
Subscriber and scheduled-task concurrency is bounded.
Actor delivery uses normal recipient backpressure and deadline rules.
Config watch disconnect retries with jitter and exposes staleness.
Shutdown stops admission, cancels subscriptions/timers, then waits within the drain deadline.
```
