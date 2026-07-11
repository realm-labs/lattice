# 04. EventBus, Scheduler, and Config

> EventBus, actor scheduler, service scheduler, bootstrap config, and runtime config store.  
> Back to: [architecture index](README.md)

---

## 20. Scheduler

lattice provides non-durable schedulers similar to Akka scheduler semantics. Scheduled tasks live in process memory and are lost after process restart, actor passivation, service stop, or ownership loss.

### 20.1 Actor Scheduler

Actor scheduler is exposed through `ActorContext`.

```text
Bound to actor lifecycle.
Cancelled on actor stop, passivation, or ownership loss.
Delivers messages only to the current actor mailbox.
Suitable for tick, debounce, short retry, and temporary timeout.
```

Example:

```rust
ctx.notify_interval(Duration::from_millis(50), || WorldTick { delta_ms: 50 });
ctx.notify_after(Duration::from_secs(3), SaveTimeout);
```

### 20.2 Service Scheduler

Service scheduler is exposed through service runtime.

```text
Bound to service instance lifecycle.
Lost on service stop or restart.
Suitable for metrics flush, local cache refresh, temporary maintenance jobs, and local subscriptions.
```

Example:

```rust
service
    .scheduler()
    .interval(Duration::from_secs(10), || async move {
        flush_metrics().await
    });
```

### 20.3 Scheduler Semantics

```text
not persistent
not migrated across processes
no restart compensation
no exactly-once or at-least-once delivery guarantee
actor scheduler only delivers to its actor mailbox
service scheduler only runs inside its service instance
```

Long-term business facts such as season ending, auction settlement, mail expiration, building completion, or any task that must survive restart should be implemented by the business database/job system, or by a business singleton/control actor that scans business state at startup and schedules in-memory timers again.

lattice does not provide a durable timer store.

---

## 21. EventBus / PubSub

lattice exposes two event buses.

### 21.1 LocalEventBus

LocalEventBus is an in-process event bus for one service instance.

```text
no cross-process propagation
no NATS dependency
no persistence
low overhead
useful for local actor/service decoupling, cache invalidation, lightweight notifications, and tests
```

LocalEventBus is not suitable for durable domain events or events that other service instances must observe.

### 21.2 Cluster EventBus

Cluster EventBus is the cross-node pub/sub abstraction. The first recommended implementation is NATS:

```text
NATS Core:
  temporary broadcast, cache invalidation, admin broadcast

NATS JetStream:
  broker-side persistence, consumer groups, replay, ack, durable subscriptions
```

Framework adapters:

```text
LocalEventBus: in-process delivery only.
InMemoryNatsEventBus: test/local NATS-like stream and durable-idempotency semantics.
NatsEventBus: real NATS client backed by async-nats; Core NATS publish/subscribe is available
and durable_name maps to a queue group. Broker-side durable replay requires JetStream-specific
adapter work or external bridge configuration.
```

Cluster EventBus is useful for:

```text
domain events such as PlayerLeveledUp, AuctionSettled, GuildCreated
asynchronous integration such as analytics, mail, notification, billing adapter
cross-instance broadcast such as cache invalidation and config reload
low-frequency fan-out such as announcements or leaderboard updates
```

It is not used for:

```text
stateful owner RPC
commands that need an immediate return value
owner routing source of truth
high-frequency gameplay input
```

### 21.3 Core Types

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subject(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventId(String);

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub event_id: EventId,
    pub subject: Subject,
    pub event_type: String,
    pub source_service: ServiceKind,
    pub source_instance: InstanceId,
    pub actor_kind: Option<ActorKind>,
    pub actor_id: Option<ActorId>,
    pub request_id: Option<RequestId>,
    pub trace: TraceContext,
    pub occurred_unix_ms: u64,
    pub payload: bytes::Bytes,
}

#[async_trait::async_trait]
pub trait EventBus: Clone + Send + Sync + 'static {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError>;
    async fn subscribe(
        &self,
        subscription: EventSubscription,
        handler: Box<dyn EventHandler>,
    ) -> Result<EventSubscriptionHandle, EventBusError>;
}
```

### 21.4 Typed Event API

Business code should prefer typed event publishers so metadata is filled by the framework:

```rust
ctx.service()
    .cluster_events()
    .publish(WorldEvents::player_entered(PlayerEnteredWorld {
        world_id: self.world_id.0,
        player_id: player_id.0,
    }))
    .await?;
```

Low-level `EventBus::publish(EventEnvelope)` remains available for framework adapters, external bridges, and advanced cases.

### 21.5 Actor Event Subscription

Events may be delivered directly into an actor message handler:

```rust
service
    .cluster_events()
    .subscribe_actor(
        SubjectFilter::new("game.guild.*"),
        EventActorRoute::by_key(|event: &GuildCreated| WorldId(event.world_id)),
        DeliveryOptions::at_least_once(),
    )
    .await?;
```

Rules:

```text
EventBus does not hold ActorHandle.
Subscribers route through ActorRef or ActorKey to the current owner.
Delivery reuses RouteResolver, epoch, NOT_OWNER retry, and fencing.
Target actor may lazy-activate.
Delivery is at-least-once; actor handlers must be idempotent by event_id or business key.
Do not use EventBus actor delivery for commands that need a synchronous reply.
```

### 21.6 Subscription Lifecycle

```text
Subscriptions registered through ServiceEvents are owned by service runtime.
subscribe_actor is also owned by service runtime, but delivery goes through actor routing.
service stop/drain/shutdown cancels subscription handles.
subscriber background work must use service scheduler or runtime-managed task scope.
subscriber failure uses configured backoff/retry.
durable replay depends on broker/NATS JetStream config.
```

### 21.7 Subject Naming

Recommended subject shape:

```text
game.<domain>.<event>
system.<service>.<event>
admin.<scope>.<event>
```

Examples:

```text
game.world.player_entered
game.guild.created
system.config.reload
admin.broadcast.world
```

### 21.8 Boundary with RPC

```text
Command:
  generated typed RPC, routed to owner, returns a result

Event:
  EventBus pub/sub, asynchronous fan-out, no synchronous result;
  reliability depends on broker and business idempotency
```

---

## 22. Bootstrap Config and ConfigStore

`LatticeService::builder(...).config(...)` reads bootstrap configuration. Runtime `ConfigStore` is a separate abstraction.

### 22.1 Bootstrap Config

Bootstrap config is read at process startup to build:

```text
placement_store
placement_authority
event_bus
config_store
telemetry
admin_http
rpc server
gateway settings
rate limit settings
```

Changes usually require restart, though some sections may be reloaded by business code.

Config source shape:

```rust
pub enum ConfigSource {
    File(PathBuf),
    FileWithFormat { path: PathBuf, format: ConfigFormat },
    Env { prefix: String },
    Inline(BootstrapConfig),
    Composite(Vec<ConfigSource>),
}

pub enum ConfigFormat {
    Toml,
    Yaml,
    Json,
}
```

Format inference:

```text
.toml -> TOML
.yaml/.yml -> YAML
.json -> JSON
file_with_format explicitly overrides inference
```

Merge rules:

```text
Composite sources merge in order.
Later sources override earlier sources.
Recommended order: default file -> environment file -> env override.
Env keys map to sections with a prefix and delimiter, for example LATTICE__PLACEMENT_STORE__ENDPOINTS.
Final config keeps source/version metadata for admin inspect.
```

### 22.2 Builder API

```rust
let service = LatticeService::builder(WORLD_SERVICE)
    .instance(InstanceConfig::from_env()?)
    .config(ConfigSource::file("config/world-service.toml"))
    .placement_store(EtcdPlacementStore::from_config())
    .placement_authority(TonicPlacementAuthority::new(authority_channel))
    .cluster_event_bus(NatsEventBus::from_config())
    .telemetry(TelemetryConfig::from_config())
    .admin_http(AdminHttpConfig::from_config())
    .build()
    .await?;
```

`from_config()` does not read global static state. It means the component reads its section from the already-loaded `BootstrapConfig` during `LatticeService::build()`.

Explicit config must also be supported for tests and non-file deployments:

```rust
.placement_store(
    EtcdPlacementStore::<RealEtcdClient>::dangerously_connect_unauthenticated(EtcdPlacementStoreConfig {
        endpoints: vec!["http://127.0.0.1:2379".into()],
        key_prefix: "/lattice/dev".into(),
        instance_lease_ttl_secs: 30,
        activation_lock_ttl_secs: 30,
    })
    .await?,
)
```

The unauthenticated form above is for local development and still requires an explicitly configured semantic authority. The single-process shortcut is intentionally conspicuous:

```rust
.dangerously_use_in_process_placement(in_memory_store, TonicLogicControl)
```

Production service processes use the `PlacementReadStore` capability for reads/watches; service assembly and route resolvers accept a `ReadOnlyPlacementStore` that cannot perform `PlacementStore` mutations. Instance registration, instance lease allocation/renewal, lifecycle transitions, and bounded singleton-owner renewal cross the authenticated semantic placement authority. Singleton renewal is filtered by service, owner instance ID, and persisted owner boot incarnation, then exact-record validated by the authority before any lease is renewed. The read connection nevertheless retains direct etcd `LeaseGrant`/`LeaseKeepAlive` protocol access until reads/watches move behind a semantic proxy or equivalent enforcement boundary, so type-level call-site separation is not yet sufficient for reclamation. Service processes do not receive the placement-authority credential. Password-file authentication keeps secret bytes out of `BootstrapConfig` and command lines:

```rust
.placement_store(
    EtcdPlacementStore::<RealEtcdClient>::connect_with_connection_options(
        EtcdPlacementStoreConfig {
            endpoints: vec!["https://etcd.internal.example:2379".into()],
            key_prefix: "/lattice/prod".into(),
            instance_lease_ttl_secs: 30,
            activation_lock_ttl_secs: 30,
        },
        EtcdConnectionOptions::password_file(EtcdPasswordAuthentication::new(
            "world-runtime-world-a",
            "/run/secrets/lattice-runtime-etcd-password",
        ))
        .with_ca_file("/run/secrets/lattice-etcd-ca.pem"),
    )
    .await?,
)
.placement_authority(TonicPlacementAuthority::new(authority_channel))
```

`EtcdPlacementStore::from_config()` accepts only an authenticated connection section and fails startup when it is absent, partial, or misspelled. For `ConfigSource::env("LATTICE")`, the nested authentication keys are `LATTICE__PLACEMENT_STORE__CONNECTION__AUTHENTICATION__USERNAME` and `LATTICE__PLACEMENT_STORE__CONNECTION__AUTHENTICATION__PASSWORD_FILE`; optional connection keys include `LATTICE__PLACEMENT_STORE__CONNECTION__CA_FILE` and `LATTICE__PLACEMENT_STORE__CONNECTION__TOKEN_REFRESH_INTERVAL_SECS`. Password and CA files must be absolute; do not put a password itself in an environment variable or configuration value.

The standalone `lattice-coordinator` binary has separate environment-only bootstrap for etcd and its RPC listener. Etcd requires `LATTICE_ETCD_USERNAME` and `LATTICE_ETCD_PASSWORD_FILE` together and accepts optional `LATTICE_ETCD_CA_FILE` and `LATTICE_ETCD_TOKEN_REFRESH_INTERVAL_SECS`. Omitting either credential fails startup; the only unauthenticated etcd escape is the exact `LATTICE_DANGEROUSLY_ALLOW_UNAUTHENTICATED_ETCD=true` setting, and that escape accepts loopback HTTP endpoints only.

Production coordinator RPC requires the complete set `LATTICE_COORDINATOR_TLS_CERT_FILE`, `LATTICE_COORDINATOR_TLS_KEY_FILE`, `LATTICE_COORDINATOR_TLS_CLIENT_CA_FILE`, and `LATTICE_COORDINATOR_TRUST_DOMAIN`. All PEM files are absolute, regular, bounded, and loaded before etcd access. Workload certificates must carry exactly one canonical `spiffe://<trust-domain>/svc/<service>/instance/<instance>/incarnation/<incarnation>` URI SAN. `InstanceConfig::new` generates a fresh UUID boot incarnation; production certificate provisioning must set the same canonical value with `with_incarnation` before build. Plaintext is rejected unless `LATTICE_DANGEROUSLY_ALLOW_UNAUTHENTICATED_COORDINATOR=true` is set exactly, no coordinator TLS fields are present, and the bind is loopback.

`placement_store` and `placement_authority` remain mandatory service components; the builder has no implicit production fallback. `placement_routing_store`, `singleton_claim_reader`, `admin_placement_reader`, and `ownership_view_reader` are independent optional components. They move generated routing/cache reads, exact-boot renewal discovery, admin snapshot/drain lookup, and the live local ownership snapshot onto `TonicPlacementReader` and its narrow routing adapter. The ownership view opens after Ready registration, is installed before the public readiness signal, and fences on watch or keepalive failure. Production bootstrap must construct these from the coordinator CA, expected server DNS name, and boot-incarnation-bound client certificate/key. The repository does not yet require the slots, generated ingress does not consume the gate, and the broad ServiceContext view retains the direct etcd connection. Those paths must be cut over before the runtime credential can be removed.

These options authenticate the placement store only. `lattice-config-etcd` has an independent connection and must receive its own least-privilege credential support before it can share an auth-enabled etcd cluster; until then, deploy it against a separate cluster rather than reusing placement authority credentials.

### 22.3 ConfigStore

ConfigStore is the runtime configuration-center abstraction for low-frequency watch/reload:

```rust
#[async_trait::async_trait]
pub trait ConfigStore: Clone + Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, ConfigStoreError>;
    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), ConfigStoreError>;
    async fn watch(&self, key: &str) -> Result<ConfigWatch, ConfigStoreError>;
}
```

`ConfigStore` lives in `lattice-config`, not in `lattice-ops`. `LocalConfigStore` is the only backend in the core config crate. Real configuration centers are adapter crates, for example `lattice-config-etcd`; a business project can implement the same trait for Nacos or an internal configuration center without changing lattice.

Read-only configuration centers are valid. They return `ConfigStoreError::UnsupportedOperation` from `put`.

Suitable for:

```text
feature flags
Gateway rate limit config
service runtime parameters
scheduler switches and local parameters
ops/admin switches
route table override after full validation
```

Not suitable for:

```text
actor business state
request_id duplicate-guard records
business event transaction logs
high-frequency player state
business data requiring complex queries or transactional consistency
```

Adapter boundary:

```text
core crate: lattice-config
  ConfigStore, ConfigWatch, LocalConfigStore

official adapter crate: lattice-config-etcd
  EtcdConfigStore, EtcdConfigStoreConfig

future or business adapter crate:
  NacosConfigStore or internal ConfigStore implementation
```

Kubernetes ConfigMap is useful for deployment config but not for high-frequency watch.

### 22.4 Config Change Rules

```text
Low-risk parameters can hot-reload, such as rate limits and feature flags.
Protocol, codegen, and actor kind changes require restart.
Route table override must be fully validated and atomically swapped.
Every change records version and source.
```
