# 05. Gateway and Operations

> Gateway routing, client codec, rate limiting, GatewaySessionRef, security, observability, admin APIs, config examples, call flows, and forbidden patterns.  
> Back to: [architecture index](README.md)

---

## 23. Gateway Routing

### 23.1 Client Codec and Message Decode

Gateway receives client-specific binary frames. The framework provides `ClientCodec` and generated bindings, while the business protocol decides the actual frame format.

Gateway flow:

```text
read binary frame
decode msg_id and payload
look up ClientMessageBinding by msg_id
decode payload into concrete proto request
extract route key through generated binding
call generated typed logic client
encode reply back to client protocol
```

Logic services receive typed gRPC requests and must not parse the client opaque bytes again.

```rust
#[async_trait::async_trait]
pub trait ClientCodec: Send + Sync + 'static {
    async fn decode(&self, bytes: bytes::Bytes) -> Result<ClientFrame, GatewayError>;
    async fn encode(&self, frame: ServerFrame) -> Result<bytes::Bytes, GatewayError>;
}

pub struct ClientFrame {
    pub msg_id: ClientMsgId,
    pub payload: bytes::Bytes,
}
```

### 23.2 RouteSpec

Routing is data-driven and generated or loaded from validated config. It is not a hardcoded business enum.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientMsgId(pub u32);

pub struct RouteSpec {
    pub msg_id: ClientMsgId,
    pub service: ServiceKind,
    pub binding: ClientMessageBindingId,
    pub mode: RpcMode,
    pub rate_class: RateClass,
}
```

Registration options:

```text
generated from proto/codegen annotations
static Rust registration for small examples
TOML/YAML/JSON config loaded at gateway startup
validated dynamic override from ConfigStore
```

For hundreds of messages, generated tables or config-driven registration should be preferred over a giant handwritten macro block.

### 23.3 Gateway Rules

```text
Gateway owns client sessions and connection state.
Gateway does not own game state.
Gateway does not write business databases.
Gateway forwards commands as typed RPC.
Gateway may publish system events such as session connected/disconnected.
Gateway validates auth and extracts principal/session context.
Gateway applies rate limits before forwarding.
```

### 23.4 Gateway Rate Limiting

Use the tower pipeline for request middleware and Governor internally for keyed rate limiting.

Recommended layers:

```text
decode frame
auth/session validation
per-connection limit
per-principal/session keyed rate limit
per-msg rate_class limit
load shedding
concurrency limit
typed forwarding
```

Keyed limiter:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RateLimitKey {
    Principal(PrincipalId),
    Session(SessionId),
    Connection(ConnectionId),
    Ip(String),
}
```

`tower::limit::RateLimit` alone is instance-wide and is not enough for per-player/per-session limits. Governor provides keyed quotas and should be wrapped as a tower layer.

### 23.5 GatewaySessionRef and Push

Logic actors sometimes need to push to the current client connection. Cross-process code must not pass `ActorHandle`; it should pass a serializable session reference.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewayId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionEpoch(u64);

#[derive(Debug, Clone)]
pub struct GatewaySessionRef {
    pub gateway_id: GatewayId,
    pub session_id: SessionId,
    pub connection_epoch: ConnectionEpoch,
}
```

Push flow:

```text
Gateway passes GatewaySessionRef to logic in typed RPC metadata or request context.
Actor stores the latest session ref if business logic needs push.
Actor calls generated GatewayPush client.
Gateway verifies session_id and connection_epoch before sending.
Stale refs are dropped safely.
```

---

## 24. Security

### 24.1 Internal RPC Identity

Internal logic/control/admin RPC should use mTLS or an equivalent service identity mechanism.

```text
business RPC between logic services: authenticated internal identity
LogicControl RPC: internal only, not exposed to Gateway or clients
Admin API: internal network plus authz
Gateway client protocol: business auth/session validation
```

### 24.2 Authorization

Authorization is layered:

```text
Gateway authenticates clients and creates AuthContext.
Generated clients propagate internal identity and TraceContext.
Logic services validate framework metadata when needed.
Admin APIs require operator identity and explicit permissions.
```

### 24.3 AuthContext

```rust
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub principal: PrincipalId,
    pub session: Option<SessionId>,
    pub claims: BTreeMap<String, String>,
}
```

---

## 25. Observability

### 25.1 Cluster State Inspect

The framework should expose structured inspection APIs and may provide an axum HTTP adapter.

```rust
#[async_trait::async_trait]
pub trait ClusterInspector: Clone + Send + Sync + 'static {
    async fn cluster_summary(&self) -> Result<ClusterSummary, InspectError>;
    async fn list_instances(&self, query: InstanceQuery) -> Result<Page<InstanceInfo>, InspectError>;
    async fn list_placements(&self, query: PlacementQuery) -> Result<Page<PlacementInfo>, InspectError>;
    async fn list_virtual_shards(&self, query: VShardQuery) -> Result<Page<VShardInfo>, InspectError>;
    async fn list_singletons(&self, query: SingletonQuery) -> Result<Page<SingletonInfo>, InspectError>;
}

#[async_trait::async_trait]
pub trait NodeInspector: Clone + Send + Sync + 'static {
    async fn mailbox_summary(&self) -> Result<MailboxSummary, InspectError>;
    async fn scheduler_summary(&self) -> Result<SchedulerSummary, InspectError>;
    async fn event_subscriptions(&self) -> Result<Vec<EventSubscriptionInfo>, InspectError>;
    async fn actor_state(&self, query: ActorInspectQuery) -> Result<Option<ActorInspectValue>, InspectError>;
}
```

Rules:

```text
list APIs must support pagination.
cluster aggregation can return partial results for unreachable nodes.
business actor inspect is optional, read-only, redacted, and timeout bounded.
operators should not need to scan etcd directly for normal diagnostics.
```

### 25.2 Metrics

Recommended metrics:

```text
rpc latency, error count, timeout count
route cache hit/miss
NOT_OWNER and FENCED count
actor count by kind and instance
mailbox depth and enqueue latency
activation success/failure
passivation, StopFailed, supervision decisions
gateway sessions, decode errors, rate-limit rejects
eventbus publish/subscribe latency and failures
scheduler pending task count
drain duration and migration results
```

Avoid high-cardinality labels such as actor_id, request_id, event_id, or session_id.

### 25.3 Telemetry and Async Tracing

OpenTelemetry is the default export model. Framework code emits spans and events through `tracing`; `lattice-telemetry-otlp` installs the `tracing-subscriber` stack and optional OTLP exporter.

Startup shape:

```rust
let _telemetry = lattice_telemetry_otlp::LatticeTelemetry::from_config(
    service_kind!("World"),
    InstanceId::new("world-a"),
    lattice_telemetry_otlp::TelemetryConfig::fmt_only("1.2.3")
        .with_otlp_endpoint("http://otel-collector:4317"),
)
.install()?;
```

TraceContext propagates through:

```text
gRPC metadata
EventBus headers
actor mailbox envelope
scheduler envelope
gateway frame context
```

Span model:

```text
Gateway receive span
generated RPC client span
logic adapter span
actor handler span
event publish span
event consumer span linked to producer context
scheduler trigger span
admin operation span
```

EventBus fan-out should use span links rather than pretending all consumers are a single child chain.

`TelemetryRecorder` in `lattice-ops` is for tests, admin snapshots, and manually aggregated framework metrics. It is not the production tracing path.

### 25.4 Trace Fields

Recommended trace fields:

```text
service.kind
service.instance_id
actor.kind
actor.id_hash
route.key_hash
route.epoch
request.id
rpc.method
gateway.id
session.id_hash
event.subject
event.type
event.id
placement.owner
placement.epoch
```

Use hashed or redacted values for high-cardinality or sensitive fields.

### 25.5 Admin API

Default axum adapter may expose:

```text
GET  /healthz
GET  /readyz
GET  /metrics
GET  /admin/cluster/summary
GET  /admin/instances
GET  /admin/placements
GET  /admin/vshards
GET  /admin/singletons
GET  /admin/node/mailboxes
GET  /admin/node/schedulers
GET  /admin/node/event-subscriptions
POST /admin/instances/{id}/drain
POST /admin/actors/{kind}/{id}/retry-stop
POST /admin/actors/{kind}/{id}/force-stop
POST /admin/actors/{kind}/{id}/migrate
```

Mutating admin APIs must be authenticated, authorized, audited, and rate-limited.

---

## 26. Config Example

```toml
[instance]
service_kind = "World"
instance_id = "world-0"
advertised_endpoint = "http://world-0.world:18080"
control_endpoint = "http://world-0.world:18081"

[etcd]
endpoints = ["http://etcd:2379"]
key_prefix = "/lattice/prod"

[event_bus.nats]
url = "nats://nats:4222"
jetstream = true

[admin_http]
bind = "0.0.0.0:19090"

[gateway.rate_limit.default]
per_second = 30
burst = 60
```

---

## 27. Common Call Flows

### 27.1 PlayerService Calls WorldService

```text
PlayerActor handler
  -> generated WorldClient.enter_world
  -> ShardedRpcCore resolve route
  -> EndpointPool gets channel
  -> gRPC metadata injects request_id/epoch/trace
  -> WorldRpcAdapter
  -> WorldActor mailbox
  -> Handler<Rpc<EnterWorldRequest>>
```

### 27.2 WorldService Pushes to Client

```text
Gateway receives client connection
  -> GatewaySessionRef attached to logic request
  -> WorldActor stores latest session ref
  -> WorldActor calls GatewayPush client
  -> Gateway validates session_id + connection_epoch
  -> Gateway sends encoded server frame
```

### 27.3 NOT_OWNER

```text
client calls stale owner
  -> stale owner returns NOT_OWNER with current/expected epoch
  -> generated client invalidates route cache
  -> resolve again
  -> retry once with same request_id
```

### 27.4 First Activation

```text
resolve explicit actor
  -> no owner found
  -> Coordinator.ActivateActor
  -> target LogicControl.ActivateActor
  -> actor factory/load
  -> placement record written with epoch
  -> data-plane RPC goes to owner
```

### 27.5 Singleton Failover

```text
owner lease expires
  -> Coordinator detects missing owner
  -> increments epoch
  -> activates singleton on new instance
  -> old owner is fenced if it returns
```

---

## 28. Forbidden Patterns

```text
Do not pass ActorHandle across RPC.
Do not put business database persistence into the framework layer.
Do not hardcode World/Player/Guild into framework enums.
Do not route every business RPC through Coordinator.
Do not access etcd on every RPC.
Do not use EventBus for commands that need an immediate result.
Do not use raw tokio::spawn for actor-owned work.
Do not expose LogicControl to clients.
Do not use high-cardinality metrics labels such as actor_id or request_id.
Do not assume exactly-once delivery.
Do not rely on Actor::stopping for crash recovery.
```
