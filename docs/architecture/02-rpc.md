# 02. RPC and Codegen

> Proto rules, codegen, typed sharded clients, server adapters, and RPC core.  
> Back to: [architecture index](README.md)

---

## 9. Proto and Codegen Rules

### 9.1 Proto Declares Only Routing

Business proto messages should not contain framework metadata fields. Framework metadata is carried through gRPC metadata.

```proto
syntax = "proto3";
package world;

import "lattice/options.proto";

service WorldRpc {
  option (lattice.options.service_kind) = "World";
  option (lattice.options.actor_kind) = "World";
  option (lattice.options.default_route_key) = "world_id";

  rpc EnterWorld(EnterWorldRequest) returns (EnterWorldReply);
}

message EnterWorldRequest {
  uint64 world_id = 1;
  uint64 player_id = 2;
}

message EnterWorldReply {
  bool ok = 1;
}
```

`RpcMeta meta = 1` is intentionally not part of every business request. This keeps business messages clean and prevents callers from having to fill unrelated `None` values.

The framework-owned option proto is:

```proto
syntax = "proto3";
package lattice.options;

import "google/protobuf/descriptor.proto";

extend google.protobuf.ServiceOptions {
  string service_kind = 51001;
  string actor_kind = 51002;
  string default_route_key = 51003;
}

extend google.protobuf.MethodOptions {
  string route_key = 51003;
  uint32 gateway_msg_id = 51004;
}
```

### 9.2 Build Script Codegen

Proto-driven code generation uses `build.rs`, not a proc macro. `lattice-codegen` wraps `tonic-prost-build`, emits tonic/prost types, parses the descriptor set plus lattice proto options, validates routing metadata, and writes `$OUT_DIR/lattice.generated.rs`.

Business build script shape:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];

    lattice_codegen::configure()
        .gateway_route_ids([(100, "world.WorldRpc.EnterWorld")])
        .compile_protos(&["proto/world.proto"], &includes)?;

    Ok(())
}
```

For large projects, gateway message ids should usually come from business-owned protocol tables instead of being embedded in RPC definitions. `gateway_route_ids` is the programmatic path for build scripts that already have an id-to-method table.

File-based route tables are also supported:

```toml
[[routes]]
msg_id = 100
method = "world.WorldRpc.EnterWorld"
```

```rust
lattice_codegen::configure()
    .gateway_routes("proto/gateway-routes.toml")
    .compile_protos(&["proto/world.proto"], &includes)?;
```

Business code includes generated bindings explicitly:

```rust
pub mod world {
    tonic::include_proto!("world");
}

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/lattice.generated.rs"));
}
```

Generated lattice RPC methods must have:

```text
service_kind on protobuf service
actor_kind on protobuf service
default_route_key on protobuf service or route_key override on protobuf method
request type
reply type
```

`default_route_key` and method-level `route_key` must name a non-optional field on the request message. The service-level `default_route_key` is the normal path; method-level `route_key` is only for rare methods that need a different key field. Supported route key field types are `uint64`, `int64`, `string`, and `bytes`.

Allowed route key fields:

```text
proto3 ordinary scalar field
proto2 required scalar field
```

Rejected during codegen:

```text
proto2 optional route key field
proto3 optional route key field
repeated route key field
oneof route key field
```

`gateway_msg_id` remains available as a convenience proto option for small projects, but it is not the preferred path for large business protocols. If a method has both a proto `gateway_msg_id` and an external route mapping, the ids must match. Duplicate gateway message ids are rejected during codegen.

### 9.3 Rust Binding

The generated code binds a proto RPC method to:

```text
ServiceKind
ActorKind
RouteKey extractor
Request and reply Rust types
Generated client method
Generated server adapter method
```

The framework does not hardcode `World`, `Player`, or `Guild`. Those names come from business proto options, constants, or codegen output.

### 9.4 Endpoint and gRPC Services

`RouteTarget.advertised_endpoint` identifies one service process and one internal gRPC endpoint. A single process can host multiple generated gRPC services:

```text
world-service instance:
  advertised_endpoint = http://10.0.1.7:18080
  services:
    WorldRpc
    RoomRpc
    ZoneRpc
```

Connection pooling is keyed by `instance_id` and `advertised_endpoint`, not by actor id.

The internal logic-service RPC transport is tonic/gRPC over HTTP/2. High-frequency actor-to-actor streams use Direct Actor Link as a separate capability, not as an alternate RPC backend.

### 9.5 Generated Artifacts

Codegen should generate:

```text
RoutedRequest implementation
RpcRequest implementation
typed sharded client wrapper
tonic server adapter
gateway client binding and route table entries
compile-time checks that target actors implement Handler<Rpc<Request>>
```

Example shape:

```rust
pub trait RoutedRequest {
    fn actor_kind(&self) -> ActorKind;
    fn route_key(&self) -> RouteKey;
}

pub trait RpcRequest: prost::Message + Default + Send + Sync + 'static {
    type Reply: prost::Message + Default + Send + Sync + 'static;
    const METHOD: &'static str;
}
```

---

## 10. Typed Sharded Client

Business code should call generated clients, not raw tonic clients:

```rust
let reply = ctx
    .clients()
    .player()
    .get_profile(GetProfileRequest {
        player_id: player_id.0,
    })
    .await?;
```

The shorter `ctx.clients().player()` style is preferred over a long `ctx.service().clients().player()` chain.

Generated clients are usually composed into an `AppClients` or `ServiceClients` struct by the business crate. The framework provides the runtime and generated building blocks; business code decides how to inject the clients into actors and services.

---

## 11. Server Adapter

Generated server adapters convert tonic requests into actor calls:

```rust
#[tonic::async_trait]
impl world_rpc_server::WorldRpc for WorldRpcAdapter {
    async fn enter_world(
        &self,
        request: tonic::Request<EnterWorldRequest>,
    ) -> Result<tonic::Response<EnterWorldReply>, tonic::Status> {
        let ctx = RpcContext::from_metadata(request.metadata())?;
        let req = request.into_inner();
        let key = req.route_key();

        let reply = self
            .runtime
            .call_actor(req.actor_kind(), key, Rpc { req, ctx })
            .await?;

        Ok(tonic::Response::new(reply))
    }
}
```

The adapter performs route epoch checks before the handler runs. Epoch/fencing should not leak into business request fields.

---

## 12. RPC Metadata and Context

Framework metadata is stored in gRPC metadata:

```rust
#[derive(Debug, Clone)]
pub struct RpcContext {
    pub request_id: RequestId,
    pub route_epoch: Option<Epoch>,
    pub source_service: ServiceKind,
    pub source_instance: InstanceId,
    pub trace: TraceContext,
    pub auth: Option<AuthContext>,
}
```

Metadata keys are implementation details, but they should cover:

```text
lattice-request-id
lattice-route-epoch
lattice-source-service
lattice-source-instance
traceparent / tracestate
authorization or internal identity metadata
```

Business proto messages must not grow framework metadata fields.

---

## 13. RpcError

The error model must distinguish routing, fencing, overload, timeout, business errors, and unknown results:

```rust
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("target owner not found")]
    NoOwner,

    #[error("route target is not owner")]
    NotOwner { expected_epoch: Option<Epoch> },

    #[error("request was fenced by newer owner")]
    Fenced { current_epoch: Epoch },

    #[error("actor is unavailable")]
    ActorUnavailable,

    #[error("mailbox is full")]
    MailboxFull,

    #[error("rpc result is unknown for {method} request {request_id}: {message}")]
    UnknownResult {
        method: &'static str,
        request_id: RequestId,
        message: String,
    },

    #[error("business error: {0}")]
    Business(String),
}
```

---

## 14. Retry and Consistency Rules

Retry rules:

```text
NOT_OWNER:
  invalidate route cache, resolve again, retry once with the same request_id.

FENCED:
  invalidate route cache, resolve again, retry once with the same request_id.

UnknownResult:
  the request may have been applied, but the client did not receive a definitive response.
  do not transparently retry. The caller should query state, reconcile an operation_id, or
  retry only through an explicit idempotent business flow.

MailboxFull / overload:
  apply caller backoff or fail fast according to policy.

Business error:
  do not retry unless business code marks it retryable.
```

Route-correction retry is configurable. It is enabled by default for generated placement-backed clients. Disable it when the caller wants the lowest possible client-side overhead and prefers to handle `NotOwner` / `Fenced` explicitly:

```rust
LatticeService::builder(WORLD_SERVICE)
    .rpc_retry_policy(RpcRetryPolicy::Disabled);
```

When retry is disabled, the placement-backed client sends the request once and moves the request body into the transport. It does not keep an encoded retry copy of the request.

### 14.1 Request ID Duplicate Guard

The framework-level request-id deduplicator is intentionally lightweight:

```text
same RPC method + same request_id reaches the same live service process:
  the first request reserves the key and enters the actor handler.
  later duplicate requests are rejected with duplicate request id.

successful handler result:
  the key remains recorded in memory, but the reply is not cached.

mailbox full or closed before business handling:
  the key is released so the caller may retry according to overload policy.
```

This avoids replay-cache memory growth and avoids treating framework dedup as a durability layer. If the first request succeeded but the response was lost, a retry with the same request_id should receive `UnknownResult`; business code should query state or reconcile an `operation_id`.

Generated `ActorService`, `RegistryService`, and `SingletonRegistryService` adapters enable this duplicate guard by default. Business code can disable it for a generated service binding when needed:

```rust
service.register_sharded_rpc(
    generated::world_rpc::Binding::for_explicit_actor::<WorldActor>(WORLD_ACTOR)
        .request_dedup(false),
);
```

Generated registry bindings declare their server ingress placement mode. Use
`for_explicit_actor` for coordinator-owned actors,
`for_virtual_sharded_actor(actor_kind, shard_count)?` for a validated nonzero
virtual-shard mapping, and only use the deliberately named
`for_static_local_actor_unfenced` for endpoint-affine actors that have no
distributed placement record. Singleton bindings always declare singleton
fencing. The generated raw registry constructor follows the same rule:
unplaced local actors require `new_static_local_unfenced`.

Low-level hand-written `ActorRpcAdapter::unary(...)` calls do not enable it unless they explicitly use the dedup variant.

### 14.2 RPC Failure and Business Consistency

lattice does not provide distributed transactions across actors, services, EventBus, and business databases.

For multi-step workflows, business code should use one or more of:

```text
operation_id stored in business state
pending operation state
idempotent commands
retry by querying current state
compensation command
manual_required state for operator intervention
transactional outbox when DB write and event publish need reliability
```

Recommended workflow shape:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationState {
    Pending,
    Applied,
    CompensationRequired,
    ManualRequired,
}
```

If a handler has already written local state and a later RPC fails with an unknown result, the handler should record the operation as pending and let a retry/reconciliation path finish or repair it. This is a business-level saga, not a framework-level transaction.

---

## 15. Gateway Decode and Typed Forwarding

Gateway receives client binary frames. It must:

```text
decode frame header
extract msg_id and payload bytes
look up generated ClientMessageBinding
decode payload into the concrete proto request type
extract route key using generated binding
call the generated logic service client with typed request
encode the typed reply back to the client protocol
```

Logic services should receive typed gRPC requests. They should not parse opaque client bytes again.

---

## 16. RouteTarget

```rust
#[derive(Debug, Clone)]
pub struct RouteTarget {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub advertised_endpoint: Uri,
    pub owner_epoch: Option<Epoch>,
}
```

`advertised_endpoint` is the address other logic services use to call this instance's business RPC services. It may differ from control-plane or admin endpoints.

---

## 17. RPC Transport Policy

The normal lattice RPC path uses generated clients over tonic/gRPC:

```text
generated Client
  -> TypedRpcClient
  -> ResolvingRpcCore
  -> RouteResolver / route cache / retry / request context
  -> GeneratedTonicEndpointTransport
  -> gRPC over HTTP/2
```

This path is the standard transport for logic-service commands that need owner routing, request/response, route epoch fencing, request id propagation, route-correction retry, deduplication, and a clear `RpcError`.

The framework may keep an internal `EndpointRpcTransport` abstraction for code organization and testing, but product-level semantics should remain:

```text
business code -> generated client -> placement-backed gRPC RPC -> owner actor
```

### 17.1 Default Tonic Transport

The logic-service RPC backend is `GeneratedTonicEndpointTransport`:

```text
wire protocol: gRPC over HTTP/2
metadata: gRPC metadata
security: RpcTransportSecurity TLS / mTLS
server: generated tonic server adapters
client pool: per-endpoint tonic Channel striping
```

This backend is standard, debuggable, proxy-friendly, and easy to integrate with existing Rust/gRPC tooling.

Use gRPC RPC for:

```text
login
inventory mutation
reward delivery
guild changes
matchmaking commands
state-changing owner actor commands
anything that needs a definitive reply or UnknownResult handling
```

### 17.2 High-Throughput Direct Actor Link

High-frequency traffic uses a separate **Direct Actor Link** model. This is an explicit actor-to-actor connection for data streams where the business accepts weaker semantics.

Direct Actor Link is not RPC:

```text
no per-message route resolver
no automatic actor rebalancing support
no transparent NOT_OWNER retry
no request/response requirement
no framework request-id dedup by default
no exactly-once guarantee
```

Use Direct Actor Link for:

```text
high-frequency position/state updates
combat simulation deltas
scene-to-scene transient synchronization
gateway session push streams
data where newest value supersedes older values
```

Do not use Direct Actor Link for:

```text
login
payment/reward/inventory operations
actor ownership changes
commands requiring fencing
commands requiring business idempotency
commands requiring a definitive success/failure reply
```

### 17.3 Direct Link Public API

A direct link is established from an actor context or service context using an `ActorRef` or a business-owned endpoint target:

```rust
let movement_stream = DirectLinkStream::new("movement-stream")
    .message::<PositionUpdate>()
    .message::<StateDelta>();

let link = ctx
    .links()
    .connect(
        target_actor_ref,
        movement_stream,
        DirectLinkOptions {
            mode: DirectLinkMode::Unidirectional,
            reconnect: ReconnectPolicy::BusinessOwned,
            backpressure: BackpressurePolicy::DropOldest { max_pending: 1024 },
            heartbeat_interval: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            max_frame_size: 256 * 1024,
        },
    )
    .await?;

link.tell(PositionUpdate {
    entity_id,
    x,
    y,
    tick,
})
.await?;
```

For bidirectional links, the connect call supplies both directional streams:

```rust
let outbound = ctx
    .links()
    .connect_bidirectional(
        target_actor_ref,
        source_to_target_stream,
        target_to_source_stream,
        DirectLinkOptions {
            mode: DirectLinkMode::Bidirectional,
            reconnect: ReconnectPolicy::BusinessOwned,
            backpressure: BackpressurePolicy::DropOldest { max_pending: 1024 },
            heartbeat_interval: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            max_frame_size: 256 * 1024,
        },
    )
    .await?;
```

The returned handle is the source-to-target send handle:

```rust
outbound.tell(InputCommand { ... }).await?;
```

The source actor receives target-to-source messages through normal `Handler<Linked<T>>` implementations.

The target actor gets its reverse send handle from the link lifecycle event:

```rust
impl Handler<LinkOpened> for BattleActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        opened: LinkOpened,
    ) -> Result<(), BattleError> {
        if opened.mode == DirectLinkMode::Bidirectional {
            let updates = ctx
                .links()
                .get::<BattleUpdateStream>(opened.link_id)?;

            self.gateway_updates = Some(updates);
        }

        Ok(())
    }
}
```

`ctx.links().get::<S>(link_id)` returns the send handle for the local actor's outbound direction for stream `S`. It fails with `UnsupportedStream` if the requested stream is not part of the opened directional session.

The first full version should support both awaited and non-blocking sends:

```rust
link.tell(message).await?;      // obeys backpressure policy
link.try_tell(message)?;        // never waits
link.close(LinkCloseReason::Done).await?;      // close this outbound direction
ctx.links().close_all(link.id(), LinkCloseReason::Done).await?;
```

`tell` means the message entered the local link send path. It does not mean the remote actor processed the message.

Link mode has only two public variants:

```rust
pub enum DirectLinkMode {
    Unidirectional,
    Bidirectional,
}
```

`Unidirectional` is one logical channel:

```text
source actor -> target actor
```

`Bidirectional` is two logical unidirectional channels sharing one underlying connection:

```text
source actor -> target actor
target actor -> source actor
```

Each direction has its own stream binding, accepted message ids, sequence numbers, and backpressure state. The framework should model bidirectional links as two directional sessions over one connection, not as one unstructured message pipe.

If both directions use the same stream, both actor types must bind that stream and implement handlers for every message in that stream:

```rust
let battle_stream = DirectLinkStream::new("battle-stream")
    .message::<PositionUpdate>()
    .message::<InputCommand>()
    .message::<StateDelta>();

service.register_direct_link(battle_stream.for_actor::<BattleActor>(BATTLE_ACTOR));
service.register_direct_link(
    battle_stream.for_actor::<GatewaySessionActor>(GATEWAY_SESSION_ACTOR),
);
```

If the two directions use different messages, model them as two streams:

```rust
let input_stream = DirectLinkStream::new("gateway-input")
    .message::<InputCommand>();

let update_stream = DirectLinkStream::new("battle-update")
    .message::<PositionUpdate>()
    .message::<StateDelta>();
```

This keeps stream binding directional without introducing per-message direction rules. Binding a stream always means the actor promises to handle every message in that stream.

Actor handlers receive a distinct message wrapper so business code can tell direct-link data from reliable RPC commands:

```rust
impl Handler<Linked<PositionUpdate>> for BattleActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        msg: Linked<PositionUpdate>,
    ) -> Result<(), BattleError> {
        self.apply_position(msg.payload);
        Ok(())
    }
}
```

`Linked<T>` carries link context outside the business payload:

```rust
pub struct Linked<T> {
    pub payload: T,
    pub context: LinkMessageContext,
}

pub struct LinkMessageContext {
    pub link_id: LinkId,
    pub source: ActorRef,
    pub target: ActorRef,
    pub sequence: u64,
    pub received_at: Instant,
    pub flags: LinkMessageFlags,
}
```

### 17.4 Link Targets and Discovery

A link target can be:

```text
ActorRef::Direct:
  connect to the embedded endpoint and target actor identity.

ActorRef::Routed:
  resolve once through placement/RPC before opening the link.
  after open, the link is direct and does not re-resolve every message.

Business endpoint:
  business code supplies endpoint + target actor identity explicitly.
```

Opening a routed direct link may use placement once:

```text
source actor
  -> resolve ActorRef through placement
  -> connect target direct-link listener
  -> open link to target actor
  -> send direct frames until closed
```

If the actor moves, passivates, crashes, or the node drains, the link closes. The framework reports the close reason; business code chooses whether to reconnect, fall back to RPC, rebuild state, or stop the stream.

### 17.5 Direct-Link Binding

Direct links must be strongly typed at the actor boundary, but business code should not register the same message types twice.

The user-facing API should mirror RPC binding registration, but direct-link message sets are declared in normal Rust code using generated Rust types:

```rust
let movement_stream = DirectLinkStream::new("movement-stream")
    .message::<PositionUpdate>()
    .message::<StateDelta>()
    .for_actor::<BattleActor>(BATTLE_ACTOR);

service.register_direct_link(movement_stream);
```

One typed direct-link binding carries both:

```text
message catalog:
  message type -> message id
  message id -> protobuf decoder
  message id -> protobuf encoder
  message id -> message metadata

actor binding:
  target actor kind
  accepted message ids
  typed dispatch into Handler<Linked<T>>
```

The same stream definition can be attached to multiple actor types when they support the same messages:

```rust
let movement_stream = DirectLinkStream::new("movement-stream")
    .message::<PositionUpdate>()
    .message::<StateDelta>();

service.register_direct_link(movement_stream.for_actor::<BattleActor>(BATTLE_ACTOR));

service.register_direct_link(
    movement_stream.for_actor::<BattleReplicaActor>(BATTLE_REPLICA_ACTOR),
);
```

The binding is internally split into catalog and actor dispatch tables, but that split is not exposed as two separate business steps.

There should not be a second declaration in `build.rs` for normal business code. `build.rs` only compiles proto files and emits message metadata for generated types. The direct-link stream is composed in application Rust code so refactors, renames, and IDE navigation operate on real Rust types.

Default generated ids should be deterministic and stable. For protobuf-defined messages, `lattice-codegen` emits metadata for each generated Rust type, including the fully-qualified protobuf message name. `DirectLinkStream::message::<T>()` derives the default id from the stream name plus `T::PROTO_FULL_NAME`, validates collisions at service build time, and stores the resulting descriptor. Manual ids are reserved for wire compatibility, cross-language integration, or collision resolution.

Auto-increment ids are only acceptable for local, single-binary test protocols because they are sensitive to registration order. Distributed direct links should use generated stable ids or explicit manual ids.

Required invariants:

```text
message_type_id is unique within a direct-link protocol namespace
generated ids are deterministic across services built from the same proto set
T: prost::Message + Default + Send + Sync + 'static
target actor implements Handler<Linked<T>>
unsupported message type is rejected during link open or closes the link with protocol error
```

The framework should prefer compile-time type checking for actor handlers and service-build validation for id collisions. Runtime registration should fail fast during service build if duplicate ids, unsupported types, or missing actor handlers are detected.


### 17.6 Link Open Protocol

Direct links use an explicit handshake before data frames.

Open request:

```text
OpenLink {
  protocol_version
  link_id
  source_actor_ref
  target_actor_ref
  requested_mode
  source_to_target:
    stream_name
    supported_message_type_ids
  target_to_source:
    stream_name
    supported_message_type_ids
    present only for Bidirectional
  backpressure_policy
  heartbeat_interval
  max_frame_size
  trace_context
  auth_context
}
```

Open response:

```text
OpenLinkAck {
  link_id
  source_to_target:
    accepted_message_type_ids
  target_to_source:
    accepted_message_type_ids
    present only for Bidirectional
  target_owner_epoch
  negotiated_heartbeat_interval
  negotiated_max_frame_size
}
```

Open rejection:

```text
OpenLinkReject {
  reason:
    NotOwner
    Fenced
    ActorUnavailable
    UnsupportedStream
    UnsupportedMessageType
    Unauthorized
    Overloaded
    ProtocolVersionMismatch
  optional_redirect: ActorRef
}
```

Open flow:

```text
1. source connects to target direct-link listener
2. source sends OpenLink
3. target authenticates source
4. target verifies target actor kind/id and optional owner epoch
5. target verifies source_to_target stream and message types against the target actor binding
6. for Bidirectional, target verifies target_to_source stream and message types against the source actor binding declared in OpenLink
7. target lazily activates actor only if the actor registration allows direct-link activation
8. target creates directional link sessions
9. target sends OpenLinkAck
10. data frames may flow
```

Lazy activation policy must be explicit. Some actors should only accept direct links after normal RPC/session setup.

### 17.7 Validation and Rejection

Direct links must reject invalid actor/message combinations before data reaches the target actor mailbox.

Validation happens in three layers.

Local send validation:

```text
DirectLink<MovementStream>::tell(PositionUpdate):
  allowed by the local stream type

DirectLink<MovementStream>::tell(LoginRequest):
  compile-time failure when the link type is known
  otherwise LinkSendError::UnsupportedMessageType
```

OpenLink validation on the target node:

```text
1. verify target_actor_ref.service_kind is hosted by this service or route owner
2. verify target_actor_ref.actor_kind is registered locally
3. verify the actor registration allows direct-link activation or the actor is already active
4. verify source_to_target.stream_name is bound to the target actor kind
5. verify every source_to_target message id exists in the service direct-link catalog
6. verify every source_to_target message id is accepted by the target actor's stream binding
7. for Bidirectional, verify target_to_source.stream_name is bound to the source actor kind declared by the source actor ref
8. for Bidirectional, verify every target_to_source message id is accepted by the source actor stream binding
9. create directional link sessions only after all checks pass
```

Open rejection mapping:

```text
target actor kind is not registered:
  OpenLinkReject::ActorUnavailable

target actor kind exists but does not bind the requested stream:
  OpenLinkReject::UnsupportedStream

stream exists but message id is unknown or not accepted by this actor:
  OpenLinkReject::UnsupportedMessageType

target is not the current owner and can identify the owner:
  OpenLinkReject::NotOwner { optional_redirect }

target is draining or overloaded:
  OpenLinkReject::Overloaded
```

Message frame validation after open:

```text
link_id must exist and be open
frame direction must match an opened directional session
message_type_id must be in that direction's negotiated accepted_message_type_ids
sequence must be valid for that direction's ordering policy
payload must decode as the registered protobuf message type
target actor must still be active or activatable by the link policy
```

Invalid message frames are protocol errors. The framework closes the link and reports the reason:

```text
Close {
  reason: ProtocolError(UnsupportedMessageType | DecodeError | InvalidSequence | UnknownLink)
}
```

Invalid frames must not be delivered to the actor mailbox and must not fall back to a dynamic handler.

### 17.8 Wire Protocol

Direct Actor Link wire protocol is purpose-built and narrow:

```text
transport:
  first implementation: TCP
  optional local optimization: Unix domain socket
  future transports: QUIC, UDP-based custom transport
  length-delimited binary frames
  protobuf payloads

frame envelope:
  magic/version
  frame_kind
  link_id
  sequence
  message_type_id
  flags
  header_length
  payload_length
  header_crc or reserved
  payload
```

Frame kinds:

```text
OpenLink
OpenLinkAck
OpenLinkReject
Message
Heartbeat
HeartbeatAck
Backpressure
CloseDirection
Close
ProtocolError
```

Message frame:

```text
Message {
  link_id
  sequence
  message_type_id
  flags
  payload
}
```

The transport may support batching later, but the first complete design should keep single-message frames as the semantic unit. Batching must not change actor mailbox ordering guarantees.

### 17.8.1 Transport Adapter Boundary

The Direct Link protocol must be transport-agnostic above the frame layer. Actor APIs, stream binding, message ids, lifecycle events, and backpressure semantics must not depend on TCP.

Direct Link must not open one TCP connection per actor pair. Physical connections are instance-to-instance and are pooled by target direct-link endpoint. Logical actor links are multiplexed over those pooled connections by `link_id`.

Required endpoint pool abstraction:

```rust
pub struct DirectLinkEndpointPoolConfig {
    pub connections_per_endpoint: NonZeroUsize,
    pub max_links_per_connection: usize,
    pub max_links_per_endpoint: usize,
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
}

pub trait DirectLinkEndpointPool: Clone + Send + Sync + 'static {
    async fn open_link(
        &self,
        endpoint: DirectLinkEndpoint,
        request: OpenLinkRequest,
    ) -> Result<PooledDirectLinkSession, LinkError>;

    async fn write_frame(
        &self,
        connection_id: DirectLinkConnectionId,
        frame: DirectLinkFrame,
    ) -> Result<(), LinkError>;
}
```

The low-level transport remains responsible for binding/listening and creating physical connections, but normal runtime code should go through the endpoint pool rather than directly opening raw connections:

```rust
pub trait DirectLinkTransport: Clone + Send + Sync + 'static {
    type Listener;
    type Connection: DirectLinkConnection;

    async fn bind(&self, config: DirectLinkListenConfig) -> Result<Self::Listener, LinkError>;
    async fn connect_physical(
        &self,
        endpoint: DirectLinkEndpoint,
    ) -> Result<Self::Connection, LinkError>;
}

pub trait DirectLinkConnection: Send + Sync + 'static {
    async fn read_frame(&mut self) -> Result<DirectLinkFrame, LinkError>;
    async fn write_frame(&mut self, frame: DirectLinkFrame) -> Result<(), LinkError>;
    async fn close(&mut self) -> Result<(), LinkError>;
}
```

Connection pool behavior:

```text
pool key: target direct_link_endpoint
physical connection count: bounded by connections_per_endpoint
stripe selection: stable hash of link_id or source_actor_id + target_actor_id
logical session key: link_id
one physical connection may carry many link_id sessions
OpenLink creates a logical session on a pooled connection
CloseDirection / Close release logical sessions, not necessarily the physical connection
peer disconnect closes every logical session multiplexed on that physical connection
```

TCP transport:

```text
default implementation
ordered reliable byte stream
length-delimited frames
one read task and one write task per connection
many logical links per connection
suitable for all Direct Link modes
```

Unix domain socket transport:

```text
optional local-node optimization
same ordered reliable frame semantics as TCP
same protocol and lifecycle behavior
```

QUIC transport:

```text
future implementation
can map each directional session to a QUIC stream or use one bidirectional QUIC stream
keeps the same DirectLinkFrame and lifecycle semantics
may reduce head-of-line blocking compared with TCP
```

UDP-based transport:

```text
future specialized implementation
must explicitly choose reliability semantics before use
not suitable for the default DirectLinkConnection contract unless it adds ordering/retransmit
may expose a separate unreliable Direct Link profile later
must not silently weaken TCP Direct Link delivery semantics
```

The first implementation should ship only TCP. The architecture keeps the adapter boundary so QUIC or UDP-based profiles can be added later without changing business actor handlers.

### 17.9 Sending Path

Source-side path:

```text
business actor handler / timer
  -> DirectLink::tell(T)
  -> encode T with prost
  -> assign link sequence
  -> apply local backpressure policy
  -> enqueue to link writer task
  -> writer task writes length-delimited frame
```

`try_tell` path:

```text
business actor handler / timer
  -> DirectLink::try_tell(T)
  -> if local queue accepts: Ok
  -> if full/closed: Err(LinkSendError)
```

Send error categories:

```rust
pub enum LinkSendError {
    Closed { reason: LinkCloseReason },
    BackpressureFull,
    UnsupportedMessageType,
    MessageTooLarge,
    Encode(String),
    Protocol(String),
}
```

### 17.10 Receiving Path

Target-side path:

```text
socket read task
  -> decode frame envelope
  -> validate link_id and sequence
  -> decode payload by message_type_id
  -> find local target actor registry
  -> enqueue Linked<T> into target actor mailbox
  -> actor handles Handler<Linked<T>> serially
```

The socket read task must never execute business handlers directly. Actor mailbox delivery preserves the actor runtime's single-writer state model.

If the target mailbox applies backpressure:

```text
Block:
  read task waits before enqueuing

DropNewest:
  drop current message and update metrics

DropOldest:
  remove older pending message from link queue when possible

Coalesce:
  replace older pending message with the same coalesce key

Disconnect:
  close link with BackpressureExceeded
```

### 17.11 Backpressure and Coalescing

Backpressure is mandatory for direct links. The policy is negotiated at link open and enforced on both local send queues and remote mailbox delivery.

Policy shape:

```rust
pub enum BackpressurePolicy {
    Block { max_pending: usize },
    FailFast { max_pending: usize },
    DropNewest { max_pending: usize },
    DropOldest { max_pending: usize },
    Coalesce {
        max_pending: usize,
        key: CoalesceKey,
    },
    Disconnect { max_pending: usize },
}
```

Recommended defaults:

```text
position update:
  DropOldest or Coalesce

state snapshot delta:
  Coalesce or Disconnect

gateway push:
  DropOldest or Disconnect

critical command:
  do not use Direct Actor Link; use RPC
```

Every drop/coalesce event must increment metrics and may optionally emit a sampled tracing event.

### 17.12 Lifecycle Events

Actors can observe link lifecycle through normal handlers:

```rust
impl Handler<LinkOpened> for BattleActor { ... }
impl Handler<LinkDirectionClosed> for BattleActor { ... }
impl Handler<LinkClosed> for BattleActor { ... }
impl Handler<LinkBackpressure> for BattleActor { ... }
impl Handler<LinkProtocolError> for BattleActor { ... }
```

Lifecycle messages:

```text
LinkOpened:
  link_id
  source
  target
  mode
  inbound_stream
  inbound_accepted_message_types
  outbound_stream: present when the local actor can send through this link
  outbound_accepted_message_types

LinkDirectionClosed:
  link_id
  direction
  stream
  reason
  last_sequence_seen

LinkClosed:
  link_id
  reason
  closed_directions
  last_sequence_seen

LinkBackpressure:
  link_id, policy, pending, dropped, coalesced

LinkProtocolError:
  link_id, error, close_action
```

Close reasons:

```text
Done
LocalClose
RemoteClose
HeartbeatTimeout
BackpressureExceeded
ProtocolError
Unauthorized
TargetPassivated
TargetMigrating
NodeDraining
ConnectionLost
```

### 17.13 Close Semantics

Direct links distinguish directional close from whole-link close.

Directional close:

```text
closes one logical send direction
keeps the underlying connection alive if another direction remains open
notifies the peer with CloseDirection
delivers LinkDirectionClosed to both endpoints that observe that direction
```

Whole-link close:

```text
closes every direction under the link_id
closes the underlying connection when no other links share it
delivers LinkClosed to both sides
causes all outstanding send handles for that link_id to fail with LinkSendError::Closed
```

Who can close:

```text
actor holding DirectLink<S>:
  may call link.close(reason) to close its own outbound direction

actor context:
  may call ctx.links().close_all(link_id, reason) to close the whole link

remote peer:
  may close its own outbound direction with CloseDirection
  may close the whole link with Close for normal shutdown or explicit protocol-level close

framework:
  may close the whole link for heartbeat timeout, protocol error, auth failure, node draining,
  target passivation, target migration, or backpressure disconnect
```

Unidirectional lifecycle:

```text
source calls link.close(Done)
  -> source outbound direction closes
  -> target receives LinkDirectionClosed
  -> link has no remaining directions
  -> both sides receive LinkClosed
```

Bidirectional lifecycle:

```text
source calls source_to_target.close(Done)
  -> only source -> target closes
  -> target receives LinkDirectionClosed for its inbound direction
  -> target -> source may continue sending

target later calls target_to_source.close(Done)
  -> target -> source closes
  -> source receives LinkDirectionClosed
  -> both directions are closed
  -> both sides receive LinkClosed
```

Protocol/error lifecycle:

```text
decode error, unsupported message type after open, invalid sequence, heartbeat timeout:
  -> framework closes the whole link immediately
  -> both directions become closed
  -> actors receive LinkClosed with the protocol/error reason
```

After an actor observes `LinkDirectionClosed` for its inbound direction, it must not assume more messages will arrive on that direction. After it observes `LinkClosed`, it must drop cached send handles for that `link_id`. Reconnect creates a new `link_id` and a new stream; sequence numbers do not continue across links.

### 17.14 Reconnect Policy

Reconnect is not transparent by default.

```rust
pub enum ReconnectPolicy {
    Disabled,
    BusinessOwned,
    FrameworkNotifyOnly,
}
```

The framework may provide helper APIs for business-owned reconnect:

```rust
ctx.links()
    .watch(link.id())
    .on_closed(|event| async move {
        // business decides whether to resolve again and reconnect
    });
```

If the business reconnects, it should treat the new link as a new stream. Sequence numbers do not imply exactly-once continuity across reconnects unless the business protocol implements recovery.

### 17.15 Security

Direct links use the same internal trust boundary as logic RPC or a stricter one.

Required security decisions before production:

```text
listener bind policy: internal only
peer authentication: mTLS, signed token, or trusted sidecar identity
authorization: allowed source service/actor kind/message type
max frame size
connection limit per source
link limit per actor
rate limit per link
```

Direct link auth must not rely on business payload fields. Source identity belongs in the link open envelope or transport security layer.

### 17.16 Observability

Metrics:

```text
link.open.count
link.open.failed.count
link.active.count
link.message.sent.count
link.message.received.count
link.message.dropped.count
link.message.coalesced.count
link.bytes.sent
link.bytes.received
link.backpressure.current
link.close.count by reason
link.decode.error.count
```

Tracing:

```text
link.open span
link.close event
sampled link.message event for debug only
backpressure sampled event
protocol error event
```

High-frequency message delivery should not create one full tracing span per message by default. Use counters, histograms, and sampled events.

### 17.17 Relationship with RPC

Direct links complement RPC; they do not replace RPC.

```text
RPC:
  command path
  owner-routed
  request/response
  route epoch and retry
  dedup and UnknownResult
  default business API

Direct Actor Link:
  data stream path
  direct connection
  business-owned lifecycle
  weaker delivery semantics
  optimized for high-frequency transient messages
```

The framework should make the difference visible in types. A direct-link send should not look like a generated RPC call.

### 17.18 Framework Integration

Direct Actor Link should be implemented as a separate framework capability, not as another `EndpointRpcTransport`.

Crate ownership:

```text
lattice-core:
  LinkId
  DirectLinkMode
  DirectLinkOptions
  BackpressurePolicy
  ReconnectPolicy
  LinkCloseReason
  LinkMessageContext
  Linked<T>
  lifecycle message types

lattice-direct-link:
  wire protocol codec
  listener
  outbound connector
  link session manager
  direct-link message registry
  typed encode/decode dispatch
  heartbeat and close protocol
  backpressure enforcement
  metrics/tracing hooks

lattice-actor:
  ActorContext::links()
  mailbox delivery of Linked<T>
  lifecycle event delivery
  actor registry lookup for inbound links
  optional direct-link actor activation policy

lattice-service:
  DirectLinkConfig
  LatticeServiceBuilder::direct_links(...)
  listener lifecycle
  direct-link endpoint publication in instance metadata
  registry wiring from actor registrations

lattice-codegen:
  optional generation of direct-link bindings
  compile-time validation that target actors implement Handler<Linked<T>>
```

The direct-link listener is a service-level listener. If enabled, the service publishes a `direct_link_endpoint` in instance metadata. Opening a routed link resolves the target actor owner through placement once, reads the owner's direct-link endpoint, then opens the direct connection.

Service builder shape:

```rust
let movement_stream = DirectLinkStream::new("movement-stream")
    .message::<game::PositionUpdate>()
    .message::<game::StateDelta>();

LatticeService::builder(BATTLE_SERVICE)
    .instance(instance)
    .dangerously_use_in_process_placement(local_store, TonicLogicControl)
    .direct_links(
        DirectLinkConfig::enabled("0.0.0.0:0")
            .max_frame_size(256 * 1024)
            .max_links_per_actor(128)
            .auth(DirectLinkAuth::InternalRpcIdentity),
    )
    .register_actor(
        ActorRegistration::builder(BATTLE_ACTOR)
            .factory(BattleActorFactory::new(app.clone()))
            .direct_link_activation(DirectLinkActivation::Allow)
            .build(),
    )
    .register_direct_link(
        movement_stream.for_actor::<BattleActor>(BATTLE_ACTOR),
    )
    .build()
    .await?;
```

Business code should not construct listeners, codecs, socket tasks, message catalogs, or dispatch tables. It declares typed direct-link streams, registers direct-link bindings, and uses `ctx.links()`.

Direct-link messages are normal protobuf messages. They do not need per-message proto options. `lattice-codegen` emits metadata traits for generated protobuf types, and the business service composes streams with real Rust types.

Example proto shape:

```proto
message PositionUpdate {
  uint64 entity_id = 1;
  float x = 2;
  float y = 3;
  uint64 tick = 4;
}
```

Build script shape stays normal:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    lattice_codegen::configure()
        .compile_protos(&["proto/game.proto"], &["proto", lattice_codegen::proto_include()])?;

    Ok(())
}
```

Generated metadata shape:

```rust
impl DirectLinkMessage for crate::game::PositionUpdate {
    const PROTO_FULL_NAME: &'static str = "game.PositionUpdate";
}

impl DirectLinkMessage for crate::game::StateDelta {
    const PROTO_FULL_NAME: &'static str = "game.StateDelta";
}
```

Business registration shape:

```rust
let movement_stream = DirectLinkStream::new("movement-stream")
    .message::<game::PositionUpdate>()
    .message::<game::StateDelta>();

service.register_direct_link(movement_stream.for_actor::<BattleActor>(BATTLE_ACTOR));
```

Manual id override remains available in Rust for cases where the direct-link protocol must carry a fixed wire id:

```rust
let legacy_stream = DirectLinkStream::new("legacy-stream")
    .message::<game::PositionUpdate>()
    .manual_id::<game::LegacySnapshot>(9001);
```

Proto option override remains available only for cases where the schema itself must carry a fixed wire id:

```proto
message LegacySnapshot {
  option (lattice.options.direct_link_msg_id) = 9001;

  bytes payload = 1;
}
```

Prefer the typed Rust declaration for ordinary business messages. Use proto options only for cross-language protocols or schema-owned compatibility contracts.

`DirectLinkStream::for_actor::<A>` carries the message catalog and actor dispatch table in one value. The stream builder should use a typestate message list so trait bounds can validate handlers at compile time:

```rust
let movement_stream = DirectLinkStream::new("movement-stream")
    .message::<game::PositionUpdate>()
    .message::<game::StateDelta>();
```

Conceptual type shape:

```rust
DirectLinkStream<(game::PositionUpdate, game::StateDelta)>
```

Actor binding shape:

```rust
impl<Messages> DirectLinkStream<Messages> {
    pub fn for_actor<A>(&self, actor_kind: ActorKind) -> DirectLinkActorBinding<A>
    where
        A: Actor,
        A: DirectLinkHandlers<Messages>,
    {
        ...
    }
}
```

The framework implements `DirectLinkHandlers<(M1, M2, ...)>` for any actor that implements `Handler<Linked<M1>>`, `Handler<Linked<M2>>`, and so on. If tuple arity becomes awkward, codegen or a small macro can generate the blanket implementations. The architectural requirement is that `for_actor::<A>` fails to compile when `A` does not implement `Handler<Linked<T>>` for every message in the stream.

Runtime registration should still fail clearly for duplicate ids, duplicate bindings, or missing actor registrations. Link-open validation then checks the source/target actor kinds and accepted message ids before any data frame is delivered.

### 17.19 Implementation Requirements

The complete implementation should include:

```text
public API:
  ActorContext::links()
  DirectLinkManager::connect(...)
  DirectLinkManager::connect_bidirectional(...)
  DirectLinkManager::get::<S>(link_id)
  DirectLinkManager::close_all(link_id, reason)
  DirectLink::tell(...)
  DirectLink::try_tell(...)
  DirectLink::close(...)
  lifecycle handlers through normal Actor Handler<T>

service integration:
  DirectLinkConfig
  DirectLinkTransportConfig with TCP as the default
  direct-link listener startup/shutdown
  direct-link endpoint in placement instance metadata
  build-time validation of actor/message bindings

transport:
  DirectLinkEndpointPool trait
  DirectLinkEndpointPoolConfig
  instance-to-instance connection pooling
  per-endpoint connection striping
  link_id multiplexing over pooled TCP connections
  DirectLinkTransport trait
  DirectLinkConnection trait
  TCP transport implementation
  transport-independent frame codec
  future QUIC/UDP adapters must preserve or explicitly declare delivery semantics

wire protocol:
  versioned frame envelope
  open/ack/reject
  message
  heartbeat/heartbeat ack
  backpressure
  close direction
  close link
  protocol error

runtime:
  one read task per physical pooled connection
  one write task per physical pooled connection
  many logical link sessions per physical connection
  no actor-pair dedicated TCP connections
  bounded outbound queues
  bounded inbound delivery queues
  mailbox delivery only through actor runtime
  no business handler execution on socket tasks

failure handling:
  heartbeat timeout
  target actor passivation
  node draining
  owner mismatch
  unsupported message type
  auth failure
  malformed frame
  backpressure overflow

observability:
  metrics for open/close/send/receive/drop/coalesce/backpressure
  sampled tracing for message flow
  structured close reasons
```

Tests must cover:

```text
link open success
open reject for wrong actor kind/id
open reject for unsupported message type
fire-and-forget delivery to Handler<Linked<T>>
ordering for one link
bidirectional connect returns the source-to-target handle
target actor can obtain target-to-source handle from LinkOpened
directional close keeps the opposite direction open
whole-link close closes both directions
LinkDirectionClosed and LinkClosed are delivered exactly once per observed transition
backpressure Block/FailFast/DropNewest/DropOldest/Coalesce/Disconnect
heartbeat timeout
target actor passivation closes link
node draining closes links
business-owned reconnect creates a new stream
codegen duplicate direct-link message id rejection
service build failure when actor binding is missing
metrics emitted for sent/received/dropped/closed
```

### 17.20 Benchmarking Requirement

Direct Actor Link should be benchmarked separately from RPC:

```text
gRPC RPC baseline
local multi-process direct-link baseline
multi-machine direct-link baseline
payload size matrix
message rate matrix
tail latency p95/p99/p999
backpressure policy matrix
drop/coalesce behavior under overload
```

The benchmark must isolate:

```text
transport cost
mailbox enqueue cost
protobuf encode/decode cost
business handler cost
OS/network scheduling cost
```

---

## 18. EndpointPool

`EndpointPool` manages RPC endpoint connections:

```text
key: instance_id + advertised_endpoint
value: shared tonic channel/client factory
```

It should support connection reuse, eviction, backoff, readiness changes, striping, and metrics. TLS/mTLS is configured through `RpcTransportSecurity`.

---

## 19. ShardedRpcCore API

The generated client delegates to a small runtime core:

```rust
#[async_trait::async_trait]
pub trait ShardedRpcCore: Clone + Send + Sync + 'static {
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest;
}
```

The core owns route resolution, route cache, retry, metadata injection, endpoint pooling, tracing, and error normalization.
