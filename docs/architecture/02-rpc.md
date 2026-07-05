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

`RouteTarget.advertised_endpoint` identifies one service process and tonic channel. A single process can host multiple generated gRPC services:

```text
world-service instance:
  advertised_endpoint = http://10.0.1.7:18080
  services:
    WorldRpc
    RoomRpc
    ZoneRpc
```

Connection pooling is keyed by `instance_id` and `advertised_endpoint`, not by actor id.

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
    generated::world_rpc::Binding::for_actor::<WorldActor>(WORLD_ACTOR)
        .request_dedup(false),
);
```

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

## 17. EndpointPool

`EndpointPool` manages tonic channels:

```text
key: instance_id + advertised_endpoint
value: shared tonic channel/client factory
```

It should support connection reuse, eviction, backoff, readiness changes, and metrics. TLS/mTLS is
configured through `RpcTransportSecurity` and applied by the generated tonic endpoint transport when
it creates or reuses channels.

---

## 18. ShardedRpcCore API

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
