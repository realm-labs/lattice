# 02. RPC and Codegen

> Proto rules, codegen, typed sharded clients, server adapters, and RPC core.  
> Back to: [architecture index](README.md)

---

## 9. Proto and Codegen Rules

### 9.1 Proto Declares Only Routing

Business proto messages should not contain framework metadata fields. Framework metadata is carried through gRPC metadata.

```proto
service WorldRpc {
  rpc EnterWorld(EnterWorldRequest) returns (EnterWorldReply) {
    option (lattice.route_key) = {
      actor_kind: "World"
      key_field: "world_id"
    };
  }
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

### 9.2 Rust Binding

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

### 9.3 Endpoint and gRPC Services

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

### 9.4 Generated Artifacts

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

    #[error("rpc timed out; result may be unknown")]
    TimeoutUnknown,

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
  old owner must not continue; client may resolve and retry if the operation is idempotent.

TimeoutUnknown:
  the request may have been applied. Retry only with the same request_id and idempotency key.

MailboxFull / overload:
  apply caller backoff or fail fast according to policy.

Business error:
  do not retry unless business code marks it retryable.
```

### 14.1 RPC Failure and Business Consistency

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

It should support connection reuse, eviction, TLS/mTLS config, backoff, readiness changes, and metrics.

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
