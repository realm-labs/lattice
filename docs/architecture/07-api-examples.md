# 07. Framework API Examples

> Public API sketches for business users. These examples constrain the shape of the framework APIs. `WorldId`, `WorldActor`, and `WorldRpcBinding` are business or codegen examples, not built-in lattice business types.  
> Back to: [architecture index](README.md)

---

## 34. API Design Principles

Framework APIs should let business code:

```text
start a service by composing lattice runtime components
register actor factories without exposing business DB schema to the framework
write logic through Handler<M>
call other actors/services through generated clients
use EventBus, ConfigStore, ServiceScheduler, and generated clients from context
pass ActorRef or GatewaySessionRef across process boundaries, never ActorHandle
avoid exposing internal metadata, epoch, fencing, and route cache as request fields
```

Layering:

```text
business crate:
  AppDeps, actor structs, repositories, business config, business clients

generated crate:
  proto types, RPC bindings, generated clients, generated adapters, gateway bindings

lattice runtime:
  actor runtime, placement, RPC core, event bus, scheduler, config, ops
```

---

## 35. Generating RPC Bindings

Business crates generate proto types and lattice RPC glue from `build.rs`:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];

    lattice_codegen::configure()
        .gateway_route_ids([(100, "world.WorldRpc.EnterWorld")])
        .compile_protos(&["proto/world.proto"], &includes)?;

    Ok(())
}
```

Large protocols can generate the `gateway_route_ids` input from a business-owned message table, or load it from a validated file:

```rust
lattice_codegen::configure()
    .gateway_routes("proto/gateway-routes.toml")
    .compile_protos(&["proto/world.proto"], &includes)?;
```

Runtime code includes both tonic/prost output and lattice bindings:

```rust
pub mod world {
    tonic::include_proto!("world");
}

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/lattice.generated.rs"));
}
```

---

## 36. Starting a Logic Service

```rust
pub const WORLD_SERVICE: ServiceKind = service_kind!("World");
pub const WORLD_ACTOR: ActorKind = actor_kind!("World");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = AppDeps::from_env().await?;
    let instance = InstanceConfig::from_env()?;
    let _telemetry = lattice_telemetry_otlp::LatticeTelemetry::from_config(
        WORLD_SERVICE,
        instance.instance_id.clone(),
        lattice_telemetry_otlp::TelemetryConfig::fmt_only(instance.version.clone()),
    )
    .install()?;

    let service = LatticeService::builder(WORLD_SERVICE)
        .instance(instance)
        .config(ConfigSource::file("config/world-service.toml"))
        .placement_store(EtcdPlacementStore::from_config())
        .cluster_event_bus(NatsEventBus::from_config())
        .local_event_bus(LocalEventBus::default())
        .config_store(lattice_config_etcd::EtcdConfigStore::from_config())
        .telemetry(TelemetryConfig::from_config())
        .admin_http(AdminHttpConfig::from_config())
        .register_actor(
            ActorRegistration::builder(WORLD_ACTOR)
                .factory(WorldActorFactory::new(app.clone()))
                .mailbox(MailboxConfig::bounded(4096))
                .passivation(PassivationPolicy::IdleTimeout(Duration::from_secs(300)))
                .build(),
        )
        .register_sharded_rpc(generated::world_rpc::Binding::for_actor::<WorldActor>(WORLD_ACTOR))
        .register_client::<generated::player_rpc::Binding>()
        .build()
        .await?;

    service.run_until_shutdown().await
}
```

`from_config()` reads the component section from the `BootstrapConfig` already loaded by `.config(...)` during `build()`. It does not read global static state.

A process may register multiple actor kinds and multiple generated gRPC services while sharing one `advertised_endpoint`.

---

## 37. InstanceConfig

```rust
let instance = InstanceConfig {
    instance_id: InstanceId::new("world-0"),
    advertised_endpoint: "http://world-0.world:18080".parse()?,
    control_endpoint: "http://world-0.world:18081".parse()?,
    version: env!("CARGO_PKG_VERSION").to_string(),
    capacity: InstanceCapacity {
        max_actors: Some(100_000),
        max_connections: None,
    },
    labels: BTreeMap::from([
        ("region".into(), "us-east".into()),
        ("zone".into(), "a".into()),
    ]),
};
```

`InstanceConfig::from_env()` builds the same value from deployment environment, such as Kubernetes downward API, environment variables, or startup args.

---

## 38. AppDeps

`AppDeps` is owned by the business crate. It contains business dependencies, not framework runtime internals.

```rust
#[derive(Clone)]
pub struct AppDeps {
    pub world_repo: Arc<WorldRepository>,
    pub item_repo: Arc<ItemRepository>,
    pub http_client: reqwest::Client,
    pub business_config: Arc<BusinessConfig>,
}
```

Framework handles created by runtime, such as generated clients, schedulers, event publishers, and config stores, should be accessed through service or actor context, or explicitly composed into a business-owned client container.

---

## 39. Actor Factory Registration

```rust
pub struct WorldActorFactory {
    app: AppDeps,
}

impl WorldActorFactory {
    pub fn new(app: AppDeps) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl ActorFactory for WorldActorFactory {
    type Actor = WorldActor;
    type Key = WorldId;

    async fn create(
        &self,
        key: WorldId,
        ctx: ActorCreateContext,
    ) -> Result<WorldActor, ActorCreateError> {
        let snapshot = self.app.world_repo.load(key).await?;

        Ok(WorldActor {
            world_id: key,
            state: snapshot,
        })
    }
}
```

If `create` fails, the runtime must not register a zombie actor. Activation waiters receive an error and later requests can retry.

Factory input may use the business typed key when codegen has enough information to decode it from `ActorId`; otherwise the lower-level factory can accept `ActorId`.

---

## 40. Actor and Handler

```rust
pub struct WorldActor {
    pub world_id: WorldId,
    pub state: WorldState,
}

#[async_trait::async_trait]
impl Actor for WorldActor {
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        ctx.notify_interval(Duration::from_millis(50), || WorldTick { delta_ms: 50 });
        Ok(())
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

#[async_trait::async_trait]
impl Handler<Rpc<EnterWorldRequest>> for WorldActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        msg: Rpc<EnterWorldRequest>,
    ) -> Result<EnterWorldReply, WorldError> {
        let player_id = PlayerId(msg.req.player_id);
        self.state.players.insert(player_id, PlayerRuntimeState::default());

        if msg.req.logout_after_enter {
            ctx.request_passivation(PassivationReason::BusinessIdle)?;
        }

        Ok(EnterWorldReply { ok: true })
    }
}
```

If `stopping` fails while saving state, the runtime enters `StopFailed`, keeps ownership, blocks unload/release, surfaces an alert, and waits for retry or manual intervention.

---

## 41. Actor State Machine Pattern

Business state machines should be explicit:

```rust
pub enum MatchState {
    Loading,
    WaitingPlayers,
    Running { tick: u64 },
    Ending,
    Ended,
}

#[async_trait::async_trait]
impl Handler<Rpc<StartMatchRequest>> for MatchActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<StartMatchRequest>,
    ) -> Result<StartMatchReply, MatchError> {
        match &mut self.state {
            MatchState::WaitingPlayers => {
                self.state = MatchState::Running { tick: 0 };
                Ok(StartMatchReply { accepted: true })
            }
            _ => Err(MatchError::InvalidState),
        }
    }
}
```

Avoid relying on a hidden mailbox stash for state-machine transitions.

---

## 42. RPC Binding and Client

Generated binding shape:

```rust
pub struct WorldRpcBinding;

impl ShardedRpcBinding for WorldRpcBinding {
    type Service = WorldRpcServer<WorldRpcAdapter>;

    fn service_kind() -> ServiceKind {
        service_kind!("World")
    }

    fn register(server: &mut RpcServerBuilder, runtime: LogicRuntime) {
        server.add_service(WorldRpcServer::new(WorldRpcAdapter::new(runtime)));
    }
}
```

Business call shape:

```rust
let profile = ctx
    .clients()
    .player()
    .get_profile(GetProfileRequest {
        player_id: player_id.0,
    })
    .await?;
```

The framework should make the common path short. Business crates can wrap generated clients into their own `AppClients`.

---

## 43. ServiceContext

```rust
#[derive(Clone)]
pub struct ServiceContext {
    pub instance_id: InstanceId,
    pub clients: ServiceClients,
    pub cluster_events: ServiceEvents,
    pub local_events: ServiceEvents,
    pub config: ConfigStoreHandle,
    pub scheduler: ServiceScheduler,
}
```

Actor context may expose a narrowed view:

```rust
impl<A: Actor> ActorContext<A> {
    pub fn clients(&self) -> &ServiceClients;
    pub fn cluster_events(&self) -> &ActorEvents;
    pub fn local_events(&self) -> &ActorEvents;
    pub fn config(&self) -> &ConfigStoreHandle;
    pub fn scheduler(&self) -> &ActorScheduler;
}
```

---

## 44. Gateway Service

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gateway = LatticeGateway::builder()
        .instance(InstanceConfig::from_env()?)
        .config(ConfigSource::file("config/gateway.toml"))
        .client_codec(GameClientCodec::new())
        .route_table(GatewayRouteTable::from_config())
        .register_binding(WorldGatewayBinding)
        .register_binding(GuildGatewayBinding)
        .rate_limiter(GovernorGatewayRateLimiter::from_config())
        .cluster_event_bus(NatsEventBus::from_config())
        .telemetry(TelemetryConfig::from_config())
        .admin_http(AdminHttpConfig::from_config())
        .build()
        .await?;

    gateway.run_until_shutdown().await
}
```

Large businesses should use generated route tables or validated config, not hundreds of handwritten route entries.

---

## 45. Placement Coordinator

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let coordinator = PlacementCoordinator::builder()
        .config(ConfigSource::file("config/coordinator.toml"))
        .placement_store(EtcdPlacementStore::from_config())
        .assigner(RendezvousShardAssigner::default())
        .telemetry(TelemetryConfig::from_config())
        .admin_http(AdminHttpConfig::from_config())
        .build()
        .await?;

    coordinator.run_until_shutdown().await
}
```

The Coordinator handles control-plane decisions only.

---

## 46. EventBus, Config, and Scheduler Usage

Typed event publish:

```rust
ctx.service()
    .cluster_events()
    .publish(WorldEvents::player_entered(PlayerEnteredWorld {
        world_id: self.world_id.0,
        player_id: player_id.0,
    }))
    .await?;
```

Typed service subscription:

```rust
service
    .cluster_events()
    .subscribe_typed(
        SubjectFilter::new("game.guild.*"),
        ConsumerGroup::new("world-cache"),
        WorldCacheInvalidationHandler::new(),
    )
    .await?;
```

Actor subscription:

```rust
service
    .cluster_events()
    .subscribe_actor(
        SubjectFilter::new("game.guild.created"),
        EventActorRoute::by_key(|event: &GuildCreated| WorldId(event.world_id)),
        DeliveryOptions::at_least_once(),
    )
    .await?;
```

Config watch:

```rust
let mut stream = service.config().watch(ConfigKey::new("gateway.rate_limit")).await?;
while let Some(change) = stream.next().await {
    service.gateway_rate_limits().apply(change.value)?;
}
```

Actor scheduler:

```rust
ctx.scheduler()
    .notify_after(Duration::from_secs(5), RetryPendingOperation { operation_id });
```

Service scheduler:

```rust
service
    .scheduler()
    .interval(Duration::from_secs(30), || async move {
        refresh_local_cache().await
    });
```

---

## 47. Minimal Business Layout

```text
crates/
  world-service/
    src/main.rs
    src/world_actor.rs
    src/world_factory.rs
    src/app_deps.rs

  player-service/
    src/main.rs
    src/player_actor.rs
    src/player_factory.rs

  gateway/
    src/main.rs
    src/client_codec.rs

proto/
  world.proto
  player.proto
  gateway.proto

config/
  world-service.toml
  player-service.toml
  gateway.toml
  coordinator.toml
```

---

## 48. API Decisions to Keep Stable

```text
ActorKind and ServiceKind are opaque string newtypes with actor_kind! and service_kind! helpers.
ActorKey conversion is generated or implemented by business key types, not hand-coded on every RPC call.
RpcContext is carried by gRPC metadata.
ActorHandle is local-only.
GatewaySessionRef is the cross-process handle for client push.
Virtual shard assignment is a trait with default implementations.
ConfigSource supports file, explicit format, env, inline, and composite sources.
ConfigStore is a trait in lattice-config; etcd and Nacos-style backends are adapter crates or business implementations.
from_config() reads component config during builder build.
EventBus has both local and cluster implementations.
subscribe_actor routes through owner resolution and actor mailbox.
Scheduler is non-durable and lifecycle-bound.
```
