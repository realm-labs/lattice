# 07. Framework API Examples

> Target API sketches for the remoting architecture. They intentionally describe the desired public shape and may not compile until the corresponding implementation phase lands.
> Back to: [architecture index](README.md)

---

## 1. Define an Actor and Typed Messages

```rust
pub struct PlayerActor {
    id: PlayerId,
    profile: PlayerProfile,
}

impl Actor for PlayerActor {}

#[derive(lattice_actor::Message)]
pub struct PositionUpdated {
    pub x: f32,
    pub y: f32,
}

#[derive(lattice_actor::Request)]
#[request(response = PlayerProfile)]
pub struct GetProfile;

#[derive(Clone)]
pub struct PlayerProfile {
    pub position: (f32, f32),
}

#[async_trait::async_trait]
impl Handler<PositionUpdated> for PlayerActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: PositionUpdated,
    ) -> Result<(), ActorError> {
        self.profile.position = (msg.x, msg.y);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Responder<GetProfile> for PlayerActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: GetProfile,
        reply_to: ReplyTo<PlayerProfile>,
    ) -> Result<(), ActorError> {
        reply_to.send(self.profile.clone())?;
        Ok(())
    }
}
```

Remote and local tells enter `Handler<M>`; asks enter `Responder<R>` with a typed reply capability. There is no `Rpc<M>` business wrapper.

## 2. Register the Remote Protocol

Use one type-checked Rust declaration as the protocol source:

```rust
lattice::actor_protocol! {
    pub PlayerProtocol for PlayerActor {
        protocol_id: 0x504c_4159_4552_0001;
        name: "player/v1";

        tell 1 => PositionUpdated {
            schema_version: 1,
            codec: PostcardCodec::default(),
        }

        ask 2 => GetProfile {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: PostcardCodec::default(),
            response_codec: PostcardCodec::default(),
        }
    }
}

let player_protocol = PlayerProtocol::build()?;
```

A business IDL, YAML, spreadsheet, or internal protocol catalogue may generate this Rust declaration. lattice itself does not scan `Handler` implementations or load Rust types from runtime config.

The following builder is the equivalent low-level form for tests, very small protocols, or custom generators:

```rust
let player_protocol = ActorProtocol::<PlayerActor>::builder(
    ProtocolId::new(0x504c_4159_4552_0001),
    "player/v1",
)
    .tell::<PositionUpdated, _>(
        1,
        1,
        PostcardCodec::default(),
    )
    .ask::<GetProfile, _, _>(
        2,
        1,
        1,
        PostcardCodec::default(),
        PostcardCodec::default(),
    )
    .build()?;
```

Codec implementations are replaceable:

```rust
pub struct CodecDescriptor {
    pub id: u64,
    pub version: u32,
}

pub trait WireCodec<T>: Send + Sync + 'static {
    const DESCRIPTOR: CodecDescriptor;

    fn encode(&self, value: &T, dst: &mut BytesMut) -> Result<(), EncodeError>;
    fn decode(&self, src: &[u8]) -> Result<T, DecodeError>;
}
```

Protocol ID, message IDs, interaction modes, codec/schema versions, and the resulting fingerprint are stable compatibility contracts. A tell registration has no response codec; an ask registration includes the associated response codec. Macro and manual registration use identical validation and fingerprint calculation.

## 3. Start a Logic Service

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = AppDeps::from_env().await?;

    let service = LatticeService::builder(NodeConfig::from_env()?)
        .remoting(
            RemotingConfig::builder()
                .listen("0.0.0.0:25520".parse()?)
                .advertise("player-0.player:25520".parse()?)
                .bulk_stripes_per_association(1)
                .max_active_associations(256)
                .security(RemotingSecurity::from_config()?)
                .build()?,
        )
        .coordinator(CoordinatorBootstrap::from_config()?)
        .event_bus(NatsEventBus::from_config()?)
        .register_actor_protocol(player_protocol.clone())
        .register_sharded_entity(
            EntityType::builder::<PlayerActor>("player")
                .shards(256)
                .hash_version(ShardHashVersion::Xxh3V1)
                .protocol(player_protocol)
                .factory(PlayerActorFactory::new(app.clone()))
                .passivate_after(Duration::from_secs(600))
                .rebalance(
                    RebalancePolicy::weighted_least_load()
                        .interval(Duration::from_secs(10))
                        .load_sample_max_age(Duration::from_secs(20))
                        .min_relative_improvement(0.10)
                        .min_shard_residence(Duration::from_secs(120))
                        .node_join_stability(Duration::from_secs(30))
                        .cooldown(Duration::from_secs(30))
                        .max_moves_per_round(4)
                        .max_concurrent_moves(8),
                )
                .build()?,
        )
        .register_singleton(
            SingletonType::builder::<MatchmakerActor>("matchmaker")
                .protocol(matchmaker_protocol())
                .factory(MatchmakerFactory::new(app))
                .required_role("matchmaker")
                .build()?,
        )
        .build()
        .await?;

    service.run_until_shutdown().await
}
```

The Coordinator may run in dedicated processes or as an eligible role in the same service binary. Business nodes do not receive a general-purpose placement-store handle.

## 4. Concrete `ActorRef`: Any Live Actor Path

```rust
let child: ActorRef<SessionWorker> = ctx
    .spawn_child("session-worker", SessionWorker::new())
    .await?;

// ActorRef is ordinary cloneable and serializable identity data.
ctx.tell(&parent_ref, WorkerReady { worker: child.clone() }).await?;

ctx.tell(&child, Flush).await?;
let stats = service
    .ask(&child, GetStats, Deadline::after(Duration::from_secs(2)))
    .await?;
service.watch(&child).await?;
```

The serialized reference contains cluster ID, node address/incarnation, hierarchical actor path, activation ID, and `ProtocolId`. Protocol fingerprints are negotiated in the Association catalogue after transport establishment. A mismatch disables that `ProtocolId` for the peer without closing unrelated protocols on the Association. A replacement at the same path is a different actor; the old reference returns `StaleActivation` or produces `Terminated`.

Remote code cannot create actors by choosing a path. It can only use a reference received through a typed message, lookup explicitly authorized by an application registry, or another framework-controlled API.

An actor can publish its own exact activation reference and preserve an original sender when routing a tell:

```rust
#[derive(Serialize, Deserialize)]
struct SubscribeToTicks {
    publisher: ActorRef<TickPublisher>,
}

ctx.tell(
    &subscriber,
    SubscribeToTicks {
        publisher: ctx.require_self_ref()?.clone(),
    },
).await?;

// Stamps this actor as sender.
ctx.tell(&target, Tick).await?;

// Preserves ctx.sender(), as in Akka/Pekko forwarding.
ctx.forward(&target, Tick).await?;

// Deserialization needs no backend-aware binding step.
let publisher: ActorRef<TickPublisher> = serde_json::from_slice(encoded)?;
service.tell(&publisher, Tick).await?;
```

The actor protocol must be registered once with the service, including on client-only nodes through `register_protocol`. After that, `ActorRef`, `EntityRef`, and `SingletonRef` values can be decoded and used directly. `ctx.sender()` is available only for the current tell and is `None` for process-originated tells and asks. Ask responses use the typed `ReplyTo` passed to `Responder<R>`. Retained `ActorRef`s identify one activation; use `EntityRef` or `SingletonRef` when the logical destination must survive activation replacement.

## 5. `EntityRef`: Sharded Logical Identity

```rust
impl ShardedActor for PlayerActor {
    type Key = PlayerId;
}

impl EntityKey for PlayerId {
    fn to_entity_id(&self) -> EntityId {
        EntityId::from_bounded_bytes(self.0.to_be_bytes())
    }

    fn try_from_entity_id(entity_id: &EntityId) -> Result<Self, EntityKeyDecodeError> {
        let bytes: [u8; 8] = entity_id
            .as_bytes()
            .try_into()
            .map_err(|_| EntityKeyDecodeError::invalid_length(8, entity_id.len()))?;
        Ok(PlayerId::new(u64::from_be_bytes(bytes)))
    }
}

let player: EntityRef<PlayerActor> = ctx
    .entity_ref::<PlayerActor>(PlayerId::new(42))?;

player.tell(PositionUpdated { x: 12.0, y: 8.5 }).await?;

let profile = player
    .ask(GetProfile, Deadline::after(Duration::from_secs(3)))
    .await?;

let player_watch = ctx.watch_current(player.clone()).await?;
```

The caller never chooses an owner node. The local ShardRegion routes, activates, buffers during handoff within limits, and follows relocation.

`watch_current` does not activate an inactive entity: it returns `WatchError::NotActive`. When it succeeds, it watches the current exact activation. Passivation or handoff emits `Terminated`; observing a later activation requires another `watch_current` call.

## 6. `SingletonRef`: Fixed Cluster Singleton

```rust
let matchmaker: SingletonRef<MatchmakerActor> =
    ctx.singleton_ref::<MatchmakerActor>("matchmaker")?;

let ticket = matchmaker
    .ask(JoinQueue { player_id }, Deadline::after(Duration::from_secs(5)))
    .await?;

let matchmaker_watch = ctx.watch_current(matchmaker.clone()).await?;
```

The local proxy follows Coordinator assignments. `watch_current` returns `WatchError::Unavailable` when no singleton activation exists. Failover terminates the old activation watch; the logical reference remains usable and the replacement must be watched explicitly.

## 7. DeathWatch

```rust
impl Handler<Terminated> for SessionOwner {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        event: Terminated,
    ) -> Result<(), ActorError> {
        match event.subject {
            WatchedSubject::Actor(actor) => self.on_exact_activation_lost(actor),
            WatchedSubject::EntityActivation { entity, activation } =>
                self.on_entity_activation_lost(entity, activation),
            WatchedSubject::SingletonActivation { kind, activation } =>
                self.on_singleton_activation_lost(kind, activation),
        }
        Ok(())
    }
}
```

Association loss may first make a remote node suspect. Concrete termination is emitted after the runtime's failure detector/membership decision establishes that the referenced incarnation cannot continue, avoiding a transient socket reset being treated as actor death.

## 8. Ask Failure and Idempotency

```rust
#[derive(lattice_actor::Request)]
#[request(response = Result<Reservation, ReserveItemError>)]
struct ReserveItem;

match inventory
    .ask(
        ReserveItem {
            operation_id,
            item_id,
        },
        Deadline::after(Duration::from_secs(3)),
    )
    .await
{
    Ok(Ok(reservation)) => use_reservation(reservation),
    Ok(Err(business_error)) => handle_expected_rejection(business_error),
    Err(AskError::UnknownResult { .. }) => reconcile_by_operation_id(operation_id).await?,
    Err(error) => return Err(error.into()),
}
```

Expected domain failures are part of the typed reply. `AskError` is reserved for transport/runtime failures represented by stable `RemoteFailureCode` values. The runtime does not automatically retry a state-changing ask; the business operation ID makes reconciliation and an intentional retry safe.

## 9. Gateway Binding

```rust
let routes = GatewayRoutes::builder()
    .ask_entity::<PlayerActor, GetProfile>(
        ClientMsgId(1001),
        |frame, auth| PlayerId::try_from(auth.principal.clone()),
        |frame| game_codec.decode::<GetProfile>(frame.payload),
        RateClass::Interactive,
    )
    .ask_singleton::<MatchmakerActor, JoinQueue>(
        ClientMsgId(2001),
        "matchmaker",
        |frame, auth| decode_join_queue(frame, auth),
        RateClass::Interactive,
    )
    .build()?;

LatticeGateway::builder(NodeConfig::from_env()?)
    .remoting(RemotingConfig::from_config()?)
    .coordinator(CoordinatorBootstrap::from_config()?)
    .client_codec(GameClientCodec::new())
    .routes(routes)
    .build()
    .await?
    .run_until_shutdown()
    .await
```

Code generation may create route tables and codec registrations, but generated code targets actor protocols and references, not tonic clients.

## 10. EventBus, Scheduler, and Config

```rust
ctx.cluster_events()
    .publish(PlayerEnteredWorld { player_id, world_id })
    .await?;

ctx.scheduler()
    .tell_after(Duration::from_secs(5), ctx.self_ref(), RetrySave { operation_id });

let mut changes = service.config().watch(ConfigKey::new("gateway.rate_limit")).await?;
while let Some(change) = changes.next().await {
    service.gateway_rate_limits().apply(change?)?;
}
```

EventBus is for broadcast/integration. A scheduled actor message still uses the recipient reference and is cancelled with its lifecycle unless explicitly registered as a service task.

## 11. Stable API Decisions

```text
ActorHandle remains process-local; ActorRef is the serializable exact-activation reference.
ActorRef, EntityRef, and SingletonRef are distinct typed values.
References are sent through ActorContext or LatticeService directly; there is no public BoundRecipient or bind step.
All remote business delivery uses registered actor protocols over lattice-remoting.
actor_protocol! is the canonical protocol source; ProtocolId and message IDs are explicit.
Message codecs are format-neutral and fingerprints include codec/schema versions.
Local and remote tells use `Handler<M>`; local and remote asks use `Responder<R>`.
Tell is at-most-once; ask has deadline/UnknownResult semantics.
All DeathWatch registrations are activation-scoped; EntityRef/SingletonRef use watch_current without activating a target.
Reliable control delivery is Association-scoped and shared by DeathWatch, Coordinator, Shard, and Singleton protocols; it never replays uncertain business messages.
Business code never manually opens or owns node transport links.
```
