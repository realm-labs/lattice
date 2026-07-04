use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::{
    Actor, ActorContext, ActorError, ActorRuntime, ActorSpawnOptions, Handler, MailboxConfig,
    Message,
};
use lattice_config::ConfigSource;
use lattice_core::{
    ActorId, ActorKey, ActorKeyDecodeError, ActorKind, RouteKey, ServiceKind, actor_kind,
    service_kind,
};
use serde::Deserialize;

pub const WORLD_SERVICE: ServiceKind = service_kind!("World");
pub const WORLD_ACTOR: ActorKind = actor_kind!("World");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorldId(pub u64);

impl ActorKey for WorldId {
    fn to_route_key(&self) -> RouteKey {
        RouteKey::U64(self.0)
    }

    fn to_actor_id(&self) -> ActorId {
        ActorId::U64(self.0)
    }

    fn try_from_actor_id(actor_id: &ActorId) -> Result<Self, ActorKeyDecodeError> {
        match actor_id {
            ActorId::U64(value) => Ok(Self(*value)),
            _ => Err(ActorKeyDecodeError {
                reason: "expected u64 actor id for WorldId".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlayerId(pub u64);

#[derive(Debug, Default)]
pub struct PlayerRuntimeState {
    ticks_seen: u64,
}

pub struct WorldActor {
    pub world_id: WorldId,
    pub tick_ms: u64,
    pub players: HashMap<PlayerId, PlayerRuntimeState>,
}

#[async_trait]
impl Actor for WorldActor {
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let tick_ms = self.tick_ms;
        ctx.notify_interval(Duration::from_millis(tick_ms), move || WorldTick {
            delta_ms: tick_ms,
        });
        Ok(())
    }
}

#[derive(Debug)]
pub struct EnterWorld {
    pub player_id: u64,
}

impl Message for EnterWorld {
    type Reply = EnterWorldReply;
}

#[derive(Debug, PartialEq, Eq)]
pub struct EnterWorldReply {
    pub ok: bool,
    pub player_count: usize,
}

#[derive(Debug)]
pub struct WorldTick {
    pub delta_ms: u64,
}

impl Message for WorldTick {
    type Reply = ();
}

#[derive(Debug)]
pub struct InspectWorld;

impl Message for InspectWorld {
    type Reply = WorldSnapshot;
}

#[derive(Debug, PartialEq, Eq)]
pub struct WorldSnapshot {
    pub world_id: WorldId,
    pub player_count: usize,
    pub total_ticks: u64,
}

#[async_trait]
impl Handler<EnterWorld> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: EnterWorld,
    ) -> Result<EnterWorldReply, ActorError> {
        let player_id = PlayerId(msg.player_id);
        self.players.entry(player_id).or_default();
        Ok(EnterWorldReply {
            ok: true,
            player_count: self.players.len(),
        })
    }
}

#[async_trait]
impl Handler<WorldTick> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: WorldTick,
    ) -> Result<(), ActorError> {
        assert_eq!(msg.delta_ms, self.tick_ms);
        for state in self.players.values_mut() {
            state.ticks_seen += 1;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<InspectWorld> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: InspectWorld,
    ) -> Result<WorldSnapshot, ActorError> {
        Ok(WorldSnapshot {
            world_id: self.world_id,
            player_count: self.players.len(),
            total_ticks: self.players.values().map(|state| state.ticks_seen).sum(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct WorldConfig {
    tick_ms: u64,
    mailbox_capacity: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: WorldConfig =
        ConfigSource::file("examples/minimal-world/config/world-service.toml")
            .load()?
            .section("world")?;
    let runtime = ActorRuntime::default();
    let world = runtime
        .spawn_actor(
            WorldActor {
                world_id: WorldId(1),
                tick_ms: config.tick_ms,
                players: HashMap::new(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(config.mailbox_capacity),
                ..ActorSpawnOptions::default()
            },
        )
        .await?;

    let reply = world.call(EnterWorld { player_id: 1001 }).await?;
    tokio::time::sleep(Duration::from_millis(config.tick_ms * 2)).await;
    let snapshot = world.call(InspectWorld).await?;

    println!(
        "{}:{} enter_ok={} players={} ticks={}",
        WORLD_SERVICE.as_str(),
        WORLD_ACTOR.as_str(),
        reply.ok,
        snapshot.player_count,
        snapshot.total_ticks
    );
    Ok(())
}
