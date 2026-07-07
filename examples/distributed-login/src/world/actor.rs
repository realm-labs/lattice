use std::collections::HashMap;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::registry::ActorCreateContext;
use lattice_actor::registry::ActorFactory;
use lattice_actor::traits::{Actor, Handler};
use lattice_core::id::ActorId;
use lattice_rpc::types::Rpc;

use crate::game::{
    InitSessionRequest, LoginAcceptedReply, LoginRequest, WorldPingReply, WorldPingRequest,
};
use crate::generated::player_rpc::{Client as PlayerRpcClient, DefaultClientCore as PlayerRpcCore};

#[derive(Debug)]
pub struct WorldActor {
    world_id: u64,
    sessions: HashMap<u64, String>,
    player_client: PlayerRpcClient<PlayerRpcCore>,
}

impl WorldActor {
    fn new(world_id: u64, player_client: PlayerRpcClient<PlayerRpcCore>) -> Self {
        Self {
            world_id,
            sessions: HashMap::new(),
            player_client,
        }
    }
}

#[async_trait]
impl Actor for WorldActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<Rpc<LoginRequest>> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<LoginRequest>,
    ) -> Result<LoginAcceptedReply, ActorError> {
        if msg.req.world_id != self.world_id {
            return Ok(LoginAcceptedReply {
                accepted: false,
                message: format!(
                    "world {} cannot serve world {}",
                    self.world_id, msg.req.world_id
                ),
            });
        }

        let session_id = format!("world-{}-player-{}", self.world_id, msg.req.player_id);
        self.sessions.insert(msg.req.player_id, session_id.clone());
        let init = self
            .player_client
            .init_session(InitSessionRequest {
                player_id: msg.req.player_id,
                world_id: self.world_id,
                session_id,
                gateway_session: msg.req.gateway_session,
            })
            .await
            .map_err(|error| ActorError::new(error.to_string()))?;

        Ok(LoginAcceptedReply {
            accepted: init.ok,
            message: if init.ok {
                "login accepted"
            } else {
                &init.message
            }
            .to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WorldActorFactory;

impl WorldActorFactory {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ActorFactory<WorldActor> for WorldActorFactory {
    async fn create(&self, ctx: ActorCreateContext) -> Result<WorldActor, ActorError> {
        let player_client = ctx
            .service
            .extension::<PlayerRpcClient<PlayerRpcCore>>()
            .ok_or_else(|| ActorError::new("missing generated Player RPC client"))?;
        match ctx.actor_id {
            ActorId::U64(world_id) => Ok(WorldActor::new(world_id, (*player_client).clone())),
            other => Err(ActorError::new(format!(
                "unsupported world actor id {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl Handler<Rpc<WorldPingRequest>> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<WorldPingRequest>,
    ) -> Result<WorldPingReply, ActorError> {
        Ok(WorldPingReply {
            ok: msg.req.world_id == self.world_id,
            world_id: self.world_id,
            session_count: self.sessions.len() as u64,
            message: format!(
                "world {} has {} sessions",
                self.world_id,
                self.sessions.len()
            ),
        })
    }
}
