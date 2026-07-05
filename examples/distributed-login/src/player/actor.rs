use std::collections::HashMap;

use async_trait::async_trait;
use lattice_actor::registry::ActorCreateContext;
use lattice_actor::{Actor, ActorContext, ActorError, ActorFactory, Handler};
use lattice_core::{ActorId, ActorRef};
use lattice_rpc::{ActorRefRpcClient, Rpc};
use prost::Message as ProstMessage;

use crate::LOGIN_MSG_ID;
use crate::game::{
    InitSessionReply, InitSessionRequest, LoginReply, PlayerPingReply, PlayerPingRequest,
    PushToClientReply, PushToClientRequest,
};
use crate::placement::DemoActorRefRpcCore;

pub struct PlayerActor {
    player_id: u64,
    sessions: HashMap<String, u64>,
    actor_ref_client: ActorRefRpcClient<DemoActorRefRpcCore>,
}

impl PlayerActor {
    fn new(player_id: u64, actor_ref_client: ActorRefRpcClient<DemoActorRefRpcCore>) -> Self {
        Self {
            player_id,
            sessions: HashMap::new(),
            actor_ref_client,
        }
    }

    async fn push_login_reply(
        &self,
        gateway_session: ActorRef,
        reply: LoginReply,
    ) -> Result<PushToClientReply, ActorError> {
        let session_id = match &gateway_session.actor_id {
            ActorId::Str(value) => value.clone(),
            other => {
                return Err(ActorError::new(format!(
                    "gateway session actor ref must use string actor id, got {other:?}"
                )));
            }
        };

        self.actor_ref_client
            .call_ref(
                gateway_session,
                PushToClientRequest {
                    session_id,
                    msg_id: LOGIN_MSG_ID,
                    payload: reply.encode_to_vec(),
                },
            )
            .await
            .map_err(|error| ActorError::new(error.to_string()))
    }
}

#[async_trait]
impl Actor for PlayerActor {}

#[async_trait]
impl Handler<Rpc<InitSessionRequest>> for PlayerActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<InitSessionRequest>,
    ) -> Result<InitSessionReply, ActorError> {
        if msg.req.player_id != self.player_id {
            return Ok(InitSessionReply {
                ok: false,
                player_id: self.player_id,
                message: format!(
                    "player actor {} cannot serve player {}",
                    self.player_id, msg.req.player_id
                ),
            });
        }

        let session_id = msg.req.session_id;
        self.sessions.insert(session_id.clone(), msg.req.world_id);
        let gateway_session = msg
            .req
            .gateway_session
            .ok_or_else(|| ActorError::new("missing gateway session actor ref"))?
            .try_into()
            .map_err(|error: lattice_rpc::RpcError| ActorError::new(error.to_string()))?;
        let push = self
            .push_login_reply(
                gateway_session,
                LoginReply {
                    ok: true,
                    world_id: msg.req.world_id,
                    player_id: self.player_id,
                    session_id,
                    message: "login complete".to_string(),
                },
            )
            .await?;
        if !push.ok {
            return Err(ActorError::new(push.message));
        }

        Ok(InitSessionReply {
            ok: true,
            player_id: self.player_id,
            message: "player session initialized and login pushed".to_string(),
        })
    }
}

#[async_trait]
impl Handler<Rpc<PlayerPingRequest>> for PlayerActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<PlayerPingRequest>,
    ) -> Result<PlayerPingReply, ActorError> {
        Ok(PlayerPingReply {
            ok: msg.req.player_id == self.player_id,
            player_id: self.player_id,
            session_count: self.sessions.len() as u64,
            message: format!(
                "player {} has {} sessions",
                self.player_id,
                self.sessions.len()
            ),
        })
    }
}

#[derive(Clone)]
pub struct PlayerActorFactory {
    actor_ref_client: ActorRefRpcClient<DemoActorRefRpcCore>,
}

impl PlayerActorFactory {
    pub fn new(actor_ref_client: ActorRefRpcClient<DemoActorRefRpcCore>) -> Self {
        Self { actor_ref_client }
    }
}

#[async_trait]
impl ActorFactory<PlayerActor> for PlayerActorFactory {
    async fn create(&self, ctx: ActorCreateContext) -> Result<PlayerActor, ActorError> {
        match ctx.actor_id {
            ActorId::U64(player_id) => {
                Ok(PlayerActor::new(player_id, self.actor_ref_client.clone()))
            }
            other => Err(ActorError::new(format!(
                "unsupported player actor id {other:?}"
            ))),
        }
    }
}
