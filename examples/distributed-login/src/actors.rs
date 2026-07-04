use std::collections::HashMap;

use async_trait::async_trait;
use lattice_actor::{Actor, ActorContext, ActorCreateContext, ActorError, ActorLoader, Handler};
use lattice_core::{ActorId, ActorRef};
use lattice_gateway::ClientFrame;
use lattice_rpc::{ActorRefRpcClient, Rpc};
use prost::Message as ProstMessage;
use tokio::sync::{mpsc, oneshot};

use crate::LOGIN_MSG_ID;
use crate::game::{
    InitSessionReply, InitSessionRequest, LoginAcceptedReply, LoginReply, LoginRequest,
    PlayerPingReply, PlayerPingRequest, PushToClientReply, PushToClientRequest, WorldPingReply,
    WorldPingRequest,
};
use crate::generated::PlayerRpcClient;
use crate::placement::{DemoActorRefRpcCore, DemoRpcCore};

pub struct WorldActor {
    world_id: u64,
    sessions: HashMap<u64, String>,
    player_client: PlayerRpcClient<DemoRpcCore>,
}

impl WorldActor {
    pub fn new(world_id: u64, player_client: PlayerRpcClient<DemoRpcCore>) -> Self {
        Self {
            world_id,
            sessions: HashMap::new(),
            player_client,
        }
    }
}

#[async_trait]
impl Actor for WorldActor {}

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
pub struct PlayerLoader {
    actor_ref_client: ActorRefRpcClient<DemoActorRefRpcCore>,
}

impl PlayerLoader {
    pub fn new(actor_ref_client: ActorRefRpcClient<DemoActorRefRpcCore>) -> Self {
        Self { actor_ref_client }
    }
}

#[async_trait]
impl ActorLoader<PlayerActor> for PlayerLoader {
    async fn load(&self, ctx: ActorCreateContext) -> Result<PlayerActor, ActorError> {
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

pub struct GatewaySessionActor {
    session_id: String,
    tx: mpsc::Sender<ClientFrame>,
    self_ref_tx: Option<oneshot::Sender<ActorRef>>,
}

impl GatewaySessionActor {
    pub fn new(
        session_id: String,
        tx: mpsc::Sender<ClientFrame>,
        self_ref_tx: oneshot::Sender<ActorRef>,
    ) -> Self {
        Self {
            session_id,
            tx,
            self_ref_tx: Some(self_ref_tx),
        }
    }
}

#[async_trait]
impl Actor for GatewaySessionActor {
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(self_ref_tx) = self.self_ref_tx.take() {
            let _ = self_ref_tx.send(ctx.require_self_ref()?.clone());
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Rpc<PushToClientRequest>> for GatewaySessionActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<PushToClientRequest>,
    ) -> Result<PushToClientReply, ActorError> {
        if msg.req.session_id != self.session_id {
            return Ok(PushToClientReply {
                ok: false,
                message: format!(
                    "gateway session actor {} cannot serve {}",
                    self.session_id, msg.req.session_id
                ),
            });
        }

        self.tx
            .send(ClientFrame {
                msg_id: msg.req.msg_id,
                payload: msg.req.payload,
            })
            .await
            .map_err(|_| ActorError::new("client writer task is closed"))?;
        Ok(PushToClientReply {
            ok: true,
            message: "pushed by gateway session actor".to_string(),
        })
    }
}
