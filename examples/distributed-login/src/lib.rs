#![cfg_attr(not(test), deny(clippy::wildcard_imports))]
use lattice_actor::context::HandlerContext;

use std::{
    collections::BTreeSet, error::Error as StdError, io::Error as IoError, sync::Arc,
    time::Duration,
};

use lattice_actor::{
    actor_protocol,
    error::ActorError,
    protocol::ProstCodec,
    registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig},
    reply::ReplyTo,
    traits::{Actor, Responder},
};
use lattice_core::{
    actor_kind,
    actor_ref::{ActorRef, ClusterId, NodeAddress, NodeIncarnation},
    id::ActorId,
};
use lattice_remoting::config::RemotingConfig;
use lattice_service::{builder::LatticeService, config::NodeConfig};

pub mod lattice {
    pub mod actor {
        include!(concat!(env!("OUT_DIR"), "/lattice.actor.rs"));
    }
}

pub mod game {
    include!(concat!(env!("OUT_DIR"), "/game.rs"));
}

use game::{InitSessionReply, InitSessionRequest, LoginAcceptedReply, LoginRequest};

pub const WORLD_PROTOCOL_ID: u64 = 0x6761_6d65_0000_0001;
pub const PLAYER_PROTOCOL_ID: u64 = 0x6761_6d65_0000_0002;

#[derive(Debug)]
pub struct WorldActor {
    pub world_id: u64,
    pub sessions: u64,
}

impl Actor for WorldActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Responder<LoginRequest> for WorldActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: LoginRequest,
        reply_to: ReplyTo<LoginAcceptedReply>,
    ) -> Result<(), ActorError> {
        let accepted = request.world_id == self.world_id && !request.token.is_empty();
        if accepted {
            self.sessions += 1;
        }
        let _ = reply_to.send(LoginAcceptedReply {
            accepted,
            message: if accepted { "accepted" } else { "rejected" }.to_owned(),
        });
        Ok(())
    }
}

#[derive(Debug)]
pub struct PlayerActor {
    pub player_id: u64,
    pub sessions: u64,
}

impl Actor for PlayerActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Responder<InitSessionRequest> for PlayerActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: InitSessionRequest,
        reply_to: ReplyTo<InitSessionReply>,
    ) -> Result<(), ActorError> {
        let ok = request.player_id == self.player_id && !request.session_id.is_empty();
        if ok {
            self.sessions += 1;
        }
        let _ = reply_to.send(InitSessionReply {
            ok,
            player_id: self.player_id,
            message: if ok { "initialized" } else { "rejected" }.to_owned(),
        });
        Ok(())
    }
}

actor_protocol! {
    pub WorldProtocol {
        protocol_id: WORLD_PROTOCOL_ID;
        name: "distributed-login/world/v1";
        ask 1 => LoginRequest {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: ProstCodec,
            response_codec: ProstCodec,
        }
    }
}

actor_protocol! {
    pub PlayerProtocol {
        protocol_id: PLAYER_PROTOCOL_ID;
        name: "distributed-login/player/v1";
        ask 1 => InitSessionRequest {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: ProstCodec,
            response_codec: ProstCodec,
        }
    }
}

pub async fn run_demo() -> Result<LoginAcceptedReply, Box<dyn StdError>> {
    let cluster_id = ClusterId::new("distributed-login")?;
    let address = NodeAddress::new("127.0.0.1", 25530)?;
    let incarnation = NodeIncarnation::generate();
    let protocol = Arc::new(WorldProtocol::bind::<WorldActor>()?);
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("World"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: address.clone(),
                node_incarnation: incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        protocol.as_ref(),
    ));
    let handle = registry
        .start(
            ActorId::U64(7),
            WorldActor {
                world_id: 7,
                sessions: 0,
            },
        )
        .await?;
    let actor_ref: ActorRef<WorldProtocol> = handle
        .typed_actor_ref()?
        .ok_or_else(|| IoError::other("missing exact World ActorRef"))?;
    let service = LatticeService::builder(NodeConfig {
        cluster_id,
        node_id: "world-a".to_owned(),
        address,
        incarnation,
        roles: BTreeSet::from(["world".to_owned()]),
        remoting: RemotingConfig::default(),
        maximum_actor_protocols: 32,
        maximum_watches: 1024,
        maximum_supervised_tasks: 1024,
        shutdown_timeout: Duration::from_secs(5),
    })?
    .register_actor(registry, protocol)?
    .build()?;
    service.start().await?;
    let reply = service
        .ask(
            &actor_ref,
            LoginRequest {
                world_id: 7,
                player_id: 42,
                token: "demo-token".to_owned(),
                gateway_session: None,
            },
            Duration::from_secs(1),
        )
        .await?;
    service.shutdown().await?;
    Ok(reply)
}
