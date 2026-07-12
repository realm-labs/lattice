use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig};
use lattice_actor::traits::{Actor, Handler, Message};
use lattice_actor::{ProstCodec, WireSchema, actor_protocol};
use lattice_core::actor_kind;
use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation, ProtocolId, RecipientRef};
use lattice_core::id::ActorId;
use lattice_remoting::RemotingConfig;
use lattice_service::{LatticeService, NodeConfig};

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

macro_rules! message_schema {
    ($message:ty, $reply:ty, $message_id:expr, $reply_id:expr) => {
        impl Message for $message {
            type Reply = $reply;
        }
        impl WireSchema for $message {
            const SCHEMA_ID: u64 = $message_id;
            const SCHEMA_VERSION: u32 = 1;
        }
        impl WireSchema for $reply {
            const SCHEMA_ID: u64 = $reply_id;
            const SCHEMA_VERSION: u32 = 1;
        }
    };
}

message_schema!(
    LoginRequest,
    LoginAcceptedReply,
    0x6761_6d65_0000_0101,
    0x6761_6d65_0000_0102
);
message_schema!(
    InitSessionRequest,
    InitSessionReply,
    0x6761_6d65_0000_0201,
    0x6761_6d65_0000_0202
);

#[derive(Debug)]
pub struct WorldActor {
    pub world_id: u64,
    pub sessions: u64,
}

#[async_trait]
impl Actor for WorldActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<LoginRequest> for WorldActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        request: LoginRequest,
    ) -> Result<LoginAcceptedReply, ActorError> {
        let accepted = request.world_id == self.world_id && !request.token.is_empty();
        if accepted {
            self.sessions += 1;
        }
        Ok(LoginAcceptedReply {
            accepted,
            message: if accepted { "accepted" } else { "rejected" }.to_owned(),
        })
    }
}

#[derive(Debug)]
pub struct PlayerActor {
    pub player_id: u64,
    pub sessions: u64,
}

#[async_trait]
impl Actor for PlayerActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<InitSessionRequest> for PlayerActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        request: InitSessionRequest,
    ) -> Result<InitSessionReply, ActorError> {
        let ok = request.player_id == self.player_id && !request.session_id.is_empty();
        if ok {
            self.sessions += 1;
        }
        Ok(InitSessionReply {
            ok,
            player_id: self.player_id,
            message: if ok { "initialized" } else { "rejected" }.to_owned(),
        })
    }
}

actor_protocol! {
    pub WorldProtocol for WorldActor {
        protocol_id: WORLD_PROTOCOL_ID;
        name: "distributed-login/world/v1";
        ask 1 => LoginRequest {
            request_codec: ProstCodec,
            reply_codec: ProstCodec,
        }
    }
}

actor_protocol! {
    pub PlayerProtocol for PlayerActor {
        protocol_id: PLAYER_PROTOCOL_ID;
        name: "distributed-login/player/v1";
        ask 1 => InitSessionRequest {
            request_codec: ProstCodec,
            reply_codec: ProstCodec,
        }
    }
}

pub async fn run_demo() -> Result<LoginAcceptedReply, Box<dyn std::error::Error>> {
    let cluster_id = ClusterId::new("distributed-login")?;
    let address = NodeAddress::new("127.0.0.1", 25530)?;
    let incarnation = NodeIncarnation::generate();
    let protocol = Arc::new(WorldProtocol::build()?);
    let registry = Arc::new(ActorRegistry::new(
        actor_kind!("World"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: address.clone(),
                node_incarnation: incarnation,
                protocol_id: ProtocolId::new(WORLD_PROTOCOL_ID)?,
            }),
            ..ActorRegistryConfig::default()
        },
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
    let actor_ref = handle
        .actor_ref()
        .ok_or_else(|| std::io::Error::other("missing exact World ActorRef"))?
        .cast();
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
    .register_actor(registry, protocol.clone())?
    .build()?;
    let recipient = service.recipient(RecipientRef::Actor(actor_ref), protocol)?;
    let reply = recipient
        .ask(
            LoginRequest {
                world_id: 7,
                player_id: 42,
                token: "demo-token".to_owned(),
                gateway_session: None,
            },
            Instant::now() + Duration::from_secs(1),
        )
        .await?;
    service.shutdown().await?;
    Ok(reply)
}
