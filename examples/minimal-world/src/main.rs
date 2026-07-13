#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lattice_actor::actor_protocol;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::protocol::ProstCodec;
use lattice_actor::registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig};
use lattice_actor::reply::ReplyTo;
use lattice_actor::traits::{Actor, Request, Responder};
use lattice_config::source::ConfigSource;
use lattice_core::actor_ref::{ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId};
use lattice_core::id::ActorId;
use lattice_core::{actor_kind, service_kind};
use lattice_remoting::config::RemotingConfig;
use lattice_service::builder::LatticeService;
use lattice_service::config::NodeConfig;
use serde::Deserialize;

pub mod world {
    include!(concat!(env!("OUT_DIR"), "/world.rs"));
}

use world::{EnterWorldReply, EnterWorldRequest};

impl Request for EnterWorldRequest {
    type Response = EnterWorldReply;
}

#[derive(Debug)]
struct WorldActor {
    world_id: u64,
    players: HashSet<u64>,
}

#[async_trait]
impl Actor for WorldActor {
    type Error = ActorError;
}

#[async_trait]
impl Responder<EnterWorldRequest> for WorldActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        request: EnterWorldRequest,
        reply_to: ReplyTo<EnterWorldReply>,
    ) -> Result<(), ActorError> {
        let ok = request.world_id == self.world_id;
        if ok {
            self.players.insert(request.player_id);
        }
        let _ = reply_to.send(EnterWorldReply {
            ok,
            player_count: self.players.len() as u64,
        });
        Ok(())
    }
}

actor_protocol! {
    pub WorldProtocol for WorldActor {
        protocol_id: 0x776f_726c_6400_0001;
        name: "minimal-world/v1";
        ask 1 => EnterWorldRequest {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: ProstCodec,
            response_codec: ProstCodec,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WorldConfig {
    mailbox_capacity: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: WorldConfig =
        ConfigSource::file("examples/minimal-world/config/world-service.toml")
            .load()?
            .section("world")?;
    let cluster_id = ClusterId::new("minimal-world")?;
    let address = NodeAddress::new("127.0.0.1", 25520)?;
    let incarnation = NodeIncarnation::generate();
    let protocol = Arc::new(WorldProtocol::build()?);
    let registry = Arc::new(ActorRegistry::new(
        actor_kind!("World"),
        ActorRegistryConfig {
            mailbox: MailboxConfig::bounded(config.mailbox_capacity),
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: address.clone(),
                node_incarnation: incarnation,
                protocol_id: ProtocolId::new(0x776f_726c_6400_0001)?,
            }),
            ..ActorRegistryConfig::default()
        },
    ));
    let handle = registry
        .start(
            ActorId::U64(1),
            WorldActor {
                world_id: 1,
                players: HashSet::new(),
            },
        )
        .await?;
    let actor_ref: ActorRef<WorldActor> = handle
        .actor_ref()
        .ok_or_else(|| std::io::Error::other("registry did not create an ActorRef"))?
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
    .register_actor(registry, protocol)?
    .build()?;
    let reply = service
        .ask(
            &actor_ref,
            EnterWorldRequest {
                world_id: 1,
                player_id: 1001,
            },
            Instant::now() + Duration::from_secs(1),
        )
        .await?;
    service.shutdown().await?;

    println!(
        "{}:{} accepted={} players={}",
        service_kind!("World"),
        actor_kind!("World"),
        reply.ok,
        reply.player_count
    );
    Ok(())
}
