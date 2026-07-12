use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::BytesMut;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig};
use lattice_actor::traits::{Actor, Handler, Message};
use lattice_actor::{DecodeError, EncodeError, WireCodec, WireSchema, actor_protocol};
use lattice_core::actor_kind;
use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation, ProtocolId, RecipientRef};
use lattice_core::id::ActorId;
use lattice_remoting::{NodeIdentity, RemotingConfig};

use crate::{LatticeService, NodeConfig};

const PROTOCOL_ID: u64 = 0x7465_7374_0000_0001;

#[derive(Debug, Clone)]
struct Ping(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pong(u64);

impl Message for Ping {
    type Reply = Pong;
}

impl WireSchema for Ping {
    const SCHEMA_ID: u64 = 1;
    const SCHEMA_VERSION: u32 = 1;
}

impl WireSchema for Pong {
    const SCHEMA_ID: u64 = 2;
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Copy)]
struct PingCodec;

impl WireCodec<Ping> for PingCodec {
    const CODEC_ID: u64 = 1;
    const CODEC_VERSION: u32 = 1;

    fn encode(&self, value: &Ping, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<Ping, DecodeError> {
        Ok(Ping(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("Ping requires eight bytes"),
        )?)))
    }
}

#[derive(Clone, Copy)]
struct PongCodec;

impl WireCodec<Pong> for PongCodec {
    const CODEC_ID: u64 = 1;
    const CODEC_VERSION: u32 = 1;

    fn encode(&self, value: &Pong, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<Pong, DecodeError> {
        Ok(Pong(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("Pong requires eight bytes"),
        )?)))
    }
}

struct PingActor;

#[async_trait]
impl Actor for PingActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<Ping> for PingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        message: Ping,
    ) -> Result<Pong, ActorError> {
        Ok(Pong(message.0 + 1))
    }
}

actor_protocol! {
    PingProtocol for PingActor {
        protocol_id: PROTOCOL_ID;
        name: "service-test/ping/v1";
        ask 1 => Ping {
            request_codec: PingCodec,
            reply_codec: PongCodec,
        }
    }
}

fn node_config(
    cluster_id: ClusterId,
    node_id: &str,
    address: NodeAddress,
    incarnation: NodeIncarnation,
) -> NodeConfig {
    NodeConfig {
        cluster_id,
        node_id: node_id.to_owned(),
        address,
        incarnation,
        roles: BTreeSet::new(),
        remoting: RemotingConfig {
            heartbeat_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(2),
            ..RemotingConfig::default()
        },
        maximum_actor_protocols: 8,
        maximum_watches: 32,
        maximum_supervised_tasks: 32,
        shutdown_timeout: Duration::from_secs(2),
    }
}

#[tokio::test]
async fn typed_recipient_asks_exact_remote_activation_over_tcp() {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = probe.local_addr().unwrap().port();
    drop(probe);
    let cluster_id = ClusterId::new("service-test").unwrap();
    let client_address = NodeAddress::new("127.0.0.1", server_port - 1).unwrap();
    let server_address = NodeAddress::new("127.0.0.1", server_port).unwrap();
    let client_incarnation = NodeIncarnation::new(1).unwrap();
    let server_incarnation = NodeIncarnation::new(2).unwrap();
    let protocol = Arc::new(PingProtocol::build().unwrap());
    let registry = Arc::new(ActorRegistry::new(
        actor_kind!("Ping"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: server_address.clone(),
                node_incarnation: server_incarnation,
                protocol_id: ProtocolId::new(PROTOCOL_ID).unwrap(),
            }),
            ..ActorRegistryConfig::default()
        },
    ));
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let target = handle.actor_ref().unwrap().cast();
    let server = LatticeService::builder(node_config(
        cluster_id.clone(),
        "server",
        server_address.clone(),
        server_incarnation,
    ))
    .unwrap()
    .register_actor(registry, protocol.clone())
    .unwrap()
    .build()
    .unwrap();
    let client = LatticeService::builder(node_config(
        cluster_id.clone(),
        "client",
        client_address,
        client_incarnation,
    ))
    .unwrap()
    .build()
    .unwrap();
    server.start().await.unwrap();
    client
        .connect_peer(NodeIdentity {
            cluster_id,
            node_id: "server".to_owned(),
            address: server_address,
            incarnation: server_incarnation,
        })
        .await
        .unwrap();
    let recipient = client
        .recipient(RecipientRef::Actor(target), protocol)
        .unwrap();
    let reply = recipient
        .ask(Ping(41), Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(reply, Pong(42));
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
