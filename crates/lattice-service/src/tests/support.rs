//! Fixtures shared by the `lattice-service` node-level test modules.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::BytesMut;
use lattice_actor::{
    actor_protocol,
    context::HandlerContext,
    error::ActorError,
    protocol::{CodecDescriptor, DecodeError, EncodeError, WireCodec},
    registry::{ActorCreateContext, ActorLoader},
    reply::ReplyTo,
    traits::{Actor, Responder},
};
use lattice_core::actor_ref::{
    ClusterId, EntityType, NodeAddress, NodeIncarnation, PlacementDomainId,
};
use lattice_placement::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter},
    runtime::{
        PlacementDomainLeaderConfig,
        host::{CoordinatorHost, CoordinatorHostConfig},
    },
    storage::InMemoryPlacementStore,
    types::NodeKey,
};
use lattice_remoting::config::RemotingConfig;

use crate::{builder::LatticeService, config::NodeConfig, registration::EntityOptions};

pub(super) const PROTOCOL_ID: u64 = 0x7465_7374_0000_0001;

pub(super) fn placement_domain() -> PlacementDomainId {
    PlacementDomainId::new("service-test").unwrap()
}

pub(super) fn secondary_domain() -> PlacementDomainId {
    PlacementDomainId::new("service-secondary").unwrap()
}

pub(super) fn proxy_options(domain: PlacementDomainId, name: &str) -> EntityOptions {
    EntityOptions::new(domain, EntityType::new(name).unwrap(), 1)
}

#[derive(Debug, Clone, lattice_actor::Request)]
#[request(response = Pong)]
pub(super) struct Ping(pub(super) u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Pong(pub(super) u64);

#[derive(Clone, Copy)]
struct PingCodec;

impl WireCodec<Ping> for PingCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

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
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

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

pub(super) struct PingActor;

#[derive(Clone, Copy)]
pub(super) struct PingLoader;

#[async_trait]
impl ActorLoader<PingActor> for PingLoader {
    async fn load(&self, _ctx: ActorCreateContext) -> Result<PingActor, ActorError> {
        Ok(PingActor)
    }
}

impl Actor for PingActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Responder<Ping> for PingActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: Ping,
        reply_to: ReplyTo<Pong>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(Pong(request.0 + 1));
        Ok(())
    }
}

actor_protocol! {
    pub(super) PingProtocol {
        protocol_id: PROTOCOL_ID;
        name: "service-test/ping/v1";
        ask 1 => Ping {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: PingCodec,
            response_codec: PongCodec,
        }
    }
}

actor_protocol! {
    pub(super) OtherPingProtocol {
        protocol_id: PROTOCOL_ID + 1;
        name: "service-test/other-ping/v1";
        ask 1 => Ping {
            request_schema_version: 1,
            response_schema_version: 1,
            request_codec: PingCodec,
            response_codec: PongCodec,
        }
    }
}

pub(super) fn node_config(
    cluster_id: ClusterId,
    node_id: &str,
    address: NodeAddress,
    incarnation: NodeIncarnation,
) -> NodeConfig {
    NodeConfig {
        release: lattice_core::release::ReleaseManifest::development(1),
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

pub(super) async fn coordinator_service(
    store: Arc<InMemoryPlacementStore>,
    cluster_id: ClusterId,
    node_id: &str,
    address: NodeAddress,
    incarnation: NodeIncarnation,
    _term: u64,
) -> LatticeService {
    coordinator_service_for_domains(
        store,
        cluster_id,
        node_id,
        address,
        incarnation,
        BTreeSet::from([placement_domain()]),
    )
    .await
}

pub(super) async fn coordinator_service_for_domains(
    store: Arc<InMemoryPlacementStore>,
    cluster_id: ClusterId,
    node_id: &str,
    address: NodeAddress,
    incarnation: NodeIncarnation,
    domains: BTreeSet<PlacementDomainId>,
) -> LatticeService {
    let builder = LatticeService::builder(node_config(
        cluster_id,
        node_id,
        address.clone(),
        incarnation,
    ))
    .unwrap();
    let host = CoordinatorHost::elect(
        store,
        builder.association_manager(),
        NodeKey {
            node_id: node_id.to_string(),
            address,
            incarnation,
        },
        domains,
        CoordinatorHostConfig {
            placement: PlacementDomainLeaderConfig {
                renewal_interval: Duration::from_millis(100),
                ..PlacementDomainLeaderConfig::default()
            },
            ..CoordinatorHostConfig::default()
        },
    )
    .await
    .unwrap();
    let (control, controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    builder
        .coordinator_host(Arc::new(control), host, controls)
        .build()
        .unwrap()
}
