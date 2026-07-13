use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::BytesMut;
use futures_util::Stream;
use lattice_actor::actor_protocol;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::protocol::CodecDescriptor;
use lattice_actor::protocol::DecodeError;
use lattice_actor::protocol::EncodeError;
use lattice_actor::protocol::WireCodec;
use lattice_actor::registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig};
use lattice_actor::reply::ReplyTo;
use lattice_actor::traits::{Actor, Request, Responder, StopReason};
use lattice_core::actor_kind;
use lattice_core::actor_ref::{ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId};
use lattice_core::id::ActorId;
use lattice_discovery::provider::{
    ClusterDiscovery, DiscoveryError, DiscoveryOrigin, DiscoverySnapshot, DiscoverySource,
    DiscoveryTarget,
};
use lattice_discovery::static_provider::{StaticDiscovery, StaticEndpoint};
use lattice_placement::control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter};
use lattice_placement::coordinator::MemberStatus;
use lattice_placement::runtime::{CoordinatorLeader, CoordinatorLeaderConfig};
use lattice_placement::storage::InMemoryPlacementStore;
use lattice_placement::types::{CoordinatorTerm, NodeKey};
use lattice_remoting::config::RemotingConfig;
use lattice_remoting::handshake::NodeIdentity;
use lattice_remoting::watch::WatchStatus;

use crate::builder::LatticeService;
use crate::config::ClusterJoinConfig;
use crate::config::NodeConfig;
use crate::lifecycle::ServiceLifecycleState;

const PROTOCOL_ID: u64 = 0x7465_7374_0000_0001;

#[derive(Debug, Clone)]
struct Ping(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pong(u64);

impl Request for Ping {
    type Response = Pong;
}

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

struct PingActor;

#[async_trait]
impl Actor for PingActor {
    type Error = ActorError;
}

#[async_trait]
impl Responder<Ping> for PingActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        request: Ping,
        reply_to: ReplyTo<Pong>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(Pong(request.0 + 1));
        Ok(())
    }
}

actor_protocol! {
    PingProtocol for PingActor {
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

struct WatchDiscovery {
    snapshots: tokio::sync::watch::Receiver<DiscoverySnapshot>,
}

impl ClusterDiscovery for WatchDiscovery {
    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<DiscoverySnapshot, DiscoveryError>> + Send + '_>> {
        let receiver = self.snapshots.clone();
        Box::pin(futures_util::stream::unfold(
            (receiver, true),
            |(mut receiver, first)| async move {
                if !first && receiver.changed().await.is_err() {
                    return None;
                }
                let snapshot = receiver.borrow_and_update().clone();
                Some((Ok(snapshot), (receiver, false)))
            },
        ))
    }
}

fn discovery_snapshot(generation: u64, node_id: &str, address: NodeAddress) -> DiscoverySnapshot {
    DiscoverySnapshot {
        generation,
        targets: vec![DiscoveryTarget {
            address,
            expected_node_id: Some(node_id.to_string()),
            source: DiscoverySource::single(DiscoveryOrigin::Static {
                name: "rollover-test".to_string(),
            }),
            priority: 1,
        }],
    }
}

async fn coordinator_service(
    store: Arc<InMemoryPlacementStore>,
    cluster_id: ClusterId,
    node_id: &str,
    address: NodeAddress,
    incarnation: NodeIncarnation,
    term: u64,
) -> LatticeService {
    let builder = LatticeService::builder(node_config(
        cluster_id,
        node_id,
        address.clone(),
        incarnation,
    ))
    .unwrap();
    let leader = CoordinatorLeader::elect(
        store,
        builder.association_manager(),
        NodeKey {
            node_id: node_id.to_string(),
            address,
            incarnation,
        },
        CoordinatorTerm::new(term).unwrap(),
        3,
        CoordinatorLeaderConfig {
            renewal_interval: Duration::from_millis(100),
            ..CoordinatorLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let (control, controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    builder
        .cluster_coordinator_runtime(Arc::new(control), leader, controls)
        .build()
        .unwrap()
}

#[tokio::test]
async fn typed_actor_ref_asks_exact_remote_activation_over_tcp() {
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
    let target: ActorRef<PingActor> = handle.actor_ref().unwrap().cast();
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
    .register_protocol(protocol)
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
    let reply = client
        .ask(&target, Ping(41), Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(reply, Pong(42));
    let watch_id = client.watch(&target).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if client.watch_status(watch_id) == WatchStatus::Active {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.stop(StopReason::Requested).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if client.watch_status(watch_id) == WatchStatus::Terminated {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn static_discovery_joins_and_leaves_without_manual_peer_connection() {
    let coordinator_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator_port = coordinator_listener.local_addr().unwrap().port();
    drop(coordinator_listener);
    let member_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let member_port = member_listener.local_addr().unwrap().port();
    drop(member_listener);

    let cluster_id = ClusterId::new("service-join-test").unwrap();
    let coordinator_address = NodeAddress::new("127.0.0.1", coordinator_port).unwrap();
    let coordinator_incarnation = NodeIncarnation::new(101).unwrap();
    let coordinator_builder = LatticeService::builder(node_config(
        cluster_id.clone(),
        "coordinator",
        coordinator_address.clone(),
        coordinator_incarnation,
    ))
    .unwrap();
    let associations = coordinator_builder.association_manager();
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let leader = CoordinatorLeader::elect(
        store,
        associations,
        NodeKey {
            node_id: "coordinator".to_string(),
            address: coordinator_address.clone(),
            incarnation: coordinator_incarnation,
        },
        CoordinatorTerm::new(1).unwrap(),
        3,
        CoordinatorLeaderConfig {
            renewal_interval: Duration::from_millis(100),
            ..CoordinatorLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let (coordinator_control, coordinator_controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let coordinator = coordinator_builder
        .cluster_coordinator_runtime(Arc::new(coordinator_control), leader, coordinator_controls)
        .build()
        .unwrap();
    coordinator.start().await.unwrap();

    let discovery = Arc::new(
        StaticDiscovery::new(
            "test",
            vec![StaticEndpoint {
                address: coordinator_address,
                expected_node_id: Some("coordinator".to_string()),
                priority: 1,
            }],
        )
        .unwrap(),
    );
    let join_config = ClusterJoinConfig {
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(100),
        join_timeout: Some(Duration::from_secs(5)),
        leave_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(3),
        ..ClusterJoinConfig::default()
    };
    let member = LatticeService::builder(node_config(
        cluster_id,
        "member",
        NodeAddress::new("127.0.0.1", member_port).unwrap(),
        NodeIncarnation::new(202).unwrap(),
    ))
    .unwrap()
    .cluster_discovery(discovery)
    .join_config(join_config)
    .member_event_capacity(64)
    .build()
    .unwrap();
    member.start().await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        let mut lifecycle = member.subscribe_lifecycle();
        while *lifecycle.borrow() != ServiceLifecycleState::Ready {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let snapshot = member.member_snapshot();
    assert!(snapshot.members.iter().any(|record| {
        record.node.node_id == "member"
            && record.node.incarnation == NodeIncarnation::new(202).unwrap()
            && record.status == MemberStatus::Up
    }));

    member
        .leave(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(member.lifecycle_state(), ServiceLifecycleState::Terminated);
    coordinator.shutdown().await.unwrap();
}

async fn unused_address() -> NodeAddress {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    NodeAddress::new("127.0.0.1", port).unwrap()
}

#[tokio::test]
async fn two_discovered_members_leave_sequentially_without_losing_coordinator_session() {
    let coordinator_address = unused_address().await;
    let first_address = unused_address().await;
    let second_address = unused_address().await;
    let cluster_id = ClusterId::new("service-multi-member-test").unwrap();
    let store = Arc::new(InMemoryPlacementStore::new(64, 64).unwrap());
    let coordinator = coordinator_service(
        store,
        cluster_id.clone(),
        "coordinator",
        coordinator_address.clone(),
        NodeIncarnation::new(301).unwrap(),
        1,
    )
    .await;
    coordinator.start().await.unwrap();
    let discovery = || {
        Arc::new(
            StaticDiscovery::new(
                "multi-member",
                vec![StaticEndpoint {
                    address: coordinator_address.clone(),
                    expected_node_id: Some("coordinator".to_owned()),
                    priority: 1,
                }],
            )
            .unwrap(),
        )
    };
    let member = |node_id: &str, address: NodeAddress, incarnation: u128| {
        LatticeService::builder(node_config(
            cluster_id.clone(),
            node_id,
            address,
            NodeIncarnation::new(incarnation).unwrap(),
        ))
        .unwrap()
        .cluster_discovery(discovery())
        .join_config(ClusterJoinConfig {
            retry_initial: Duration::from_millis(10),
            retry_max: Duration::from_millis(100),
            join_timeout: Some(Duration::from_secs(5)),
            leave_timeout: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(3),
            ..ClusterJoinConfig::default()
        })
        .member_event_capacity(64)
        .build()
        .unwrap()
    };
    let first = member("first", first_address, 401);
    let second = member("second", second_address, 402);
    first.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut lifecycle = first.subscribe_lifecycle();
        while *lifecycle.borrow() != ServiceLifecycleState::Ready {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    second.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut lifecycle = second.subscribe_lifecycle();
        while *lifecycle.borrow() != ServiceLifecycleState::Ready {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    first
        .leave(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    second
        .leave(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn coordinator_rollover_requires_reconciliation_before_ready() {
    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_a = listener_a.local_addr().unwrap().port();
    drop(listener_a);
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_b = listener_b.local_addr().unwrap().port();
    drop(listener_b);
    let member_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let member_port = member_listener.local_addr().unwrap().port();
    drop(member_listener);

    let cluster_id = ClusterId::new("service-rollover-test").unwrap();
    let address_a = NodeAddress::new("127.0.0.1", port_a).unwrap();
    let address_b = NodeAddress::new("127.0.0.1", port_b).unwrap();
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let coordinator_a = coordinator_service(
        store.clone(),
        cluster_id.clone(),
        "coordinator-a",
        address_a.clone(),
        NodeIncarnation::new(301).unwrap(),
        1,
    )
    .await;
    coordinator_a.start().await.unwrap();

    let (discovery_tx, discovery_rx) =
        tokio::sync::watch::channel(discovery_snapshot(1, "coordinator-a", address_a));
    let member = LatticeService::builder(node_config(
        cluster_id.clone(),
        "rollover-member",
        NodeAddress::new("127.0.0.1", member_port).unwrap(),
        NodeIncarnation::new(303).unwrap(),
    ))
    .unwrap()
    .cluster_discovery(Arc::new(WatchDiscovery {
        snapshots: discovery_rx,
    }))
    .join_config(ClusterJoinConfig {
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(100),
        join_timeout: Some(Duration::from_secs(5)),
        leave_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(3),
        ..ClusterJoinConfig::default()
    })
    .build()
    .unwrap();
    member.start().await.unwrap();
    let mut lifecycle = member.subscribe_lifecycle();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *lifecycle.borrow() != ServiceLifecycleState::Ready {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    coordinator_a.force_shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *lifecycle.borrow() != ServiceLifecycleState::Degraded {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    let coordinator_b = coordinator_service(
        store,
        cluster_id,
        "coordinator-b",
        address_b.clone(),
        NodeIncarnation::new(302).unwrap(),
        2,
    )
    .await;
    coordinator_b.start().await.unwrap();
    discovery_tx
        .send(discovery_snapshot(2, "coordinator-b", address_b))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *lifecycle.borrow() != ServiceLifecycleState::Ready {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let members = member.member_snapshot().members;
    assert_eq!(
        members
            .iter()
            .filter(|record| record.node.node_id == "rollover-member")
            .count(),
        1
    );

    member
        .leave(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    coordinator_b.shutdown().await.unwrap();
}
