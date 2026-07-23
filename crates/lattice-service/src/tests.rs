use lattice_actor::context::HandlerContext;
use std::{
    collections::BTreeSet,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc as std_mpsc,
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::BytesMut;
use futures_util::Stream;
use lattice_actor::{
    actor_protocol,
    context::ActorContext,
    error::{ActorError, ActorStopError},
    protocol::{CodecDescriptor, DecodeError, EncodeError, WireCodec},
    recipient::ProtocolRegistrationError,
    registry::{
        ActorCreateContext, ActorLoader, ActorRefConfig, ActorRegistry, ActorRegistryConfig,
    },
    reply::ReplyTo,
    traits::{Actor, ActorLifecycleState, Responder, StopReason},
};
use lattice_core::{
    actor_kind,
    actor_ref::{
        ActorRef, ClusterId, EntityId, EntityRef, EntityType, NodeAddress, NodeIncarnation,
        PlacementDomainId, ProtocolId,
    },
    coordinator::CoordinatorScope,
    failpoint::Failpoint,
    id::ActorId,
};
use lattice_discovery::{
    provider::{
        CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
        DiscoverySource, DiscoveryTarget,
    },
    static_provider::{StaticDiscovery, StaticEndpoint},
};
use lattice_placement::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter},
    coordinator::MemberStatus,
    region::EntityConfig,
    runtime::{
        PlacementDomainLeaderConfig,
        host::{CoordinatorHost, CoordinatorHostConfig},
    },
    storage::{InMemoryPlacementStore, MembershipStore},
    types::NodeKey,
};
use lattice_remoting::{config::RemotingConfig, handshake::NodeIdentity, watch::WatchStatus};
use tokio::{sync::watch::Receiver, time::Instant};

use crate::{
    builder::LatticeService,
    cluster::ClusterEvent,
    config::{ClusterJoinConfig, NodeConfig},
    error::ServiceError,
    lifecycle::{NodeLifecycleState, PlacementDomainState},
    registration::EntityOptions,
    test_support::{network_test_guard, unused_address},
};

mod remote_tell;

const PROTOCOL_ID: u64 = 0x7465_7374_0000_0001;

fn placement_domain() -> PlacementDomainId {
    PlacementDomainId::new("service-test").unwrap()
}

fn secondary_domain() -> PlacementDomainId {
    PlacementDomainId::new("service-secondary").unwrap()
}

fn proxy_options(domain: PlacementDomainId, name: &str) -> EntityOptions {
    EntityOptions::new(domain, EntityType::new(name).unwrap(), 1)
}

#[derive(Debug, Clone, lattice_actor::Request)]
#[request(response = Pong)]
struct Ping(u64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pong(u64);

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

#[derive(Clone, Copy)]
struct PingLoader;

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
    PingProtocol {
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
    OtherPingProtocol {
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

#[test]
fn actor_registration_rejects_a_registry_bound_to_another_protocol() {
    let ping = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let other = OtherPingProtocol::bind::<PingActor>().unwrap();
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Ping"),
        ActorRegistryConfig::default(),
        &other,
    ));
    let config = node_config(
        ClusterId::new("service-test").unwrap(),
        "protocol-mismatch",
        NodeAddress::new("127.0.0.1", 25250).unwrap(),
        NodeIncarnation::new(1).unwrap(),
    );

    let result = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry, ping);

    assert!(matches!(
        result,
        Err(ServiceError::ProtocolRegistration(
            ProtocolRegistrationError::RegistryProtocolMismatch { .. }
        ))
    ));
}

#[tokio::test]
async fn force_shutdown_forces_retained_actor_before_publishing_terminated() {
    let _network = network_test_guard().await;
    struct ForceShutdownActor {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for ForceShutdownActor {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Actor for ForceShutdownActor {
        type Error = ActorError;
        type Behavior = ::lattice_actor::state_machine::Stateless;

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            Err(ActorStopError::new("store unavailable"))
        }
    }

    impl Responder<Ping> for ForceShutdownActor {
        async fn respond(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            request: Ping,
            reply_to: ReplyTo<Pong>,
        ) -> Result<(), ActorError> {
            let _ = reply_to.send(Pong(request.0));
            Ok(())
        }
    }

    let binding = Arc::new(PingProtocol::bind::<ForceShutdownActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("ForceShutdownActor"),
        ActorRegistryConfig::default(),
        binding.as_ref(),
    ));
    let dropped = Arc::new(AtomicUsize::new(0));
    let handle = registry
        .start(
            ActorId::U64(1),
            ForceShutdownActor {
                dropped: dropped.clone(),
            },
        )
        .await
        .unwrap();
    let mut data_loss = handle.subscribe_forced_data_loss();
    let config = node_config(
        ClusterId::new("force-shutdown-test").unwrap(),
        "force-shutdown",
        NodeAddress::new("127.0.0.1", 25251).unwrap(),
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry.clone(), binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();

    let mut lifecycle = handle.subscribe_lifecycle();
    handle.stop(StopReason::Requested).await.unwrap();
    while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
        lifecycle.changed().await.unwrap();
    }
    let retained = service.retained_actor_cells();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].local_ref, handle.local_ref());
    assert!(retained[0].stop_failure.is_some());

    service.force_shutdown().await.unwrap();

    assert_eq!(
        service.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
    assert!(registry.live_cells().is_empty());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let event = tokio::time::timeout(Duration::from_secs(1), data_loss.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.reason, "service force shutdown");
    assert!(event.ticket.starts_with("force-shutdown-"));
}

#[tokio::test]
async fn terminal_shutdown_drains_local_actors_without_a_migration_target() {
    let _network = network_test_guard().await;
    let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("TerminalShutdownActor"),
        ActorRegistryConfig::default(),
        binding.as_ref(),
    ));
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let config = node_config(
        ClusterId::new("terminal-shutdown-test").unwrap(),
        "terminal-shutdown",
        NodeAddress::new("127.0.0.1", 25253).unwrap(),
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry.clone(), binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();

    service.terminal_shutdown().await.unwrap();

    assert_eq!(
        service.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
    assert!(registry.live_cells().is_empty());
}

#[tokio::test]
async fn service_retry_api_resolves_retained_actor_cell() {
    let _network = network_test_guard().await;
    struct RetryShutdownActor {
        persistence_available: Arc<AtomicBool>,
    }

    impl Actor for RetryShutdownActor {
        type Error = ActorError;
        type Behavior = ::lattice_actor::state_machine::Stateless;

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            self.persistence_available
                .load(Ordering::SeqCst)
                .then_some(())
                .ok_or_else(|| ActorStopError::new("store unavailable"))
        }
    }

    impl Responder<Ping> for RetryShutdownActor {
        async fn respond(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            request: Ping,
            reply_to: ReplyTo<Pong>,
        ) -> Result<(), ActorError> {
            let _ = reply_to.send(Pong(request.0));
            Ok(())
        }
    }

    let binding = Arc::new(PingProtocol::bind::<RetryShutdownActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("RetryShutdownActor"),
        ActorRegistryConfig::default(),
        binding.as_ref(),
    ));
    let persistence_available = Arc::new(AtomicBool::new(false));
    let handle = registry
        .start(
            ActorId::U64(1),
            RetryShutdownActor {
                persistence_available: persistence_available.clone(),
            },
        )
        .await
        .unwrap();
    let config = node_config(
        ClusterId::new("retry-shutdown-test").unwrap(),
        "retry-shutdown",
        NodeAddress::new("127.0.0.1", 25252).unwrap(),
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry, binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();

    let mut lifecycle = handle.subscribe_lifecycle();
    handle.stop(StopReason::Requested).await.unwrap();
    while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
        lifecycle.changed().await.unwrap();
    }
    persistence_available.store(true, Ordering::SeqCst);
    service.retry_actor_stop(handle.local_ref()).await.unwrap();

    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
    assert!(service.retained_actor_cells().is_empty());
    service.shutdown().await.unwrap();
}

struct WatchDiscovery {
    scope: CoordinatorScope,
    snapshots: Receiver<CoordinatorDirectorySnapshot>,
}

impl CoordinatorDiscovery for WatchDiscovery {
    fn scope(&self) -> &CoordinatorScope {
        &self.scope
    }

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
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

fn discovery_snapshot(
    generation: u64,
    node_id: &str,
    address: NodeAddress,
) -> CoordinatorDirectorySnapshot {
    CoordinatorDirectorySnapshot {
        scope: CoordinatorScope::Placement(placement_domain()),
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

async fn coordinator_service_for_domains(
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

#[tokio::test]
async fn typed_actor_ref_asks_exact_remote_activation_over_tcp() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("service-test").unwrap();
    let first_address = unused_address().await;
    let second_address = unused_address().await;
    let (client_address, server_address) = if first_address < second_address {
        (first_address, second_address)
    } else {
        (second_address, first_address)
    };
    let client_incarnation = NodeIncarnation::new(1).unwrap();
    let server_incarnation = NodeIncarnation::new(2).unwrap();
    let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Ping"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: server_address.clone(),
                node_incarnation: server_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        binding.as_ref(),
    ));
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let target: ActorRef<PingProtocol> = handle.typed_actor_ref().unwrap().unwrap();
    let server = LatticeService::builder(node_config(
        cluster_id.clone(),
        "server",
        server_address.clone(),
        server_incarnation,
    ))
    .unwrap()
    .register_actor(registry, binding)
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
    .use_protocol::<PingProtocol>()
    .unwrap()
    .build()
    .unwrap();
    server.start().await.unwrap();
    client.start().await.unwrap();
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
        .ask(&target, Ping(41), Duration::from_secs(1))
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
    let _network = network_test_guard().await;

    let cluster_id = ClusterId::new("service-join-test").unwrap();
    let coordinator_address = unused_address().await;
    let member_address = unused_address().await;
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
    let host = CoordinatorHost::elect(
        store.clone(),
        associations,
        NodeKey {
            node_id: "coordinator".to_string(),
            address: coordinator_address.clone(),
            incarnation: coordinator_incarnation,
        },
        BTreeSet::from([placement_domain()]),
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
    let (coordinator_control, coordinator_controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let coordinator = coordinator_builder
        .coordinator_host(Arc::new(coordinator_control), host, coordinator_controls)
        .build()
        .unwrap();
    coordinator.start().await.unwrap();

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
        member_address,
        NodeIncarnation::new(202).unwrap(),
    ))
    .unwrap()
    .coordinator_discovery(Arc::new(
        StaticDiscovery::new(
            CoordinatorScope::Membership,
            "test-membership",
            vec![StaticEndpoint {
                address: coordinator_address,
                expected_node_id: Some("coordinator".to_string()),
                priority: 1,
            }],
        )
        .unwrap(),
    ))
    .unwrap()
    .join_config(join_config)
    .member_event_capacity(64)
    .build()
    .unwrap();
    member.start().await.unwrap();

    let ready = tokio::time::timeout(Duration::from_secs(15), async {
        let mut lifecycle = member.subscribe_node_lifecycle();
        while *lifecycle.borrow() != NodeLifecycleState::Ready {
            lifecycle.changed().await.unwrap();
        }
    })
    .await;
    assert!(ready.is_ok(), "health: {:?}", member.health_snapshot());
    let snapshot = member.member_snapshot();
    assert!(snapshot.members.iter().any(|record| {
        record.node.node_id == "member"
            && record.node.incarnation == NodeIncarnation::new(202).unwrap()
            && record.status == MemberStatus::Up
    }));

    member
        .leave(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        member.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    assert!(
        member
            .health_snapshot()
            .domains
            .values()
            .all(|state| *state == PlacementDomainState::Terminated)
    );
    assert!(store.get_member("member").await.unwrap().is_none());
    assert!(
        member
            .member_snapshot()
            .members
            .iter()
            .all(|record| record.node.incarnation != NodeIncarnation::new(202).unwrap())
    );
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn two_discovered_members_leave_sequentially_without_losing_coordinator_session() {
    let _network = network_test_guard().await;
    let coordinator_address = unused_address().await;
    let first_address = unused_address().await;
    let second_address = unused_address().await;
    let cluster_id = ClusterId::new("service-multi-member-test").unwrap();
    let store = Arc::new(InMemoryPlacementStore::new(64, 64).unwrap());
    let coordinator = coordinator_service(
        store.clone(),
        cluster_id.clone(),
        "coordinator",
        coordinator_address.clone(),
        NodeIncarnation::new(301).unwrap(),
        1,
    )
    .await;
    coordinator.start().await.unwrap();
    let discovery = |scope| {
        Arc::new(
            StaticDiscovery::new(
                scope,
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
        .proxy_entity::<PingProtocol>(proxy_options(placement_domain(), "membership-probe"))
        .unwrap()
        .domain_capacity(placement_domain(), 1)
        .unwrap()
        .coordinator_discovery(discovery(CoordinatorScope::Membership))
        .unwrap()
        .coordinator_discovery(discovery(CoordinatorScope::Placement(placement_domain())))
        .unwrap()
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
        let mut lifecycle = first.subscribe_node_lifecycle();
        while *lifecycle.borrow() != NodeLifecycleState::Ready {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    second.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut lifecycle = second.subscribe_node_lifecycle();
        while *lifecycle.borrow() != NodeLifecycleState::Ready {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    first.terminal_shutdown().await.unwrap();
    assert!(store.get_member("first").await.unwrap().is_none());
    second
        .leave(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_domain_coordinator_loss_leaves_other_domain_ready() {
    let _network = network_test_guard().await;
    let membership_address = unused_address().await;
    let coordinator_a_address = unused_address().await;
    let coordinator_b_address = unused_address().await;
    let member_address = unused_address().await;
    let cluster_id = ClusterId::new("service-domain-isolation-test").unwrap();
    let store = Arc::new(InMemoryPlacementStore::new(64, 64).unwrap());
    let domain_a = placement_domain();
    let domain_b = secondary_domain();
    let membership_coordinator = coordinator_service_for_domains(
        store.clone(),
        cluster_id.clone(),
        "membership-coordinator",
        membership_address.clone(),
        NodeIncarnation::new(400).unwrap(),
        BTreeSet::new(),
    )
    .await;
    let coordinator_a = coordinator_service_for_domains(
        store.clone(),
        cluster_id.clone(),
        "coordinator-a",
        coordinator_a_address.clone(),
        NodeIncarnation::new(401).unwrap(),
        BTreeSet::from([domain_a.clone()]),
    )
    .await;
    let coordinator_b = coordinator_service_for_domains(
        store,
        cluster_id.clone(),
        "coordinator-b",
        coordinator_b_address.clone(),
        NodeIncarnation::new(402).unwrap(),
        BTreeSet::from([domain_b.clone()]),
    )
    .await;
    membership_coordinator.start().await.unwrap();
    coordinator_a.start().await.unwrap();
    coordinator_b.start().await.unwrap();

    let discovery = |scope, name: &'static str, node_id: &'static str, address| {
        Arc::new(
            StaticDiscovery::new(
                scope,
                name,
                vec![StaticEndpoint {
                    address,
                    expected_node_id: Some(node_id.to_string()),
                    priority: 1,
                }],
            )
            .unwrap(),
        )
    };
    let member = LatticeService::builder(node_config(
        cluster_id,
        "multi-domain-member",
        member_address,
        NodeIncarnation::new(403).unwrap(),
    ))
    .unwrap()
    .proxy_entity::<PingProtocol>(proxy_options(domain_a.clone(), "domain-a-proxy"))
    .unwrap()
    .proxy_entity::<PingProtocol>(proxy_options(domain_b.clone(), "domain-b-proxy"))
    .unwrap()
    .domain_capacity(domain_a.clone(), 1)
    .unwrap()
    .domain_capacity(domain_b.clone(), 1)
    .unwrap()
    .coordinator_discovery(discovery(
        CoordinatorScope::Membership,
        "membership",
        "membership-coordinator",
        membership_address,
    ))
    .unwrap()
    .coordinator_discovery(discovery(
        CoordinatorScope::Placement(domain_a.clone()),
        "domain-a",
        "coordinator-a",
        coordinator_a_address,
    ))
    .unwrap()
    .coordinator_discovery(discovery(
        CoordinatorScope::Placement(domain_b.clone()),
        "domain-b",
        "coordinator-b",
        coordinator_b_address,
    ))
    .unwrap()
    .join_config(ClusterJoinConfig {
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(100),
        join_timeout: Some(Duration::from_secs(5)),
        ..ClusterJoinConfig::default()
    })
    .build()
    .unwrap();
    member.start().await.unwrap();
    let mut health = member.subscribe_health();
    let ready_result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = health.borrow().clone();
            if snapshot.node == NodeLifecycleState::Ready
                && snapshot.domains.get(&domain_a) == Some(&PlacementDomainState::Ready)
                && snapshot.domains.get(&domain_b) == Some(&PlacementDomainState::Ready)
            {
                break;
            }
            health.changed().await.unwrap();
        }
    })
    .await;
    assert!(ready_result.is_ok(), "health: {:?}", health.borrow());

    coordinator_a.force_shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = health.borrow().clone();
            if snapshot.node == NodeLifecycleState::Ready
                && snapshot.domains.get(&domain_a) == Some(&PlacementDomainState::Degraded)
                && snapshot.domains.get(&domain_b) == Some(&PlacementDomainState::Ready)
            {
                break;
            }
            health.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(member.node_lifecycle_state(), NodeLifecycleState::Ready);

    member.force_shutdown().await.unwrap();
    coordinator_b.force_shutdown().await.unwrap();
    membership_coordinator.force_shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_rollover_recovers_after_blocked_session_registration() {
    let _network = network_test_guard().await;

    let cluster_id = ClusterId::new("service-rollover-test").unwrap();
    let address_a = unused_address().await;
    let address_b = unused_address().await;
    let member_address = unused_address().await;
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
    let member_incarnation = NodeIncarnation::new(303).unwrap();
    let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("RolloverPing"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: member_address.clone(),
                node_incarnation: member_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        binding.as_ref(),
    ));
    let entity_config = EntityConfig::new(
        placement_domain(),
        EntityType::new("rollover-ping").unwrap(),
        ProtocolId::new(PROTOCOL_ID).unwrap(),
        8,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let target = entity_config
        .entity_ref::<PingProtocol>(
            cluster_id.clone(),
            EntityId::new(b"entity-1".to_vec()).unwrap(),
        )
        .unwrap();
    let member = LatticeService::builder(node_config(
        cluster_id.clone(),
        "rollover-member",
        member_address,
        member_incarnation,
    ))
    .unwrap()
    .host_entity_with_registry(entity_config, registry, binding, PingLoader)
    .unwrap()
    .domain_capacity(placement_domain(), 1)
    .unwrap()
    .coordinator_discovery(Arc::new(WatchDiscovery {
        scope: CoordinatorScope::Membership,
        snapshots: discovery_rx.clone(),
    }))
    .unwrap()
    .coordinator_discovery(Arc::new(WatchDiscovery {
        scope: CoordinatorScope::Placement(placement_domain()),
        snapshots: discovery_rx,
    }))
    .unwrap()
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
    let cluster = member.cluster();
    let ready = cluster.wait_ready(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        ready
            .self_member()
            .map(|member| member.node.node_id.as_str()),
        Some("rollover-member")
    );
    let mut cluster_events = cluster.subscribe();
    assert!(matches!(
        cluster_events.recv().await,
        Some(ClusterEvent::CurrentState(state)) if state.is_ready()
    ));
    assert_eq!(
        ping(&member, target.clone(), 1, "before rollover").await,
        Pong(2)
    );

    coordinator_a.force_shutdown().await.unwrap();
    cluster
        .wait_for(Duration::from_secs(5), |state| {
            state.health.node == NodeLifecycleState::JoiningMembership
                && state.health.domains.get(&placement_domain())
                    == Some(&PlacementDomainState::Degraded)
        })
        .await
        .expect("member did not observe membership loss before Coordinator replacement");

    let coordinator_b = coordinator_service(
        store,
        cluster_id,
        "coordinator-b",
        address_b.clone(),
        NodeIncarnation::new(302).unwrap(),
        2,
    )
    .await;
    let (registration_reached_tx, registration_reached_rx) = std_mpsc::sync_channel(1);
    let (registration_release_tx, registration_release_rx) = std_mpsc::sync_channel(1);
    let registration_release_rx = Arc::new(Mutex::new(registration_release_rx));
    let release = registration_release_rx.clone();
    let block_once = Arc::new(AtomicBool::new(true));
    let block = block_once.clone();
    let failpoint = lattice_core::failpoint::install_hook(move |point| {
        if point == Failpoint::MemberBeforeGuardedCommit && block.swap(false, Ordering::AcqRel) {
            registration_reached_tx
                .send(())
                .expect("blocked registration observer dropped");
            release
                .lock()
                .expect("registration release poisoned")
                .recv()
                .expect("blocked registration was not released");
        }
    });
    coordinator_b.start().await.unwrap();
    discovery_tx
        .send(discovery_snapshot(2, "coordinator-b", address_b))
        .unwrap();
    tokio::task::spawn_blocking(move || {
        registration_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("new-term MemberHello did not reach the guarded store boundary");
    })
    .await
    .unwrap();
    assert!(
        !cluster.state().is_ready(),
        "node published Ready before its new Coordinator session was committed"
    );
    registration_release_tx
        .send(())
        .expect("blocked registration hook dropped");
    drop(failpoint);
    cluster
        .wait_ready(Duration::from_secs(5))
    .await
    .unwrap_or_else(|_| {
        panic!(
            "placement domain did not return to Ready; lifecycle: {:?}; health: {:?}; members: {:?}",
            member.node_lifecycle_state(),
            member.health_snapshot(),
            member.member_snapshot(),
        )
    });
    assert_eq!(ping(&member, target, 2, "after rollover").await, Pong(3));
    let members = member.member_snapshot().members;
    assert_eq!(
        members
            .iter()
            .filter(|record| record.node.node_id == "rollover-member")
            .count(),
        1
    );

    member.force_shutdown().await.unwrap();
    coordinator_b.shutdown().await.unwrap();
}

async fn ping(
    service: &LatticeService,
    target: EntityRef<PingProtocol>,
    value: u64,
    phase: &str,
) -> Pong {
    service
        .ask(target, Ping(value), Duration::from_secs(2))
        .await
        .unwrap_or_else(|error| {
        panic!(
            "{phase} ping failed after Ready; error: {error:?}; lifecycle: {:?}; health: {:?}; members: {:?}",
            service.node_lifecycle_state(),
            service.health_snapshot(),
            service.member_snapshot(),
        )
    })
}
