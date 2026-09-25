//! Cluster membership: discovery-driven join, leave, per-group health and Coordinator rollover.

use lattice_actor::runtime::ActorRuntime;
use lattice_coordination::storage::candidates::provision_candidate;
use lattice_model::actor::ActorId;

use lattice_actor_distributed::registry::ActorDefinition;
use std::{
    collections::BTreeSet,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc,
    },
    time::Duration,
};

use futures_util::Stream;
use lattice_actor_distributed::registry::{ActorAddressConfig, ActorRegistry, ActorRegistryConfig};
use lattice_coordination::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter},
    coordinator::MemberStatus,
    failpoints,
    region::EntityConfig,
    runtime::{
        GroupCoordinatorConfig,
        host::{CoordinatorHost, CoordinatorHostConfig},
    },
    storage::{ActorGroupStore, InMemoryCoordinationStore, MembershipStore},
    types::{NodeKey, PlacementSlotKey},
};
use lattice_discovery::{
    provider::{
        CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
        DiscoverySource, DiscoveryTarget,
    },
    static_provider::{StaticDiscovery, StaticEndpoint},
};
use lattice_model::{
    actor::{EntityAddress, ProtocolId},
    cluster::CoordinatorScope,
    cluster::{ClusterId, EntityType, NodeEndpoint, NodeIncarnation},
};
use tokio::{sync::watch::Receiver, time::Instant};

use super::support::*;
use crate::{
    builder::{LatticeService, LatticeServiceBuilder},
    cluster::api::ClusterEvent,
    config::ClusterJoinConfig,
    lifecycle::{ActorGroupState, NodeLifecycleState},
    test_support::{network_test_guard, unused_address},
};

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
        let scope = self.scope.clone();
        Box::pin(futures_util::stream::unfold(
            (receiver, true, scope),
            |(mut receiver, first, scope)| async move {
                if !first && receiver.changed().await.is_err() {
                    return None;
                }
                // This test shares endpoint updates, not an unscoped directory:
                // each provider must emit the scope it advertises to its client.
                let mut snapshot = receiver.borrow_and_update().clone();
                snapshot.scope = scope.clone();
                Some((Ok(snapshot), (receiver, false, scope)))
            },
        ))
    }
}

fn discovery_snapshot(
    generation: u64,
    node_id: &str,
    address: NodeEndpoint,
) -> CoordinatorDirectorySnapshot {
    CoordinatorDirectorySnapshot {
        leader_hint: None,
        scope: CoordinatorScope::Group(actor_group()),
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

async fn ping(
    service: &LatticeService,
    target: EntityAddress<PingProtocol>,
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

async fn ping_other(
    service: &LatticeService,
    target: EntityAddress<OtherPingProtocol>,
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
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
    for scope in [
        CoordinatorScope::Cluster,
        CoordinatorScope::Group(actor_group()),
    ] {
        provision_candidate(store.as_ref(), scope, "coordinator".to_owned())
            .await
            .unwrap();
    }
    let host = CoordinatorHost::elect(
        store.clone(),
        associations,
        NodeKey {
            node_id: "coordinator".to_string(),
            address: coordinator_address.clone(),
            incarnation: coordinator_incarnation,
        },
        BTreeSet::from([actor_group()]),
        CoordinatorHostConfig {
            group: GroupCoordinatorConfig {
                renewal_interval: Duration::from_millis(100),
                ..GroupCoordinatorConfig::default()
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
            CoordinatorScope::Cluster,
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
            .groups
            .values()
            .all(|state| *state == ActorGroupState::Terminated)
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
    let store = Arc::new(InMemoryCoordinationStore::new(64, 64).unwrap());
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
    let member = |node_id: &str, address: NodeEndpoint, incarnation: u128| {
        LatticeService::builder(node_config(
            cluster_id.clone(),
            node_id,
            address,
            NodeIncarnation::new(incarnation).unwrap(),
        ))
        .unwrap()
        .proxy_entity::<PingProtocol>(proxy_options(actor_group(), "membership-probe"))
        .unwrap()
        .group_capacity(actor_group(), 1)
        .unwrap()
        .coordinator_discovery(discovery(CoordinatorScope::Cluster))
        .unwrap()
        .coordinator_discovery(discovery(CoordinatorScope::Group(actor_group())))
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
async fn one_group_coordinator_loss_leaves_other_group_ready() {
    let _network = network_test_guard().await;
    let membership_address = unused_address().await;
    let coordinator_a_address = unused_address().await;
    let coordinator_b_address = unused_address().await;
    let member_address = unused_address().await;
    let cluster_id = ClusterId::new("service-group-isolation-test").unwrap();
    let store = Arc::new(InMemoryCoordinationStore::new(64, 64).unwrap());
    let group_a = actor_group();
    let group_b = secondary_group();
    let membership_coordinator = coordinator_service_for_groups(
        store.clone(),
        cluster_id.clone(),
        "membership-coordinator",
        membership_address.clone(),
        NodeIncarnation::new(400).unwrap(),
        BTreeSet::new(),
    )
    .await;
    let coordinator_a = coordinator_service_for_groups(
        store.clone(),
        cluster_id.clone(),
        "coordinator-a",
        coordinator_a_address.clone(),
        NodeIncarnation::new(401).unwrap(),
        BTreeSet::from([group_a.clone()]),
    )
    .await;
    let coordinator_b = coordinator_service_for_groups(
        store,
        cluster_id.clone(),
        "coordinator-b",
        coordinator_b_address.clone(),
        NodeIncarnation::new(402).unwrap(),
        BTreeSet::from([group_b.clone()]),
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
        "multi-group-member",
        member_address,
        NodeIncarnation::new(403).unwrap(),
    ))
    .unwrap()
    .proxy_entity::<PingProtocol>(proxy_options(group_a.clone(), "group-a-proxy"))
    .unwrap()
    .proxy_entity::<PingProtocol>(proxy_options(group_b.clone(), "group-b-proxy"))
    .unwrap()
    .group_capacity(group_a.clone(), 1)
    .unwrap()
    .group_capacity(group_b.clone(), 1)
    .unwrap()
    .coordinator_discovery(discovery(
        CoordinatorScope::Cluster,
        "membership",
        "membership-coordinator",
        membership_address,
    ))
    .unwrap()
    .coordinator_discovery(discovery(
        CoordinatorScope::Group(group_a.clone()),
        "group-a",
        "coordinator-a",
        coordinator_a_address,
    ))
    .unwrap()
    .coordinator_discovery(discovery(
        CoordinatorScope::Group(group_b.clone()),
        "group-b",
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
                && snapshot.groups.get(&group_a) == Some(&ActorGroupState::Ready)
                && snapshot.groups.get(&group_b) == Some(&ActorGroupState::Ready)
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
                && snapshot.groups.get(&group_a) == Some(&ActorGroupState::Degraded)
                && snapshot.groups.get(&group_b) == Some(&ActorGroupState::Ready)
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
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
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
    let actor_runtime = ActorRuntime::default();
    let registry = Arc::new(ActorRegistry::<RolloverPingDefinition, _>::new_bound(
        actor_runtime.spawner(),
        ActorRegistryConfig {
            address: Some(ActorAddressConfig {
                cluster_id: cluster_id.clone(),
                node_address: member_address.clone(),
                node_incarnation: member_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        binding.as_ref(),
    ));
    let entity_config = EntityConfig::new(
        actor_group(),
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
            ActorId::new(b"entity-1".to_vec()).unwrap(),
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
    .group_capacity(actor_group(), 1)
    .unwrap()
    .coordinator_discovery(Arc::new(WatchDiscovery {
        scope: CoordinatorScope::Cluster,
        snapshots: discovery_rx.clone(),
    }))
    .unwrap()
    .coordinator_discovery(Arc::new(WatchDiscovery {
        scope: CoordinatorScope::Group(actor_group()),
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
                && state.health.groups.get(&actor_group()) == Some(&ActorGroupState::Degraded)
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
    let failpoint = lattice_failpoint::install_hook(move |point| {
        if point == failpoints::MEMBER_BEFORE_GUARDED_COMMIT && block.swap(false, Ordering::AcqRel)
        {
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
            "placement group did not return to Ready; lifecycle: {:?}; health: {:?}; members: {:?}",
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

#[tokio::test]
async fn an_active_shard_recovers_after_a_transient_association_loss() {
    let _network = network_test_guard().await;
    let coordinator_address = unused_address().await;
    let member_address = unused_address().await;
    let cluster_id = ClusterId::new("service-association-recovery-test").unwrap();
    let store = Arc::new(InMemoryCoordinationStore::new(64, 64).unwrap());
    let coordinator_incarnation = NodeIncarnation::new(451).unwrap();
    let member_incarnation = NodeIncarnation::new(452).unwrap();
    let primary_group = actor_group();
    let secondary_group = secondary_group();
    let coordinator = coordinator_service_for_groups(
        store.clone(),
        cluster_id.clone(),
        "coordinator",
        coordinator_address.clone(),
        coordinator_incarnation,
        BTreeSet::from([primary_group.clone(), secondary_group.clone()]),
    )
    .await;
    coordinator.start().await.unwrap();
    let discovery = |scope| {
        Arc::new(
            StaticDiscovery::new(
                scope,
                "association-recovery",
                vec![StaticEndpoint {
                    address: coordinator_address.clone(),
                    expected_node_id: Some("coordinator".to_owned()),
                    priority: 1,
                }],
            )
            .unwrap(),
        )
    };
    let primary_entity_type = EntityType::new("association-recovery-primary").unwrap();
    let primary_config = EntityConfig::new(
        primary_group.clone(),
        primary_entity_type.clone(),
        ProtocolId::new(PROTOCOL_ID).unwrap(),
        1,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let secondary_entity_type = EntityType::new("association-recovery-secondary").unwrap();
    let secondary_config = EntityConfig::new(
        secondary_group.clone(),
        secondary_entity_type.clone(),
        ProtocolId::new(PROTOCOL_ID + 1).unwrap(),
        1,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let primary_binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let actor_runtime = ActorRuntime::default();
    let primary_registry = Arc::new(
        ActorRegistry::<AssociationRecoveryPrimaryDefinition, _>::new_bound(
            actor_runtime.spawner(),
            ActorRegistryConfig {
                address: Some(ActorAddressConfig {
                    cluster_id: cluster_id.clone(),
                    node_address: member_address.clone(),
                    node_incarnation: member_incarnation,
                }),
                ..ActorRegistryConfig::default()
            },
            primary_binding.as_ref(),
        ),
    );
    let secondary_binding = Arc::new(OtherPingProtocol::bind::<PingActor>().unwrap());
    let actor_runtime = ActorRuntime::default();
    let secondary_registry = Arc::new(
        ActorRegistry::<AssociationRecoverySecondaryDefinition, _>::new_bound(
            actor_runtime.spawner(),
            ActorRegistryConfig {
                address: Some(ActorAddressConfig {
                    cluster_id: cluster_id.clone(),
                    node_address: member_address.clone(),
                    node_incarnation: member_incarnation,
                }),
                ..ActorRegistryConfig::default()
            },
            secondary_binding.as_ref(),
        ),
    );
    let member = LatticeService::builder(node_config(
        cluster_id.clone(),
        "member",
        member_address.clone(),
        member_incarnation,
    ))
    .unwrap()
    .host_entity_with_registry(
        primary_config.clone(),
        primary_registry,
        primary_binding,
        PingLoader,
    )
    .unwrap()
    .host_entity_with_registry(
        secondary_config.clone(),
        secondary_registry,
        secondary_binding,
        PingLoader,
    )
    .unwrap()
    .group_capacity(primary_group.clone(), 1)
    .unwrap()
    .group_capacity(secondary_group.clone(), 1)
    .unwrap()
    .coordinator_discovery(discovery(CoordinatorScope::Cluster))
    .unwrap()
    .coordinator_discovery(discovery(CoordinatorScope::Group(primary_group.clone())))
    .unwrap()
    .coordinator_discovery(discovery(CoordinatorScope::Group(secondary_group.clone())))
    .unwrap()
    .join_config(ClusterJoinConfig {
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(100),
        leadership_refresh_interval: Duration::from_millis(100),
        join_timeout: Some(Duration::from_secs(5)),
        leave_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(3),
        ..ClusterJoinConfig::default()
    })
    .build()
    .unwrap();
    member.start().await.unwrap();
    let cluster = member.cluster();
    cluster.wait_ready(Duration::from_secs(5)).await.unwrap();
    let primary_entity_id = ActorId::new(b"association-recovery-primary".to_vec()).unwrap();
    let secondary_entity_id = ActorId::new(b"association-recovery-secondary".to_vec()).unwrap();
    let primary_target = primary_config
        .entity_ref::<PingProtocol>(cluster_id.clone(), primary_entity_id.clone())
        .unwrap();
    let secondary_target = secondary_config
        .entity_ref::<OtherPingProtocol>(cluster_id.clone(), secondary_entity_id.clone())
        .unwrap();
    assert_eq!(
        ping(
            &member,
            primary_target.clone(),
            1,
            "primary before association loss",
        )
        .await,
        Pong(2)
    );
    assert_eq!(
        ping_other(
            &member,
            secondary_target.clone(),
            2,
            "secondary before association loss",
        )
        .await,
        Pong(3)
    );
    let primary_slot_key = PlacementSlotKey::Shard {
        group: primary_group.clone(),
        entity_type: primary_entity_type,
        shard_id: primary_config.shard_for(&primary_entity_id).unwrap(),
    };
    let secondary_slot_key = PlacementSlotKey::Shard {
        group: secondary_group.clone(),
        entity_type: secondary_entity_type,
        shard_id: secondary_config.shard_for(&secondary_entity_id).unwrap(),
    };

    // Freeze the single-threaded runtime long enough for both endpoints to observe the real
    // default heartbeat timeout, then verify that membership and placement recover together.
    std::thread::sleep(Duration::from_secs(7));
    cluster
        .wait_for(Duration::from_secs(5), |state| {
            state.health.groups.get(&primary_group) == Some(&ActorGroupState::Degraded)
                && state.health.groups.get(&secondary_group) == Some(&ActorGroupState::Degraded)
        })
        .await
        .expect("placement group did not observe the transient association loss");
    cluster
        .wait_ready(Duration::from_secs(10))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "member did not recover; lifecycle: {:?}; health: {:?}; members: {:?}",
                member.node_lifecycle_state(),
                member.health_snapshot(),
                member.member_snapshot(),
            )
        });
    assert_eq!(
        ping(
            &member,
            primary_target,
            3,
            "primary after association recovery",
        )
        .await,
        Pong(4)
    );
    assert_eq!(
        ping_other(
            &member,
            secondary_target,
            4,
            "secondary after association recovery",
        )
        .await,
        Pong(5)
    );
    for slot_key in [primary_slot_key, secondary_slot_key] {
        let slot = store
            .get_slot(&slot_key)
            .await
            .unwrap()
            .expect("the active shard must survive association recovery");
        assert_eq!(
            slot.owner.as_ref().map(|owner| owner.node_id.as_str()),
            Some("member")
        );
    }

    member.force_shutdown().await.unwrap();
    coordinator.force_shutdown().await.unwrap();
}

/// A member that hosts nothing drains for free, so the leave that proves anything is the one that
/// has to hand a live shard over first. It has to finish inside its own deadline: a graceful leave
/// that falls back on the failure detector is slower than the crash it replaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_hosting_a_shard_leaves_by_handing_it_over_rather_than_timing_out() {
    let _network = network_test_guard().await;
    let coordinator_address = unused_address().await;
    let first_address = unused_address().await;
    let second_address = unused_address().await;
    let cluster_id = ClusterId::new("service-drain-handover-test").unwrap();
    let store = Arc::new(InMemoryCoordinationStore::new(64, 64).unwrap());
    let coordinator = coordinator_service(
        store.clone(),
        cluster_id.clone(),
        "coordinator",
        coordinator_address.clone(),
        NodeIncarnation::new(501).unwrap(),
        1,
    )
    .await;
    coordinator.start().await.unwrap();
    let discovery = |scope| {
        Arc::new(
            StaticDiscovery::new(
                scope,
                "drain-handover",
                vec![StaticEndpoint {
                    address: coordinator_address.clone(),
                    expected_node_id: Some("coordinator".to_owned()),
                    priority: 1,
                }],
            )
            .unwrap(),
        )
    };
    let entity_type = EntityType::new("drained-ping").unwrap();
    let entity_config = EntityConfig::new(
        actor_group(),
        entity_type.clone(),
        ProtocolId::new(PROTOCOL_ID).unwrap(),
        1,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let host = |node_id: &str, address: NodeEndpoint, incarnation: u128| {
        let incarnation = NodeIncarnation::new(incarnation).unwrap();
        let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
        let actor_runtime = ActorRuntime::default();
        let registry = Arc::new(ActorRegistry::<DrainedPingDefinition, _>::new_bound(
            actor_runtime.spawner(),
            ActorRegistryConfig {
                address: Some(ActorAddressConfig {
                    cluster_id: cluster_id.clone(),
                    node_address: address.clone(),
                    node_incarnation: incarnation,
                }),
                ..ActorRegistryConfig::default()
            },
            binding.as_ref(),
        ));
        LatticeServiceBuilder::with_actor_runtime(
            node_config(cluster_id.clone(), node_id, address, incarnation),
            actor_runtime,
        )
        .unwrap()
        .host_entity_with_registry(entity_config.clone(), registry, binding, PingLoader)
        .unwrap()
        .group_capacity(actor_group(), 1)
        .unwrap()
        .coordinator_discovery(discovery(CoordinatorScope::Cluster))
        .unwrap()
        .coordinator_discovery(discovery(CoordinatorScope::Group(actor_group())))
        .unwrap()
        .join_config(ClusterJoinConfig {
            retry_initial: Duration::from_millis(10),
            retry_max: Duration::from_millis(100),
            leadership_refresh_interval: Duration::from_millis(200),
            join_timeout: Some(Duration::from_secs(10)),
            leave_timeout: Duration::from_secs(15),
            shutdown_timeout: Duration::from_secs(20),
            ..ClusterJoinConfig::default()
        })
        .member_event_capacity(64)
        .build()
        .unwrap()
    };
    let first = host("first", first_address, 502);
    let second = host("second", second_address, 503);
    for member in [&first, &second] {
        member.start().await.unwrap();
        member
            .cluster()
            .wait_ready(Duration::from_secs(10))
            .await
            .unwrap();
    }
    let entity_id = ActorId::new(b"drained-entity".to_vec()).unwrap();
    let target = entity_config
        .entity_ref::<PingProtocol>(cluster_id.clone(), entity_id.clone())
        .unwrap();
    assert_eq!(
        ping(&first, target.clone(), 1, "before drain").await,
        Pong(2)
    );

    let slot_key = PlacementSlotKey::Shard {
        group: actor_group(),
        entity_type,
        shard_id: entity_config.shard_for(&entity_id).unwrap(),
    };
    let owner = store
        .get_slot(&slot_key)
        .await
        .unwrap()
        .expect("the probe must have allocated the shard")
        .owner
        .expect("an allocated shard has an owner");
    let (leaving, observer) = if owner.node_id == "first" {
        (&first, &second)
    } else {
        (&second, &first)
    };

    leaving
        .leave(Instant::now() + Duration::from_secs(15))
        .await
        .unwrap();
    assert_eq!(
        leaving.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    let moved = store
        .get_slot(&slot_key)
        .await
        .unwrap()
        .expect("the shard must survive the handover");
    assert_ne!(
        moved.owner.as_ref(),
        Some(&owner),
        "the drained member left while still owning its shard"
    );
    assert_eq!(ping(observer, target, 5, "after drain").await, Pong(6));

    observer.force_shutdown().await.unwrap();
    coordinator.shutdown().await.unwrap();
}

#[derive(Debug)]
struct RolloverPingDefinition;

impl ActorDefinition for RolloverPingDefinition {
    const NAME: &'static str = "RolloverPing";
    type Protocol = PingProtocol;
}

#[derive(Debug)]
struct AssociationRecoveryPrimaryDefinition;

impl ActorDefinition for AssociationRecoveryPrimaryDefinition {
    const NAME: &'static str = "AssociationRecoveryPrimary";
    type Protocol = PingProtocol;
}

#[derive(Debug)]
struct AssociationRecoverySecondaryDefinition;

impl ActorDefinition for AssociationRecoverySecondaryDefinition {
    const NAME: &'static str = "AssociationRecoverySecondary";
    type Protocol = OtherPingProtocol;
}

#[derive(Debug)]
struct DrainedPingDefinition;

impl ActorDefinition for DrainedPingDefinition {
    const NAME: &'static str = "DrainedPing";
    type Protocol = PingProtocol;
}
