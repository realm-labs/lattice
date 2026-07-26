//! Admission scopes: what a node keeps serving when it loses membership, and what every
//! termination path still has to refuse.
//!
//! Losing a membership session used to close the node's single admission gate, which cut local
//! actor traffic and exact `ActorRef` traffic that membership never vouched for in the first
//! place. These tests hold the line in both directions: the cluster-internal scopes survive a
//! membership session loss, and no termination path is allowed to inherit that leniency.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use lattice_actor::{
    registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig},
    traits::StopReason,
};
use lattice_core::{
    actor_kind,
    actor_ref::{
        ActorRef, ClusterId, EntityId, EntityType, NodeAddress, NodeIncarnation, ProtocolId,
    },
    coordinator::CoordinatorScope,
    id::ActorId,
};
use lattice_discovery::static_provider::{StaticDiscovery, StaticEndpoint};
use lattice_placement::{region::EntityConfig, storage::InMemoryPlacementStore};
use lattice_remoting::handshake::NodeIdentity;

use super::support::*;
use crate::{
    builder::LatticeService,
    config::ClusterJoinConfig,
    lifecycle::{NodeLifecycleState, PlacementDomainState},
    test_support::{network_test_guard, unused_address},
};

fn join_config() -> ClusterJoinConfig {
    ClusterJoinConfig {
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(50),
        join_timeout: Some(Duration::from_secs(20)),
        leave_timeout: Duration::from_secs(5),
        shutdown_timeout: Duration::from_secs(5),
        ..ClusterJoinConfig::default()
    }
}

/// A node hosting one exact activation, wired so that its `ActorRef` can be addressed from
/// another process.
fn hosted_ping(
    cluster_id: &ClusterId,
    address: &NodeAddress,
    incarnation: NodeIncarnation,
) -> (
    Arc<ActorRegistry<PingActor>>,
    Arc<lattice_actor::protocol::ActorProtocolBinding<PingActor, PingProtocol>>,
) {
    let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("AdmissionPing"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: address.clone(),
                node_incarnation: incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        binding.as_ref(),
    ));
    (registry, binding)
}

async fn await_lifecycle(service: &LatticeService, expected: NodeLifecycleState) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut lifecycle = service.subscribe_node_lifecycle();
        while *lifecycle.borrow() != expected {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "node never reached {expected:?}; state: {:?}; health: {:?}",
            service.node_lifecycle_state(),
            service.health_snapshot(),
        )
    });
}

/// The regression this split exists for.
///
/// A membership session is lost and regained. Throughout the gap the node keeps answering both
/// process-local asks and exact `ActorRef` asks arriving from another process, because neither
/// depends on membership to be safe: the reference names one incarnation and one activation, and
/// a stale reference resolves to nothing rather than to a replacement. Only the external edge —
/// the traffic nothing but membership vouches for — is shed, and it comes back when the node is a
/// full member again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_loss_sheds_the_edge_while_local_and_exact_traffic_keep_serving() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("service-admission-scope").unwrap();
    let first_coordinator_address = unused_address().await;
    let second_coordinator_address = unused_address().await;
    let member_address = unused_address().await;
    let client_address = unused_address().await;
    let member_incarnation = NodeIncarnation::new(901).unwrap();
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());

    let first_coordinator = coordinator_service_for_domains(
        store.clone(),
        cluster_id.clone(),
        "coordinator-a",
        first_coordinator_address.clone(),
        NodeIncarnation::new(801).unwrap(),
        BTreeSet::new(),
    )
    .await;
    first_coordinator.start().await.unwrap();

    // Both coordinator addresses are published up front so the rejoin is driven by the node's own
    // retry loop rather than by a test-only discovery push.
    let discovery = Arc::new(
        StaticDiscovery::new(
            CoordinatorScope::Membership,
            "admission-scope",
            vec![
                StaticEndpoint {
                    address: first_coordinator_address,
                    expected_node_id: Some("coordinator-a".to_owned()),
                    priority: 1,
                },
                StaticEndpoint {
                    address: second_coordinator_address.clone(),
                    expected_node_id: Some("coordinator-b".to_owned()),
                    priority: 2,
                },
            ],
        )
        .unwrap(),
    );

    let (registry, binding) = hosted_ping(&cluster_id, &member_address, member_incarnation);
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let target: ActorRef<PingProtocol> = handle.typed_actor_ref().unwrap().unwrap();
    let member = LatticeService::builder(node_config(
        cluster_id.clone(),
        "member",
        member_address.clone(),
        member_incarnation,
    ))
    .unwrap()
    .register_actor(registry, binding)
    .unwrap()
    .coordinator_discovery(discovery)
    .unwrap()
    .join_config(join_config())
    .member_event_capacity(64)
    .build()
    .unwrap();
    member.start().await.unwrap();

    // The client never joins the cluster; it only holds an exact reference into the member. That
    // keeps the assertion about inbound exact admission free of the member's own lifecycle.
    let client = LatticeService::builder(node_config(
        cluster_id.clone(),
        "client",
        client_address,
        NodeIncarnation::new(902).unwrap(),
    ))
    .unwrap()
    .use_protocol::<PingProtocol>()
    .unwrap()
    .build()
    .unwrap();
    client.start().await.unwrap();
    client
        .connect_peer(NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: "member".to_owned(),
            address: member_address,
            incarnation: member_incarnation,
        })
        .await
        .unwrap();

    await_lifecycle(&member, NodeLifecycleState::Ready).await;
    assert!(member.admission_snapshot().fully_open());
    assert!(member.external_ingress().is_open());
    assert_eq!(
        client
            .ask(&target, Ping(1), Duration::from_secs(5))
            .await
            .unwrap(),
        Pong(2)
    );

    first_coordinator.force_shutdown().await.unwrap();
    await_lifecycle(&member, NodeLifecycleState::JoiningMembership).await;

    let admission = member.admission_snapshot();
    assert!(
        !admission.external,
        "a node without a membership session must stop taking new external work"
    );
    assert!(
        admission.exact,
        "an exact ActorRef is fenced by its own incarnation and activation, not by membership"
    );
    assert!(
        admission.logical,
        "logical routes are fenced by placement claim deadlines, not by membership"
    );
    assert!(member.recovering_membership());
    assert!(!member.external_ingress().is_open());
    assert!(
        member
            .external_ingress()
            .ask(&target, Ping(100), Duration::from_secs(1))
            .await
            .is_err(),
        "external ingress must refuse while the node has no membership session"
    );

    // The two paths the old single gate cut: process-local dispatch, and inbound exact dispatch
    // from a peer.
    assert_eq!(
        member
            .actor_system()
            .ask(&target, Ping(10), Duration::from_secs(5))
            .await
            .unwrap(),
        Pong(11),
        "local actor traffic never depended on membership"
    );
    assert_eq!(
        client
            .ask(&target, Ping(20), Duration::from_secs(5))
            .await
            .unwrap(),
        Pong(21),
        "an exact remote ActorRef must survive the membership gap"
    );

    let second_coordinator = coordinator_service_for_domains(
        store,
        cluster_id,
        "coordinator-b",
        second_coordinator_address,
        NodeIncarnation::new(802).unwrap(),
        BTreeSet::new(),
    )
    .await;
    second_coordinator.start().await.unwrap();
    await_lifecycle(&member, NodeLifecycleState::Ready).await;
    assert!(
        member.admission_snapshot().fully_open(),
        "a recovered member must admit external traffic again"
    );
    assert!(!member.recovering_membership());
    assert_eq!(
        member
            .external_ingress()
            .ask(&target, Ping(30), Duration::from_secs(5))
            .await
            .unwrap(),
        Pong(31)
    );

    handle.stop(StopReason::Requested).await.unwrap();
    client.force_shutdown().await.unwrap();
    member.force_shutdown().await.unwrap();
    second_coordinator.force_shutdown().await.unwrap();
}

/// Logical routing is governed by the placement domain session and its claim deadline, which the
/// membership session never participates in. With the two coordinators in separate processes,
/// killing the membership one must leave the domain Ready and its entity traffic flowing — the
/// claim is what says the shard is still this node's, and it is still installed and renewing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_loss_leaves_placement_governed_entity_traffic_serving() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("service-admission-logical").unwrap();
    let membership_address = unused_address().await;
    let domain_address = unused_address().await;
    let member_address = unused_address().await;
    let member_incarnation = NodeIncarnation::new(905).unwrap();
    let domain = placement_domain();
    let store = Arc::new(InMemoryPlacementStore::new(64, 64).unwrap());

    let membership_coordinator = coordinator_service_for_domains(
        store.clone(),
        cluster_id.clone(),
        "membership-coordinator",
        membership_address.clone(),
        NodeIncarnation::new(811).unwrap(),
        BTreeSet::new(),
    )
    .await;
    let domain_coordinator = coordinator_service_for_domains(
        store,
        cluster_id.clone(),
        "domain-coordinator",
        domain_address.clone(),
        NodeIncarnation::new(812).unwrap(),
        BTreeSet::from([domain.clone()]),
    )
    .await;
    membership_coordinator.start().await.unwrap();
    domain_coordinator.start().await.unwrap();

    let discovery = |scope, name: &'static str, node_id: &'static str, address| {
        Arc::new(
            StaticDiscovery::new(
                scope,
                name,
                vec![StaticEndpoint {
                    address,
                    expected_node_id: Some(node_id.to_owned()),
                    priority: 1,
                }],
            )
            .unwrap(),
        )
    };
    let entity_config = EntityConfig::new(
        domain.clone(),
        EntityType::new("admission-ping").unwrap(),
        ProtocolId::new(PROTOCOL_ID).unwrap(),
        1,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let (registry, binding) = hosted_ping(&cluster_id, &member_address, member_incarnation);
    let member = LatticeService::builder(node_config(
        cluster_id.clone(),
        "member",
        member_address,
        member_incarnation,
    ))
    .unwrap()
    .host_entity_with_registry(entity_config.clone(), registry, binding, PingLoader)
    .unwrap()
    .domain_capacity(domain.clone(), 1)
    .unwrap()
    .coordinator_discovery(discovery(
        CoordinatorScope::Membership,
        "membership",
        "membership-coordinator",
        membership_address,
    ))
    .unwrap()
    .coordinator_discovery(discovery(
        CoordinatorScope::Placement(domain.clone()),
        "domain",
        "domain-coordinator",
        domain_address,
    ))
    .unwrap()
    .join_config(join_config())
    .member_event_capacity(64)
    .build()
    .unwrap();
    member.start().await.unwrap();
    member
        .cluster()
        .wait_ready(Duration::from_secs(20))
        .await
        .unwrap();

    let target = entity_config
        .entity_ref::<PingProtocol>(cluster_id, EntityId::new(b"admitted".to_vec()).unwrap())
        .unwrap();
    assert_eq!(
        member
            .actor_system()
            .ask(target.clone(), Ping(1), Duration::from_secs(10))
            .await
            .unwrap(),
        Pong(2)
    );

    membership_coordinator.force_shutdown().await.unwrap();
    await_lifecycle(&member, NodeLifecycleState::JoiningMembership).await;
    assert_eq!(
        member.health_snapshot().domains.get(&domain),
        Some(&PlacementDomainState::Ready),
        "losing membership must not degrade a placement domain with its own live session"
    );
    assert!(member.admission_snapshot().logical);
    assert_eq!(
        member
            .actor_system()
            .ask(target, Ping(5), Duration::from_secs(10))
            .await
            .unwrap(),
        Pong(6),
        "entity traffic is fenced by the placement claim, not by the membership session"
    );

    member.force_shutdown().await.unwrap();
    domain_coordinator.force_shutdown().await.unwrap();
}

/// Splitting the gate must not make giving up cluster authority any softer. A cordoned node
/// refuses every scope, including the process-local dispatch that survives a membership loss.
#[tokio::test]
async fn cordon_closes_every_admission_scope_including_local_dispatch() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("service-admission-drain").unwrap();
    let address = unused_address().await;
    let incarnation = NodeIncarnation::new(903).unwrap();
    let (registry, binding) = hosted_ping(&cluster_id, &address, incarnation);
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let target: ActorRef<PingProtocol> = handle.typed_actor_ref().unwrap().unwrap();
    let service = LatticeService::builder(node_config(cluster_id, "drained", address, incarnation))
        .unwrap()
        .register_actor(registry, binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();
    assert!(service.admission_snapshot().fully_open());
    assert_eq!(
        service
            .actor_system()
            .ask(&target, Ping(1), Duration::from_secs(2))
            .await
            .unwrap(),
        Pong(2)
    );

    service.cordon().unwrap();
    assert_eq!(service.node_lifecycle_state(), NodeLifecycleState::Draining);
    let admission = service.admission_snapshot();
    assert!(
        !admission.external,
        "a draining node admits no external work"
    );
    assert!(!admission.exact, "a draining node admits no exact traffic");
    assert!(
        !admission.logical,
        "a draining node admits no logical traffic"
    );
    assert!(!admission.serves_cluster_traffic());
    assert!(
        service
            .actor_system()
            .ask(&target, Ping(2), Duration::from_secs(2))
            .await
            .is_err(),
        "draining must refuse the local dispatch that a membership loss preserves"
    );
    assert!(!service.external_ingress().is_open());

    service.force_shutdown().await.unwrap();
    assert_eq!(
        service.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    assert!(!service.admission_snapshot().serves_cluster_traffic());
}

/// Force stop skips the drain entirely, so it is the path most likely to be missed by a change
/// that narrows a close. It has to fence every scope by itself.
#[tokio::test]
async fn force_stop_closes_every_admission_scope() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("service-admission-force").unwrap();
    let address = unused_address().await;
    let incarnation = NodeIncarnation::new(904).unwrap();
    let (registry, binding) = hosted_ping(&cluster_id, &address, incarnation);
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let target: ActorRef<PingProtocol> = handle.typed_actor_ref().unwrap().unwrap();
    let service = LatticeService::builder(node_config(cluster_id, "forced", address, incarnation))
        .unwrap()
        .register_actor(registry, binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();
    assert!(service.admission_snapshot().fully_open());

    service.force_shutdown().await.unwrap();
    let admission = service.admission_snapshot();
    assert!(!admission.external);
    assert!(!admission.exact);
    assert!(!admission.logical);
    assert!(
        service
            .actor_system()
            .ask(&target, Ping(1), Duration::from_secs(1))
            .await
            .is_err()
    );
}
