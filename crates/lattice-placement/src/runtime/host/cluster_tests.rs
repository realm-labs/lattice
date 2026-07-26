use std::{collections::BTreeSet, sync::Arc, time::Duration};

use lattice_core::{
    actor_ref::{NodeAddress, NodeIncarnation, PlacementDomainId},
    coordinator::CoordinatorScope,
};
use lattice_remoting::{association::AssociationManager, config::RemotingConfig};
use tokio::sync::{mpsc, watch};

use super::{
    CoordinatorHost, CoordinatorHostConfig, CoordinatorHostScopeState, MembershipLeaderConfig,
    PlacementDomainLeaderConfig, election::candidate_delay_duration,
};
use crate::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter},
    storage::{CoordinatorLeaseStore, InMemoryPlacementStore, ScopedElectionStore},
    types::NodeKey,
};

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn associations(node: &NodeKey) -> Arc<AssociationManager> {
    Arc::new(
        AssociationManager::new(
            node.address.clone(),
            node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    )
}

fn config() -> CoordinatorHostConfig {
    CoordinatorHostConfig {
        membership: MembershipLeaderConfig {
            leader_lease_ttl: Duration::from_millis(500),
            member_lease_ttl: Duration::from_millis(500),
            renewal_interval: Duration::from_millis(50),
            ..MembershipLeaderConfig::default()
        },
        placement: PlacementDomainLeaderConfig {
            leader_lease_ttl: Duration::from_millis(500),
            member_lease_ttl: Duration::from_millis(500),
            claim_ttl: Duration::from_millis(500),
            renewal_interval: Duration::from_millis(50),
            ..PlacementDomainLeaderConfig::default()
        },
        renewal_interval: Duration::from_millis(50),
        ..CoordinatorHostConfig::default()
    }
}

#[tokio::test(start_paused = true)]
async fn placement_domain_campaigns_run_concurrently_off_the_host_loop() {
    let store = Arc::new(InMemoryPlacementStore::new(64, 64).unwrap());
    let holder = node("campaign-holder", 40, 33140);
    let candidate = node("campaign-candidate", 41, 33141);
    let domains = (0..16)
        .map(|index| PlacementDomainId::new(format!("campaign-{index}")).unwrap())
        .collect::<BTreeSet<_>>();
    let config = CoordinatorHostConfig {
        membership: MembershipLeaderConfig {
            leader_lease_ttl: Duration::from_secs(30),
            member_lease_ttl: Duration::from_secs(30),
            renewal_interval: Duration::from_secs(5),
            ..MembershipLeaderConfig::default()
        },
        placement: PlacementDomainLeaderConfig {
            leader_lease_ttl: Duration::from_secs(30),
            member_lease_ttl: Duration::from_secs(30),
            claim_ttl: Duration::from_secs(15),
            renewal_interval: Duration::from_secs(5),
            ..PlacementDomainLeaderConfig::default()
        },
        maximum_candidate_jitter: Duration::from_millis(300),
        ..CoordinatorHostConfig::default()
    };
    let delays = domains
        .iter()
        .map(|domain| {
            candidate_delay_duration(
                &CoordinatorScope::Placement(domain.clone()),
                &candidate,
                config.maximum_candidate_jitter,
            )
        })
        .collect::<Vec<_>>();
    let serialized = delays.iter().copied().sum::<Duration>();
    let concurrent = delays.iter().copied().max().unwrap()
        + candidate_delay_duration(
            &CoordinatorScope::Membership,
            &candidate,
            config.maximum_candidate_jitter,
        );
    assert!(serialized > concurrent * 2);

    let holder_host = CoordinatorHost::elect(
        store.clone(),
        associations(&holder),
        holder,
        domains.clone(),
        config.clone(),
    )
    .await
    .unwrap();
    let candidate_host = CoordinatorHost::elect(
        store.clone(),
        associations(&candidate),
        candidate,
        domains.clone(),
        config,
    )
    .await
    .unwrap();
    let mut scope_states = candidate_host.subscribe_scope_states();
    for hosted in holder_host.domains.values() {
        let lease = hosted.leader.as_ref().unwrap().leader_lease_id;
        store.revoke_lease(lease).await.unwrap();
    }

    let (_controls, control_receiver) = mpsc::channel(8);
    let (stop, stop_rx) = watch::channel(false);
    let started = tokio::time::Instant::now();
    let task = tokio::spawn(candidate_host.run(control_receiver, stop_rx));
    loop {
        let elected = scope_states
            .borrow_and_update()
            .iter()
            .filter(|(scope, state)| {
                matches!(scope, CoordinatorScope::Placement(_))
                    && matches!(state, CoordinatorHostScopeState::Active(_))
            })
            .count();
        if elected == domains.len() {
            break;
        }
        scope_states.changed().await.unwrap();
    }
    let settled = started.elapsed();

    assert!(
        settled <= concurrent,
        "{} campaigns took {settled:?}; concurrent campaigning bounds them by {concurrent:?} \
         while serial campaigning costs {serialized:?}",
        domains.len()
    );
    let _ = stop.send(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn dedicated_membership_host_needs_no_placement_domains() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let local = node("membership-host", 10, 33010);
    let host = CoordinatorHost::elect(
        store.clone(),
        associations(&local),
        local.clone(),
        BTreeSet::new(),
        config(),
    )
    .await
    .unwrap();

    assert!(host.domains.is_empty());
    assert!(matches!(
        host.scope_state(&CoordinatorScope::Membership),
        Some(CoordinatorHostScopeState::Active(_))
    ));
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Membership)
            .await
            .unwrap()
            .unwrap()
            .node,
        local
    );
}

#[tokio::test]
async fn competing_hosts_produce_exactly_one_active_leader_per_domain() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let domain = PlacementDomainId::new("single-leader-domain").unwrap();
    let first = node("first-candidate", 11, 33011);
    let second = node("second-candidate", 12, 33012);
    let first_host = CoordinatorHost::elect(
        store.clone(),
        associations(&first),
        first.clone(),
        BTreeSet::from([domain.clone()]),
        config(),
    )
    .await
    .unwrap();
    let second_host = CoordinatorHost::elect(
        store.clone(),
        associations(&second),
        second,
        BTreeSet::from([domain.clone()]),
        config(),
    )
    .await
    .unwrap();

    assert!(matches!(
        first_host.scope_state(&CoordinatorScope::Placement(domain.clone())),
        Some(CoordinatorHostScopeState::Active(_))
    ));
    assert!(matches!(
        second_host.scope_state(&CoordinatorScope::Placement(domain.clone())),
        Some(CoordinatorHostScopeState::Standby)
    ));
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Placement(domain))
            .await
            .unwrap()
            .unwrap()
            .node,
        first
    );
}

#[tokio::test]
async fn different_hosts_can_lead_different_domains_concurrently() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let host_a_node = node("host-a", 1, 33001);
    let host_b_node = node("host-b", 2, 33002);
    let domain_a = PlacementDomainId::new("domain-a").unwrap();
    let domain_b = PlacementDomainId::new("domain-b").unwrap();
    let host_a = CoordinatorHost::elect(
        store.clone(),
        associations(&host_a_node),
        host_a_node.clone(),
        BTreeSet::from([domain_a.clone()]),
        config(),
    )
    .await
    .unwrap();
    let host_b = CoordinatorHost::elect(
        store.clone(),
        associations(&host_b_node),
        host_b_node.clone(),
        BTreeSet::from([domain_b.clone()]),
        config(),
    )
    .await
    .unwrap();

    assert!(matches!(
        host_a.scope_state(&CoordinatorScope::Membership),
        Some(CoordinatorHostScopeState::Active(_))
    ));
    assert!(matches!(
        host_b.scope_state(&CoordinatorScope::Membership),
        Some(CoordinatorHostScopeState::Standby)
    ));
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Placement(domain_a))
            .await
            .unwrap()
            .unwrap()
            .node,
        host_a_node
    );
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Placement(domain_b))
            .await
            .unwrap()
            .unwrap()
            .node,
        host_b_node
    );
}

#[tokio::test]
async fn losing_one_domain_lease_reenters_only_that_election() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let local = node("host", 3, 33103);
    let domain_a = PlacementDomainId::new("isolated-a").unwrap();
    let domain_b = PlacementDomainId::new("isolated-b").unwrap();
    let host = CoordinatorHost::elect(
        store.clone(),
        associations(&local),
        local,
        BTreeSet::from([domain_a.clone(), domain_b.clone()]),
        config(),
    )
    .await
    .unwrap();
    let lost_lease = host.domains[&domain_a]
        .leader
        .as_ref()
        .unwrap()
        .leader_lease_id;
    let original_a = store
        .get_leader(&CoordinatorScope::Placement(domain_a.clone()))
        .await
        .unwrap()
        .unwrap();
    let original_b = store
        .get_leader(&CoordinatorScope::Placement(domain_b.clone()))
        .await
        .unwrap()
        .unwrap();
    let domain_a_scope = CoordinatorScope::Placement(domain_a.clone());
    let mut scope_states = host.subscribe_scope_states();
    let (_router, controls) =
        PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let (stop, stop_rx) = watch::channel(false);
    let task = tokio::spawn(host.run(controls, stop_rx));
    store.revoke_lease(lost_lease).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let reelected = matches!(
                scope_states.borrow_and_update().get(&domain_a_scope),
                Some(CoordinatorHostScopeState::Active(leader))
                    if leader.term > original_a.term
            );
            if reelected {
                break;
            }
            scope_states.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    assert!(
        store
            .get_leader(&CoordinatorScope::Placement(domain_a))
            .await
            .unwrap()
            .is_some_and(|leader| leader.term > original_a.term)
    );
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Placement(domain_b))
            .await
            .unwrap()
            .unwrap(),
        original_b
    );
    assert!(
        store
            .get_leader(&CoordinatorScope::Membership)
            .await
            .unwrap()
            .is_some()
    );
    let _ = stop.send(true);
    task.await.unwrap().unwrap();
}
