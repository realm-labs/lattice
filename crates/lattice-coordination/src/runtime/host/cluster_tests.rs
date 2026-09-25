use std::{collections::BTreeSet, sync::Arc, time::Duration};

use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, NodeEndpoint, NodeIncarnation},
};
use lattice_remoting::{association::AssociationManager, config::RemotingConfig};
use tokio::sync::{mpsc, watch};

use super::{
    ClusterCoordinatorConfig, CoordinatorHost, CoordinatorHostConfig, CoordinatorHostScopeState,
    GroupCoordinatorConfig, election::candidate_delay_duration,
};
use crate::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter},
    storage::{CoordinatorLeaseStore, InMemoryCoordinationStore, ScopedElectionStore},
    types::NodeKey,
};

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeEndpoint::new("127.0.0.1", port).unwrap(),
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
        cluster: ClusterCoordinatorConfig {
            leader_lease_ttl: Duration::from_millis(500),
            member_lease_ttl: Duration::from_millis(500),
            renewal_interval: Duration::from_millis(50),
            ..ClusterCoordinatorConfig::default()
        },
        group: GroupCoordinatorConfig {
            leader_lease_ttl: Duration::from_millis(500),
            member_lease_ttl: Duration::from_millis(500),
            claim_ttl: Duration::from_millis(500),
            renewal_interval: Duration::from_millis(50),
            ..GroupCoordinatorConfig::default()
        },
        renewal_interval: Duration::from_millis(50),
        ..CoordinatorHostConfig::default()
    }
}

#[tokio::test(start_paused = true)]
async fn panicked_group_is_unadvertised_and_recampaigned_without_losing_other_scopes() {
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
    let local = node("panic-host", 90, 33190);
    let failed = ActorGroupId::new("failed-group").unwrap();
    let healthy = ActorGroupId::new("healthy-group").unwrap();
    let mut host = CoordinatorHost::elect(
        store.clone(),
        associations(&local),
        local,
        BTreeSet::from([failed.clone(), healthy.clone()]),
        CoordinatorHostConfig {
            election_interval: Duration::from_millis(50),
            maximum_candidate_jitter: Duration::ZERO,
            ..config()
        },
    )
    .await
    .unwrap();
    let failed_scope = CoordinatorScope::Group(failed.clone());
    let healthy_scope = CoordinatorScope::Group(healthy);
    let initial_directory = host.subscribe_directory().borrow().clone();
    let initial_term = initial_directory[&failed_scope].term;
    let failed_lease = host.groups[&failed]
        .leader
        .as_ref()
        .unwrap()
        .leader_lease_id;
    // Inject a panic in this task's interval construction after election. The host's
    // configuration stays valid, so a replacement leader can run normally.
    host.groups
        .get_mut(&failed)
        .unwrap()
        .leader
        .as_mut()
        .unwrap()
        .config
        .rebalance_interval = Duration::ZERO;
    let mut directory = host.subscribe_directory();
    let (_controls, receiver) = mpsc::channel(8);
    let (stop, stop_rx) = watch::channel(false);
    let task = tokio::spawn(host.run(receiver, stop_rx));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            directory.changed().await.unwrap();
            let entries = directory.borrow_and_update();
            assert_eq!(
                entries.get(&healthy_scope),
                initial_directory.get(&healthy_scope)
            );
            assert_eq!(
                entries.get(&CoordinatorScope::Cluster),
                initial_directory.get(&CoordinatorScope::Cluster)
            );
            if !entries.contains_key(&failed_scope) {
                break;
            }
        }
        // InMemoryCoordinationStore does not expire leases with Tokio's clock. After
        // observing withdrawal, explicitly model expiry of the panicked leader's lease.
        store.revoke_lease(failed_lease).await.unwrap();
        loop {
            directory.changed().await.unwrap();
            if directory
                .borrow_and_update()
                .get(&failed_scope)
                .is_some_and(|record| record.term > initial_term)
            {
                break;
            }
        }
    })
    .await
    .expect("failed group must leave the directory and recover");
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn placement_group_campaigns_run_concurrently_off_the_host_loop() {
    let store = Arc::new(InMemoryCoordinationStore::new(64, 64).unwrap());
    let holder = node("campaign-holder", 40, 33140);
    let candidate = node("campaign-candidate", 41, 33141);
    let groups = (0..16)
        .map(|index| ActorGroupId::new(format!("campaign-{index}")).unwrap())
        .collect::<BTreeSet<_>>();
    let config = CoordinatorHostConfig {
        cluster: ClusterCoordinatorConfig {
            leader_lease_ttl: Duration::from_secs(30),
            member_lease_ttl: Duration::from_secs(30),
            renewal_interval: Duration::from_secs(5),
            ..ClusterCoordinatorConfig::default()
        },
        group: GroupCoordinatorConfig {
            leader_lease_ttl: Duration::from_secs(30),
            member_lease_ttl: Duration::from_secs(30),
            claim_ttl: Duration::from_secs(15),
            renewal_interval: Duration::from_secs(5),
            ..GroupCoordinatorConfig::default()
        },
        maximum_candidate_jitter: Duration::from_millis(300),
        ..CoordinatorHostConfig::default()
    };
    let delays = groups
        .iter()
        .map(|group| {
            candidate_delay_duration(
                &CoordinatorScope::Group(group.clone()),
                &candidate,
                config.maximum_candidate_jitter,
            )
        })
        .collect::<Vec<_>>();
    let serialized = delays.iter().copied().sum::<Duration>();
    let concurrent = delays.iter().copied().max().unwrap()
        + candidate_delay_duration(
            &CoordinatorScope::Cluster,
            &candidate,
            config.maximum_candidate_jitter,
        );
    assert!(serialized > concurrent * 2);

    let holder_host = CoordinatorHost::elect(
        store.clone(),
        associations(&holder),
        holder,
        groups.clone(),
        config.clone(),
    )
    .await
    .unwrap();
    let candidate_host = CoordinatorHost::elect(
        store.clone(),
        associations(&candidate),
        candidate,
        groups.clone(),
        config,
    )
    .await
    .unwrap();
    let mut scope_states = candidate_host.subscribe_scope_states();
    for hosted in holder_host.groups.values() {
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
                matches!(scope, CoordinatorScope::Group(_))
                    && matches!(state, CoordinatorHostScopeState::Active(_))
            })
            .count();
        if elected == groups.len() {
            break;
        }
        scope_states.changed().await.unwrap();
    }
    let settled = started.elapsed();

    assert!(
        settled <= concurrent,
        "{} campaigns took {settled:?}; concurrent campaigning bounds them by {concurrent:?} \
         while serial campaigning costs {serialized:?}",
        groups.len()
    );
    let _ = stop.send(true);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn dedicated_membership_host_needs_no_placement_groups() {
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
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

    assert!(host.groups.is_empty());
    assert!(matches!(
        host.scope_state(&CoordinatorScope::Cluster),
        Some(CoordinatorHostScopeState::Active(_))
    ));
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Cluster)
            .await
            .unwrap()
            .unwrap()
            .node,
        local
    );
}

#[tokio::test]
async fn competing_hosts_produce_exactly_one_active_leader_per_group() {
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
    let group = ActorGroupId::new("single-leader-group").unwrap();
    let first = node("first-candidate", 11, 33011);
    let second = node("second-candidate", 12, 33012);
    let first_host = CoordinatorHost::elect(
        store.clone(),
        associations(&first),
        first.clone(),
        BTreeSet::from([group.clone()]),
        config(),
    )
    .await
    .unwrap();
    let second_host = CoordinatorHost::elect(
        store.clone(),
        associations(&second),
        second,
        BTreeSet::from([group.clone()]),
        config(),
    )
    .await
    .unwrap();

    assert!(matches!(
        first_host.scope_state(&CoordinatorScope::Group(group.clone())),
        Some(CoordinatorHostScopeState::Active(_))
    ));
    assert!(matches!(
        second_host.scope_state(&CoordinatorScope::Group(group.clone())),
        Some(CoordinatorHostScopeState::Standby)
    ));
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Group(group))
            .await
            .unwrap()
            .unwrap()
            .node,
        first
    );
}

#[tokio::test]
async fn different_hosts_can_lead_different_groups_concurrently() {
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
    let host_a_node = node("host-a", 1, 33001);
    let host_b_node = node("host-b", 2, 33002);
    let group_a = ActorGroupId::new("group-a").unwrap();
    let group_b = ActorGroupId::new("group-b").unwrap();
    let host_a = CoordinatorHost::elect(
        store.clone(),
        associations(&host_a_node),
        host_a_node.clone(),
        BTreeSet::from([group_a.clone()]),
        config(),
    )
    .await
    .unwrap();
    let host_b = CoordinatorHost::elect(
        store.clone(),
        associations(&host_b_node),
        host_b_node.clone(),
        BTreeSet::from([group_b.clone()]),
        config(),
    )
    .await
    .unwrap();

    assert!(matches!(
        host_a.scope_state(&CoordinatorScope::Cluster),
        Some(CoordinatorHostScopeState::Active(_))
    ));
    assert!(matches!(
        host_b.scope_state(&CoordinatorScope::Cluster),
        Some(CoordinatorHostScopeState::Standby)
    ));
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Group(group_a))
            .await
            .unwrap()
            .unwrap()
            .node,
        host_a_node
    );
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Group(group_b))
            .await
            .unwrap()
            .unwrap()
            .node,
        host_b_node
    );
}

#[tokio::test]
async fn losing_one_group_lease_reenters_only_that_election() {
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
    let local = node("host", 3, 33103);
    let group_a = ActorGroupId::new("isolated-a").unwrap();
    let group_b = ActorGroupId::new("isolated-b").unwrap();
    let host = CoordinatorHost::elect(
        store.clone(),
        associations(&local),
        local,
        BTreeSet::from([group_a.clone(), group_b.clone()]),
        config(),
    )
    .await
    .unwrap();
    let lost_lease = host.groups[&group_a]
        .leader
        .as_ref()
        .unwrap()
        .leader_lease_id;
    let original_a = store
        .get_leader(&CoordinatorScope::Group(group_a.clone()))
        .await
        .unwrap()
        .unwrap();
    let original_b = store
        .get_leader(&CoordinatorScope::Group(group_b.clone()))
        .await
        .unwrap()
        .unwrap();
    let group_a_scope = CoordinatorScope::Group(group_a.clone());
    let mut scope_states = host.subscribe_scope_states();
    let (_router, controls) =
        PlacementControlRouter::bounded(32, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let (stop, stop_rx) = watch::channel(false);
    let task = tokio::spawn(host.run(controls, stop_rx));
    store.revoke_lease(lost_lease).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let reelected = matches!(
                scope_states.borrow_and_update().get(&group_a_scope),
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
            .get_leader(&CoordinatorScope::Group(group_a))
            .await
            .unwrap()
            .is_some_and(|leader| leader.term > original_a.term)
    );
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Group(group_b))
            .await
            .unwrap()
            .unwrap(),
        original_b
    );
    assert!(
        store
            .get_leader(&CoordinatorScope::Cluster)
            .await
            .unwrap()
            .is_some()
    );
    let _ = stop.send(true);
    task.await.unwrap().unwrap();
}
