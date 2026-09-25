use super::{TestHelloSpec, attach_test_session, group, node, register_up, test_hello};
use crate::{
    authority::{AuthorityEvent, PlacementAuthority},
    candidate_fixture::elect_group,
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlCommand, decode_control_command},
    region::EntityConfig,
    runtime::GroupCoordinatorConfig,
    storage::{
        ActorGroupStore, CoordinatorLeaseStore,
        etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
        records::DurableStorageLimits,
    },
    types::{CoordinatorTerm, MonotonicTime, PlacementSlotKey, ShardId},
};
use lattice_model::{
    actor::ProtocolId,
    cluster::{ClusterId, CoordinatorScope, EntityType},
};
use lattice_remoting::{
    association::{AssociationManager, LaneKind},
    config::RemotingConfig,
};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn etcd_correlated_grant_deducts_ttl_uncertainty_and_never_uses_receive_time() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let store = Arc::new(
        EtcdCoordinationStore::connect(EtcdCoordinationConfig {
            endpoints,
            cluster_prefix: format!("/lattice-grant-test/{}", uuid::Uuid::new_v4().simple()),
            list_page_size: 8,
            limits: DurableStorageLimits {
                maximum_slots: 8,
                maximum_plans: 8,
                maximum_members: 8,
                maximum_admin_operations: 8,
                maximum_entity_configs: 8,
                maximum_singleton_configs: 8,
            },
            connect_options: None,
        })
        .await
        .unwrap(),
    );
    let cluster = ClusterId::new("etcd-correlated-grant").unwrap();
    let (coordinator, _) = node(&cluster, "coordinator", 34900, 34900);
    let (owner, _) = node(&cluster, "owner", 34901, 34901);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let association_key = attach_test_session(
        &associations,
        &cluster,
        coordinator.incarnation,
        &owner,
        34900,
    );
    let association = associations.get(&association_key).unwrap();
    let mut outgoing = association.take_lane_receiver(LaneKind::Control).unwrap();
    let mut leader = elect_group(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("grant-entity").unwrap();
    let entity = EntityConfig::new(
        group(),
        entity_type.clone(),
        ProtocolId::new(991).unwrap(),
        8,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    register_up(
        &mut leader,
        test_hello(
            owner.clone(),
            TestHelloSpec {
                capacity_units: 1,
                hosted_entity_types: [entity_type.clone()].into_iter().collect(),
                entity_configs: vec![entity],
                ..TestHelloSpec::default()
            },
        ),
        association_key.clone(),
    )
    .await;
    leader
        .ensure_shard_allocated(entity_type.clone(), ShardId::new(0))
        .await
        .unwrap();
    let key = PlacementSlotKey::Shard {
        group: group(),
        entity_type,
        shard_id: ShardId::new(0),
    };
    let slot = store.get_slot(&key).await.unwrap().unwrap();
    while outgoing.try_recv().is_ok() {}
    let mut authority = PlacementAuthority::new(owner, Duration::from_millis(100)).unwrap();
    authority
        .transition(AuthorityEvent::ReconcileSlot(slot.clone()))
        .unwrap();
    let request_started = Instant::now();
    authority
        .transition(AuthorityEvent::BeginRenewal {
            request_id: 77,
            now: MonotonicTime::from_millis(0),
        })
        .unwrap();
    // Time before reaching the coordinator and after its commit both consume the
    // owner's original request interval, rather than creating a fresh TTL on receive.
    tokio::time::sleep(Duration::from_millis(75)).await;
    leader
        .issue_requested_claim(
            &association_key,
            77,
            key.clone(),
            slot.assignment_generation,
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(75)).await;
    let frame = outgoing.recv().await.unwrap();
    let PlacementControlCommand::ClaimGranted(grant) =
        decode_control_command(frame.payload(), DEFAULT_MAX_CONTROL_PAYLOAD)
            .unwrap()
            .command
    else {
        panic!("expected correlated grant")
    };
    assert_eq!(grant.request_id, 77);
    assert!(
        grant.ttl < Duration::from_secs(14),
        "integer TTL, rate and resolution slack must be deducted"
    );
    assert!(grant.ttl > Duration::from_secs(5));
    assert_eq!(store.get_claim(&key).await.unwrap().unwrap().grant, grant);
    let received = MonotonicTime::from_millis(request_started.elapsed().as_millis() as u64);
    authority
        .transition(AuthorityEvent::InstallGrant {
            grant: grant.clone(),
            now: received,
        })
        .unwrap();
    let deadline = grant.ttl.as_millis() as u64 - 100;
    assert!(authority.admission_open_at(MonotonicTime::from_millis(deadline - 1)));
    assert!(!authority.admission_open_at(MonotonicTime::from_millis(deadline)));
    assert!(
        authority
            .transition(AuthorityEvent::InstallGrant {
                grant: grant.clone(),
                now: received
            })
            .is_err()
    );
    store
        .revoke_lease(leader.leader.candidate_lease_id)
        .await
        .unwrap();
    assert!(
        leader
            .issue_requested_claim(
                &association_key,
                78,
                key.clone(),
                slot.assignment_generation
            )
            .await
            .is_err()
    );
    assert_eq!(store.get_claim(&key).await.unwrap().unwrap().grant, grant);
    assert!(outgoing.try_recv().is_err());
}
