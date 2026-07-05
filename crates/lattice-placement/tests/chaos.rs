use std::collections::BTreeMap;

use lattice_core::instance::InstanceCapacity;
use lattice_core::{ActorId, Epoch, InstanceId, actor_kind, service_kind};
use lattice_placement::coordinator::{FailoverReport, NoopLogicControl, PlacementCoordinator};
use lattice_placement::instance::{InstanceRecord, InstanceState};
use lattice_placement::store::{
    ActorPlacementKey, ActorPlacementRecord, InMemoryPlacementStore, LeaseId, PlacementPrefix,
    PlacementState, PlacementStore, SingletonKey, SingletonPlacementRecord,
};

#[tokio::test]
async fn node_crash_lease_expiry_reassigns_owned_actors_with_new_epoch() {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/chaos"));
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    store
        .compare_and_put_actor(key.clone(), None, actor_record(7, "world-a", 3, LeaseId(9)))
        .await
        .unwrap();
    let singleton_key = SingletonKey {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: "global".to_string(),
    };
    store
        .compare_and_put_singleton(
            singleton_key.clone(),
            None,
            singleton_record("global", "world-a", 5, LeaseId(11)),
        )
        .await
        .unwrap();
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);

    let report = coordinator
        .failover_expired_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await
        .unwrap();
    let reassigned = store.get_actor(&key).await.unwrap().unwrap().1;
    let reassigned_singleton = store
        .get_singleton(&singleton_key)
        .await
        .unwrap()
        .unwrap()
        .1;

    assert_eq!(
        report,
        FailoverReport {
            failed_instance: InstanceId::new("world-a"),
            reassigned_actors: 1,
            reassigned_singletons: 1,
        }
    );
    assert_eq!(reassigned.owner, InstanceId::new("world-b"));
    assert_eq!(reassigned.epoch, Epoch(4));
    assert_eq!(reassigned.lease_id, LeaseId(10));
    assert_eq!(reassigned_singleton.owner, InstanceId::new("world-b"));
    assert_eq!(reassigned_singleton.epoch, Epoch(6));
    assert_eq!(
        store.instance_lease_keepalive_count(reassigned_singleton.lease_id),
        Some(0)
    );
}

fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
        lease_id: LeaseId(1),
        advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
        control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
        version: "test".to_string(),
        state,
        capacity: InstanceCapacity::default(),
        labels: BTreeMap::new(),
    }
}

fn actor_record(actor_id: u64, owner: &str, epoch: u64, lease_id: LeaseId) -> ActorPlacementRecord {
    ActorPlacementRecord {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id,
        state: PlacementState::Running,
    }
}

fn singleton_record(
    scope: &str,
    owner: &str,
    epoch: u64,
    lease_id: LeaseId,
) -> SingletonPlacementRecord {
    SingletonPlacementRecord {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: scope.to_string(),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id,
        state: PlacementState::Running,
    }
}
