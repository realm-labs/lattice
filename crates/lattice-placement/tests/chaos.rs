use std::collections::BTreeMap;

use lattice_core::{ActorId, Epoch, InstanceCapacity, InstanceId, actor_kind, service_kind};
use lattice_placement::{
    ActorPlacementKey, ActorPlacementRecord, FailoverReport, InMemoryPlacementStore,
    InstanceRecord, InstanceState, LeaseId, NoopLogicControl, PlacementCoordinator,
    PlacementPrefix, PlacementState, PlacementStore,
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
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);

    let report = coordinator
        .failover_expired_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await
        .unwrap();
    let reassigned = store.get_actor(&key).await.unwrap().unwrap().1;

    assert_eq!(
        report,
        FailoverReport {
            failed_instance: InstanceId::new("world-a"),
            reassigned_actors: 1,
        }
    );
    assert_eq!(reassigned.owner, InstanceId::new("world-b"));
    assert_eq!(reassigned.epoch, Epoch(4));
    assert_eq!(reassigned.lease_id, LeaseId(10));
}

fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
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
