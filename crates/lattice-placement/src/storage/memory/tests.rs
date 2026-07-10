use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::{actor_kind, service_kind};

use super::InMemoryPlacementStore;
use crate::error::PlacementError;
use crate::sharding::VirtualShardId;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, LeaseId, PlacementEpochKey, PlacementPrefix,
    PlacementState, PlacementStore, PlacementVersion, SingletonKey, SingletonPlacementRecord,
    VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

const PREFIX: &str = "/lattice/memory-epoch-tests";

#[tokio::test]
async fn durable_floors_survive_delete_and_shared_store_restart_for_every_family() {
    let store = store();
    let actor_key = actor_key(1);
    let shard_key = shard_key(1);
    let singleton_key = singleton_key("global");

    let actor_reservation = store
        .reserve_actor_epoch(actor_key.clone(), None, None)
        .await
        .unwrap();
    assert_eq!(actor_reservation.epoch(), Epoch(1));
    let old_actor_token = store
        .commit_actor_epoch(
            actor_reservation,
            actor_record(1, "world-a", 1, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();

    let shard_reservation = store
        .reserve_virtual_shard_epoch(shard_key.clone(), None)
        .await
        .unwrap();
    assert_eq!(shard_reservation.epoch(), Epoch(1));
    store
        .commit_virtual_shard_epoch(shard_reservation, shard_record(1, "world-a", 1))
        .await
        .unwrap();

    let singleton_reservation = store
        .reserve_singleton_epoch(singleton_key.clone(), None, None)
        .await
        .unwrap();
    assert_eq!(singleton_reservation.epoch(), Epoch(1));
    store
        .commit_singleton_epoch(
            singleton_reservation,
            singleton_record("global", "world-a", 1, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();

    assert!(store.remove_actor_for_test(&actor_key).is_some());
    assert!(store.remove_virtual_shard_for_test(&shard_key).is_some());
    assert!(store.remove_singleton_for_test(&singleton_key).is_some());
    assert!(matches!(
        store
            .reserve_actor_epoch(actor_key.clone(), Some(old_actor_token), None)
            .await,
        Err(PlacementError::CompareAndPutFailed)
    ));

    let restarted = InMemoryPlacementStore::with_shared_inner(PlacementPrefix::new(PREFIX), &store);
    let actor_reservation = restarted
        .reserve_actor_epoch(actor_key.clone(), None, None)
        .await
        .unwrap();
    let shard_reservation = restarted
        .reserve_virtual_shard_epoch(shard_key.clone(), None)
        .await
        .unwrap();
    let singleton_reservation = restarted
        .reserve_singleton_epoch(singleton_key.clone(), None, None)
        .await
        .unwrap();
    assert_eq!(actor_reservation.epoch(), Epoch(2));
    assert_eq!(shard_reservation.epoch(), Epoch(2));
    assert_eq!(singleton_reservation.epoch(), Epoch(2));

    restarted
        .commit_actor_epoch(
            actor_reservation,
            actor_record(1, "world-b", 2, LeaseId(20), PlacementState::Running),
        )
        .await
        .unwrap();
    restarted
        .commit_virtual_shard_epoch(shard_reservation, shard_record(1, "world-b", 2))
        .await
        .unwrap();
    restarted
        .commit_singleton_epoch(
            singleton_reservation,
            singleton_record("global", "world-b", 2, LeaseId(20), PlacementState::Running),
        )
        .await
        .unwrap();

    assert_eq!(
        restarted
            .get_actor(&actor_key)
            .await
            .unwrap()
            .unwrap()
            .1
            .epoch,
        Epoch(2)
    );
    assert_eq!(
        restarted
            .get_virtual_shard(&shard_key)
            .await
            .unwrap()
            .unwrap()
            .1
            .epoch,
        Epoch(2)
    );
    assert_eq!(
        restarted
            .get_singleton(&singleton_key)
            .await
            .unwrap()
            .unwrap()
            .1
            .epoch,
        Epoch(2)
    );
    for key in [
        PlacementEpochKey::Actor(actor_key),
        PlacementEpochKey::VirtualShard(shard_key),
        PlacementEpochKey::Singleton(singleton_key),
    ] {
        assert_eq!(
            restarted.epoch_floor_for_test(&key).unwrap().1.epoch,
            Epoch(2)
        );
    }
}

#[tokio::test]
async fn legacy_create_cannot_bypass_retained_floors_after_deletion() {
    let store = store();
    let actor_key = actor_key(1);
    let shard_key = shard_key(1);
    let singleton_key = singleton_key("global");
    store
        .compare_and_put_actor(
            actor_key.clone(),
            None,
            actor_record(1, "world-a", 5, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();
    store
        .compare_and_put_virtual_shard(shard_key.clone(), None, shard_record(1, "world-a", 5))
        .await
        .unwrap();
    store
        .compare_and_put_singleton(
            singleton_key.clone(),
            None,
            singleton_record("global", "world-a", 5, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();
    store.remove_actor_for_test(&actor_key).unwrap();
    store.remove_virtual_shard_for_test(&shard_key).unwrap();
    store.remove_singleton_for_test(&singleton_key).unwrap();

    assert_eq!(
        store
            .compare_and_put_actor(
                actor_key.clone(),
                None,
                actor_record(1, "world-b", 5, LeaseId(20), PlacementState::Running),
            )
            .await,
        Err(PlacementError::EpochRegression {
            current: Epoch(5),
            incoming: Epoch(5),
        })
    );
    assert_eq!(
        store
            .compare_and_put_virtual_shard(shard_key.clone(), None, shard_record(1, "world-b", 4),)
            .await,
        Err(PlacementError::EpochRegression {
            current: Epoch(5),
            incoming: Epoch(4),
        })
    );
    assert_eq!(
        store
            .compare_and_put_singleton(
                singleton_key.clone(),
                None,
                singleton_record("global", "world-b", 5, LeaseId(20), PlacementState::Running,),
            )
            .await,
        Err(PlacementError::EpochRegression {
            current: Epoch(5),
            incoming: Epoch(5),
        })
    );
    assert!(store.get_actor(&actor_key).await.unwrap().is_none());
    assert!(store.get_virtual_shard(&shard_key).await.unwrap().is_none());
    assert!(store.get_singleton(&singleton_key).await.unwrap().is_none());

    store
        .compare_and_put_actor(
            actor_key,
            None,
            actor_record(1, "world-b", 7, LeaseId(20), PlacementState::Running),
        )
        .await
        .unwrap();
    store
        .compare_and_put_virtual_shard(shard_key, None, shard_record(1, "world-b", 7))
        .await
        .unwrap();
    store
        .compare_and_put_singleton(
            singleton_key,
            None,
            singleton_record("global", "world-b", 7, LeaseId(20), PlacementState::Running),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn live_records_without_floors_fail_closed_without_mutation_for_every_family() {
    let store = store();
    let actor_key = actor_key(11);
    let shard_key = shard_key(11);
    let singleton_key = singleton_key("missing-floor");
    let actor = actor_record(11, "world-a", 1, LeaseId(10), PlacementState::Running);
    let shard = shard_record(11, "world-a", 1);
    let singleton = singleton_record(
        "missing-floor",
        "world-a",
        1,
        LeaseId(10),
        PlacementState::Running,
    );
    let actor_token = store
        .compare_and_put_actor(actor_key.clone(), None, actor.clone())
        .await
        .unwrap();
    let shard_token = store
        .compare_and_put_virtual_shard(shard_key.clone(), None, shard.clone())
        .await
        .unwrap();
    let singleton_token = store
        .compare_and_put_singleton(singleton_key.clone(), None, singleton.clone())
        .await
        .unwrap();
    remove_floor(&store, &PlacementEpochKey::Actor(actor_key.clone()));
    remove_floor(&store, &PlacementEpochKey::VirtualShard(shard_key.clone()));
    remove_floor(&store, &PlacementEpochKey::Singleton(singleton_key.clone()));
    let revision = placement_revision(&store);

    assert_eq!(
        store
            .reserve_actor_epoch(actor_key.clone(), Some(actor_token), None)
            .await
            .unwrap_err(),
        PlacementError::EpochFloorUnproven {
            record: actor_token,
            floor: None,
        }
    );
    assert_eq!(
        store
            .compare_and_put_actor(
                actor_key.clone(),
                Some(actor_token),
                ActorPlacementRecord {
                    state: PlacementState::Draining,
                    ..actor.clone()
                },
            )
            .await,
        Err(PlacementError::EpochFloorUnproven {
            record: actor_token,
            floor: None,
        })
    );
    assert_eq!(
        store
            .reserve_virtual_shard_epoch(shard_key.clone(), Some(shard_token))
            .await
            .unwrap_err(),
        PlacementError::EpochFloorUnproven {
            record: shard_token,
            floor: None,
        }
    );
    assert_eq!(
        store
            .compare_and_put_virtual_shard(shard_key.clone(), Some(shard_token), shard.clone())
            .await,
        Err(PlacementError::EpochFloorUnproven {
            record: shard_token,
            floor: None,
        })
    );
    assert_eq!(
        store
            .reserve_singleton_epoch(singleton_key.clone(), Some(singleton_token), None)
            .await
            .unwrap_err(),
        PlacementError::EpochFloorUnproven {
            record: singleton_token,
            floor: None,
        }
    );
    assert_eq!(
        store
            .compare_and_put_singleton(
                singleton_key.clone(),
                Some(singleton_token),
                SingletonPlacementRecord {
                    state: PlacementState::Draining,
                    ..singleton.clone()
                },
            )
            .await,
        Err(PlacementError::EpochFloorUnproven {
            record: singleton_token,
            floor: None,
        })
    );

    assert_eq!(
        store.get_actor(&actor_key).await.unwrap(),
        Some((actor_token, actor))
    );
    assert_eq!(
        store.get_virtual_shard(&shard_key).await.unwrap(),
        Some((shard_token, shard))
    );
    assert_eq!(
        store.get_singleton(&singleton_key).await.unwrap(),
        Some((singleton_token, singleton))
    );
    assert_eq!(placement_revision(&store), revision);
}

#[tokio::test]
async fn record_only_replays_cannot_be_laundered_for_every_family() {
    let store = store();
    let actor_key = actor_key(12);
    let shard_key = shard_key(12);
    let singleton_key = singleton_key("replayed");
    let original_actor = actor_record(12, "world-a", 1, LeaseId(10), PlacementState::Running);
    let original_shard = shard_record(12, "world-a", 1);
    let original_singleton = singleton_record(
        "replayed",
        "world-a",
        1,
        LeaseId(10),
        PlacementState::Running,
    );
    let original_actor_token = store
        .compare_and_put_actor(actor_key.clone(), None, original_actor.clone())
        .await
        .unwrap();
    let original_shard_token = store
        .compare_and_put_virtual_shard(shard_key.clone(), None, original_shard.clone())
        .await
        .unwrap();
    let original_singleton_token = store
        .compare_and_put_singleton(singleton_key.clone(), None, original_singleton.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .reserve_actor_epoch(actor_key.clone(), Some(original_actor_token), None)
            .await
            .unwrap()
            .epoch(),
        Epoch(2)
    );
    assert_eq!(
        store
            .reserve_virtual_shard_epoch(shard_key.clone(), Some(original_shard_token))
            .await
            .unwrap()
            .epoch(),
        Epoch(2)
    );
    assert_eq!(
        store
            .reserve_singleton_epoch(singleton_key.clone(), Some(original_singleton_token), None,)
            .await
            .unwrap()
            .epoch(),
        Epoch(2)
    );
    let actor_floor = store
        .epoch_floor_for_test(&PlacementEpochKey::Actor(actor_key.clone()))
        .unwrap();
    let shard_floor = store
        .epoch_floor_for_test(&PlacementEpochKey::VirtualShard(shard_key.clone()))
        .unwrap();
    let singleton_floor = store
        .epoch_floor_for_test(&PlacementEpochKey::Singleton(singleton_key.clone()))
        .unwrap();
    assert_eq!(actor_floor.1.epoch, Epoch(2));
    assert_eq!(shard_floor.1.epoch, Epoch(2));
    assert_eq!(singleton_floor.1.epoch, Epoch(2));

    let replayed_actor = ActorPlacementRecord {
        owner: InstanceId::new("world-b"),
        lease_id: LeaseId(20),
        ..original_actor
    };
    let replayed_shard = VirtualShardPlacementRecord {
        owner: InstanceId::new("world-b"),
        ..original_shard
    };
    let replayed_singleton = SingletonPlacementRecord {
        owner: InstanceId::new("world-b"),
        lease_id: LeaseId(20),
        ..original_singleton
    };
    let actor_token = put_actor_record_only(&store, actor_key.clone(), replayed_actor.clone());
    let shard_token = put_shard_record_only(&store, shard_key.clone(), replayed_shard.clone());
    let singleton_token =
        put_singleton_record_only(&store, singleton_key.clone(), replayed_singleton.clone());
    assert!(actor_floor.0.modification_revision() < actor_token.modification_revision());
    assert!(shard_floor.0.modification_revision() < shard_token.modification_revision());
    assert!(singleton_floor.0.modification_revision() < singleton_token.modification_revision());
    let revision = placement_revision(&store);

    assert_eq!(
        store
            .reserve_actor_epoch(actor_key.clone(), Some(actor_token), None)
            .await
            .unwrap_err(),
        PlacementError::EpochFloorUnproven {
            record: actor_token,
            floor: Some(actor_floor.0),
        }
    );
    assert_eq!(
        store
            .compare_and_put_actor(
                actor_key.clone(),
                Some(actor_token),
                ActorPlacementRecord {
                    epoch: Epoch(3),
                    ..replayed_actor.clone()
                },
            )
            .await,
        Err(PlacementError::EpochFloorUnproven {
            record: actor_token,
            floor: Some(actor_floor.0),
        })
    );
    assert_eq!(
        store
            .reserve_virtual_shard_epoch(shard_key.clone(), Some(shard_token))
            .await
            .unwrap_err(),
        PlacementError::EpochFloorUnproven {
            record: shard_token,
            floor: Some(shard_floor.0),
        }
    );
    assert_eq!(
        store
            .compare_and_put_virtual_shard(
                shard_key.clone(),
                Some(shard_token),
                VirtualShardPlacementRecord {
                    epoch: Epoch(3),
                    ..replayed_shard.clone()
                },
            )
            .await,
        Err(PlacementError::EpochFloorUnproven {
            record: shard_token,
            floor: Some(shard_floor.0),
        })
    );
    assert_eq!(
        store
            .reserve_singleton_epoch(singleton_key.clone(), Some(singleton_token), None)
            .await
            .unwrap_err(),
        PlacementError::EpochFloorUnproven {
            record: singleton_token,
            floor: Some(singleton_floor.0),
        }
    );
    assert_eq!(
        store
            .compare_and_put_singleton(
                singleton_key.clone(),
                Some(singleton_token),
                SingletonPlacementRecord {
                    epoch: Epoch(3),
                    ..replayed_singleton.clone()
                },
            )
            .await,
        Err(PlacementError::EpochFloorUnproven {
            record: singleton_token,
            floor: Some(singleton_floor.0),
        })
    );

    assert_eq!(
        store.get_actor(&actor_key).await.unwrap(),
        Some((actor_token, replayed_actor))
    );
    assert_eq!(
        store.get_virtual_shard(&shard_key).await.unwrap(),
        Some((shard_token, replayed_shard))
    );
    assert_eq!(
        store.get_singleton(&singleton_key).await.unwrap(),
        Some((singleton_token, replayed_singleton))
    );
    assert_eq!(
        store
            .epoch_floor_for_test(&PlacementEpochKey::Actor(actor_key))
            .unwrap(),
        actor_floor
    );
    assert_eq!(
        store
            .epoch_floor_for_test(&PlacementEpochKey::VirtualShard(shard_key))
            .unwrap(),
        shard_floor
    );
    assert_eq!(
        store
            .epoch_floor_for_test(&PlacementEpochKey::Singleton(singleton_key))
            .unwrap(),
        singleton_floor
    );
    assert_eq!(placement_revision(&store), revision);
}

#[tokio::test]
async fn burned_reservation_lineage_remains_valid_for_every_family() {
    let store = store();
    let actor_key = actor_key(13);
    let shard_key = shard_key(13);
    let singleton_key = singleton_key("burned");
    let actor_token = store
        .compare_and_put_actor(
            actor_key.clone(),
            None,
            actor_record(13, "world-a", 1, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();
    let shard_token = store
        .compare_and_put_virtual_shard(shard_key.clone(), None, shard_record(13, "world-a", 1))
        .await
        .unwrap();
    let singleton_token = store
        .compare_and_put_singleton(
            singleton_key.clone(),
            None,
            singleton_record("burned", "world-a", 1, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .reserve_actor_epoch(actor_key.clone(), Some(actor_token), None)
            .await
            .unwrap()
            .epoch(),
        Epoch(2)
    );
    assert_eq!(
        store
            .reserve_virtual_shard_epoch(shard_key.clone(), Some(shard_token))
            .await
            .unwrap()
            .epoch(),
        Epoch(2)
    );
    assert_eq!(
        store
            .reserve_singleton_epoch(singleton_key.clone(), Some(singleton_token), None)
            .await
            .unwrap()
            .epoch(),
        Epoch(2)
    );
    assert_eq!(
        store
            .reserve_actor_epoch(actor_key.clone(), Some(actor_token), None)
            .await
            .unwrap()
            .epoch(),
        Epoch(3)
    );
    assert_eq!(
        store
            .reserve_virtual_shard_epoch(shard_key.clone(), Some(shard_token))
            .await
            .unwrap()
            .epoch(),
        Epoch(3)
    );
    assert_eq!(
        store
            .reserve_singleton_epoch(singleton_key.clone(), Some(singleton_token), None)
            .await
            .unwrap()
            .epoch(),
        Epoch(3)
    );

    store
        .compare_and_put_actor(
            actor_key.clone(),
            Some(actor_token),
            actor_record(13, "world-b", 4, LeaseId(20), PlacementState::Running),
        )
        .await
        .unwrap();
    store
        .compare_and_put_virtual_shard(
            shard_key.clone(),
            Some(shard_token),
            shard_record(13, "world-b", 4),
        )
        .await
        .unwrap();
    store
        .compare_and_put_singleton(
            singleton_key.clone(),
            Some(singleton_token),
            singleton_record("burned", "world-b", 4, LeaseId(20), PlacementState::Running),
        )
        .await
        .unwrap();

    assert_eq!(
        store.get_actor(&actor_key).await.unwrap().unwrap().1.epoch,
        Epoch(4)
    );
    assert_eq!(
        store
            .get_virtual_shard(&shard_key)
            .await
            .unwrap()
            .unwrap()
            .1
            .epoch,
        Epoch(4)
    );
    assert_eq!(
        store
            .get_singleton(&singleton_key)
            .await
            .unwrap()
            .unwrap()
            .1
            .epoch,
        Epoch(4)
    );
}

#[tokio::test]
async fn competing_reservations_burn_gaps_and_only_latest_floor_can_commit() {
    let store = store();
    let key = actor_key(7);
    let token = store
        .compare_and_put_actor(
            key.clone(),
            None,
            actor_record(7, "world-a", 1, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();

    let (first, second) = tokio::join!(
        store.reserve_actor_epoch(key.clone(), Some(token), None),
        store.reserve_actor_epoch(key.clone(), Some(token), None),
    );
    let mut reservations = vec![first.unwrap(), second.unwrap()];
    reservations.sort_by_key(|reservation| reservation.epoch().0);
    let older = reservations.remove(0);
    let latest = reservations.remove(0);
    assert_eq!(older.epoch(), Epoch(2));
    assert_eq!(latest.epoch(), Epoch(3));

    let older_epoch = older.epoch().0;
    assert_eq!(
        store
            .commit_actor_epoch(
                older,
                actor_record(
                    7,
                    "world-b",
                    older_epoch,
                    LeaseId(20),
                    PlacementState::Running,
                ),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );
    let latest_epoch = latest.epoch().0;
    let latest_token = store
        .commit_actor_epoch(
            latest,
            actor_record(
                7,
                "world-c",
                latest_epoch,
                LeaseId(30),
                PlacementState::Running,
            ),
        )
        .await
        .unwrap();
    assert_ne!(latest_token, token);
    assert!(matches!(
        store
            .reserve_actor_epoch(key.clone(), Some(token), None)
            .await,
        Err(PlacementError::CompareAndPutFailed)
    ));
    assert_eq!(
        store.get_actor(&key).await.unwrap().unwrap().1.epoch,
        Epoch(3)
    );

    let shard_key = shard_key(7);
    let shard_token = store
        .compare_and_put_virtual_shard(shard_key.clone(), None, shard_record(7, "world-a", 1))
        .await
        .unwrap();
    let (first, second) = tokio::join!(
        store.reserve_virtual_shard_epoch(shard_key.clone(), Some(shard_token)),
        store.reserve_virtual_shard_epoch(shard_key.clone(), Some(shard_token)),
    );
    let mut reservations = vec![first.unwrap(), second.unwrap()];
    reservations.sort_by_key(|reservation| reservation.epoch().0);
    let older = reservations.remove(0);
    let latest = reservations.remove(0);
    let older_epoch = older.epoch().0;
    assert_eq!(
        store
            .commit_virtual_shard_epoch(older, shard_record(7, "world-b", older_epoch))
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );
    let latest_epoch = latest.epoch().0;
    store
        .commit_virtual_shard_epoch(latest, shard_record(7, "world-c", latest_epoch))
        .await
        .unwrap();
    assert!(matches!(
        store
            .reserve_virtual_shard_epoch(shard_key.clone(), Some(shard_token))
            .await,
        Err(PlacementError::CompareAndPutFailed)
    ));
    assert_eq!(
        store
            .get_virtual_shard(&shard_key)
            .await
            .unwrap()
            .unwrap()
            .1
            .epoch,
        Epoch(3)
    );

    let singleton_key = singleton_key("race");
    let singleton_token = store
        .compare_and_put_singleton(
            singleton_key.clone(),
            None,
            singleton_record("race", "world-a", 1, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();
    let (first, second) = tokio::join!(
        store.reserve_singleton_epoch(singleton_key.clone(), Some(singleton_token), None),
        store.reserve_singleton_epoch(singleton_key.clone(), Some(singleton_token), None),
    );
    let mut reservations = vec![first.unwrap(), second.unwrap()];
    reservations.sort_by_key(|reservation| reservation.epoch().0);
    let older = reservations.remove(0);
    let latest = reservations.remove(0);
    let older_epoch = older.epoch().0;
    assert_eq!(
        store
            .commit_singleton_epoch(
                older,
                singleton_record(
                    "race",
                    "world-b",
                    older_epoch,
                    LeaseId(20),
                    PlacementState::Running,
                ),
            )
            .await,
        Err(PlacementError::CompareAndPutFailed)
    );
    let latest_epoch = latest.epoch().0;
    store
        .commit_singleton_epoch(
            latest,
            singleton_record(
                "race",
                "world-c",
                latest_epoch,
                LeaseId(30),
                PlacementState::Running,
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .reserve_singleton_epoch(singleton_key.clone(), Some(singleton_token), None)
            .await,
        Err(PlacementError::CompareAndPutFailed)
    ));
    assert_eq!(
        store
            .get_singleton(&singleton_key)
            .await
            .unwrap()
            .unwrap()
            .1
            .epoch,
        Epoch(3)
    );
}

#[tokio::test]
async fn actor_and_singleton_reservations_recheck_lock_guards_at_commit() {
    let store = store();
    let actor_key = actor_key(1);
    let actor_lock = store
        .acquire_activation_lock(actor_key.clone())
        .await
        .unwrap();
    let actor_reservation = store
        .reserve_actor_epoch(actor_key.clone(), None, Some(actor_lock))
        .await
        .unwrap();
    store
        .release_activation_lock(&actor_key, actor_lock)
        .await
        .unwrap();
    assert_eq!(
        store
            .commit_actor_epoch(
                actor_reservation,
                actor_record(1, "world-a", 1, LeaseId(10), PlacementState::Running),
            )
            .await,
        Err(PlacementError::ActivationLockLost)
    );
    assert!(store.get_actor(&actor_key).await.unwrap().is_none());

    let singleton_key = singleton_key("global");
    let singleton_lock = store
        .acquire_singleton_lock(singleton_key.clone())
        .await
        .unwrap();
    let singleton_reservation = store
        .reserve_singleton_epoch(singleton_key.clone(), None, Some(singleton_lock))
        .await
        .unwrap();
    store
        .release_singleton_lock(&singleton_key, singleton_lock)
        .await
        .unwrap();
    assert_eq!(
        store
            .commit_singleton_epoch(
                singleton_reservation,
                singleton_record("global", "world-a", 1, LeaseId(10), PlacementState::Running,),
            )
            .await,
        Err(PlacementError::SingletonLockLost)
    );
    assert!(store.get_singleton(&singleton_key).await.unwrap().is_none());

    let next_actor_lock = store
        .acquire_activation_lock(actor_key.clone())
        .await
        .unwrap();
    let next_actor = store
        .reserve_actor_epoch(actor_key.clone(), None, Some(next_actor_lock))
        .await
        .unwrap();
    assert_eq!(next_actor.epoch(), Epoch(2));
}

#[tokio::test]
async fn legacy_writes_preserve_state_epochs_and_require_advances_for_new_authority() {
    let store = store();

    let actor_key = actor_key(1);
    let actor = actor_record(1, "world-a", 3, LeaseId(10), PlacementState::Running);
    let actor_token = store
        .compare_and_put_actor(actor_key.clone(), None, actor.clone())
        .await
        .unwrap();
    let draining = ActorPlacementRecord {
        state: PlacementState::Draining,
        ..actor.clone()
    };
    let draining_token = store
        .compare_and_put_actor(actor_key.clone(), Some(actor_token), draining.clone())
        .await
        .unwrap();
    let equal_conflict = ActorPlacementRecord {
        owner: InstanceId::new("world-b"),
        lease_id: LeaseId(20),
        ..draining.clone()
    };
    assert_eq!(
        store
            .compare_and_put_actor(
                actor_key.clone(),
                Some(draining_token),
                equal_conflict.clone(),
            )
            .await,
        Err(PlacementError::EpochAuthorityConflict { epoch: Epoch(3) })
    );
    let moved = ActorPlacementRecord {
        epoch: Epoch(5),
        ..equal_conflict
    };
    let moved_token = store
        .compare_and_put_actor(actor_key.clone(), Some(draining_token), moved.clone())
        .await
        .unwrap();
    let stopped = ActorPlacementRecord {
        state: PlacementState::Stopped,
        ..moved.clone()
    };
    let stopped_token = store
        .compare_and_put_actor(actor_key.clone(), Some(moved_token), stopped.clone())
        .await
        .unwrap();
    let same_epoch_restart = ActorPlacementRecord {
        state: PlacementState::Running,
        ..stopped.clone()
    };
    assert_eq!(
        store
            .compare_and_put_actor(actor_key.clone(), Some(stopped_token), same_epoch_restart)
            .await,
        Err(PlacementError::EpochReactivation { epoch: Epoch(5) })
    );
    let restarted = ActorPlacementRecord {
        epoch: Epoch(6),
        state: PlacementState::Running,
        ..stopped
    };
    store
        .compare_and_put_actor(actor_key, Some(stopped_token), restarted)
        .await
        .unwrap();

    let shard_key = shard_key(1);
    let shard = shard_record(1, "world-a", 3);
    let shard_token = store
        .compare_and_put_virtual_shard(shard_key.clone(), None, shard.clone())
        .await
        .unwrap();
    let same_shard_token = store
        .compare_and_put_virtual_shard(shard_key.clone(), Some(shard_token), shard.clone())
        .await
        .unwrap();
    let equal_shard_conflict = VirtualShardPlacementRecord {
        owner: InstanceId::new("world-b"),
        ..shard.clone()
    };
    assert_eq!(
        store
            .compare_and_put_virtual_shard(
                shard_key.clone(),
                Some(same_shard_token),
                equal_shard_conflict.clone(),
            )
            .await,
        Err(PlacementError::EpochAuthorityConflict { epoch: Epoch(3) })
    );
    store
        .compare_and_put_virtual_shard(
            shard_key,
            Some(same_shard_token),
            VirtualShardPlacementRecord {
                epoch: Epoch(4),
                ..equal_shard_conflict
            },
        )
        .await
        .unwrap();

    let singleton_key = singleton_key("global");
    let singleton = singleton_record("global", "world-a", 3, LeaseId(10), PlacementState::Running);
    let singleton_token = store
        .compare_and_put_singleton(singleton_key.clone(), None, singleton.clone())
        .await
        .unwrap();
    let draining_singleton = SingletonPlacementRecord {
        state: PlacementState::Draining,
        ..singleton.clone()
    };
    let draining_singleton_token = store
        .compare_and_put_singleton(
            singleton_key.clone(),
            Some(singleton_token),
            draining_singleton.clone(),
        )
        .await
        .unwrap();
    let equal_singleton_conflict = SingletonPlacementRecord {
        owner: InstanceId::new("world-b"),
        lease_id: LeaseId(20),
        ..draining_singleton
    };
    assert_eq!(
        store
            .compare_and_put_singleton(
                singleton_key.clone(),
                Some(draining_singleton_token),
                equal_singleton_conflict.clone(),
            )
            .await,
        Err(PlacementError::EpochAuthorityConflict { epoch: Epoch(3) })
    );
    store
        .compare_and_put_singleton(
            singleton_key,
            Some(draining_singleton_token),
            SingletonPlacementRecord {
                epoch: Epoch(4),
                ..equal_singleton_conflict
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn exhausted_floors_reject_reservation_for_every_family_without_mutation() {
    let store = store();
    let actor_key = actor_key(1);
    let shard_key = shard_key(1);
    let singleton_key = singleton_key("global");
    store
        .compare_and_put_actor(
            actor_key.clone(),
            None,
            actor_record(1, "world-a", u64::MAX, LeaseId(10), PlacementState::Running),
        )
        .await
        .unwrap();
    store
        .compare_and_put_virtual_shard(
            shard_key.clone(),
            None,
            shard_record(1, "world-a", u64::MAX),
        )
        .await
        .unwrap();
    store
        .compare_and_put_singleton(
            singleton_key.clone(),
            None,
            singleton_record(
                "global",
                "world-a",
                u64::MAX,
                LeaseId(10),
                PlacementState::Running,
            ),
        )
        .await
        .unwrap();
    store.remove_actor_for_test(&actor_key).unwrap();
    store.remove_virtual_shard_for_test(&shard_key).unwrap();
    store.remove_singleton_for_test(&singleton_key).unwrap();

    assert!(matches!(
        store
            .reserve_actor_epoch(actor_key.clone(), None, None)
            .await,
        Err(PlacementError::EpochExhausted)
    ));
    assert!(matches!(
        store
            .reserve_virtual_shard_epoch(shard_key.clone(), None)
            .await,
        Err(PlacementError::EpochExhausted)
    ));
    assert!(matches!(
        store
            .reserve_singleton_epoch(singleton_key.clone(), None, None)
            .await,
        Err(PlacementError::EpochExhausted)
    ));
    assert!(store.get_actor(&actor_key).await.unwrap().is_none());
    assert!(store.get_virtual_shard(&shard_key).await.unwrap().is_none());
    assert!(store.get_singleton(&singleton_key).await.unwrap().is_none());
}

#[test]
fn live_record_ahead_of_floor_is_rejected_as_corruption() {
    assert_eq!(
        crate::storage::validate_legacy_epoch(
            Some(Epoch(5)),
            Some(Epoch(4)),
            Epoch(5),
            false,
            false,
        ),
        Err(PlacementError::EpochFloorCorrupt {
            floor: Epoch(4),
            record: Epoch(5),
        })
    );
}

fn store() -> InMemoryPlacementStore {
    InMemoryPlacementStore::new(PlacementPrefix::new(PREFIX))
}

fn actor_key(actor_id: u64) -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
    }
}

fn actor_record(
    actor_id: u64,
    owner: &str,
    epoch: u64,
    lease_id: LeaseId,
    state: PlacementState,
) -> ActorPlacementRecord {
    ActorPlacementRecord {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(actor_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id,
        state,
    }
}

fn shard_key(shard_id: u32) -> VirtualShardPlacementKey {
    VirtualShardPlacementKey {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        shard_id: VirtualShardId(shard_id),
    }
}

fn shard_record(shard_id: u32, owner: &str, epoch: u64) -> VirtualShardPlacementRecord {
    VirtualShardPlacementRecord {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        shard_id: VirtualShardId(shard_id),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
    }
}

fn singleton_key(scope: &str) -> SingletonKey {
    SingletonKey {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("Season"),
        scope: scope.to_string(),
    }
}

fn singleton_record(
    scope: &str,
    owner: &str,
    epoch: u64,
    lease_id: LeaseId,
    state: PlacementState,
) -> SingletonPlacementRecord {
    SingletonPlacementRecord {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("Season"),
        scope: scope.to_string(),
        owner: InstanceId::new(owner),
        epoch: Epoch(epoch),
        lease_id,
        state,
    }
}

fn remove_floor(store: &InMemoryPlacementStore, key: &PlacementEpochKey) {
    let storage_key = store.prefixed_epoch_key(key);
    assert!(
        store
            .inner
            .lock()
            .expect("placement store mutex poisoned")
            .epoch_floors
            .remove(&storage_key)
            .is_some()
    );
}

fn put_actor_record_only(
    store: &InMemoryPlacementStore,
    key: ActorPlacementKey,
    record: ActorPlacementRecord,
) -> PlacementVersion {
    let storage_key = store.prefixed_actor_key(&key);
    let mut inner = store.inner.lock().expect("placement store mutex poisoned");
    let revision = inner.next_placement_revision();
    let token = super::placement_token(revision);
    inner.actors.insert(storage_key, (token, revision, record));
    token
}

fn put_shard_record_only(
    store: &InMemoryPlacementStore,
    key: VirtualShardPlacementKey,
    record: VirtualShardPlacementRecord,
) -> PlacementVersion {
    let storage_key = store.prefixed_vshard_key(&key);
    let mut inner = store.inner.lock().expect("placement store mutex poisoned");
    let revision = inner.next_placement_revision();
    let token = super::placement_token(revision);
    inner.vshards.insert(storage_key, (token, revision, record));
    token
}

fn put_singleton_record_only(
    store: &InMemoryPlacementStore,
    key: SingletonKey,
    record: SingletonPlacementRecord,
) -> PlacementVersion {
    let storage_key = store.prefixed_singleton_key(&key);
    let mut inner = store.inner.lock().expect("placement store mutex poisoned");
    let revision = inner.next_placement_revision();
    let token = super::placement_token(revision);
    inner
        .singletons
        .insert(storage_key, (token, revision, record));
    token
}

fn placement_revision(store: &InMemoryPlacementStore) -> u64 {
    store
        .inner
        .lock()
        .expect("placement store mutex poisoned")
        .placement_revision
}
