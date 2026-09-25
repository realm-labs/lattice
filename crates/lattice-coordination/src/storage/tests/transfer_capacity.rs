use super::{allocating_slot, elected_placement, group, node, persist_authority_records};
use crate::allocation::RebalanceLimits;
use crate::storage::transfer_capacity::{TransferReservation, release_memory, reserve_memory};
use crate::{
    storage::StorageError,
    types::{PlacementSlotKey, PlacementSlotState, ShardId},
};
use lattice_model::cluster::{EntityType, SingletonKind};

#[tokio::test]
async fn reservations_cover_singletons_and_survive_policy_lowering() {
    let (store, guard, _) = elected_placement(group()).await;
    let target = node("capacity-target", 201, 31201);
    persist_authority_records(&store, &guard, target.clone()).await;
    let source = node("capacity-source", 202, 31202);
    let mut shard = allocating_slot(
        PlacementSlotKey::Shard {
            group: group(),
            entity_type: EntityType::new("capacity").unwrap(),
            shard_id: ShardId::new(0),
        },
        source.clone(),
        3,
    );
    shard.target = Some(target.clone());
    shard.active_move = Some(100);
    shard.state = PlacementSlotState::BeginHandoff;
    let mut singleton = shard.clone();
    singleton.key = PlacementSlotKey::Singleton {
        group: group(),
        kind: SingletonKind::new("capacity-singleton").unwrap(),
    };
    singleton.active_move = Some(101);
    let limits = RebalanceLimits {
        concurrent_group: 2,
        concurrent_source: 1,
        concurrent_target: 2,
        ..RebalanceLimits::default()
    };
    let mut state = store.inner.lock().unwrap();
    reserve_memory(&mut state, &shard, limits).unwrap();
    assert_eq!(
        reserve_memory(&mut state, &singleton, limits),
        Err(StorageError::Capacity)
    );
    assert_eq!(state.transfer_reservations.len(), 1);
    let raised = RebalanceLimits {
        concurrent_source: 2,
        ..limits
    };
    reserve_memory(&mut state, &singleton, raised).unwrap();
    // A fresh Coordinator reads the same store: neither policy lowering nor a
    // missing plan can hide the singleton's existing reservation.
    let mut another = shard.clone();
    another.key = PlacementSlotKey::Shard {
        group: group(),
        entity_type: EntityType::new("capacity").unwrap(),
        shard_id: ShardId::new(1),
    };
    another.active_move = Some(102);
    assert_eq!(
        reserve_memory(&mut state, &another, limits),
        Err(StorageError::Capacity)
    );
    singleton.owner = Some(target.clone());
    singleton.state = PlacementSlotState::Allocating;
    release_memory(&mut state, &singleton).unwrap();
    shard.owner = Some(target);
    shard.state = PlacementSlotState::Allocating;
    release_memory(&mut state, &shard).unwrap();
    assert!(state.transfer_reservations.is_empty());
    // Missing evidence never underflows a counter or pretends release succeeded.
    assert_eq!(
        release_memory(&mut state, &shard),
        Err(StorageError::StorageMetadataMismatch)
    );
}

#[test]
fn transfer_record_does_not_grow_with_participant_count() {
    let mut slot = allocating_slot(
        PlacementSlotKey::Shard {
            group: group(),
            entity_type: EntityType::new("capacity").unwrap(),
            shard_id: ShardId::new(0),
        },
        node("source", 1, 31001),
        3,
    );
    slot.target = Some(node("target", 2, 31002));
    slot.active_move = Some(u128::MAX);
    let reservation = TransferReservation::from_slot(&slot).unwrap();
    assert!(serde_json::to_vec(&reservation).unwrap().len() < 4096);
}
