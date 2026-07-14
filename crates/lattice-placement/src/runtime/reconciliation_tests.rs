use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use lattice_core::actor_ref::{ConfigFingerprint, NodeAddress, NodeIncarnation};
use lattice_remoting::association::AssociationManager;
use lattice_remoting::config::RemotingConfig;

use super::{CoordinatorLeader, CoordinatorLeaderConfig};
use crate::coordinator::{LeaderGuard, LeaderRecord};
use crate::storage::domain::{AllocateInitial, LeasedClaim};
use crate::storage::{CoordinatorStore, InMemoryPlacementStore, PlacementStore};
use crate::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, NodeKey, PlacementSlot,
    PlacementSlotKey, PlacementSlotState, Revision, ShardId, StateVersion,
};

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn slot(owner: NodeKey, version: StateVersion) -> PlacementSlot {
    PlacementSlot {
        key: PlacementSlotKey::Shard {
            entity_type: lattice_core::actor_ref::EntityType::new("reconcile").unwrap(),
            shard_id: ShardId::new(1),
        },
        config_fingerprint: ConfigFingerprint::new([3; 32]),
        owner: Some(owner),
        target: None,
        assignment_generation: AssignmentGeneration::new(7).unwrap(),
        version,
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: BTreeSet::new(),
    }
}

fn associations(local: &NodeKey) -> Arc<AssociationManager> {
    Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn election_adopts_committed_claim_without_changing_owner_or_generation() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    store.ensure_schema_generation().await.unwrap();
    let old_leader = node("old", 1, 32101);
    let old_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let record = LeaderRecord {
        node: old_leader,
        protocol_generation: 4,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&record, old_lease).await.unwrap());
    let guard = LeaderGuard::new(record);
    let owner = node("owner", 2, 32102);
    let mut persisted = slot(
        owner.clone(),
        StateVersion::new(guard.term(), Revision::new(2).unwrap()),
    );
    persisted.state = PlacementSlotState::Allocating;
    let claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let grant = ClaimGrant {
        slot: persisted.key.clone(),
        owner: owner.clone(),
        coordinator_term: guard.term(),
        assignment_generation: persisted.assignment_generation,
        grant_sequence: GrantSequence::new(1).unwrap(),
        ttl: Duration::from_secs(10),
    };
    store
        .allocate_initial(
            &guard,
            AllocateInitial {
                slot: persisted.clone(),
                claim: LeasedClaim {
                    grant,
                    lease_id: claim_lease,
                },
            },
        )
        .await
        .unwrap();
    store.revoke_lease(old_lease).await.unwrap();

    let new_leader = node("new", 3, 32103);
    let leader = CoordinatorLeader::elect(
        store.clone(),
        associations(&new_leader),
        new_leader,
        CoordinatorTerm::new(2).unwrap(),
        4,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    let adopted = store.get_slot(&persisted.key).await.unwrap().unwrap();
    let claim = store.get_claim(&persisted.key).await.unwrap().unwrap();
    assert_eq!(adopted.owner, Some(owner));
    assert_eq!(
        adopted.assignment_generation,
        persisted.assignment_generation
    );
    assert_eq!(adopted.version.term, CoordinatorTerm::new(2).unwrap());
    assert_eq!(
        claim.grant.coordinator_term,
        CoordinatorTerm::new(2).unwrap()
    );
    assert!(leader.claims.contains_key(&persisted.key));
}

#[tokio::test]
async fn legacy_allocating_without_claim_is_deterministically_fenced() {
    let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    store.ensure_schema_generation().await.unwrap();
    let coordinator = node("coordinator", 10, 32210);
    let mut legacy = slot(
        node("owner", 11, 32211),
        StateVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
    );
    legacy.state = PlacementSlotState::Allocating;
    store.insert_generation_three_slot(legacy.clone());
    let leader = CoordinatorLeader::elect(
        store.clone(),
        associations(&coordinator),
        coordinator,
        CoordinatorTerm::new(1).unwrap(),
        4,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    let repaired = store.get_slot(&legacy.key).await.unwrap().unwrap();
    assert_eq!(repaired.state, PlacementSlotState::Fenced);
    assert_eq!(repaired.owner, legacy.owner);
    assert_eq!(repaired.assignment_generation, legacy.assignment_generation);
    assert!(leader.reconciliation.initial_complete);
    assert!(leader.reconciliation.quarantined.is_empty());
}

#[tokio::test]
async fn automatic_pause_and_operation_result_survive_leader_failover() {
    let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    let first_node = node("first", 40, 32340);
    let mut first = CoordinatorLeader::elect(
        store.clone(),
        associations(&first_node),
        first_node,
        CoordinatorTerm::new(1).unwrap(),
        4,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    let entity = lattice_core::actor_ref::EntityType::new("paused").unwrap();
    first
        .set_automatic_paused("pause-stable".to_owned(), Some(entity.clone()), true)
        .await
        .unwrap();
    store.revoke_lease(first.leader_lease_id).await.unwrap();

    let second_node = node("second", 41, 32341);
    let mut second = CoordinatorLeader::elect(
        store.clone(),
        associations(&second_node),
        second_node,
        CoordinatorTerm::new(2).unwrap(),
        4,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();
    assert!(second.paused_entity_types.contains(&entity));
    second
        .set_automatic_paused("pause-stable".to_owned(), Some(entity.clone()), true)
        .await
        .unwrap();
    assert!(matches!(
        second
            .set_automatic_paused("pause-stable".to_owned(), Some(entity), false)
            .await,
        Err(super::CoordinatorRuntimeError::IdempotencyConflict)
    ));
    assert!(
        store
            .get_admin_operation("pause-stable")
            .await
            .unwrap()
            .is_some()
    );
}
