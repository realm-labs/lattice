use std::collections::BTreeSet;
use std::time::Duration;

use lattice_core::actor_ref::{ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation};

use super::domain::{
    ActivateAuthority, AllocateInitial, CreateMember, CreatePlan, LeasedClaim, RemoveMember,
    ReserveMove, UpdateMember,
};
use super::{CoordinatorStore, InMemoryPlacementStore, PlacementStore, StorageError};
use crate::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};
use crate::coordinator::{LeaderGuard, LeaderRecord, MemberRecord, MemberStatus, NodeHello};
use crate::plan::RebalancePlan;
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

fn hello(node: NodeKey) -> NodeHello {
    NodeHello {
        node,
        roles: BTreeSet::new(),
        capacity_units: 1,
        hosted_entity_types: BTreeSet::new(),
        proxied_entity_types: BTreeSet::new(),
        singleton_eligibility: BTreeSet::new(),
        used_singletons: BTreeSet::new(),
        protocols: Vec::new(),
        entity_configs: Vec::new(),
        singleton_configs: Vec::new(),
    }
}

async fn elected() -> (InMemoryPlacementStore, LeaderGuard, i64) {
    let store = InMemoryPlacementStore::new(32, 32).unwrap();
    store.ensure_schema_generation().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        node: node("leader", 1, 31001),
        protocol_generation: 4,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    (store, LeaderGuard::new(leader), lease)
}

fn allocating_slot(key: PlacementSlotKey, owner: NodeKey, revision: u64) -> PlacementSlot {
    PlacementSlot {
        key,
        config_fingerprint: ConfigFingerprint::new([9; 32]),
        owner: Some(owner),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: StateVersion::new(
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(revision).unwrap(),
        ),
        state: PlacementSlotState::Allocating,
        active_move: None,
        barrier_sessions: BTreeSet::new(),
    }
}

fn claim(slot: &PlacementSlot, lease_id: i64) -> LeasedClaim {
    LeasedClaim {
        grant: ClaimGrant {
            slot: slot.key.clone(),
            owner: slot.owner.clone().unwrap(),
            coordinator_term: slot.version.term,
            assignment_generation: slot.assignment_generation,
            grant_sequence: GrantSequence::new(1).unwrap(),
            ttl: Duration::from_secs(10),
        },
        lease_id,
    }
}

#[tokio::test]
async fn revoked_exact_leader_fences_member_plan_and_authority_families() {
    let (store, guard, leader_lease) = elected().await;
    let resource_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let owner = node("owner", 2, 31002);
    let key = PlacementSlotKey::Shard {
        entity_type: EntityType::new("fenced").unwrap(),
        shard_id: ShardId::new(0),
    };
    let slot = allocating_slot(key, owner.clone(), 2);
    let member = MemberRecord {
        node: owner.clone(),
        hello: hello(owner),
        status: MemberStatus::Joining,
        version: StateVersion::new(guard.term(), Revision::new(2).unwrap()),
        lease_id: resource_lease,
    };
    let proposal = RebalanceProposal {
        policy_id: "test",
        policy_version: 1,
        base_version: StateVersion::new(guard.term(), Revision::new(1).unwrap()),
        trigger: RebalanceTrigger::Automatic,
        moves: vec![ProposedMove {
            entity_type: EntityType::new("fenced").unwrap(),
            shard_id: ShardId::new(0),
            expected_generation: AssignmentGeneration::new(1).unwrap(),
            source: node("source", 3, 31003),
            target: node("target", 4, 31004),
            estimated_weight: 1,
        }],
    };
    let plan = RebalancePlan::from_proposal(
        proposal,
        EntityType::new("fenced").unwrap(),
        guard.term(),
        1,
    )
    .unwrap();
    store.revoke_lease(leader_lease).await.unwrap();

    assert!(matches!(
        store.create_member(&guard, CreateMember { member }).await,
        Err(StorageError::LeadershipLost)
    ));
    assert!(matches!(
        store.create_plan(&guard, CreatePlan { plan }).await,
        Err(StorageError::LeadershipLost)
    ));
    assert!(matches!(
        store
            .allocate_initial(
                &guard,
                AllocateInitial {
                    claim: claim(&slot, resource_lease),
                    slot,
                },
            )
            .await,
        Err(StorageError::LeadershipLost)
    ));
}

#[tokio::test]
async fn allocation_and_move_commits_are_all_or_nothing() {
    let (store, guard, _) = elected().await;
    let claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let source = node("source", 10, 31110);
    let target = node("target", 11, 31111);
    let entity_type = EntityType::new("atomic").unwrap();
    let key = PlacementSlotKey::Shard {
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(1),
    };
    let allocating = allocating_slot(key.clone(), source.clone(), 2);
    let leased_claim = claim(&allocating, claim_lease);
    let committed = store
        .allocate_initial(
            &guard,
            AllocateInitial {
                slot: allocating.clone(),
                claim: leased_claim.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(store.get_slot(&key).await.unwrap(), Some(committed.slot));
    assert_eq!(
        store.get_claim(&key).await.unwrap(),
        Some(leased_claim.clone())
    );

    let mut running = allocating.clone();
    running.state = PlacementSlotState::Running;
    running.version = StateVersion::new(guard.term(), Revision::new(3).unwrap());
    store
        .activate_authority(
            &guard,
            ActivateAuthority {
                expected_slot: allocating,
                expected_claim: leased_claim.grant,
                slot: running.clone(),
            },
        )
        .await
        .unwrap();

    let proposal = RebalanceProposal {
        policy_id: "test",
        policy_version: 1,
        base_version: StateVersion::new(guard.term(), Revision::new(3).unwrap()),
        trigger: RebalanceTrigger::Manual {
            source: Some(source.clone()),
            target: Some(target.clone()),
            bypass_improvement: true,
        },
        moves: vec![ProposedMove {
            entity_type: entity_type.clone(),
            shard_id: ShardId::new(1),
            expected_generation: AssignmentGeneration::new(1).unwrap(),
            source,
            target: target.clone(),
            estimated_weight: 1,
        }],
    };
    let pending = RebalancePlan::from_proposal(proposal, entity_type, guard.term(), 1).unwrap();
    store
        .create_plan(
            &guard,
            CreatePlan {
                plan: pending.clone(),
            },
        )
        .await
        .unwrap();
    let mut handoff_plan = pending.clone();
    handoff_plan
        .begin_move(ShardId::new(1), AssignmentGeneration::new(1).unwrap(), None)
        .unwrap();
    handoff_plan
        .install_barrier(
            ShardId::new(1),
            StateVersion::new(guard.term(), Revision::new(4).unwrap()),
            BTreeSet::new(),
        )
        .unwrap();
    handoff_plan.record_revision = handoff_plan.record_revision.next().unwrap();
    let mut handoff_slot = running.clone();
    handoff_slot.target = Some(target);
    handoff_slot.state = PlacementSlotState::BeginHandoff;
    handoff_slot.active_move = Some(handoff_plan.plan_id);
    handoff_slot.version = StateVersion::new(guard.term(), Revision::new(4).unwrap());
    store
        .reserve_move(
            &guard,
            ReserveMove {
                expected_plan: pending,
                plan: handoff_plan.clone(),
                expected_slot: running,
                slot: handoff_slot.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(store.get_slot(&key).await.unwrap(), Some(handoff_slot));
    assert_eq!(
        store.get_plan(handoff_plan.plan_id).await.unwrap(),
        Some(handoff_plan)
    );
}

#[tokio::test]
async fn member_store_allows_one_incarnation_and_exact_record_cas_only() {
    let (store, guard, _) = elected().await;
    let first_lease = store.grant_lease(Duration::from_secs(5)).await.unwrap();
    let second_lease = store.grant_lease(Duration::from_secs(5)).await.unwrap();
    let first = node("same-id", 20, 31200);
    let second = node("same-id", 21, 31201);
    let joining = MemberRecord {
        node: first.clone(),
        hello: hello(first),
        status: MemberStatus::Joining,
        version: StateVersion::new(guard.term(), Revision::new(2).unwrap()),
        lease_id: first_lease,
    };
    let mut replacement = MemberRecord {
        node: second.clone(),
        hello: hello(second),
        status: MemberStatus::Joining,
        version: StateVersion::new(guard.term(), Revision::new(3).unwrap()),
        lease_id: second_lease,
    };
    store
        .create_member(
            &guard,
            CreateMember {
                member: joining.clone(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .create_member(
                &guard,
                CreateMember {
                    member: replacement.clone()
                }
            )
            .await,
        Err(StorageError::IncarnationConflict)
    ));
    let mut up = joining.clone();
    up.status = MemberStatus::Up;
    up.version = StateVersion::new(guard.term(), Revision::new(3).unwrap());
    store
        .update_member(
            &guard,
            UpdateMember {
                expected: joining.clone(),
                member: up.clone(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .remove_member(&guard, RemoveMember { expected: joining })
            .await,
        Err(StorageError::CompareFailed)
    ));
    store
        .remove_member(&guard, RemoveMember { expected: up })
        .await
        .unwrap();
    replacement.version = StateVersion::new(guard.term(), Revision::new(5).unwrap());
    store
        .create_member(
            &guard,
            CreateMember {
                member: replacement,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn reconciliation_pages_never_exceed_the_requested_bound() {
    let store = InMemoryPlacementStore::new(8, 8).unwrap();
    store.ensure_schema_generation().await.unwrap();
    for shard in 0..5 {
        let mut slot = allocating_slot(
            PlacementSlotKey::Shard {
                entity_type: EntityType::new("paged").unwrap(),
                shard_id: ShardId::new(shard),
            },
            node("owner", 30, 31300),
            u64::from(shard) + 1,
        );
        slot.state = PlacementSlotState::Fenced;
        store.insert_generation_three_slot(slot);
    }
    let first = store.list_slots_page(&[], 0, 2).await.unwrap();
    assert_eq!(first.records.len(), 2);
    assert_eq!(first.total, 5);
    let second = store
        .list_slots_page(&[], first.next_offset.unwrap(), 2)
        .await
        .unwrap();
    assert_eq!(second.records.len(), 2);
    assert_eq!(second.total, 5);
    assert!(second.next_offset.is_some());
}
