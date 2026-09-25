use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use lattice_model::actor::ProtocolId;

use lattice_model::cluster::CoordinatorScope;
use lattice_model::cluster::{
    ActorGroupId, ConfigFingerprint, EntityType, NodeEndpoint, NodeIncarnation, SingletonKind,
};

use super::records::{
    ActivateAuthority, AllocateInitial, CreateGroupMember, CreateMember, CreatePlan, LeasedClaim,
    PutEntityConfig, PutSingletonConfig, RemoveMember, ReserveMove, UpdateMember,
};
use super::{ActorGroupStore, InMemoryCoordinationStore, MembershipStore, StorageError};
use crate::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};
use crate::coordinator::{
    ActorGroupHello, ClusterLeaderGuard, GroupLeaderGuard, GroupMemberRecord, GroupMemberStatus,
    LeaderRecord, MemberHello, MemberRecord, MemberStatus, SingletonConfig,
};
use crate::plan::RebalancePlan;
use crate::region::EntityConfig;
use crate::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, MembershipVersion, NodeKey,
    PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion, Revision, ShardId,
};

fn group() -> ActorGroupId {
    ActorGroupId::new("test-group").unwrap()
}

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeEndpoint::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn hello(node: NodeKey) -> MemberHello {
    MemberHello {
        node,
        roles: BTreeSet::new(),
        failure_domains: BTreeMap::new(),
        protocols: Vec::new(),
        remoting_capabilities: BTreeSet::new(),
    }
}

fn group_hello(node: NodeKey, group: ActorGroupId) -> ActorGroupHello {
    ActorGroupHello::builder(node, group, 1).build()
}

fn authority_records(
    owner: NodeKey,
    actor_group: ActorGroupId,
    lease_id: i64,
    membership_version: MembershipVersion,
    placement_version: PlacementVersion,
) -> (MemberRecord, GroupMemberRecord) {
    let global = MemberRecord {
        node: owner.clone(),
        hello: hello(owner.clone()),
        status: MemberStatus::Up,
        version: membership_version,
        lease_id,
    };
    let group = GroupMemberRecord {
        node: owner.clone(),
        hello: group_hello(owner, actor_group),
        status: GroupMemberStatus::Up,
        version: placement_version,
    };
    (global, group)
}

async fn persist_authority_records(
    store: &InMemoryCoordinationStore,
    placement_guard: &GroupLeaderGuard,
    owner: NodeKey,
) -> (MemberRecord, GroupMemberRecord) {
    let member_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let membership_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let membership_leader = LeaderRecord {
        scope: CoordinatorScope::Cluster,
        node: node("membership-leader", 90, 31990),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(
        store
            .campaign_leader(&membership_leader, membership_lease)
            .await
            .unwrap()
    );
    let membership_guard = ClusterLeaderGuard::new(membership_leader).unwrap();
    let membership_version = MembershipVersion::new(
        membership_guard.term(),
        store
            .get_membership_revision()
            .await
            .unwrap()
            .next()
            .unwrap(),
    );
    let actor_group = placement_guard.group().clone();
    let placement_version = PlacementVersion::new(
        actor_group.clone(),
        placement_guard.term(),
        store
            .get_placement_revision(&actor_group)
            .await
            .unwrap()
            .next()
            .unwrap(),
    );
    let (global, group) = authority_records(
        owner,
        actor_group,
        member_lease,
        membership_version,
        placement_version,
    );
    store
        .create_member(
            &membership_guard,
            CreateMember {
                member: global.clone(),
            },
        )
        .await
        .unwrap();
    store
        .create_group_member(
            placement_guard,
            CreateGroupMember {
                expected_global_member: global.clone(),
                member: group.clone(),
            },
        )
        .await
        .unwrap();
    (global, group)
}

async fn elected_placement(
    group: ActorGroupId,
) -> (InMemoryCoordinationStore, GroupLeaderGuard, i64) {
    let store = InMemoryCoordinationStore::new(32, 32).unwrap();
    store.ensure_framework().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        scope: CoordinatorScope::Group(group),
        node: node("leader", 1, 31001),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    (store, GroupLeaderGuard::new(leader).unwrap(), lease)
}

async fn elected_membership() -> (InMemoryCoordinationStore, ClusterLeaderGuard, i64) {
    let store = InMemoryCoordinationStore::new(32, 32).unwrap();
    store.ensure_framework().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        scope: CoordinatorScope::Cluster,
        node: node("leader", 1, 31001),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    (store, ClusterLeaderGuard::new(leader).unwrap(), lease)
}

fn allocating_slot(key: PlacementSlotKey, owner: NodeKey, revision: u64) -> PlacementSlot {
    let group = key.group().clone();
    PlacementSlot {
        key,
        config_fingerprint: ConfigFingerprint::new([9; 32]),
        owner: Some(owner),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(
            group,
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
            group: slot.key.group().clone(),
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
    let actor_group = group();
    let (store, guard, leader_lease) = elected_placement(actor_group.clone()).await;
    let resource_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let owner = node("owner", 2, 31002);
    let key = PlacementSlotKey::Shard {
        group: actor_group.clone(),
        entity_type: EntityType::new("fenced").unwrap(),
        shard_id: ShardId::new(0),
    };
    let slot = allocating_slot(key, owner.clone(), 2);
    let (expected_global_member, expected_group_member) = authority_records(
        owner,
        actor_group.clone(),
        resource_lease,
        MembershipVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
        PlacementVersion::new(actor_group.clone(), guard.term(), Revision::new(1).unwrap()),
    );
    let proposal = RebalanceProposal {
        group: actor_group.clone(),
        policy_id: "test",
        policy_version: 1,
        base_version: PlacementVersion::new(
            actor_group.clone(),
            guard.term(),
            Revision::new(1).unwrap(),
        ),
        trigger: RebalanceTrigger::Automatic,
        moves: vec![ProposedMove {
            group: actor_group,
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
        store.create_plan(&guard, CreatePlan { plan }).await,
        Err(StorageError::LeadershipLost)
    ));
    assert!(matches!(
        store
            .allocate_initial(
                &guard,
                AllocateInitial {
                    expected_global_member,
                    expected_group_member,
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
    let actor_group = group();
    let (store, guard, _) = elected_placement(actor_group.clone()).await;
    let claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let source = node("source", 10, 31110);
    let (expected_global_member, expected_group_member) =
        persist_authority_records(&store, &guard, source.clone()).await;
    let target = node("target", 11, 31111);
    let entity_type = EntityType::new("atomic").unwrap();
    let key = PlacementSlotKey::Shard {
        group: actor_group.clone(),
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(1),
    };
    let allocating = allocating_slot(key.clone(), source.clone(), 3);
    let leased_claim = claim(&allocating, claim_lease);
    let committed = store
        .allocate_initial(
            &guard,
            AllocateInitial {
                expected_global_member,
                expected_group_member,
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
    running.version =
        PlacementVersion::new(actor_group.clone(), guard.term(), Revision::new(4).unwrap());
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
        group: actor_group.clone(),
        policy_id: "test",
        policy_version: 1,
        base_version: PlacementVersion::new(
            actor_group.clone(),
            guard.term(),
            Revision::new(4).unwrap(),
        ),
        trigger: RebalanceTrigger::Manual {
            source: Some(source.clone()),
            target: Some(target.clone()),
            bypass_improvement: true,
        },
        moves: vec![ProposedMove {
            group: actor_group.clone(),
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
            PlacementVersion::new(actor_group.clone(), guard.term(), Revision::new(5).unwrap()),
            BTreeSet::new(),
        )
        .unwrap();
    handoff_plan.record_revision = handoff_plan.record_revision.next().unwrap();
    let mut handoff_slot = running.clone();
    handoff_slot.target = Some(target);
    handoff_slot.state = PlacementSlotState::BeginHandoff;
    handoff_slot.active_move = Some(handoff_plan.plan_id);
    handoff_slot.version =
        PlacementVersion::new(actor_group.clone(), guard.term(), Revision::new(5).unwrap());
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
        store
            .get_plan(&actor_group, handoff_plan.plan_id)
            .await
            .unwrap(),
        Some(handoff_plan)
    );
}

#[tokio::test]
async fn member_store_allows_one_incarnation_and_exact_record_cas_only() {
    let (store, guard, _) = elected_membership().await;
    let first_lease = store.grant_lease(Duration::from_secs(5)).await.unwrap();
    let second_lease = store.grant_lease(Duration::from_secs(5)).await.unwrap();
    let first = node("same-id", 20, 31200);
    let second = node("same-id", 21, 31201);
    let joining = MemberRecord {
        node: first.clone(),
        hello: hello(first),
        status: MemberStatus::Joining,
        version: MembershipVersion::new(guard.term(), Revision::new(2).unwrap()),
        lease_id: first_lease,
    };
    let mut replacement = MemberRecord {
        node: second.clone(),
        hello: hello(second),
        status: MemberStatus::Joining,
        version: MembershipVersion::new(guard.term(), Revision::new(3).unwrap()),
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
    up.version = MembershipVersion::new(guard.term(), Revision::new(3).unwrap());
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
    replacement.version = MembershipVersion::new(guard.term(), Revision::new(5).unwrap());
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

fn paged_shard_key(shard: u32) -> PlacementSlotKey {
    PlacementSlotKey::Shard {
        group: group(),
        entity_type: EntityType::new("paged").unwrap(),
        shard_id: ShardId::new(shard),
    }
}

fn seed_paged_slots(store: &InMemoryCoordinationStore, shards: impl Iterator<Item = u32>) {
    for shard in shards {
        let mut slot = allocating_slot(
            paged_shard_key(shard),
            node("owner", 30, 31300),
            u64::from(shard) + 1,
        );
        slot.state = PlacementSlotState::Fenced;
        store.insert_slot_record(slot);
    }
}

/// Drives a whole sweep and reports every shard the sweep handed back, in order, so a duplicate is
/// as visible as a skip.
async fn sweep_shards(store: &InMemoryCoordinationStore, limit: usize) -> Vec<u32> {
    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = store
            .list_slots_page(&group(), &[], cursor.as_ref(), limit)
            .await
            .unwrap();
        assert!(page.records.len() <= limit);
        seen.extend(page.records.iter().map(shard_of));
        let Some(next) = page.next_cursor else {
            return seen;
        };
        cursor = Some(next);
    }
}

fn shard_of(slot: &PlacementSlot) -> u32 {
    match &slot.key {
        PlacementSlotKey::Shard { shard_id, .. } => shard_id.get(),
        PlacementSlotKey::Singleton { .. } => unreachable!("paged fixtures only seed shards"),
    }
}

#[tokio::test]
async fn reconciliation_pages_never_exceed_the_requested_bound() {
    let store = InMemoryCoordinationStore::new(8, 8).unwrap();
    store.ensure_framework().await.unwrap();
    seed_paged_slots(&store, 0..5);
    let actor_group = group();
    let first = store
        .list_slots_page(&actor_group, &[], None, 2)
        .await
        .unwrap();
    assert_eq!(first.records.len(), 2);
    assert_eq!(first.remaining, 3);
    let second = store
        .list_slots_page(&actor_group, &[], first.next_cursor.as_ref(), 2)
        .await
        .unwrap();
    assert_eq!(second.records.len(), 2);
    assert_eq!(second.remaining, 1);
    assert!(second.next_cursor.is_some());
    let third = store
        .list_slots_page(&actor_group, &[], second.next_cursor.as_ref(), 2)
        .await
        .unwrap();
    assert_eq!(third.records.len(), 1);
    assert_eq!(third.remaining, 0);
    assert!(third.next_cursor.is_none());
    assert_eq!(sweep_shards(&store, 2).await, vec![0, 1, 2, 3, 4]);
}

/// A record removed behind the cursor moves every later offset down by one, so an offset-based
/// resume silently skips the record that slid into the position already consumed. A key cursor
/// names a position in the key space instead, so nothing that stayed put moves.
#[tokio::test]
async fn removing_a_record_behind_the_cursor_skips_no_later_record() {
    let store = InMemoryCoordinationStore::new(8, 8).unwrap();
    store.ensure_framework().await.unwrap();
    seed_paged_slots(&store, 0..6);
    let actor_group = group();
    let first = store
        .list_slots_page(&actor_group, &[], None, 2)
        .await
        .unwrap();
    assert_eq!(
        first.records.iter().map(shard_of).collect::<Vec<_>>(),
        vec![0, 1]
    );
    store.remove_slot_record(&paged_shard_key(0));

    let mut cursor = first.next_cursor;
    let mut seen = Vec::new();
    while let Some(position) = cursor {
        let page = store
            .list_slots_page(&actor_group, &[], Some(&position), 2)
            .await
            .unwrap();
        seen.extend(page.records.iter().map(shard_of));
        cursor = page.next_cursor;
    }
    assert_eq!(seen, vec![2, 3, 4, 5]);
}

/// The mirror image: a record inserted behind the cursor shifts every later offset up by one, so an
/// offset-based resume hands the same record back twice.
#[tokio::test]
async fn inserting_a_record_behind_the_cursor_repeats_no_later_record() {
    let store = InMemoryCoordinationStore::new(8, 8).unwrap();
    store.ensure_framework().await.unwrap();
    seed_paged_slots(&store, [1, 2, 4, 5, 6].into_iter());
    let actor_group = group();
    let first = store
        .list_slots_page(&actor_group, &[], None, 2)
        .await
        .unwrap();
    assert_eq!(
        first.records.iter().map(shard_of).collect::<Vec<_>>(),
        vec![1, 2]
    );
    seed_paged_slots(&store, [0].into_iter());

    let mut cursor = first.next_cursor;
    let mut seen = Vec::new();
    while let Some(position) = cursor {
        let page = store
            .list_slots_page(&actor_group, &[], Some(&position), 2)
            .await
            .unwrap();
        seen.extend(page.records.iter().map(shard_of));
        cursor = page.next_cursor;
    }
    assert_eq!(seen, vec![4, 5, 6]);
}

#[tokio::test]
async fn a_paged_sweep_reads_only_the_records_of_the_pages_it_asked_for() {
    let store = InMemoryCoordinationStore::new(64, 8).unwrap();
    store.ensure_framework().await.unwrap();
    seed_paged_slots(&store, 0..40);
    store.reset_read_counts();
    let page = store.list_slots_page(&group(), &[], None, 4).await.unwrap();
    let counts = store.read_counts();
    assert_eq!(page.records.len(), 4);
    assert_eq!(counts.list_slots, 0);
    assert_eq!(counts.list_slots_page, 1);
    assert_eq!(counts.slot_records, 4);
}

#[tokio::test]
async fn a_zero_width_page_is_rejected_rather_than_looping_forever() {
    let store = InMemoryCoordinationStore::new(8, 8).unwrap();
    store.ensure_framework().await.unwrap();
    seed_paged_slots(&store, 0..2);
    assert!(matches!(
        store.list_slots_page(&group(), &[], None, 0).await,
        Err(StorageError::BackendArgument)
    ));
}

#[tokio::test]
async fn placement_guard_cannot_mutate_or_count_another_group() {
    let group_a = ActorGroupId::new("group-a").unwrap();
    let group_b = ActorGroupId::new("group-b").unwrap();
    let (store, guard, _) = elected_placement(group_a.clone()).await;
    let claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let owner = node("owner", 41, 31401);
    let foreign_key = PlacementSlotKey::Shard {
        group: group_b.clone(),
        entity_type: EntityType::new("foreign").unwrap(),
        shard_id: ShardId::new(1),
    };
    let foreign_slot = PlacementSlot {
        key: foreign_key.clone(),
        config_fingerprint: ConfigFingerprint::new([7; 32]),
        owner: Some(owner.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(group_b.clone(), guard.term(), Revision::new(2).unwrap()),
        state: PlacementSlotState::Allocating,
        active_move: None,
        barrier_sessions: BTreeSet::new(),
    };
    let (expected_global_member, expected_group_member) = authority_records(
        owner.clone(),
        group_b.clone(),
        claim_lease,
        MembershipVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
        PlacementVersion::new(group_b.clone(), guard.term(), Revision::new(1).unwrap()),
    );
    assert!(matches!(
        store
            .allocate_initial(
                &guard,
                AllocateInitial {
                    expected_global_member,
                    expected_group_member,
                    claim: claim(&foreign_slot, claim_lease),
                    slot: foreign_slot,
                },
            )
            .await,
        Err(StorageError::InvalidRecord)
    ));

    let foreign_plan = RebalancePlan::from_proposal(
        RebalanceProposal {
            group: group_b.clone(),
            policy_id: "test",
            policy_version: 1,
            base_version: PlacementVersion::new(
                group_b.clone(),
                guard.term(),
                Revision::new(1).unwrap(),
            ),
            trigger: RebalanceTrigger::Automatic,
            moves: vec![ProposedMove {
                group: group_b.clone(),
                entity_type: EntityType::new("foreign").unwrap(),
                shard_id: ShardId::new(1),
                expected_generation: AssignmentGeneration::new(1).unwrap(),
                source: owner,
                target: node("target", 42, 31402),
                estimated_weight: 1,
            }],
        },
        EntityType::new("foreign").unwrap(),
        guard.term(),
        9,
    )
    .unwrap();
    assert!(matches!(
        store
            .create_plan(&guard, CreatePlan { plan: foreign_plan })
            .await,
        Err(StorageError::InvalidRecord)
    ));
    assert!(store.get_slot(&foreign_key).await.unwrap().is_none());
    assert!(store.list_slots(&group_a).await.unwrap().is_empty());
    assert!(store.list_slots(&group_b).await.unwrap().is_empty());
    assert!(store.list_plans(&group_a).await.unwrap().is_empty());
    assert!(store.list_plans(&group_b).await.unwrap().is_empty());
    assert_eq!(
        store.get_placement_revision(&group_a).await.unwrap(),
        Revision::new(1).unwrap()
    );
    assert_eq!(
        store.get_placement_revision(&group_b).await.unwrap(),
        Revision::new(1).unwrap()
    );
}

#[tokio::test]
async fn group_configuration_is_durable_revisioned_and_exactly_scoped() {
    let store = InMemoryCoordinationStore::new(4, 4).unwrap();
    store.ensure_framework().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        scope: CoordinatorScope::Group(group()),
        node: node("config-leader", 60, 31600),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = GroupLeaderGuard::new(leader).unwrap();
    let entity = EntityConfig::new(
        group(),
        EntityType::new("invoice").unwrap(),
        ProtocolId::new(11).unwrap(),
        32,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let entity_commit = store
        .put_entity_config(
            &guard,
            PutEntityConfig {
                expected: None,
                config: entity.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(entity_commit.version.revision.get(), 2);
    assert_eq!(
        store
            .get_entity_config(&group(), &entity.entity_type)
            .await
            .unwrap(),
        Some(entity.clone())
    );
    assert!(matches!(
        store
            .put_entity_config(
                &guard,
                PutEntityConfig {
                    expected: None,
                    config: entity,
                },
            )
            .await,
        Err(StorageError::CompareFailed)
    ));

    let singleton = SingletonConfig::new(
        group(),
        SingletonKind::new("scheduler").unwrap(),
        ProtocolId::new(12).unwrap(),
    );
    let singleton_commit = store
        .put_singleton_config(
            &guard,
            PutSingletonConfig {
                expected: None,
                config: singleton.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(singleton_commit.version.revision.get(), 3);
    assert_eq!(
        store.list_singleton_configs(&group()).await.unwrap(),
        vec![singleton]
    );
}
