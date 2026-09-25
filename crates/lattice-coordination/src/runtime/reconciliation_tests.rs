use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, ConfigFingerprint, EntityType, NodeEndpoint, NodeIncarnation},
};
use lattice_remoting::{association::AssociationManager, config::RemotingConfig};

use super::{GroupCoordinator, GroupCoordinatorConfig};
use crate::{
    coordinator::{
        ActorGroupHello, ClusterLeaderGuard, GroupLeaderGuard, GroupMemberRecord,
        GroupMemberStatus, LeaderRecord, MemberHello, MemberRecord, MemberStatus,
    },
    storage::{
        ActorGroupStore, CoordinatorLeaseStore, InMemoryCoordinationStore, MembershipStore,
        ScopedElectionStore,
        records::{AllocateInitial, CreateGroupMember, CreateMember, LeasedClaim, RemoveMember},
    },
    types::{
        AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, MembershipVersion,
        NodeKey, PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion, Revision,
        ShardId,
    },
};

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeEndpoint::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn group() -> ActorGroupId {
    ActorGroupId::new("reconcile").unwrap()
}

fn slot(owner: NodeKey, version: PlacementVersion) -> PlacementSlot {
    PlacementSlot {
        key: PlacementSlotKey::Shard {
            group: version.group.clone(),
            entity_type: EntityType::new("reconcile").unwrap(),
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

async fn persist_authority_records(
    store: &InMemoryCoordinationStore,
    placement_guard: &GroupLeaderGuard,
    owner: NodeKey,
) -> (MemberRecord, GroupMemberRecord, i64) {
    let membership_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let membership_leader = LeaderRecord {
        scope: CoordinatorScope::Cluster,
        node: node("membership", 91, 32991),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(
        store
            .campaign_leader(&membership_leader, membership_lease)
            .await
            .unwrap()
    );
    let membership_guard = ClusterLeaderGuard::new(membership_leader).unwrap();
    let member_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let global = MemberRecord {
        node: owner.clone(),
        hello: MemberHello {
            node: owner.clone(),
            roles: BTreeSet::new(),
            failure_domains: BTreeMap::new(),
            protocols: Vec::new(),
            remoting_capabilities: BTreeSet::new(),
        },
        status: MemberStatus::Up,
        version: MembershipVersion::new(
            membership_guard.term(),
            store
                .get_membership_revision()
                .await
                .unwrap()
                .next()
                .unwrap(),
        ),
        lease_id: member_lease,
    };
    store
        .create_member(
            &membership_guard,
            CreateMember {
                member: global.clone(),
            },
        )
        .await
        .unwrap();
    let actor_group = group();
    let group_member = GroupMemberRecord {
        node: owner.clone(),
        hello: ActorGroupHello::builder(owner, actor_group.clone(), 1).build(),
        status: GroupMemberStatus::Up,
        version: PlacementVersion::new(
            actor_group.clone(),
            placement_guard.term(),
            store
                .get_placement_revision(&actor_group)
                .await
                .unwrap()
                .next()
                .unwrap(),
        ),
    };
    store
        .create_group_member(
            placement_guard,
            CreateGroupMember {
                expected_global_member: global.clone(),
                member: group_member.clone(),
            },
        )
        .await
        .unwrap();
    (global, group_member, membership_lease)
}

#[tokio::test]
async fn election_adopts_claim_without_rewriting_the_durable_slot() {
    let store = Arc::new(InMemoryCoordinationStore::new(32, 32).unwrap());
    store.ensure_framework().await.unwrap();
    let old_leader = node("old", 1, 32101);
    let old_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let record = LeaderRecord {
        scope: CoordinatorScope::Group(group()),
        node: old_leader,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&record, old_lease).await.unwrap());
    let guard = GroupLeaderGuard::new(record).unwrap();
    let owner = node("owner", 2, 32102);
    let (expected_global_member, expected_group_member, membership_leader_lease) =
        persist_authority_records(store.as_ref(), &guard, owner.clone()).await;
    let mut persisted = slot(
        owner.clone(),
        PlacementVersion::new(group(), guard.term(), Revision::new(3).unwrap()),
    );
    persisted.state = PlacementSlotState::Allocating;
    let claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let grant = ClaimGrant {
        group: group(),
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
                expected_global_member,
                expected_group_member,
                slot: persisted.clone(),
                claim: LeasedClaim {
                    grant,
                    lease_id: claim_lease,
                },
            },
        )
        .await
        .unwrap();
    let durable_revision = store.get_placement_revision(&group()).await.unwrap();
    store.revoke_lease(membership_leader_lease).await.unwrap();
    store.revoke_lease(old_lease).await.unwrap();

    let new_leader = node("new", 3, 32103);
    let leader = GroupCoordinator::elect(
        store.clone(),
        associations(&new_leader),
        new_leader,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(2).unwrap(),
        GroupCoordinatorConfig::default(),
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
    assert_eq!(adopted.version, persisted.version);
    assert_eq!(
        store.get_placement_revision(&group()).await.unwrap(),
        durable_revision,
        "claim adoption must not advance the durable placement revision"
    );
    assert_eq!(
        claim.grant.coordinator_term,
        CoordinatorTerm::new(2).unwrap()
    );
    assert!(leader.claims.contains_key(&persisted.key));
}

#[tokio::test]
async fn legacy_allocating_without_claim_is_deterministically_fenced() {
    let store = Arc::new(InMemoryCoordinationStore::new(8, 8).unwrap());
    store.ensure_framework().await.unwrap();
    let coordinator = node("coordinator", 10, 32210);
    let mut legacy = slot(
        node("owner", 11, 32211),
        PlacementVersion::new(
            group(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
    );
    legacy.state = PlacementSlotState::Allocating;
    store.insert_slot_record(legacy.clone());
    let leader = GroupCoordinator::elect(
        store.clone(),
        associations(&coordinator),
        coordinator,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        GroupCoordinatorConfig::default(),
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
async fn election_removes_orphaned_group_members_before_recovery() {
    let store = Arc::new(InMemoryCoordinationStore::new(8, 8).unwrap());
    store.ensure_framework().await.unwrap();
    let old_leader = node("old", 20, 32220);
    let placement_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let placement_record = LeaderRecord {
        scope: CoordinatorScope::Group(group()),
        node: old_leader,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(
        store
            .campaign_leader(&placement_record, placement_lease)
            .await
            .unwrap()
    );
    let placement_guard = GroupLeaderGuard::new(placement_record).unwrap();
    let owner = node("orphan", 21, 32221);
    let (global, _group_member, membership_leader_lease) =
        persist_authority_records(store.as_ref(), &placement_guard, owner.clone()).await;
    let membership_record = LeaderRecord {
        scope: CoordinatorScope::Cluster,
        node: node("membership", 91, 32991),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    let membership_guard = ClusterLeaderGuard::new(membership_record).unwrap();
    store
        .remove_member(&membership_guard, RemoveMember { expected: global })
        .await
        .unwrap();
    store.revoke_lease(membership_leader_lease).await.unwrap();
    store.revoke_lease(placement_lease).await.unwrap();

    let coordinator = node("new", 22, 32222);
    let leader = GroupCoordinator::elect(
        store.clone(),
        associations(&coordinator),
        coordinator,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(2).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();

    assert!(
        store
            .get_group_member(&group(), &owner.node_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(leader.reconciliation.initial_complete);
}

#[tokio::test]
async fn automatic_pause_and_operation_result_survive_leader_failover() {
    let store = Arc::new(InMemoryCoordinationStore::new(8, 8).unwrap());
    let first_node = node("first", 40, 32340);
    let mut first = GroupCoordinator::elect(
        store.clone(),
        associations(&first_node),
        first_node,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let entity = EntityType::new("paused").unwrap();
    first
        .set_automatic_paused("pause-stable".to_owned(), Some(entity.clone()), true)
        .await
        .unwrap();
    store.revoke_lease(first.leader_lease_id).await.unwrap();

    let second_node = node("second", 41, 32341);
    let mut second = GroupCoordinator::elect(
        store.clone(),
        associations(&second_node),
        second_node,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(2).unwrap(),
        GroupCoordinatorConfig::default(),
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
            .get_admin_operation(&group(), "pause-stable")
            .await
            .unwrap()
            .is_some()
    );
}
