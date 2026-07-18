use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use lattice_core::actor_ref::{
    ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation, PlacementDomainId, ProtocolId,
    SingletonKind,
};
use lattice_core::coordinator::CoordinatorScope;

use super::domain::{
    ActivateAuthority, AllocateInitial, CreateDomainMember, CreateMember, CreatePlan, LeasedClaim,
    PutEntityConfig, PutSingletonConfig, RemoveMember, ReserveMove, UpdateMember,
};
use super::{
    InMemoryPlacementStore, MembershipStore, PlacementDomainStore, ScopedElectionStore,
    StorageError,
};
use crate::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};
use crate::coordinator::{
    DomainMemberRecord, DomainMemberStatus, LeaderRecord, MemberHello, MemberRecord, MemberStatus,
    MembershipLeaderGuard, PlacementDomainHello, PlacementLeaderGuard, SingletonConfig,
};
use crate::plan::RebalancePlan;
use crate::region::EntityConfig;
use crate::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, MembershipVersion, NodeKey,
    PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion, Revision, ShardId,
};

fn domain() -> PlacementDomainId {
    PlacementDomainId::new("test-domain").unwrap()
}

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
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

fn domain_hello(node: NodeKey, domain: PlacementDomainId) -> PlacementDomainHello {
    PlacementDomainHello::builder(node, domain, 1).build()
}

fn authority_records(
    owner: NodeKey,
    placement_domain: PlacementDomainId,
    lease_id: i64,
    membership_version: MembershipVersion,
    placement_version: PlacementVersion,
) -> (MemberRecord, DomainMemberRecord) {
    let global = MemberRecord {
        node: owner.clone(),
        hello: hello(owner.clone()),
        status: MemberStatus::Up,
        version: membership_version,
        lease_id,
    };
    let domain = DomainMemberRecord {
        node: owner.clone(),
        hello: domain_hello(owner, placement_domain),
        status: DomainMemberStatus::Up,
        version: placement_version,
    };
    (global, domain)
}

async fn persist_authority_records(
    store: &InMemoryPlacementStore,
    placement_guard: &PlacementLeaderGuard,
    owner: NodeKey,
) -> (MemberRecord, DomainMemberRecord) {
    let member_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let membership_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let membership_leader = LeaderRecord {
        scope: CoordinatorScope::Membership,
        node: node("membership-leader", 90, 31990),
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(
        store
            .campaign_leader(&membership_leader, membership_lease)
            .await
            .unwrap()
    );
    let membership_guard = MembershipLeaderGuard::new(membership_leader).unwrap();
    let membership_version = MembershipVersion::new(
        membership_guard.term(),
        store
            .get_membership_revision()
            .await
            .unwrap()
            .next()
            .unwrap(),
    );
    let placement_domain = placement_guard.domain().clone();
    let placement_version = PlacementVersion::new(
        placement_domain.clone(),
        placement_guard.term(),
        store
            .get_placement_revision(&placement_domain)
            .await
            .unwrap()
            .next()
            .unwrap(),
    );
    let (global, domain) = authority_records(
        owner,
        placement_domain,
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
        .create_domain_member(
            placement_guard,
            CreateDomainMember {
                expected_global_member: global.clone(),
                member: domain.clone(),
            },
        )
        .await
        .unwrap();
    (global, domain)
}

async fn elected_placement(
    domain: PlacementDomainId,
) -> (InMemoryPlacementStore, PlacementLeaderGuard, i64) {
    let store = InMemoryPlacementStore::new(32, 32).unwrap();
    store.ensure_schema_generation().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        scope: CoordinatorScope::Placement(domain),
        node: node("leader", 1, 31001),
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    (store, PlacementLeaderGuard::new(leader).unwrap(), lease)
}

async fn elected_membership() -> (InMemoryPlacementStore, MembershipLeaderGuard, i64) {
    let store = InMemoryPlacementStore::new(32, 32).unwrap();
    store.ensure_schema_generation().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        scope: CoordinatorScope::Membership,
        node: node("leader", 1, 31001),
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    (store, MembershipLeaderGuard::new(leader).unwrap(), lease)
}

fn allocating_slot(key: PlacementSlotKey, owner: NodeKey, revision: u64) -> PlacementSlot {
    let domain = key.domain().clone();
    PlacementSlot {
        key,
        config_fingerprint: ConfigFingerprint::new([9; 32]),
        owner: Some(owner),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(
            domain,
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
            domain: slot.key.domain().clone(),
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
    let placement_domain = domain();
    let (store, guard, leader_lease) = elected_placement(placement_domain.clone()).await;
    let resource_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let owner = node("owner", 2, 31002);
    let key = PlacementSlotKey::Shard {
        domain: placement_domain.clone(),
        entity_type: EntityType::new("fenced").unwrap(),
        shard_id: ShardId::new(0),
    };
    let slot = allocating_slot(key, owner.clone(), 2);
    let (expected_global_member, expected_domain_member) = authority_records(
        owner,
        placement_domain.clone(),
        resource_lease,
        MembershipVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
        PlacementVersion::new(
            placement_domain.clone(),
            guard.term(),
            Revision::new(1).unwrap(),
        ),
    );
    let proposal = RebalanceProposal {
        domain: placement_domain.clone(),
        policy_id: "test",
        policy_version: 1,
        base_version: PlacementVersion::new(
            placement_domain.clone(),
            guard.term(),
            Revision::new(1).unwrap(),
        ),
        trigger: RebalanceTrigger::Automatic,
        moves: vec![ProposedMove {
            domain: placement_domain,
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
                    expected_domain_member,
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
    let placement_domain = domain();
    let (store, guard, _) = elected_placement(placement_domain.clone()).await;
    let claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let source = node("source", 10, 31110);
    let (expected_global_member, expected_domain_member) =
        persist_authority_records(&store, &guard, source.clone()).await;
    let target = node("target", 11, 31111);
    let entity_type = EntityType::new("atomic").unwrap();
    let key = PlacementSlotKey::Shard {
        domain: placement_domain.clone(),
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
                expected_domain_member,
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
    running.version = PlacementVersion::new(
        placement_domain.clone(),
        guard.term(),
        Revision::new(4).unwrap(),
    );
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
        domain: placement_domain.clone(),
        policy_id: "test",
        policy_version: 1,
        base_version: PlacementVersion::new(
            placement_domain.clone(),
            guard.term(),
            Revision::new(4).unwrap(),
        ),
        trigger: RebalanceTrigger::Manual {
            source: Some(source.clone()),
            target: Some(target.clone()),
            bypass_improvement: true,
        },
        moves: vec![ProposedMove {
            domain: placement_domain.clone(),
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
            PlacementVersion::new(
                placement_domain.clone(),
                guard.term(),
                Revision::new(5).unwrap(),
            ),
            BTreeSet::new(),
        )
        .unwrap();
    handoff_plan.record_revision = handoff_plan.record_revision.next().unwrap();
    let mut handoff_slot = running.clone();
    handoff_slot.target = Some(target);
    handoff_slot.state = PlacementSlotState::BeginHandoff;
    handoff_slot.active_move = Some(handoff_plan.plan_id);
    handoff_slot.version = PlacementVersion::new(
        placement_domain.clone(),
        guard.term(),
        Revision::new(5).unwrap(),
    );
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
            .get_plan(&placement_domain, handoff_plan.plan_id)
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

#[tokio::test]
async fn reconciliation_pages_never_exceed_the_requested_bound() {
    let store = InMemoryPlacementStore::new(8, 8).unwrap();
    store.ensure_schema_generation().await.unwrap();
    for shard in 0..5 {
        let mut slot = allocating_slot(
            PlacementSlotKey::Shard {
                domain: domain(),
                entity_type: EntityType::new("paged").unwrap(),
                shard_id: ShardId::new(shard),
            },
            node("owner", 30, 31300),
            u64::from(shard) + 1,
        );
        slot.state = PlacementSlotState::Fenced;
        store.insert_generation_three_slot(slot);
    }
    let placement_domain = domain();
    let first = store
        .list_slots_page(&placement_domain, &[], 0, 2)
        .await
        .unwrap();
    assert_eq!(first.records.len(), 2);
    assert_eq!(first.total, 5);
    let second = store
        .list_slots_page(&placement_domain, &[], first.next_offset.unwrap(), 2)
        .await
        .unwrap();
    assert_eq!(second.records.len(), 2);
    assert_eq!(second.total, 5);
    assert!(second.next_offset.is_some());
}

#[tokio::test]
async fn placement_guard_cannot_mutate_or_count_another_domain() {
    let domain_a = PlacementDomainId::new("domain-a").unwrap();
    let domain_b = PlacementDomainId::new("domain-b").unwrap();
    let (store, guard, _) = elected_placement(domain_a.clone()).await;
    let claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let owner = node("owner", 41, 31401);
    let foreign_key = PlacementSlotKey::Shard {
        domain: domain_b.clone(),
        entity_type: EntityType::new("foreign").unwrap(),
        shard_id: ShardId::new(1),
    };
    let foreign_slot = PlacementSlot {
        key: foreign_key.clone(),
        config_fingerprint: ConfigFingerprint::new([7; 32]),
        owner: Some(owner.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(domain_b.clone(), guard.term(), Revision::new(2).unwrap()),
        state: PlacementSlotState::Allocating,
        active_move: None,
        barrier_sessions: BTreeSet::new(),
    };
    let (expected_global_member, expected_domain_member) = authority_records(
        owner.clone(),
        domain_b.clone(),
        claim_lease,
        MembershipVersion::new(CoordinatorTerm::new(1).unwrap(), Revision::new(1).unwrap()),
        PlacementVersion::new(domain_b.clone(), guard.term(), Revision::new(1).unwrap()),
    );
    assert!(matches!(
        store
            .allocate_initial(
                &guard,
                AllocateInitial {
                    expected_global_member,
                    expected_domain_member,
                    claim: claim(&foreign_slot, claim_lease),
                    slot: foreign_slot,
                },
            )
            .await,
        Err(StorageError::InvalidRecord)
    ));

    let foreign_plan = RebalancePlan::from_proposal(
        RebalanceProposal {
            domain: domain_b.clone(),
            policy_id: "test",
            policy_version: 1,
            base_version: PlacementVersion::new(
                domain_b.clone(),
                guard.term(),
                Revision::new(1).unwrap(),
            ),
            trigger: RebalanceTrigger::Automatic,
            moves: vec![ProposedMove {
                domain: domain_b.clone(),
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
    assert!(store.list_slots(&domain_a).await.unwrap().is_empty());
    assert!(store.list_slots(&domain_b).await.unwrap().is_empty());
    assert!(store.list_plans(&domain_a).await.unwrap().is_empty());
    assert!(store.list_plans(&domain_b).await.unwrap().is_empty());
    assert_eq!(
        store.get_placement_revision(&domain_a).await.unwrap(),
        Revision::new(1).unwrap()
    );
    assert_eq!(
        store.get_placement_revision(&domain_b).await.unwrap(),
        Revision::new(1).unwrap()
    );
}

#[tokio::test]
async fn generation_four_leader_record_is_rejected_before_election() {
    let store = InMemoryPlacementStore::new(4, 4).unwrap();
    store.ensure_schema_generation().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let result = store
        .campaign_leader(
            &LeaderRecord {
                scope: CoordinatorScope::Placement(domain()),
                node: node("old", 50, 31500),
                protocol_generation: 4,
                term: CoordinatorTerm::new(1).unwrap(),
            },
            lease,
        )
        .await;
    assert!(matches!(result, Err(StorageError::InvalidRecord)));
    assert!(
        store
            .get_leader(&CoordinatorScope::Placement(domain()))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn domain_configuration_is_durable_revisioned_and_exactly_scoped() {
    let store = InMemoryPlacementStore::new(4, 4).unwrap();
    store.ensure_schema_generation().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        scope: CoordinatorScope::Placement(domain()),
        node: node("config-leader", 60, 31600),
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = PlacementLeaderGuard::new(leader).unwrap();
    let entity = EntityConfig::new(
        domain(),
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
            .get_entity_config(&domain(), &entity.entity_type)
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
        domain(),
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
        store.list_singleton_configs(&domain()).await.unwrap(),
        vec![singleton]
    );
}
