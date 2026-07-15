use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use lattice_core::actor_ref::{ConfigFingerprint, NodeAddress, NodeIncarnation, PlacementDomainId};
use lattice_core::coordinator::CoordinatorScope;
use lattice_remoting::association::AssociationManager;
use lattice_remoting::config::RemotingConfig;

use super::{PlacementDomainLeader, PlacementDomainLeaderConfig};
use crate::coordinator::{
    DomainMemberRecord, DomainMemberStatus, LeaderRecord, MemberHello, MemberRecord, MemberStatus,
    MembershipLeaderGuard, PlacementDomainHello, PlacementLeaderGuard,
};
use crate::storage::domain::{AllocateInitial, CreateDomainMember, CreateMember, LeasedClaim};
use crate::storage::{
    CoordinatorLeaseStore, InMemoryPlacementStore, MembershipStore, PlacementDomainStore,
    ScopedElectionStore,
};
use crate::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, MembershipVersion, NodeKey,
    PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion, Revision, ShardId,
};

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn domain() -> PlacementDomainId {
    PlacementDomainId::new("reconcile").unwrap()
}

fn slot(owner: NodeKey, version: PlacementVersion) -> PlacementSlot {
    PlacementSlot {
        key: PlacementSlotKey::Shard {
            domain: version.domain.clone(),
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

async fn persist_authority_records(
    store: &InMemoryPlacementStore,
    placement_guard: &PlacementLeaderGuard,
    owner: NodeKey,
) -> (MemberRecord, DomainMemberRecord, i64) {
    let membership_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let membership_leader = LeaderRecord {
        scope: CoordinatorScope::Membership,
        node: node("membership", 91, 32991),
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
    let placement_domain = domain();
    let domain_member = DomainMemberRecord {
        node: owner.clone(),
        hello: PlacementDomainHello::new(
            owner,
            placement_domain.clone(),
            1,
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            Vec::new(),
            Vec::new(),
            BTreeMap::new(),
        ),
        status: DomainMemberStatus::Up,
        version: PlacementVersion::new(
            placement_domain.clone(),
            placement_guard.term(),
            store
                .get_placement_revision(&placement_domain)
                .await
                .unwrap()
                .next()
                .unwrap(),
        ),
    };
    store
        .create_domain_member(
            placement_guard,
            CreateDomainMember {
                expected_global_member: global.clone(),
                member: domain_member.clone(),
            },
        )
        .await
        .unwrap();
    (global, domain_member, membership_lease)
}

#[tokio::test]
async fn election_adopts_committed_claim_without_changing_owner_or_generation() {
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    store.ensure_schema_generation().await.unwrap();
    let old_leader = node("old", 1, 32101);
    let old_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let record = LeaderRecord {
        scope: CoordinatorScope::Placement(domain()),
        node: old_leader,
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&record, old_lease).await.unwrap());
    let guard = PlacementLeaderGuard::new(record).unwrap();
    let owner = node("owner", 2, 32102);
    let (expected_global_member, expected_domain_member, membership_leader_lease) =
        persist_authority_records(store.as_ref(), &guard, owner.clone()).await;
    let mut persisted = slot(
        owner.clone(),
        PlacementVersion::new(domain(), guard.term(), Revision::new(3).unwrap()),
    );
    persisted.state = PlacementSlotState::Allocating;
    let claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let grant = ClaimGrant {
        domain: domain(),
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
                expected_domain_member,
                slot: persisted.clone(),
                claim: LeasedClaim {
                    grant,
                    lease_id: claim_lease,
                },
            },
        )
        .await
        .unwrap();
    store.revoke_lease(membership_leader_lease).await.unwrap();
    store.revoke_lease(old_lease).await.unwrap();

    let new_leader = node("new", 3, 32103);
    let leader = PlacementDomainLeader::elect(
        store.clone(),
        associations(&new_leader),
        new_leader,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(2).unwrap(),
        PlacementDomainLeaderConfig::default(),
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
        PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
    );
    legacy.state = PlacementSlotState::Allocating;
    store.insert_generation_three_slot(legacy.clone());
    let leader = PlacementDomainLeader::elect(
        store.clone(),
        associations(&coordinator),
        coordinator,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
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
    let mut first = PlacementDomainLeader::elect(
        store.clone(),
        associations(&first_node),
        first_node,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
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
    let mut second = PlacementDomainLeader::elect(
        store.clone(),
        associations(&second_node),
        second_node,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(2).unwrap(),
        PlacementDomainLeaderConfig::default(),
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
            .get_admin_operation(&domain(), "pause-stable")
            .await
            .unwrap()
            .is_some()
    );
}
