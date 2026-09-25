mod candidate_fixture;
use candidate_fixture::prepare_leader;
use lattice_coordination::candidates::CandidateGeneration;
use lattice_model::run::RunEpoch;
use std::{sync::Arc, time::Duration};

use etcd_client::Client;
use lattice_coordination::{
    allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger},
    coordinator::{
        ClusterLeaderGuard, GroupLeaderGuard, GroupMemberRecord, GroupMemberStatus, LeaderRecord,
        MemberRecord, MemberStatus, SingletonConfig,
    },
    plan::{MoveProgress, PlanStatus, RebalancePlan},
    region::EntityConfig,
    storage::{
        ActorGroupStore, CoordinatorLeaseStore, MembershipStore, ScopedElectionStore, StorageError,
        etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
        records::{
            ActivateAuthority, AdoptAuthority, AllocateInitial, CreateGroupMember, CreateMember,
            CreatePlan, DeletePlan, DurableStorageLimits, LeasedClaim, PutEntityConfig,
            PutSingletonConfig, RemoveMember, TransitionSlot, UpdateMember,
        },
    },
    types::{
        AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, MembershipVersion,
        NodeKey, PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion, Revision,
        ShardId,
    },
};
use lattice_model::{
    actor::ProtocolId,
    cluster::CoordinatorScope,
    cluster::{
        ActorGroupId, ConfigFingerprint, EntityType, NodeEndpoint, NodeIncarnation, SingletonKind,
    },
};
use tokio::{sync::Barrier, task::JoinSet, time::Instant};

#[path = "etcd_acceptance/paged_reads.rs"]
mod paged_reads;

fn group() -> ActorGroupId {
    ActorGroupId::new("etcd-acceptance").unwrap()
}

fn endpoints() -> Option<Vec<String>> {
    std::env::var("LATTICE_ETCD_ENDPOINTS")
        .ok()
        .map(|value| value.split(',').map(str::to_owned).collect())
}

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeEndpoint::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn limits(maximum_slots: usize) -> DurableStorageLimits {
    DurableStorageLimits {
        maximum_slots,
        maximum_plans: 64,
        maximum_members: 64,
        maximum_admin_operations: 64,
        maximum_entity_configs: 64,
        maximum_singleton_configs: 64,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_framework_initialization_is_idempotent_and_rejects_legacy_data() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = format!(
        "/lattice-schema-race-tests/{}",
        uuid::Uuid::new_v4().simple()
    );
    let mut stores = Vec::new();
    for _ in 0..16 {
        stores.push(
            EtcdCoordinationStore::connect(EtcdCoordinationConfig {
                endpoints: endpoints.clone(),
                cluster_prefix: prefix.clone(),
                list_page_size: 16,
                limits: limits(64),
                connect_options: None,
            })
            .await
            .unwrap(),
        );
    }
    let barrier = Arc::new(Barrier::new(stores.len()));
    let mut initializers = JoinSet::new();
    for store in stores {
        let barrier = barrier.clone();
        initializers.spawn(async move {
            barrier.wait().await;
            store.ensure_framework().await
        });
    }
    while let Some(result) = initializers.join_next().await {
        result.unwrap().unwrap();
    }

    let legacy_prefix = format!(
        "/lattice-schema-legacy-tests/{}",
        uuid::Uuid::new_v4().simple()
    );
    let mut raw = Client::connect(endpoints.clone(), None).await.unwrap();
    raw.put(format!("{legacy_prefix}/legacy/member"), "present", None)
        .await
        .unwrap();
    let legacy = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints,
        cluster_prefix: legacy_prefix,
        list_page_size: 16,
        limits: limits(64),
        connect_options: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        legacy.ensure_framework().await,
        Err(StorageError::FrameworkMismatch)
    ));
}

#[tokio::test]
async fn real_etcd_guarded_group_commits_and_lease_expiry() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = format!("/lattice-tests/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 16,
        limits: DurableStorageLimits {
            maximum_slots: 64,
            maximum_plans: 64,
            maximum_members: 64,
            maximum_admin_operations: 64,
            maximum_entity_configs: 64,
            maximum_singleton_configs: 64,
        },
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_framework().await.unwrap();
    store.ensure_framework().await.unwrap();
    let different_limits = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 16,
        limits: limits(65),
        connect_options: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        different_limits.ensure_framework().await,
        Err(StorageError::StorageMetadataMismatch)
    ));

    let membership_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let membership_leader = LeaderRecord {
        candidate_generation: CandidateGeneration::new(1).unwrap(),
        candidate_lease_id: 1,
        epoch: RunEpoch::INITIAL,
        scope: CoordinatorScope::Cluster,
        node: node("coordinator", 1, 29001),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    let membership_leader = prepare_leader(&store, membership_leader, membership_lease)
        .await
        .unwrap();
    assert!(
        store
            .campaign_leader(&membership_leader, membership_lease)
            .await
            .unwrap()
    );
    assert_eq!(
        store.get_leader(&CoordinatorScope::Cluster).await.unwrap(),
        Some(membership_leader.clone())
    );
    let membership_guard = ClusterLeaderGuard::new(membership_leader).unwrap();
    let leader_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        candidate_generation: CandidateGeneration::new(1).unwrap(),
        candidate_lease_id: 1,
        epoch: RunEpoch::INITIAL,
        scope: CoordinatorScope::Group(group()),
        node: node("coordinator", 1, 29001),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    let leader = prepare_leader(&store, leader, leader_lease).await.unwrap();
    assert!(store.campaign_leader(&leader, leader_lease).await.unwrap());
    let guard = GroupLeaderGuard::new(leader).unwrap();

    let foreign_group = ActorGroupId::new("foreign-group").unwrap();
    let foreign_key = PlacementSlotKey::Shard {
        group: foreign_group.clone(),
        entity_type: EntityType::new("foreign").unwrap(),
        shard_id: ShardId::new(0),
    };
    let foreign_owner = node("foreign-owner", 99, 29099);
    let foreign_slot = PlacementSlot {
        key: foreign_key.clone(),
        config_fingerprint: ConfigFingerprint::new([3; 32]),
        owner: Some(foreign_owner.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(
            foreign_group.clone(),
            guard.term(),
            Revision::new(2).unwrap(),
        ),
        state: PlacementSlotState::Allocating,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let foreign_claim_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let foreign_global_member = MemberRecord {
        node: foreign_owner.clone(),
        status: MemberStatus::Up,
        version: MembershipVersion::new(membership_guard.term(), Revision::new(1).unwrap()),
        lease_id: foreign_claim_lease,
    };
    let foreign_group_member = GroupMemberRecord {
        node: foreign_owner.clone(),
        status: GroupMemberStatus::Up,
        version: PlacementVersion::new(
            foreign_group.clone(),
            guard.term(),
            Revision::new(1).unwrap(),
        ),
    };
    let foreign_result = store
        .allocate_initial(
            &guard,
            AllocateInitial {
                expected_global_member: foreign_global_member,
                expected_group_member: foreign_group_member,
                slot: foreign_slot,
                claim: LeasedClaim {
                    grant: ClaimGrant {
                        request_id: 0,
                        group: foreign_group.clone(),
                        slot: foreign_key.clone(),
                        owner: foreign_owner,
                        coordinator_term: guard.term(),
                        assignment_generation: AssignmentGeneration::new(1).unwrap(),
                        grant_sequence: GrantSequence::new(1).unwrap(),
                        ttl: Duration::from_secs(10),
                    },
                    lease_id: foreign_claim_lease,
                },
            },
        )
        .await;
    assert!(matches!(foreign_result, Err(StorageError::InvalidRecord)));
    assert!(store.get_slot(&foreign_key).await.unwrap().is_none());
    assert!(store.list_slots(&foreign_group).await.unwrap().is_empty());
    assert_eq!(
        store.get_placement_revision(&foreign_group).await.unwrap(),
        Revision::new(1).unwrap()
    );

    let member_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let member_node = node("member", 7, 29007);
    let joining = MemberRecord {
        node: member_node,
        status: MemberStatus::Joining,
        version: MembershipVersion::new(membership_guard.term(), Revision::new(2).unwrap()),
        lease_id: member_lease,
    };
    store
        .create_member(
            &membership_guard,
            CreateMember {
                member: joining.clone(),
            },
        )
        .await
        .unwrap();
    let mut up = joining.clone();
    up.status = MemberStatus::Up;
    up.version = MembershipVersion::new(membership_guard.term(), Revision::new(3).unwrap());
    store
        .update_member(
            &membership_guard,
            UpdateMember {
                expected: joining.clone(),
                member: up.clone(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .remove_member(&membership_guard, RemoveMember { expected: joining },)
            .await,
        Err(StorageError::CompareFailed)
    ));
    store
        .remove_member(&membership_guard, RemoveMember { expected: up })
        .await
        .unwrap();

    let key = PlacementSlotKey::Shard {
        group: group(),
        entity_type: EntityType::new("etcd-acceptance").unwrap(),
        shard_id: ShardId::new(1),
    };
    let owner = node("owner", 2, 29002);
    let owner_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let owner_member = MemberRecord {
        node: owner.clone(),
        status: MemberStatus::Up,
        version: MembershipVersion::new(membership_guard.term(), Revision::new(5).unwrap()),
        lease_id: owner_lease,
    };
    store
        .create_member(
            &membership_guard,
            CreateMember {
                member: owner_member.clone(),
            },
        )
        .await
        .unwrap();
    let owner_group_member = GroupMemberRecord {
        node: owner.clone(),
        status: GroupMemberStatus::Up,
        version: PlacementVersion::new(group(), guard.term(), Revision::new(2).unwrap()),
    };
    store
        .create_group_member(
            &guard,
            CreateGroupMember {
                expected_global_member: owner_member.clone(),
                member: owner_group_member.clone(),
            },
        )
        .await
        .unwrap();
    let allocating = PlacementSlot {
        key: key.clone(),
        config_fingerprint: ConfigFingerprint::new([8; 32]),
        owner: Some(owner.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(group(), guard.term(), Revision::new(3).unwrap()),
        state: PlacementSlotState::Allocating,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let claim_lease = store.grant_lease(Duration::from_secs(2)).await.unwrap();
    let claim = ClaimGrant {
        request_id: 1,
        group: group(),
        slot: key.clone(),
        owner,
        coordinator_term: guard.term(),
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        grant_sequence: GrantSequence::new(1).unwrap(),
        ttl: Duration::from_secs(2),
    };
    store
        .allocate_initial(
            &guard,
            AllocateInitial {
                expected_global_member: owner_member.clone(),
                expected_group_member: owner_group_member.clone(),
                slot: allocating.clone(),
                claim: LeasedClaim {
                    grant: claim.clone(),
                    lease_id: claim_lease,
                },
            },
        )
        .await
        .unwrap();
    assert!(store.get_slot(&key).await.unwrap().is_some());
    assert!(store.get_claim(&key).await.unwrap().is_some());

    // Renewal must bind a nonzero owner request and compare the exact previously
    // issued grant. A duplicate commit cannot silently mint another sequence.
    let mut renewal = claim.clone();
    renewal.request_id = 123;
    renewal.grant_sequence = GrantSequence::new(2).unwrap();
    let request = AdoptAuthority {
        expected_global_member: owner_member,
        expected_group_member: owner_group_member,
        expected_slot: allocating.clone(),
        expected_claim: claim,
        claim: LeasedClaim {
            grant: renewal.clone(),
            lease_id: claim_lease,
        },
    };
    let mut unsolicited = request.clone();
    unsolicited.claim.grant.request_id = 0;
    assert!(matches!(
        store.adopt_authority(&guard, unsolicited).await,
        Err(StorageError::InvalidTransition)
    ));
    store
        .adopt_authority(&guard, request.clone())
        .await
        .unwrap();
    assert!(matches!(
        store.adopt_authority(&guard, request).await,
        Err(StorageError::CompareFailed)
    ));
    assert_eq!(store.get_claim(&key).await.unwrap().unwrap().grant, renewal);
    let claim = renewal;

    let mut running = allocating.clone();
    running.state = PlacementSlotState::Running;
    running.version = PlacementVersion::new(group(), guard.term(), Revision::new(4).unwrap());
    store
        .activate_authority(
            &guard,
            ActivateAuthority {
                expected_slot: allocating,
                expected_claim: claim,
                slot: running.clone(),
            },
        )
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if store.get_claim(&key).await.unwrap().is_none() {
            break;
        }
        assert!(Instant::now() < deadline, "leased claim did not expire");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    store.revoke_lease(leader_lease).await.unwrap();
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Group(group()))
            .await
            .unwrap(),
        None
    );
    let mut stale_expected = running;
    stale_expected.state = PlacementSlotState::BeginHandoff;
    stale_expected.target = Some(node("stale-target", 3, 29003));
    stale_expected.active_move = Some(42);
    let mut stale_transition = stale_expected.clone();
    stale_transition.state = PlacementSlotState::Stopping;
    stale_transition.version =
        PlacementVersion::new(group(), guard.term(), Revision::new(4).unwrap());
    let stale_result = store
        .transition_slot(
            &guard,
            TransitionSlot {
                expected: stale_expected,
                slot: stale_transition,
            },
        )
        .await;
    assert!(
        matches!(stale_result, Err(StorageError::LeadershipLost)),
        "unexpected stale-leader result: {stale_result:?}"
    );

    let mismatch_prefix = format!("{prefix}-mismatch");
    let mut raw = Client::connect(endpoints.clone(), None).await.unwrap();
    raw.put(format!("{mismatch_prefix}/schema_generation"), "1", None)
        .await
        .unwrap();
    let mismatch = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints,
        cluster_prefix: mismatch_prefix,
        list_page_size: 8,
        limits: DurableStorageLimits {
            maximum_slots: 8,
            maximum_plans: 8,
            maximum_members: 8,
            maximum_admin_operations: 8,
            maximum_entity_configs: 8,
            maximum_singleton_configs: 8,
        },
        connect_options: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        mismatch.ensure_framework().await,
        Err(StorageError::FrameworkMismatch)
    ));
}

#[tokio::test]
async fn real_etcd_group_configuration_is_durable_and_cross_group_guarded() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = format!("/lattice-config-tests/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 2,
        limits: limits(8),
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_framework().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        candidate_generation: CandidateGeneration::new(1).unwrap(),
        candidate_lease_id: 1,
        epoch: RunEpoch::INITIAL,
        scope: CoordinatorScope::Group(group()),
        node: node("config-leader", 61, 29261),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    let leader = prepare_leader(&store, leader, lease).await.unwrap();
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = GroupLeaderGuard::new(leader).unwrap();
    let entity = EntityConfig::new(
        group(),
        EntityType::new("durable-invoice").unwrap(),
        ProtocolId::new(61).unwrap(),
        16,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    store
        .put_entity_config(
            &guard,
            PutEntityConfig {
                expected: None,
                config: entity.clone(),
            },
        )
        .await
        .unwrap();
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
        SingletonKind::new("durable-scheduler").unwrap(),
        ProtocolId::new(62).unwrap(),
    );
    store
        .put_singleton_config(
            &guard,
            PutSingletonConfig {
                expected: None,
                config: singleton.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.list_singleton_configs(&group()).await.unwrap(),
        vec![singleton]
    );

    let foreign = ActorGroupId::new("foreign-config-group").unwrap();
    let foreign_entity = EntityConfig::new(
        foreign.clone(),
        EntityType::new("foreign-invoice").unwrap(),
        ProtocolId::new(63).unwrap(),
        8,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    assert!(matches!(
        store
            .put_entity_config(
                &guard,
                PutEntityConfig {
                    expected: None,
                    config: foreign_entity,
                },
            )
            .await,
        Err(StorageError::InvalidRecord)
    ));
    assert!(
        store
            .list_entity_configs(&foreign)
            .await
            .unwrap()
            .is_empty()
    );

    let mut client = Client::connect(endpoints, None).await.unwrap();
    client
        .delete(
            prefix,
            Some(etcd_client::DeleteOptions::new().with_prefix()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn real_etcd_plan_capacity_is_exact_and_recovers_after_guarded_delete() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = format!("/lattice-capacity-tests/{}", uuid::Uuid::new_v4().simple());
    let mut storage_limits = limits(8);
    storage_limits.maximum_plans = 1;
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 2,
        limits: storage_limits,
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_framework().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        candidate_generation: CandidateGeneration::new(1).unwrap(),
        candidate_lease_id: 1,
        epoch: RunEpoch::INITIAL,
        scope: CoordinatorScope::Group(group()),
        node: node("capacity-leader", 50, 29250),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    let leader = prepare_leader(&store, leader, lease).await.unwrap();
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = GroupLeaderGuard::new(leader).unwrap();
    let entity_type = EntityType::new("capacity-entity").unwrap();
    let make_plan = |shard: u32| {
        RebalancePlan::from_proposal(
            RebalanceProposal {
                group: group(),
                policy_id: "capacity",
                policy_version: 1,
                base_version: PlacementVersion::new(
                    group(),
                    guard.term(),
                    Revision::new(1).unwrap(),
                ),
                trigger: RebalanceTrigger::Manual {
                    source: None,
                    target: None,
                    bypass_improvement: true,
                },
                moves: vec![ProposedMove {
                    group: group(),
                    entity_type: entity_type.clone(),
                    shard_id: ShardId::new(shard),
                    expected_generation: AssignmentGeneration::new(1).unwrap(),
                    source: node("capacity-source", 51, 29251),
                    target: node("capacity-target", 52, 29252),
                    estimated_weight: 1,
                }],
            },
            entity_type.clone(),
            guard.term(),
            1,
        )
        .unwrap()
    };
    let mut first = make_plan(0);
    let mut second = make_plan(1);
    // Capacity bounds admitted work/history, never unstarted policy proposals.
    first.moves[0].progress = MoveProgress::Completed;
    first.status = PlanStatus::Completed;
    second.moves[0].progress = MoveProgress::Completed;
    second.status = PlanStatus::Completed;
    store
        .create_plan(
            &guard,
            CreatePlan {
                plan: first.clone(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .create_plan(
                &guard,
                CreatePlan {
                    plan: second.clone(),
                },
            )
            .await,
        Err(StorageError::Capacity)
    ));
    store
        .delete_plan(&guard, DeletePlan { expected: first })
        .await
        .unwrap();
    store
        .create_plan(
            &guard,
            CreatePlan {
                plan: second.clone(),
            },
        )
        .await
        .unwrap();
    store.revoke_lease(lease).await.unwrap();
    let mut raw = Client::connect(endpoints, None).await.unwrap();
    let counter = raw
        .get(
            format!("{prefix}/runs/1/groups/{}/counters/plans", group().as_str()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(counter.kvs()[0].value(), b"1");
    assert_eq!(store.list_plans(&group()).await.unwrap().len(), 1);
}
