use std::time::Duration;

use etcd_client::Client;
use lattice_core::actor_ref::{ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation};
use lattice_placement::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};
use lattice_placement::coordinator::{
    LeaderGuard, LeaderRecord, MemberRecord, MemberStatus, NodeHello,
};
use lattice_placement::plan::RebalancePlan;
use lattice_placement::storage::domain::{
    ActivateAuthority, AllocateInitial, CreateMember, CreatePlan, DeletePlan, DurableStorageLimits,
    LeasedClaim, RemoveMember, TransitionSlot, UpdateMember,
};
use lattice_placement::storage::etcd::migration::{
    CardinalityMode, MigrationConfig, MigrationError, MigrationMode, execute as migrate,
    execute_cardinality,
};
use lattice_placement::storage::etcd::{EtcdPlacementConfig, EtcdPlacementStore};
use lattice_placement::storage::{CoordinatorStore, PlacementStore, StorageError};
use lattice_placement::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, NodeKey, PlacementSlot,
    PlacementSlotKey, PlacementSlotState, Revision, ShardId, StateVersion,
};

fn endpoints() -> Option<Vec<String>> {
    std::env::var("LATTICE_ETCD_ENDPOINTS")
        .ok()
        .map(|value| value.split(',').map(str::to_owned).collect())
}

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
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

#[tokio::test]
async fn real_etcd_guarded_domain_commits_and_lease_expiry() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = format!("/lattice-tests/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdPlacementStore::connect(EtcdPlacementConfig {
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
    store.ensure_schema_generation().await.unwrap();
    store.ensure_schema_generation().await.unwrap();
    let different_limits = EtcdPlacementStore::connect(EtcdPlacementConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 16,
        limits: limits(65),
        connect_options: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        different_limits.ensure_schema_generation().await,
        Err(StorageError::SchemaGenerationMismatch)
    ));

    let leader_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        node: node("coordinator", 1, 29001),
        protocol_generation: 2,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, leader_lease).await.unwrap());
    assert_eq!(store.get_leader().await.unwrap(), Some(leader.clone()));
    let guard = LeaderGuard::new(leader);

    let member_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let member_node = node("member", 7, 29007);
    let hello = NodeHello {
        node: member_node.clone(),
        roles: Default::default(),
        capacity_units: 1,
        hosted_entity_types: Default::default(),
        proxied_entity_types: Default::default(),
        singleton_eligibility: Default::default(),
        used_singletons: Default::default(),
        protocols: Vec::new(),
        entity_configs: Vec::new(),
        singleton_configs: Vec::new(),
    };
    let joining = MemberRecord {
        node: member_node,
        hello,
        status: MemberStatus::Joining,
        version: StateVersion::new(guard.term(), Revision::new(2).unwrap()),
        lease_id: member_lease,
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
            .remove_member(&guard, RemoveMember { expected: joining },)
            .await,
        Err(StorageError::CompareFailed)
    ));
    store
        .remove_member(&guard, RemoveMember { expected: up })
        .await
        .unwrap();

    let key = PlacementSlotKey::Shard {
        entity_type: EntityType::new("etcd-acceptance").unwrap(),
        shard_id: ShardId::new(1),
    };
    let owner = node("owner", 2, 29002);
    let allocating = PlacementSlot {
        key: key.clone(),
        config_fingerprint: ConfigFingerprint::new([8; 32]),
        owner: Some(owner.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: StateVersion::new(guard.term(), Revision::new(5).unwrap()),
        state: PlacementSlotState::Allocating,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let claim_lease = store.grant_lease(Duration::from_secs(2)).await.unwrap();
    let claim = ClaimGrant {
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

    let mut running = allocating.clone();
    running.state = PlacementSlotState::Running;
    running.version = StateVersion::new(guard.term(), Revision::new(6).unwrap());
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if store.get_claim(&key).await.unwrap().is_none() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leased claim did not expire"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    store.revoke_lease(leader_lease).await.unwrap();
    assert_eq!(store.get_leader().await.unwrap(), None);
    let mut stale_expected = running;
    stale_expected.state = PlacementSlotState::BeginHandoff;
    stale_expected.target = Some(node("stale-target", 3, 29003));
    stale_expected.active_move = Some(42);
    let mut stale_transition = stale_expected.clone();
    stale_transition.state = PlacementSlotState::Stopping;
    stale_transition.version = StateVersion::new(guard.term(), Revision::new(7).unwrap());
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
    let mismatch = EtcdPlacementStore::connect(EtcdPlacementConfig {
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
        mismatch.ensure_schema_generation().await,
        Err(StorageError::SchemaGenerationMismatch)
    ));
}

#[tokio::test]
async fn real_etcd_generation_four_migration_dry_run_apply_and_resume() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let mut raw = Client::connect(endpoints.clone(), None).await.unwrap();
    let prefix = format!("/lattice-migration-tests/{}", uuid::Uuid::new_v4().simple());
    let term = CoordinatorTerm::new(7).unwrap();
    let member_node = node("migration-member", 9, 29109);
    let member = MemberRecord {
        node: member_node.clone(),
        hello: NodeHello {
            node: member_node,
            roles: Default::default(),
            capacity_units: 1,
            hosted_entity_types: Default::default(),
            proxied_entity_types: Default::default(),
            singleton_eligibility: Default::default(),
            used_singletons: Default::default(),
            protocols: Vec::new(),
            entity_configs: Vec::new(),
            singleton_configs: Vec::new(),
        },
        status: MemberStatus::Up,
        version: StateVersion::new(term, Revision::new(12).unwrap()),
        lease_id: 99,
    };
    let mut legacy_member = serde_json::to_value(&member).unwrap();
    let member_revision = legacy_member
        .as_object_mut()
        .unwrap()
        .remove("version")
        .unwrap()["revision"]
        .clone();
    legacy_member["revision"] = member_revision;

    let key = PlacementSlotKey::Shard {
        entity_type: EntityType::new("migration-entity").unwrap(),
        shard_id: ShardId::new(3),
    };
    let slot = PlacementSlot {
        key: key.clone(),
        config_fingerprint: ConfigFingerprint::new([4; 32]),
        owner: Some(node("migration-owner", 10, 29110)),
        target: None,
        assignment_generation: AssignmentGeneration::new(2).unwrap(),
        version: StateVersion::new(CoordinatorTerm::new(6).unwrap(), Revision::new(15).unwrap()),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let mut legacy_slot = serde_json::to_value(&slot).unwrap();
    let slot_version = legacy_slot
        .as_object_mut()
        .unwrap()
        .remove("version")
        .unwrap();
    legacy_slot["coordinator_term"] = slot_version["term"].clone();
    legacy_slot["revision"] = slot_version["revision"].clone();

    raw.put(format!("{prefix}/schema_generation"), "3", None)
        .await
        .unwrap();
    raw.put(format!("{prefix}/coordinator/term"), "7", None)
        .await
        .unwrap();
    let member_key = format!("{prefix}/members/migration-member");
    let slot_key = format!("{prefix}/shards/migration-entity/3");
    let member_bytes = serde_json::to_vec(&legacy_member).unwrap();
    let slot_bytes = serde_json::to_vec(&legacy_slot).unwrap();
    raw.put(member_key.clone(), member_bytes.clone(), None)
        .await
        .unwrap();
    raw.put(slot_key.clone(), slot_bytes.clone(), None)
        .await
        .unwrap();

    let migration_limits = limits(64);
    let report = migrate(
        MigrationMode::DryRun,
        MigrationConfig {
            endpoints: endpoints.clone(),
            cluster_prefix: prefix.clone(),
            page_size: 1,
            limits: migration_limits,
            backup_path: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.state_revision, 15);
    assert_eq!(report.quarantined_records, 1);
    assert_eq!(
        raw.get(member_key.as_str(), None).await.unwrap().kvs()[0].value(),
        member_bytes
    );
    assert_eq!(
        raw.get(slot_key.as_str(), None).await.unwrap().kvs()[0].value(),
        slot_bytes
    );

    let backup_dir = tempfile::tempdir().unwrap();
    let backup = backup_dir.path().join("generation-3.json");
    let report = migrate(
        MigrationMode::Apply,
        MigrationConfig {
            endpoints: endpoints.clone(),
            cluster_prefix: prefix.clone(),
            page_size: 1,
            limits: migration_limits,
            backup_path: Some(backup.clone()),
        },
    )
    .await
    .unwrap();
    assert!(report.completed);
    assert!(backup.is_file());
    let migrated: PlacementSlot =
        serde_json::from_slice(raw.get(slot_key.as_str(), None).await.unwrap().kvs()[0].value())
            .unwrap();
    assert_eq!(migrated.version.term, term);
    assert_eq!(migrated.state, PlacementSlotState::Fenced);
    assert_eq!(
        raw.get(format!("{prefix}/schema_generation"), None)
            .await
            .unwrap()
            .kvs()[0]
            .value(),
        b"4"
    );
    let cardinality = execute_cardinality(
        CardinalityMode::Inspect,
        MigrationConfig {
            endpoints: endpoints.clone(),
            cluster_prefix: prefix.clone(),
            page_size: 1,
            limits: migration_limits,
            backup_path: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(cardinality.slots, cardinality.stored_slots);
    assert_eq!(cardinality.members, cardinality.stored_members);

    let resume_prefix = format!("{prefix}-resume");
    raw.put(
        format!("{resume_prefix}/schema_generation"),
        "migrating-to-4",
        None,
    )
    .await
    .unwrap();
    raw.put(format!("{resume_prefix}/coordinator/term"), "7", None)
        .await
        .unwrap();
    let resume_member_key = format!("{resume_prefix}/members/migration-member");
    raw.put(
        resume_member_key.clone(),
        serde_json::to_vec(&member).unwrap(),
        None,
    )
    .await
    .unwrap();
    let resume_slot_key = format!("{resume_prefix}/shards/migration-entity/3");
    raw.put(resume_slot_key, slot_bytes, None).await.unwrap();
    let marker = format!(
        "{{\"last_key\":{},\"coordinator_term\":7,\"limits\":{},\"completed\":false,\"backup_path\":\"operator-retained-backup.json\"}}",
        serde_json::to_string(&resume_member_key).unwrap(),
        serde_json::to_string(&migration_limits).unwrap(),
    );
    raw.put(
        format!("{resume_prefix}/migration/generation-3-to-4"),
        marker,
        None,
    )
    .await
    .unwrap();
    let report = migrate(
        MigrationMode::Resume,
        MigrationConfig {
            endpoints: endpoints.clone(),
            cluster_prefix: resume_prefix.clone(),
            page_size: 1,
            limits: migration_limits,
            backup_path: None,
        },
    )
    .await
    .unwrap();
    assert!(report.completed);
    assert_eq!(report.slots, 1);
    assert_eq!(report.members, 1);
    assert_eq!(report.state_revision, 15);

    let active_prefix = format!("{prefix}-active");
    raw.put(format!("{active_prefix}/schema_generation"), "3", None)
        .await
        .unwrap();
    raw.put(format!("{active_prefix}/coordinator/term"), "7", None)
        .await
        .unwrap();
    raw.put(
        format!("{active_prefix}/coordinator/leader"),
        b"present".to_vec(),
        None,
    )
    .await
    .unwrap();
    assert!(matches!(
        migrate(
            MigrationMode::DryRun,
            MigrationConfig {
                endpoints: endpoints.clone(),
                cluster_prefix: active_prefix,
                page_size: 2,
                limits: migration_limits,
                backup_path: None,
            },
        )
        .await,
        Err(MigrationError::LeaderPresent)
    ));

    let malformed_prefix = format!("{prefix}-malformed");
    raw.put(format!("{malformed_prefix}/schema_generation"), "3", None)
        .await
        .unwrap();
    raw.put(format!("{malformed_prefix}/coordinator/term"), "7", None)
        .await
        .unwrap();
    raw.put(
        format!("{malformed_prefix}/members/bad"),
        b"not-json".to_vec(),
        None,
    )
    .await
    .unwrap();
    assert!(matches!(
        migrate(
            MigrationMode::DryRun,
            MigrationConfig {
                endpoints,
                cluster_prefix: malformed_prefix,
                page_size: 2,
                limits: migration_limits,
                backup_path: None,
            },
        )
        .await,
        Err(MigrationError::Codec(_)) | Err(MigrationError::InvalidRecord)
    ));
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
    let store = EtcdPlacementStore::connect(EtcdPlacementConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 2,
        limits: storage_limits,
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_schema_generation().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        node: node("capacity-leader", 50, 29250),
        protocol_generation: 4,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = LeaderGuard::new(leader);
    let entity_type = EntityType::new("capacity-entity").unwrap();
    let make_plan = |shard: u32| {
        RebalancePlan::from_proposal(
            RebalanceProposal {
                policy_id: "capacity",
                policy_version: 1,
                base_version: StateVersion::new(guard.term(), Revision::new(1).unwrap()),
                trigger: RebalanceTrigger::Manual {
                    source: None,
                    target: None,
                    bypass_improvement: true,
                },
                moves: vec![ProposedMove {
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
    let first = make_plan(0);
    let second = make_plan(1);
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
    let report = execute_cardinality(
        CardinalityMode::Inspect,
        MigrationConfig {
            endpoints,
            cluster_prefix: prefix,
            page_size: 1,
            limits: storage_limits,
            backup_path: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.plans, 1);
    assert_eq!(report.stored_plans, 1);
}
