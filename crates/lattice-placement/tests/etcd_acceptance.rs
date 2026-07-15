use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use etcd_client::Client;
use lattice_core::actor_ref::{
    ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation, PlacementDomainId, ProtocolId,
    SingletonKind,
};
use lattice_core::coordinator::CoordinatorScope;
use lattice_placement::allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger};
use lattice_placement::coordinator::{
    DomainMemberRecord, DomainMemberStatus, LeaderRecord, MemberHello, MemberRecord, MemberStatus,
    MembershipLeaderGuard, PlacementDomainHello, PlacementLeaderGuard, SingletonConfig,
};
use lattice_placement::plan::RebalancePlan;
use lattice_placement::region::EntityConfig;
use lattice_placement::storage::domain::{
    ActivateAuthority, AllocateInitial, CreateDomainMember, CreateMember, CreatePlan, DeletePlan,
    DurableStorageLimits, LeasedClaim, PutEntityConfig, PutSingletonConfig, RemoveMember,
    TransitionSlot, UpdateMember,
};
use lattice_placement::storage::etcd::migration::{
    CardinalityMode, MigrationConfig, MigrationDomainMapping, MigrationError, MigrationMode,
    execute as migrate, execute_cardinality,
};
use lattice_placement::storage::etcd::{EtcdPlacementConfig, EtcdPlacementStore};
use lattice_placement::storage::{
    CoordinatorLeaseStore, MembershipStore, PlacementDomainStore, ScopedElectionStore, StorageError,
};
use lattice_placement::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, MembershipVersion, NodeKey,
    PlacementSlot, PlacementSlotKey, PlacementSlotState, PlacementVersion, Revision, ShardId,
};

#[path = "etcd_acceptance/bounded_migration.rs"]
mod bounded_migration;

fn domain() -> PlacementDomainId {
    PlacementDomainId::new("etcd-acceptance").unwrap()
}

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

fn member_hello(node: NodeKey) -> MemberHello {
    MemberHello {
        node,
        roles: BTreeSet::new(),
        failure_domains: BTreeMap::new(),
        protocols: Vec::new(),
        remoting_capabilities: BTreeSet::new(),
    }
}

fn domain_hello(node: NodeKey, domain: PlacementDomainId) -> PlacementDomainHello {
    PlacementDomainHello::new(
        node,
        domain,
        1,
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    )
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

fn migration_mapping() -> MigrationDomainMapping {
    MigrationDomainMapping {
        entity_types: BTreeMap::from([("migration-entity".to_owned(), domain())]),
        singleton_kinds: BTreeMap::new(),
    }
}

fn legacy_migration_slot_bytes() -> Vec<u8> {
    let slot = PlacementSlot {
        key: PlacementSlotKey::Shard {
            domain: domain(),
            entity_type: EntityType::new("migration-entity").unwrap(),
            shard_id: ShardId::new(3),
        },
        config_fingerprint: ConfigFingerprint::new([4; 32]),
        owner: Some(node("migration-owner", 10, 29110)),
        target: None,
        assignment_generation: AssignmentGeneration::new(2).unwrap(),
        version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(6).unwrap(),
            Revision::new(15).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let mut legacy = serde_json::to_value(slot).unwrap();
    legacy["version"].as_object_mut().unwrap().remove("domain");
    serde_json::to_vec(&legacy).unwrap()
}

async fn seed_generation_four_slot(raw: &mut Client, prefix: &str) -> (String, Vec<u8>) {
    raw.put(format!("{prefix}/schema_generation"), "4", None)
        .await
        .unwrap();
    raw.put(format!("{prefix}/coordinator/term"), "7", None)
        .await
        .unwrap();
    let slot_key = format!("{prefix}/shards/migration-entity/3");
    let slot_bytes = legacy_migration_slot_bytes();
    raw.put(slot_key.clone(), slot_bytes.clone(), None)
        .await
        .unwrap();
    (slot_key, slot_bytes)
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

    let membership_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let membership_leader = LeaderRecord {
        scope: CoordinatorScope::Membership,
        node: node("coordinator", 1, 29001),
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(
        store
            .campaign_leader(&membership_leader, membership_lease)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Membership)
            .await
            .unwrap(),
        Some(membership_leader.clone())
    );
    let membership_guard = MembershipLeaderGuard::new(membership_leader).unwrap();
    let leader_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        scope: CoordinatorScope::Placement(domain()),
        node: node("coordinator", 1, 29001),
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, leader_lease).await.unwrap());
    let guard = PlacementLeaderGuard::new(leader).unwrap();

    let foreign_domain = PlacementDomainId::new("foreign-domain").unwrap();
    let foreign_key = PlacementSlotKey::Shard {
        domain: foreign_domain.clone(),
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
            foreign_domain.clone(),
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
        hello: member_hello(foreign_owner.clone()),
        status: MemberStatus::Up,
        version: MembershipVersion::new(membership_guard.term(), Revision::new(1).unwrap()),
        lease_id: foreign_claim_lease,
    };
    let foreign_domain_member = DomainMemberRecord {
        node: foreign_owner.clone(),
        hello: domain_hello(foreign_owner.clone(), foreign_domain.clone()),
        status: DomainMemberStatus::Up,
        version: PlacementVersion::new(
            foreign_domain.clone(),
            guard.term(),
            Revision::new(1).unwrap(),
        ),
    };
    let foreign_result = store
        .allocate_initial(
            &guard,
            AllocateInitial {
                expected_global_member: foreign_global_member,
                expected_domain_member: foreign_domain_member,
                slot: foreign_slot,
                claim: lattice_placement::storage::domain::LeasedClaim {
                    grant: ClaimGrant {
                        domain: foreign_domain.clone(),
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
    assert!(store.list_slots(&foreign_domain).await.unwrap().is_empty());
    assert_eq!(
        store.get_placement_revision(&foreign_domain).await.unwrap(),
        Revision::new(1).unwrap()
    );

    let member_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let member_node = node("member", 7, 29007);
    let hello = member_hello(member_node.clone());
    let joining = MemberRecord {
        node: member_node,
        hello,
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
        domain: domain(),
        entity_type: EntityType::new("etcd-acceptance").unwrap(),
        shard_id: ShardId::new(1),
    };
    let owner = node("owner", 2, 29002);
    let owner_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let owner_member = MemberRecord {
        node: owner.clone(),
        hello: member_hello(owner.clone()),
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
    let owner_domain_member = DomainMemberRecord {
        node: owner.clone(),
        hello: domain_hello(owner.clone(), domain()),
        status: DomainMemberStatus::Up,
        version: PlacementVersion::new(domain(), guard.term(), Revision::new(2).unwrap()),
    };
    store
        .create_domain_member(
            &guard,
            CreateDomainMember {
                expected_global_member: owner_member.clone(),
                member: owner_domain_member.clone(),
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
        version: PlacementVersion::new(domain(), guard.term(), Revision::new(3).unwrap()),
        state: PlacementSlotState::Allocating,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let claim_lease = store.grant_lease(Duration::from_secs(2)).await.unwrap();
    let claim = ClaimGrant {
        domain: domain(),
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
                expected_global_member: owner_member,
                expected_domain_member: owner_domain_member,
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
    running.version = PlacementVersion::new(domain(), guard.term(), Revision::new(4).unwrap());
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
    assert_eq!(
        store
            .get_leader(&CoordinatorScope::Placement(domain()))
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
        PlacementVersion::new(domain(), guard.term(), Revision::new(4).unwrap());
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
        hello: member_hello(member_node),
        status: MemberStatus::Up,
        version: MembershipVersion::new(term, Revision::new(12).unwrap()),
        lease_id: 99,
    };
    let legacy_member = serde_json::to_value(&member).unwrap();

    raw.put(format!("{prefix}/schema_generation"), "4", None)
        .await
        .unwrap();
    raw.put(format!("{prefix}/coordinator/term"), "7", None)
        .await
        .unwrap();
    let member_key = format!("{prefix}/members/migration-member");
    let slot_key = format!("{prefix}/shards/migration-entity/3");
    let member_bytes = serde_json::to_vec(&legacy_member).unwrap();
    let slot_bytes = legacy_migration_slot_bytes();
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
            mapping: migration_mapping(),
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
    let backup = backup_dir.path().join("generation-4.json");
    let report = migrate(
        MigrationMode::Apply,
        MigrationConfig {
            endpoints: endpoints.clone(),
            cluster_prefix: prefix.clone(),
            page_size: 1,
            limits: migration_limits,
            backup_path: Some(backup.clone()),
            mapping: migration_mapping(),
        },
    )
    .await
    .unwrap();
    assert!(report.completed);
    assert!(backup.is_file());
    let migrated_key = format!(
        "{prefix}/domains/{}/shards/migration-entity/3",
        domain().as_str()
    );
    assert!(
        raw.get(slot_key.as_str(), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
    let migrated: PlacementSlot = serde_json::from_slice(
        raw.get(migrated_key.as_str(), None).await.unwrap().kvs()[0].value(),
    )
    .unwrap();
    assert_eq!(migrated.version.term, term);
    assert_eq!(migrated.state, PlacementSlotState::Fenced);
    assert_eq!(
        raw.get(format!("{prefix}/schema_generation"), None)
            .await
            .unwrap()
            .kvs()[0]
            .value(),
        b"5"
    );
    let cardinality = execute_cardinality(
        CardinalityMode::Inspect,
        MigrationConfig {
            endpoints: endpoints.clone(),
            cluster_prefix: prefix.clone(),
            page_size: 1,
            limits: migration_limits,
            backup_path: None,
            mapping: migration_mapping(),
        },
    )
    .await
    .unwrap();
    assert_eq!(cardinality.slots, cardinality.stored_slots);
    assert_eq!(cardinality.members, cardinality.stored_members);

    let resume_prefix = format!("{prefix}-resume");
    raw.put(
        format!("{resume_prefix}/schema_generation"),
        "migrating-to-5",
        None,
    )
    .await
    .unwrap();
    raw.put(format!("{resume_prefix}/coordinator/term"), "7", None)
        .await
        .unwrap();
    let resume_member_key = format!("{resume_prefix}/members/migration-member");
    let resume_slot_key = format!("{resume_prefix}/shards/migration-entity/3");
    raw.put(resume_slot_key, slot_bytes, None).await.unwrap();
    let marker = serde_json::to_vec(&serde_json::json!({
        "last_key": resume_member_key,
        "coordinator_term": 7,
        "limits": migration_limits,
        "completed": false,
        "backup_path": "operator-retained-backup.json",
        "mapping": migration_mapping(),
    }))
    .unwrap();
    raw.put(
        format!("{resume_prefix}/migration/generation-4-to-5"),
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
            mapping: migration_mapping(),
        },
    )
    .await
    .unwrap();
    assert!(report.completed);
    assert_eq!(report.slots, 1);
    assert_eq!(report.members, 0);
    assert_eq!(report.state_revision, 15);

    let active_prefix = format!("{prefix}-active");
    raw.put(format!("{active_prefix}/schema_generation"), "4", None)
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
                mapping: migration_mapping(),
            },
        )
        .await,
        Err(MigrationError::LeaderPresent)
    ));

    let malformed_prefix = format!("{prefix}-malformed");
    raw.put(format!("{malformed_prefix}/schema_generation"), "4", None)
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
                mapping: migration_mapping(),
            },
        )
        .await,
        Err(MigrationError::Codec(_)) | Err(MigrationError::InvalidRecord)
    ));
}

#[tokio::test]
async fn real_etcd_migration_rejects_collision_and_unmapped_type_without_writes() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let mut raw = Client::connect(endpoints.clone(), None).await.unwrap();
    let root = format!(
        "/lattice-migration-boundaries/{}",
        uuid::Uuid::new_v4().simple()
    );
    let collision_prefix = format!("{root}-collision");
    let (legacy_key, legacy_value) = seed_generation_four_slot(&mut raw, &collision_prefix).await;
    let target_key = format!(
        "{collision_prefix}/domains/{}/shards/migration-entity/3",
        domain().as_str()
    );
    let mut target_value: serde_json::Value = serde_json::from_slice(&legacy_value).unwrap();
    target_value["version"]["domain"] = serde_json::to_value(domain()).unwrap();
    target_value["owner"] = serde_json::Value::Null;
    target_value["state"] = serde_json::json!("Fenced");
    let target_value = serde_json::to_vec(&target_value).unwrap();
    raw.put(target_key.clone(), target_value.clone(), None)
        .await
        .unwrap();
    assert!(matches!(
        migrate(
            MigrationMode::DryRun,
            MigrationConfig {
                endpoints: endpoints.clone(),
                cluster_prefix: collision_prefix,
                page_size: 2,
                limits: limits(64),
                backup_path: None,
                mapping: migration_mapping(),
            },
        )
        .await,
        Err(MigrationError::Collision)
    ));
    assert_eq!(
        raw.get(legacy_key, None).await.unwrap().kvs()[0].value(),
        legacy_value
    );
    assert_eq!(
        raw.get(target_key, None).await.unwrap().kvs()[0].value(),
        target_value
    );

    let unmapped_prefix = format!("{root}-unmapped");
    let (legacy_key, legacy_value) = seed_generation_four_slot(&mut raw, &unmapped_prefix).await;
    assert!(matches!(
        migrate(
            MigrationMode::DryRun,
            MigrationConfig {
                endpoints,
                cluster_prefix: unmapped_prefix,
                page_size: 2,
                limits: limits(64),
                backup_path: None,
                mapping: MigrationDomainMapping {
                    entity_types: BTreeMap::new(),
                    singleton_kinds: BTreeMap::new(),
                },
            },
        )
        .await,
        Err(MigrationError::UnmappedType)
    ));
    assert_eq!(
        raw.get(legacy_key, None).await.unwrap().kvs()[0].value(),
        legacy_value
    );
}

#[cfg(feature = "test-failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_etcd_migration_finalization_compare_failure_is_atomic_and_resumable() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let mut raw = Client::connect(endpoints.clone(), None).await.unwrap();
    let prefix = format!(
        "/lattice-migration-rollback/{}",
        uuid::Uuid::new_v4().simple()
    );
    let (legacy_key, _) = seed_generation_four_slot(&mut raw, &prefix).await;
    let backup_dir = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("generation-4.json");
    let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
    let resume_rx = std::sync::Arc::new(std::sync::Mutex::new(resume_rx));
    let hook_resume = std::sync::Arc::clone(&resume_rx);
    let guard = lattice_core::failpoint::install_hook(move |point| {
        if point == lattice_core::failpoint::Failpoint::MigrationBeforeFinalize {
            reached_tx.send(()).unwrap();
            hook_resume.lock().unwrap().recv().unwrap();
        }
    });
    let apply = tokio::spawn(migrate(
        MigrationMode::Apply,
        MigrationConfig {
            endpoints: endpoints.clone(),
            cluster_prefix: prefix.clone(),
            page_size: 1,
            limits: limits(64),
            backup_path: Some(backup_path),
            mapping: migration_mapping(),
        },
    ));
    tokio::task::spawn_blocking(move || reached_rx.recv().unwrap())
        .await
        .unwrap();
    raw.put(
        format!("{prefix}/schema_generation"),
        "rollback-probe",
        None,
    )
    .await
    .unwrap();
    resume_tx.send(()).unwrap();
    assert!(matches!(
        apply.await.unwrap(),
        Err(MigrationError::FinalizeCompareFailed)
    ));
    drop(guard);

    let target_key = format!(
        "{prefix}/domains/{}/shards/migration-entity/3",
        domain().as_str()
    );
    assert!(raw.get(legacy_key, None).await.unwrap().kvs().is_empty());
    assert_eq!(raw.get(target_key, None).await.unwrap().kvs().len(), 1);
    assert!(
        raw.get(format!("{prefix}/membership/state_revision"), None)
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );

    raw.put(
        format!("{prefix}/schema_generation"),
        "migrating-to-5",
        None,
    )
    .await
    .unwrap();
    let report = migrate(
        MigrationMode::Resume,
        MigrationConfig {
            endpoints,
            cluster_prefix: prefix.clone(),
            page_size: 1,
            limits: limits(64),
            backup_path: None,
            mapping: migration_mapping(),
        },
    )
    .await
    .unwrap();
    assert!(report.completed);
    assert_eq!(
        raw.get(format!("{prefix}/schema_generation"), None)
            .await
            .unwrap()
            .kvs()[0]
            .value(),
        b"5"
    );
}

#[tokio::test]
async fn real_etcd_domain_configuration_is_durable_and_cross_domain_guarded() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = format!("/lattice-config-tests/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdPlacementStore::connect(EtcdPlacementConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 2,
        limits: limits(8),
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_schema_generation().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        scope: CoordinatorScope::Placement(domain()),
        node: node("config-leader", 61, 29261),
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = PlacementLeaderGuard::new(leader).unwrap();
    let entity = EntityConfig::new(
        domain(),
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
        store.list_singleton_configs(&domain()).await.unwrap(),
        vec![singleton]
    );

    let foreign = PlacementDomainId::new("foreign-config-domain").unwrap();
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
        scope: CoordinatorScope::Placement(domain()),
        node: node("capacity-leader", 50, 29250),
        protocol_generation: 5,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = PlacementLeaderGuard::new(leader).unwrap();
    let entity_type = EntityType::new("capacity-entity").unwrap();
    let make_plan = |shard: u32| {
        RebalancePlan::from_proposal(
            RebalanceProposal {
                domain: domain(),
                policy_id: "capacity",
                policy_version: 1,
                base_version: PlacementVersion::new(
                    domain(),
                    guard.term(),
                    Revision::new(1).unwrap(),
                ),
                trigger: RebalanceTrigger::Manual {
                    source: None,
                    target: None,
                    bypass_improvement: true,
                },
                moves: vec![ProposedMove {
                    domain: domain(),
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
            mapping: migration_mapping(),
        },
    )
    .await
    .unwrap();
    assert_eq!(report.plans, 1);
    assert_eq!(report.stored_plans, 1);
}
