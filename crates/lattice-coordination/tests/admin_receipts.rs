mod candidate_fixture;

use candidate_fixture::prepare_leader;
use lattice_coordination::{
    candidates::CandidateGeneration,
    coordinator::{GroupLeaderGuard, LeaderRecord},
    runtime::CoordinatorInspection,
    storage::{
        ActorGroupStore, CoordinatorLeaseStore, ScopedElectionStore, StorageError,
        etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
        records::{
            AdminOperationRecord, AdminOperationResult, AdminOperationStatus,
            AutomaticBalanceSettings, CommitAutomaticSettings, CompactAdminOperations,
            DurableStorageLimits,
        },
    },
    types::{CoordinatorTerm, NodeKey, PlacementVersion},
};
use lattice_model::cluster::{ActorGroupId, CoordinatorScope, NodeEndpoint, NodeIncarnation};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn receipt_gc_cannot_reexecute_an_old_revision_bound_operation() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let limits = DurableStorageLimits {
        maximum_slots: 8,
        maximum_plans: 8,
        maximum_members: 8,
        maximum_admin_operations: 8,
        maximum_entity_configs: 8,
        maximum_singleton_configs: 8,
    };
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints,
        cluster_prefix: format!("/lattice-admin-receipts/{}", uuid::Uuid::new_v4()),
        list_page_size: 8,
        limits,
        connect_options: None,
    })
    .await
    .unwrap();
    let epoch = store.ensure_framework().await.unwrap();
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let group = ActorGroupId::new("admin").unwrap();
    let leader = prepare_leader(
        &store,
        LeaderRecord {
            epoch,
            scope: CoordinatorScope::Group(group.clone()),
            node: NodeKey {
                node_id: "admin-leader".to_owned(),
                address: NodeEndpoint::new("localhost", 32199).unwrap(),
                incarnation: NodeIncarnation::new(1).unwrap(),
            },
            term: CoordinatorTerm::new(1).unwrap(),
            candidate_generation: CandidateGeneration::new(1).unwrap(),
            candidate_lease_id: lease,
        },
        lease,
    )
    .await
    .unwrap();
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = GroupLeaderGuard::new(leader).unwrap();
    let version = PlacementVersion::new(
        group.clone(),
        guard.term(),
        store.get_placement_revision(&group).await.unwrap(),
    );
    let inspection = CoordinatorInspection {
        epoch,
        version: version.clone(),
        automatic_globally_paused: false,
        paused_entity_types: vec![],
        slots: vec![],
        plans: vec![],
        reconciliation_backlog: 0,
        reconciliation_oldest_pending_millis: None,
        reconciliation_last_success_age_millis: None,
        quarantined_records: vec![],
        durable_limits: limits,
        retained_admin_operations: 0,
        leadership_loss_count: 0,
        commit_conflict_count: 0,
        unknown_outcome_count: 0,
        capacity_rejection_count: 0,
    };
    let committed = PlacementVersion::new(
        group.clone(),
        guard.term(),
        version.revision.next().unwrap(),
    );
    let operation = AdminOperationRecord {
        operation_id: inspection.new_operation_id(),
        fingerprint: "pause".to_owned(),
        status: AdminOperationStatus::Completed,
        result: AdminOperationResult::AutomaticBalanceUpdated,
        version: committed.clone(),
        created_unix_millis: 1,
        expires_unix_millis: 2,
    };
    let settings = AutomaticBalanceSettings {
        globally_paused: true,
        paused_entity_types: Default::default(),
        version: committed.clone(),
    };
    store
        .commit_automatic_settings(
            &guard,
            CommitAutomaticSettings {
                expected: None,
                settings: settings.clone(),
                operation: operation.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_placement_revision(&group).await.unwrap(),
        committed.revision
    );
    store
        .compact_admin_operations(
            &guard,
            CompactAdminOperations {
                expected: vec![operation.clone()],
            },
        )
        .await
        .unwrap();
    assert!(
        store
            .get_admin_operation(&group, &operation.operation_id)
            .await
            .unwrap()
            .is_none()
    );
    let mut replay = operation;
    replay.version.revision = committed.revision.next().unwrap();
    let mut replacement = settings.clone();
    replacement.globally_paused = false;
    replacement.version = replay.version.clone();
    assert!(matches!(
        store
            .commit_automatic_settings(
                &guard,
                CommitAutomaticSettings {
                    expected: Some(settings.clone()),
                    settings: replacement,
                    operation: replay,
                }
            )
            .await,
        Err(StorageError::InvalidRecord)
    ));
    assert_eq!(
        store.get_automatic_settings(&group).await.unwrap(),
        Some(settings)
    );
    store.revoke_lease(lease).await.unwrap();
}
