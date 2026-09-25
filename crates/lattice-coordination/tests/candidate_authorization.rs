use std::time::Duration;

use lattice_coordination::{
    candidates::{CandidateChange, CandidateRegistration},
    coordinator::{ClusterLeaderGuard, LeaderRecord},
    storage::{
        ClusterLifecycleStore, InMemoryCoordinationStore, ScopedElectionStore, StorageError,
        candidates::CandidateStore,
        etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
        records::DurableStorageLimits,
    },
    types::{CoordinatorTerm, NodeKey},
};
use lattice_model::{
    cluster::{CoordinatorScope, NodeEndpoint, NodeIncarnation},
    run::{ControlOperationId, RunEpoch},
};

fn change(
    epoch: RunEpoch,
    revision: u64,
    node: &str,
    enabled: bool,
    operation: &str,
) -> CandidateChange {
    CandidateChange {
        epoch,
        scope: CoordinatorScope::Cluster,
        expected_revision: revision,
        node_id: node.to_owned(),
        enabled,
        operation: ControlOperationId::new(operation).unwrap(),
    }
}

async fn candidate_contract<S: CandidateStore>(store: &S) {
    let epoch = store.ensure_framework().await.unwrap();
    let first_request = change(epoch, 0, "a", true, "enable-a");
    let first = store.change_candidate(first_request.clone()).await.unwrap();
    assert_eq!(
        store.change_candidate(first_request.clone()).await.unwrap(),
        first
    );
    let old_authorization = first.authorization.unwrap();
    let second = store
        .change_candidate(change(epoch, 1, "b", true, "enable-b"))
        .await
        .unwrap();
    assert_eq!(second.eligible_count, 2);
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let registration = CandidateRegistration {
        epoch,
        authorization: old_authorization.clone(),
        lease_id: lease,
        node: NodeKey {
            node_id: "a".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 32105).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
    };
    store.register_candidate(&registration).await.unwrap();
    assert_eq!(
        store
            .online_candidates(&CoordinatorScope::Cluster)
            .await
            .unwrap(),
        vec![registration.clone()]
    );

    // Both requests observed two candidates. The scope CAS permits at most one.
    let (a, b) = tokio::join!(
        store.change_candidate(change(epoch, 2, "a", false, "remove-a")),
        store.change_candidate(change(epoch, 2, "b", false, "remove-b")),
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let (removed, survivor) = if a.is_ok() { ("a", "b") } else { ("b", "a") };
    let state = store
        .candidate_set(&CoordinatorScope::Cluster)
        .await
        .unwrap();
    assert_eq!(state.eligible_count, 1);
    assert!(matches!(
        store
            .change_candidate(change(epoch, state.revision, survivor, false, "last"))
            .await,
        Err(StorageError::LastCandidate)
    ));
    store
        .change_candidate(change(
            epoch,
            state.revision,
            removed,
            true,
            "restore-redundancy",
        ))
        .await
        .unwrap();

    // Always exercise a's revoke/re-add, regardless of which concurrent removal won.
    let revision = store
        .candidate_set(&CoordinatorScope::Cluster)
        .await
        .unwrap()
        .revision;
    store
        .change_candidate(change(epoch, revision, "a", false, "revoke-a"))
        .await
        .unwrap();
    assert!(
        store
            .online_candidates(&CoordinatorScope::Cluster)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store.register_candidate(&registration).await,
        Err(StorageError::CandidateNotEligible)
    ));
    let revision = store
        .candidate_set(&CoordinatorScope::Cluster)
        .await
        .unwrap()
        .revision;
    let fresh = store
        .change_candidate(change(epoch, revision, "a", true, "readd-a"))
        .await
        .unwrap()
        .authorization
        .unwrap();
    assert!(fresh.generation.get() > old_authorization.generation.get());
    let replacement = CandidateRegistration {
        authorization: fresh,
        ..registration.clone()
    };
    store.register_candidate(&replacement).await.unwrap();
    assert!(matches!(
        store.unregister_candidate(&registration).await,
        Err(StorageError::CompareFailed)
    ));
    assert_eq!(
        store
            .online_candidates(&CoordinatorScope::Cluster)
            .await
            .unwrap(),
        vec![replacement]
    );
    assert!(
        matches!(
            store.change_candidate(first_request).await,
            Err(StorageError::CompareFailed)
        ),
        "a delayed initial enable must not gain a new generation"
    );
    store.revoke_lease(lease).await.unwrap();
    assert!(
        store
            .online_candidates(&CoordinatorScope::Cluster)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn memory_candidate_changes_are_serialized_and_readdition_is_not_aba() {
    candidate_contract(&InMemoryCoordinationStore::new(8, 8).unwrap()).await;
}

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn etcd_candidate_changes_are_serialized_and_readdition_is_not_aba() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS").expect("isolated etcd");
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.split(',').map(str::to_owned).collect(),
        cluster_prefix: format!("/lattice-candidates-test/{}", uuid::Uuid::new_v4().simple()),
        list_page_size: 4,
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
    candidate_contract(&store).await;
}
async fn revoked_candidate_fences_existing_leader<
    S: ScopedElectionStore + ClusterLifecycleStore,
>(
    store: &S,
) {
    let epoch = store.ensure_framework().await.unwrap();
    let authorization = store
        .change_candidate(change(epoch, 0, "a", true, "leader-a"))
        .await
        .unwrap()
        .authorization
        .unwrap();
    store
        .change_candidate(change(epoch, 1, "b", true, "standby-b"))
        .await
        .unwrap();
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let leader = LeaderRecord {
        epoch,
        scope: CoordinatorScope::Cluster,
        candidate_generation: authorization.generation,
        candidate_lease_id: lease,
        node: NodeKey {
            node_id: "a".into(),
            address: NodeEndpoint::new("127.0.0.1", 32106).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
        term: CoordinatorTerm::new(1).unwrap(),
    };
    // Authorization is not enough: the exact online registration is mandatory.
    assert!(!store.campaign_leader(&leader, lease).await.unwrap_or(false));
    store
        .register_candidate(&leader.candidate_registration())
        .await
        .unwrap();
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    let guard = ClusterLeaderGuard::new(leader.clone()).unwrap();
    store
        .change_candidate(change(epoch, 2, "a", false, "remove-leader"))
        .await
        .unwrap();
    assert!(store.lease_time_to_live(lease).await.unwrap().is_some());
    assert!(
        store
            .begin_shutdown(&guard, ControlOperationId::new("revoked-write").unwrap())
            .await
            .is_err()
    );
    let readded = store
        .change_candidate(change(epoch, 3, "a", true, "readd-leader"))
        .await
        .unwrap()
        .authorization
        .unwrap();
    assert_ne!(readded.generation, leader.candidate_generation);
    assert!(
        store
            .register_candidate(&leader.candidate_registration())
            .await
            .is_err()
    );
    assert!(
        store
            .begin_shutdown(&guard, ControlOperationId::new("aba-write").unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn memory_revocation_fences_live_leader_without_waiting_for_lease_expiry() {
    revoked_candidate_fences_existing_leader(&InMemoryCoordinationStore::new(8, 8).unwrap()).await;
}

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn etcd_revocation_fences_live_leader_without_waiting_for_lease_expiry() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints,
        cluster_prefix: format!("/lattice-candidate-fence/{}", uuid::Uuid::new_v4()),
        list_page_size: 32,
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
    revoked_candidate_fences_existing_leader(&store).await;
}
