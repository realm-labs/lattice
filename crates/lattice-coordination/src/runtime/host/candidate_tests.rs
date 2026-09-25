use std::{collections::BTreeSet, sync::Arc, time::Duration};

use lattice_model::{
    cluster::{CoordinatorScope, NodeEndpoint, NodeIncarnation},
    run::ControlOperationId,
};
use lattice_remoting::{association::AssociationManager, config::RemotingConfig};

use super::{CoordinatorHost, CoordinatorHostConfig, CoordinatorHostScopeState};
use crate::{
    candidates::CandidateChange,
    storage::{
        CoordinatorLeaseStore, InMemoryCoordinationStore, ScopedElectionStore, StorageError,
        candidates::{CandidateStore, provision_candidate},
    },
    types::NodeKey,
};

#[tokio::test]
async fn running_host_can_be_promoted_revoked_and_readded_without_restart() {
    let store = Arc::new(InMemoryCoordinationStore::new(8, 8).unwrap());
    let epoch = store.ensure_framework().await.unwrap();
    let scope = CoordinatorScope::Cluster;
    // Eligible backup is intentionally offline: eligibility is not availability.
    provision_candidate(store.as_ref(), scope.clone(), "backup".into())
        .await
        .unwrap();
    let local = NodeKey {
        node_id: "dynamic-candidate".into(),
        address: NodeEndpoint::new("127.0.0.1", 39091).unwrap(),
        incarnation: NodeIncarnation::new(991).unwrap(),
    };
    let associations = Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let mut host = CoordinatorHost::elect(
        store.clone(),
        associations,
        local.clone(),
        BTreeSet::new(),
        CoordinatorHostConfig {
            maximum_candidate_jitter: Duration::ZERO,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    host.renew_membership().await;
    assert_eq!(host.membership_state, CoordinatorHostScopeState::Standby);
    assert!(store.get_leader(&scope).await.unwrap().is_none());

    provision_candidate(store.as_ref(), scope.clone(), local.node_id.clone())
        .await
        .unwrap();
    host.refresh_candidate_registrations().await.unwrap();
    host.renew_membership().await;
    let original = store.get_leader(&scope).await.unwrap().unwrap();
    assert_eq!(original.node, local);
    let registration = original.candidate_registration();

    let revision = store.candidate_set(&scope).await.unwrap().revision;
    store
        .change_candidate(CandidateChange {
            epoch,
            scope: scope.clone(),
            expected_revision: revision,
            node_id: local.node_id.clone(),
            enabled: false,
            operation: ControlOperationId::new("revoke-running-host").unwrap(),
        })
        .await
        .unwrap();
    host.refresh_candidate_registrations().await.unwrap();
    host.renew_membership().await;
    assert_eq!(host.membership_state, CoordinatorHostScopeState::Standby);
    assert!(store.get_leader(&scope).await.unwrap().is_none());
    assert!(store.online_candidates(&scope).await.unwrap().is_empty());

    provision_candidate(store.as_ref(), scope.clone(), local.node_id.clone())
        .await
        .unwrap();
    host.refresh_candidate_registrations().await.unwrap();
    host.renew_membership().await;
    let replacement = store.get_leader(&scope).await.unwrap().unwrap();
    assert_eq!(replacement.node, local);
    assert!(replacement.term > original.term);
    assert!(replacement.candidate_generation.get() > original.candidate_generation.get());
    assert_eq!(
        store.unregister_candidate(&registration).await,
        Err(StorageError::CompareFailed)
    );
    assert_eq!(
        store.online_candidates(&scope).await.unwrap(),
        vec![replacement.candidate_registration()]
    );
    host.membership.take().unwrap().shutdown().await.unwrap();
}
