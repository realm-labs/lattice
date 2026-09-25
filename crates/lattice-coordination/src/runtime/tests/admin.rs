use super::*;
use crate::{
    candidate_fixture::elect_group,
    storage::records::{AdminOperationResult, CompactAdminOperations},
};

#[tokio::test]
async fn admin_pause_is_idempotent_fingerprinted_and_inspectable() {
    let cluster_id = ClusterId::new("admin-test").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26300, 300);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let store = Arc::new(InMemoryCoordinationStore::new(8, 8).unwrap());
    let mut leader = elect_group(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        GroupCoordinatorConfig::default(),
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("admin-entity").unwrap();
    let operation_id = leader.inspect().await.unwrap().new_operation_id();
    leader
        .set_automatic_paused(operation_id.clone(), Some(entity_type.clone()), true)
        .await
        .unwrap();
    leader
        .set_automatic_paused(operation_id.clone(), Some(entity_type.clone()), true)
        .await
        .unwrap();
    assert!(matches!(
        leader
            .set_automatic_paused(operation_id.clone(), Some(entity_type.clone()), false)
            .await,
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
    let mut receipt = store
        .get_admin_operation(&group(), &operation_id)
        .await
        .unwrap()
        .unwrap();
    receipt.operation_id = "lost-proposal".to_owned();
    receipt.fingerprint = "proposal".to_owned();
    receipt.result = AdminOperationResult::PlanCreated { plan_id: 999 };
    leader
        .applied_admin_operations
        .insert(receipt.operation_id.clone(), receipt);
    assert!(matches!(
        leader.prior_admin_operation("lost-proposal", "proposal"),
        Err(CoordinatorRuntimeError::OperationExpired)
    ));
    assert!(
        leader.plans.is_empty(),
        "a receipt cannot recreate an unstarted proposal"
    );
    let inspection = leader.inspect().await.unwrap();
    assert_eq!(inspection.version.term, CoordinatorTerm::new(1).unwrap());
    assert_eq!(inspection.paused_entity_types, vec![entity_type.clone()]);

    assert!(
        store
            .get_automatic_settings(&group())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .get_admin_operation(&group(), &operation_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        leader.prior_admin_operation(&operation_id, "move:b"),
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
    let receipt = store
        .get_admin_operation(&group(), &operation_id)
        .await
        .unwrap()
        .unwrap();
    store
        .compact_admin_operations(
            &leader.leader_guard,
            CompactAdminOperations {
                expected: vec![receipt],
            },
        )
        .await
        .unwrap();
    leader.applied_admin_operations.remove(&operation_id);
    assert!(matches!(
        leader
            .set_automatic_paused(operation_id, Some(entity_type.clone()), true)
            .await,
        Err(CoordinatorRuntimeError::OperationExpired)
    ));
    let fresh = leader.inspect().await.unwrap().new_operation_id();
    leader
        .set_automatic_paused(fresh, Some(entity_type), false)
        .await
        .unwrap();
}
