use super::*;

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
    let store = Arc::new(InMemoryPlacementStore::new(8, 8).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("admin-entity").unwrap();
    leader
        .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), true)
        .await
        .unwrap();
    leader
        .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), true)
        .await
        .unwrap();
    assert!(matches!(
        leader
            .set_automatic_paused("pause-1".to_owned(), Some(entity_type.clone()), false)
            .await,
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
    let inspection = leader.inspect().await.unwrap();
    assert_eq!(inspection.version.term, CoordinatorTerm::new(1).unwrap());
    assert_eq!(inspection.paused_entity_types, vec![entity_type]);

    assert!(
        store
            .get_automatic_settings(&domain())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .get_admin_operation(&domain(), "pause-1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        leader.prior_admin_operation("pause-1", "move:b"),
        Err(CoordinatorRuntimeError::IdempotencyConflict)
    ));
}
