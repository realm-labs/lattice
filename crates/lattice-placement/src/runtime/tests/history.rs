use super::*;

#[tokio::test]
async fn terminal_plan_history_compacts_oldest_persisted_record() {
    let cluster_id = ClusterId::new("history-test").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26310, 310);
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
        PlacementDomainLeaderConfig {
            maximum_completed_plan_history: 2,
            ..PlacementDomainLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("history-entity").unwrap();
    for id in 1..=3_u128 {
        let plan = RebalancePlan {
            domain: domain(),
            plan_id: id,
            entity_type: entity_type.clone(),
            reason: PlanReason::Manual,
            coordinator_term: CoordinatorTerm::new(1).unwrap(),
            base_version: PlacementVersion::new(
                domain(),
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(id as u64).unwrap(),
            ),
            record_revision: PlanRevision::new(1).unwrap(),
            policy_id: "test".to_owned(),
            policy_version: 1,
            status: PlanStatus::Completed,
            moves: Vec::new(),
        };
        store
            .create_plan(&leader.leader_guard, CreatePlan { plan: plan.clone() })
            .await
            .unwrap();
        leader.plans.insert(id, plan);
    }
    leader.compact_plan_history().await.unwrap();
    assert!(store.get_plan(&domain(), 1).await.unwrap().is_none());
    assert!(store.get_plan(&domain(), 2).await.unwrap().is_some());
    assert!(store.get_plan(&domain(), 3).await.unwrap().is_some());
    assert_eq!(leader.plans.len(), 2);
}
