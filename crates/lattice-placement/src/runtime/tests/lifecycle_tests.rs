use super::*;

#[tokio::test]
async fn join_drain_and_force_remove_are_revisioned_idempotent_and_fenced() {
    let cluster = ClusterId::new("member-lifecycle").unwrap();
    let (coordinator, coordinator_identity) = node(&cluster, "coordinator", 30100, 100);
    let (joining, _) = node(&cluster, "joining", 30101, 101);
    let (forced, _) = node(&cluster, "forced", 30102, 102);
    let (old_reused, _) = node(&cluster, "reused", 30103, 103);
    let (new_reused, _) = node(&cluster, "reused", 30104, 104);
    let config = RemotingConfig::default();
    let associations = Arc::new(
        AssociationManager::new(coordinator.address.clone(), coordinator.incarnation, config)
            .unwrap(),
    );
    let joining_key = attach_test_session(
        &associations,
        &cluster,
        coordinator_identity.incarnation,
        &joining,
        1000,
    );
    let forced_key = attach_test_session(
        &associations,
        &cluster,
        coordinator_identity.incarnation,
        &forced,
        2000,
    );
    let old_reused_key = attach_test_session(
        &associations,
        &cluster,
        coordinator_identity.incarnation,
        &old_reused,
        3000,
    );
    let new_reused_key = attach_test_session(
        &associations,
        &cluster,
        coordinator_identity.incarnation,
        &new_reused,
        4000,
    );
    let store = Arc::new(InMemoryPlacementStore::new(16, 16).unwrap());
    let mut leader = CoordinatorLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorTerm::new(1).unwrap(),
        3,
        CoordinatorLeaderConfig::default(),
    )
    .await
    .unwrap();

    leader
        .register(empty_hello(joining.clone()), joining_key.clone())
        .await
        .unwrap();
    let joining_version = leader.version;
    assert_eq!(
        store.get_member("joining").await.unwrap().unwrap().status,
        MemberStatus::Joining
    );
    assert!(matches!(
        leader
            .mark_member_up(
                joining.incarnation,
                joining_version.next_revision().unwrap(),
                &joining_key,
            )
            .await,
        Err(CoordinatorRuntimeError::StaleMember)
    ));
    leader
        .mark_member_up(joining.incarnation, joining_version, &joining_key)
        .await
        .unwrap();
    let up = store.get_member("joining").await.unwrap().unwrap();
    assert_eq!(up.status, MemberStatus::Up);
    leader
        .mark_member_up(joining.incarnation, joining_version, &joining_key)
        .await
        .unwrap();

    assert!(
        leader
            .begin_member_drain(
                joining.incarnation,
                "drain-1".to_string(),
                NodeIncarnation::new(999).unwrap(),
            )
            .await
            .is_err()
    );
    leader
        .begin_member_drain(
            joining.incarnation,
            "drain-1".to_string(),
            joining.incarnation,
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_member("joining").await.unwrap().unwrap().status,
        MemberStatus::Leaving
    );
    assert!(
        leader
            .complete_member_drain(joining.incarnation, "other", joining.incarnation)
            .await
            .is_err()
    );
    leader
        .complete_member_drain(joining.incarnation, "drain-1", joining.incarnation)
        .await
        .unwrap();
    assert!(store.get_member("joining").await.unwrap().is_none());

    register_up(&mut leader, empty_hello(forced.clone()), forced_key).await;
    let request = ForceRemoveRequest {
        operation_id: "force-1".to_string(),
        node_id: forced.node_id.clone(),
        expected_incarnation: forced.incarnation,
    };
    assert!(
        leader
            .force_remove(ForceRemoveRequest {
                expected_incarnation: NodeIncarnation::new(999).unwrap(),
                ..request.clone()
            })
            .await
            .is_err()
    );
    leader.force_remove(request.clone()).await.unwrap();
    leader.force_remove(request).await.unwrap();
    assert!(store.get_member("forced").await.unwrap().is_none());

    register_up(&mut leader, empty_hello(old_reused.clone()), old_reused_key).await;
    assert!(matches!(
        leader
            .register(empty_hello(new_reused.clone()), new_reused_key.clone())
            .await,
        Err(CoordinatorRuntimeError::IncarnationPending {
            predecessor,
            remaining_ttl: Some(_),
        }) if predecessor == old_reused.incarnation
    ));
    leader
        .sessions
        .get_mut(&old_reused.incarnation)
        .unwrap()
        .last_heartbeat = Instant::now() - Duration::from_secs(60);
    leader
        .register(empty_hello(new_reused.clone()), new_reused_key)
        .await
        .unwrap();
    let current = store.get_member("reused").await.unwrap().unwrap();
    assert_eq!(current.node, new_reused);
    assert_eq!(current.status, MemberStatus::Joining);
}
