use super::*;
use crate::runtime::membership_plane::{MembershipLeader, MembershipLeaderConfig};

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
    let mut membership = MembershipLeader::elect(
        store.clone(),
        coordinator.clone(),
        CoordinatorTerm::new(1).unwrap(),
        MembershipLeaderConfig::default(),
    )
    .await
    .unwrap();
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

    let joining_hello = empty_hello(joining.clone());
    membership.join(joining_hello.member.clone()).await.unwrap();
    membership.mark_up(&joining).await.unwrap();
    leader
        .register(joining_hello.domain, joining_key.clone())
        .await
        .unwrap();
    let joining_version = leader.membership_version;
    assert_eq!(
        store.get_member("joining").await.unwrap().unwrap().status,
        MemberStatus::Up
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
        store
            .get_domain_member(&domain(), "joining")
            .await
            .unwrap()
            .unwrap()
            .status,
        DomainMemberStatus::Leaving
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
    assert!(
        store
            .get_domain_member(&domain(), "joining")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.get_member("joining").await.unwrap().unwrap().status,
        MemberStatus::Up
    );
    membership.begin_leave(&joining).await.unwrap();
    membership
        .remove(&joining, MemberRemovalReason::GracefulLeave)
        .await
        .unwrap();

    let forced_hello = empty_hello(forced.clone());
    membership.join(forced_hello.member.clone()).await.unwrap();
    membership.mark_up(&forced).await.unwrap();
    register_up(&mut leader, forced_hello, forced_key).await;
    let request = ForceRemoveRequest {
        domain: domain(),
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
    assert!(
        store
            .get_domain_member(&domain(), "forced")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get_member("forced").await.unwrap().is_some());

    let old_reused_hello = empty_hello(old_reused.clone());
    membership
        .join(old_reused_hello.member.clone())
        .await
        .unwrap();
    membership.mark_up(&old_reused).await.unwrap();
    register_up(&mut leader, old_reused_hello, old_reused_key).await;
    let reused_hello = empty_hello(new_reused.clone());
    assert!(matches!(
            membership.join(reused_hello.member.clone())
            .await,
        Err(CoordinatorRuntimeError::IncarnationPending {
            predecessor,
            remaining_ttl: Some(_),
        }) if predecessor == old_reused.incarnation
    ));
    membership.begin_leave(&old_reused).await.unwrap();
    membership
        .remove(&old_reused, MemberRemovalReason::IncarnationReplaced)
        .await
        .unwrap();
    let reused_hello = empty_hello(new_reused.clone());
    membership.join(reused_hello.member.clone()).await.unwrap();
    membership.mark_up(&new_reused).await.unwrap();
    leader
        .register(reused_hello.domain, new_reused_key)
        .await
        .unwrap();
    let current = store.get_member("reused").await.unwrap().unwrap();
    assert_eq!(current.node, new_reused);
    assert_eq!(current.status, MemberStatus::Up);
}
