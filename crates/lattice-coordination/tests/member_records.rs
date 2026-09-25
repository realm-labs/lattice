use std::time::Duration;

use etcd_client::Client;
use lattice_coordination::{
    coordinator::{
        ClusterLeaderGuard, GroupLeaderGuard, GroupMemberRecord, GroupMemberStatus, LeaderRecord,
        MemberRecord, MemberStatus,
    },
    storage::{
        ActorGroupStore, CoordinatorLeaseStore, MembershipStore, ScopedElectionStore,
        candidates::{CandidateStore, provision_candidate},
        etcd::EtcdCoordinationStore,
        records::{CreateGroupMember, CreateMember, DurableStorageLimits},
    },
    types::{CoordinatorTerm, MembershipVersion, NodeKey, PlacementVersion, Revision},
};
use lattice_model::{
    cluster::{ActorGroupId, CoordinatorScope, NodeEndpoint, NodeIncarnation},
    run::RunEpoch,
};

fn node(id: &str, incarnation: u128) -> NodeKey {
    NodeKey {
        node_id: id.into(),
        address: NodeEndpoint::new("127.0.0.1", 31700).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

async fn elect(
    store: &EtcdCoordinationStore,
    scope: CoordinatorScope,
    epoch: RunEpoch,
) -> LeaderRecord {
    let authorization = provision_candidate(store, scope.clone(), "coordinator".into())
        .await
        .unwrap();
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let leader = LeaderRecord {
        candidate_generation: authorization.generation,
        candidate_lease_id: lease,
        epoch,
        scope,
        node: node("coordinator", 1),
        term: CoordinatorTerm::new(1).unwrap(),
    };
    store
        .register_candidate(&leader.candidate_registration())
        .await
        .unwrap();
    assert!(store.campaign_leader(&leader, lease).await.unwrap());
    leader
}

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn durable_members_are_minimal_and_group_participation_expires_with_global_lease() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut raw = Client::connect(endpoints, None).await.unwrap();
    let prefix = format!("/lattice-member-test/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdCoordinationStore::from_client(
        raw.clone(),
        prefix.clone(),
        8,
        DurableStorageLimits {
            maximum_slots: 8,
            maximum_plans: 8,
            maximum_members: 2,
            maximum_admin_operations: 8,
            maximum_entity_configs: 8,
            maximum_singleton_configs: 8,
        },
    )
    .unwrap();
    let epoch = store.ensure_framework().await.unwrap();
    let group = ActorGroupId::new("gameplay").unwrap();
    let cluster_guard =
        ClusterLeaderGuard::new(elect(&store, CoordinatorScope::Cluster, epoch).await).unwrap();
    let group_guard =
        GroupLeaderGuard::new(elect(&store, CoordinatorScope::Group(group.clone()), epoch).await)
            .unwrap();
    // Repeatedly reuse one exact node identity. No stop proof is fabricated: the
    // same retained shutdown obligation stays present throughout this test.
    for revision in 2..=5 {
        let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
        let member = MemberRecord {
            node: node("member", 2),
            status: MemberStatus::Up,
            version: MembershipVersion::new(cluster_guard.term(), Revision::new(revision).unwrap()),
            lease_id: lease,
        };
        store
            .create_member(
                &cluster_guard,
                CreateMember {
                    member: member.clone(),
                },
            )
            .await
            .unwrap();
        let participation = GroupMemberRecord {
            node: member.node.clone(),
            status: GroupMemberStatus::Up,
            version: PlacementVersion::new(
                group.clone(),
                group_guard.term(),
                Revision::new(revision).unwrap(),
            ),
        };
        store
            .create_group_member(
                &group_guard,
                CreateGroupMember {
                    expected_global_member: member,
                    member: participation,
                },
            )
            .await
            .unwrap();
        for key in [
            format!("{prefix}/runs/1/membership/members/member"),
            format!("{prefix}/runs/1/groups/gameplay/members/member"),
        ] {
            let result = raw.get(key, None).await.unwrap();
            let kv = &result.kvs()[0];
            assert_eq!(kv.lease(), lease);
            let value: serde_json::Value = serde_json::from_slice(kv.value()).unwrap();
            assert!(value.get("hello").is_none());
            assert!(kv.value().len() < 1024);
        }
        store.revoke_lease(lease).await.unwrap();
        assert!(store.get_member("member").await.unwrap().is_none());
        assert!(
            store
                .get_group_member(&group, "member")
                .await
                .unwrap()
                .is_none()
        );
    }
}
