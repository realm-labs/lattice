mod candidate_fixture;

use std::{collections::BTreeSet, time::Duration};

use candidate_fixture::prepare_leader;
use etcd_client::{Client, GetOptions};
use lattice_coordination::{
    allocation::{ProposedMove, RebalanceProposal, RebalanceTrigger},
    candidates::CandidateGeneration,
    coordinator::{GroupLeaderGuard, GroupMemberRecord, GroupMemberStatus, LeaderRecord},
    plan::{MoveProgress, PlanStatus, RebalancePlan},
    storage::{
        ActorGroupStore, CoordinatorLeaseStore, ScopedElectionStore, StorageError,
        barrier::{BarrierProgress, TransferBarrierStore},
        etcd::{EtcdCoordinationConfig, EtcdCoordinationStore},
        records::{CreatePlan, DurableStorageLimits},
    },
    types::{
        AssignmentGeneration, CoordinatorTerm, NodeKey, PlacementSlot, PlacementSlotKey,
        PlacementSlotState, PlacementVersion, Revision, ShardId,
    },
};
use lattice_model::cluster::{
    ActorGroupId, ConfigFingerprint, CoordinatorScope, EntityType, NodeEndpoint, NodeIncarnation,
};

fn node(index: u128) -> NodeKey {
    NodeKey {
        node_id: format!("member-{index}"),
        address: NodeEndpoint::new("127.0.0.1", 32199).unwrap(),
        incarnation: NodeIncarnation::new(index).unwrap(),
    }
}

#[tokio::test]
#[ignore = "requires isolated etcd via LATTICE_ETCD_ENDPOINTS"]
async fn split_plan_and_barrier_values_are_bounded_and_missing_evidence_fails() {
    let endpoints = std::env::var("LATTICE_ETCD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let prefix = format!("/lattice-transfer-records/{}", uuid::Uuid::new_v4());
    let store = EtcdCoordinationStore::connect(EtcdCoordinationConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        list_page_size: 16,
        limits: DurableStorageLimits {
            maximum_slots: 128,
            maximum_plans: 16,
            maximum_members: 128,
            maximum_admin_operations: 16,
            maximum_entity_configs: 16,
            maximum_singleton_configs: 16,
        },
        connect_options: None,
    })
    .await
    .unwrap();
    let epoch = store.ensure_framework().await.unwrap();
    let group = ActorGroupId::new("transfers").unwrap();
    let entity = EntityType::new("entity").unwrap();
    let lease = store.grant_lease(Duration::from_secs(30)).await.unwrap();
    let leader = prepare_leader(
        &store,
        LeaderRecord {
            epoch,
            scope: CoordinatorScope::Group(group.clone()),
            node: node(999),
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
    let version = PlacementVersion::new(group.clone(), guard.term(), Revision::new(2).unwrap());
    let mut raw = Client::connect(&endpoints, None).await.unwrap();
    let mut participants = BTreeSet::new();
    for index in 1..=65 {
        let identity = node(index);
        participants.insert(identity.incarnation);
        let member = GroupMemberRecord {
            node: identity.clone(),
            status: GroupMemberStatus::Up,
            version: version.clone(),
        };
        raw.put(
            format!("{prefix}/runs/1/groups/transfers/members/member-{index}"),
            serde_json::to_vec(&member).unwrap(),
            None,
        )
        .await
        .unwrap();
    }
    let key = PlacementSlotKey::Shard {
        group: group.clone(),
        entity_type: entity.clone(),
        shard_id: ShardId::new(0),
    };
    let barrier = store
        .prepare_transfer_barrier(&guard, &key, 123, &version, participants.clone())
        .await
        .unwrap();
    assert_eq!(barrier.manifest.count, 65);
    assert_eq!(barrier.required(), participants);
    let first = NodeIncarnation::new(1).unwrap();
    store
        .advance_transfer_barrier(&guard, &key, 123, first, BarrierProgress::Applied)
        .await
        .unwrap();
    store
        .advance_transfer_barrier(&guard, &key, 123, first, BarrierProgress::Applied)
        .await
        .unwrap();
    assert_eq!(
        store
            .transfer_barrier(&key, 123)
            .await
            .unwrap()
            .participants[&first],
        BarrierProgress::Applied
    );
    let mut plan = RebalancePlan::from_proposal(
        RebalanceProposal {
            group: group.clone(),
            policy_id: "split-test",
            policy_version: 1,
            base_version: version.clone(),
            trigger: RebalanceTrigger::Automatic,
            moves: (0..64)
                .map(|index| ProposedMove {
                    group: group.clone(),
                    entity_type: entity.clone(),
                    shard_id: ShardId::new(index),
                    expected_generation: AssignmentGeneration::new(1).unwrap(),
                    source: node(1),
                    target: node(2),
                    estimated_weight: 1,
                })
                .collect(),
        },
        entity,
        guard.term(),
        64,
    )
    .unwrap();
    store
        .create_plan(&guard, CreatePlan { plan: plan.clone() })
        .await
        .unwrap();
    assert_eq!(
        store.get_plan(&group, plan.plan_id).await.unwrap(),
        None,
        "unstarted proposals are not recovery state"
    );
    // Seed completed history to exercise the maximum bounded row shape.
    for movement in &mut plan.moves {
        movement.progress = MoveProgress::Completed;
    }
    plan.status = PlanStatus::Completed;
    store
        .create_plan(&guard, CreatePlan { plan: plan.clone() })
        .await
        .unwrap();
    assert_eq!(
        store.get_plan(&group, plan.plan_id).await.unwrap(),
        Some(plan.clone())
    );
    assert_eq!(
        store
            .list_plans_page(&group, None, 1)
            .await
            .unwrap()
            .records,
        vec![plan]
    );
    let records = raw
        .get(
            format!("{prefix}/runs/1/groups/transfers/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .unwrap();
    let mut victim = None;
    for kv in records.kvs() {
        assert!(
            kv.value().len() <= 4096,
            "oversized value for {:?}",
            kv.key_str()
        );
        if kv
            .key()
            .windows(b"/participants/".len())
            .any(|part| part == b"/participants/")
        {
            victim = Some(kv.key().to_vec());
        }
    }
    raw.delete(victim.unwrap(), None).await.unwrap();
    assert_eq!(
        store.transfer_barrier(&key, 123).await,
        Err(StorageError::StorageMetadataMismatch)
    );
    // A fresh leader/pass resumes bounded cleanup even with missing progress.
    let barrier_prefix = format!("{prefix}/runs/1/groups/transfers/transfers/barriers/");
    let mut cursor = None;
    let mut previous = 65; // 64 surviving participant rows plus the header.
    for pass in 0..16 {
        if pass == 2 {
            cursor = None;
        } // Simulate leader/process loss mid-GC.
        cursor = store
            .reclaim_orphan_barriers(&guard, cursor.as_deref())
            .await
            .unwrap();
        let remaining = raw
            .get(
                barrier_prefix.clone(),
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .unwrap()
            .kvs()
            .len();
        assert!(
            previous - remaining <= 16,
            "one pass exceeded its deletion budget"
        );
        previous = remaining;
        if remaining == 0 {
            break;
        }
    }
    assert_eq!(
        previous, 0,
        "orphan cleanup must survive a missing participant"
    );
    // Model a crashed builder: header and only the first batch reached etcd.
    let slot = PlacementSlot {
        key: key.clone(),
        config_fingerprint: ConfigFingerprint::new([0; 32]),
        owner: Some(node(1)),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: version.clone(),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: BTreeSet::new(),
    };
    raw.put(
        format!("{prefix}/runs/1/groups/transfers/shards/entity/0"),
        serde_json::to_vec(&slot).unwrap(),
        None,
    )
    .await
    .unwrap();
    let digest = blake3::hash(&serde_json::to_vec(&key).unwrap()).to_hex();
    let resumed_prefix = format!("{barrier_prefix}{digest}/{:032x}/", 124);
    let mut unfinished = barrier.manifest.clone();
    unfinished.operation = 124;
    unfinished.sealed = false;
    raw.put(
        format!("{resumed_prefix}manifest"),
        serde_json::to_vec(&unfinished).unwrap(),
        None,
    )
    .await
    .unwrap();
    for identity in participants.iter().take(16) {
        raw.put(
            format!("{resumed_prefix}participants/{:032x}", identity.get()),
            serde_json::to_vec(&(*identity, BarrierProgress::Pending)).unwrap(),
            None,
        )
        .await
        .unwrap();
    }
    let resumed = store
        .prepare_transfer_barrier(&guard, &key, 124, &version, participants.clone())
        .await
        .unwrap();
    assert!(resumed.manifest.sealed);
    assert_eq!(resumed.required(), participants);

    // A second interrupted build whose fixed source revision was compacted must
    // discard only unpublished rows. It must never seal a latest-value mixture.
    unfinished.operation = 125;
    let compacted_prefix = format!("{barrier_prefix}{digest}/{:032x}/", 125);
    raw.put(
        format!("{compacted_prefix}manifest"),
        serde_json::to_vec(&unfinished).unwrap(),
        None,
    )
    .await
    .unwrap();
    let latest = raw
        .get(format!("{compacted_prefix}manifest"), None)
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    raw.compact(latest, None).await.unwrap();
    assert_eq!(
        store
            .prepare_transfer_barrier(&guard, &key, 125, &version, participants.clone())
            .await,
        Err(StorageError::SnapshotCompacted)
    );
    assert!(
        raw.get(compacted_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .unwrap()
            .kvs()
            .is_empty()
    );
    assert!(
        store
            .get_slot(&key)
            .await
            .unwrap()
            .unwrap()
            .active_move
            .is_none()
    );
    let fresh = store
        .prepare_transfer_barrier(&guard, &key, 125, &version, participants)
        .await
        .unwrap();
    assert!(fresh.manifest.source_revision > unfinished.source_revision);
    assert!(fresh.manifest.sealed);
    store.revoke_lease(lease).await.unwrap();
}
