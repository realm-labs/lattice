use std::time::Duration;

use etcd_client::Client;
use lattice_core::actor_ref::{ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation};
use lattice_placement::coordinator::{LeaderRecord, MemberRecord, MemberStatus, NodeHello};
use lattice_placement::storage::etcd::{EtcdPlacementConfig, EtcdPlacementStore};
use lattice_placement::storage::{CoordinatorStore, PlacementStore, StorageError};
use lattice_placement::types::AssignmentGeneration;
use lattice_placement::types::ClaimGrant;
use lattice_placement::types::CoordinatorTerm;
use lattice_placement::types::GrantSequence;
use lattice_placement::types::NodeKey;
use lattice_placement::types::PlacementSlot;
use lattice_placement::types::PlacementSlotKey;
use lattice_placement::types::PlacementSlotState;
use lattice_placement::types::Revision;
use lattice_placement::types::ShardId;

fn endpoints() -> Option<Vec<String>> {
    std::env::var("LATTICE_ETCD_ENDPOINTS")
        .ok()
        .map(|value| value.split(',').map(str::to_owned).collect())
}

fn node(id: &str, incarnation: u128, port: u16) -> NodeKey {
    NodeKey {
        node_id: id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

#[tokio::test]
async fn real_etcd_schema_leases_leadership_slots_and_exact_claim_cas() {
    let Some(endpoints) = endpoints() else {
        eprintln!("LATTICE_ETCD_ENDPOINTS is absent; Docker acceptance owns this test");
        return;
    };
    let prefix = format!("/lattice-tests/{}", uuid::Uuid::new_v4().simple());
    let store = EtcdPlacementStore::connect(EtcdPlacementConfig {
        endpoints: endpoints.clone(),
        cluster_prefix: prefix.clone(),
        maximum_list_records: 64,
        connect_options: None,
    })
    .await
    .unwrap();
    store.ensure_schema_generation().await.unwrap();
    store.ensure_schema_generation().await.unwrap();

    let leader_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let leader = LeaderRecord {
        node: node("coordinator", 1, 29001),
        protocol_generation: 2,
        term: CoordinatorTerm::new(1).unwrap(),
    };
    assert!(store.campaign_leader(&leader, leader_lease).await.unwrap());
    assert_eq!(store.get_leader().await.unwrap(), Some(leader));

    let member_lease = store.grant_lease(Duration::from_secs(10)).await.unwrap();
    let member_node = node("member", 7, 29007);
    let hello = NodeHello {
        node: member_node.clone(),
        roles: Default::default(),
        capacity_units: 1,
        hosted_entity_types: Default::default(),
        proxied_entity_types: Default::default(),
        singleton_eligibility: Default::default(),
        used_singletons: Default::default(),
        protocols: Vec::new(),
        entity_configs: Vec::new(),
        singleton_configs: Vec::new(),
    };
    let joining = MemberRecord {
        node: member_node,
        hello,
        status: MemberStatus::Joining,
        revision: Revision::new(1).unwrap(),
        lease_id: member_lease,
    };
    store.create_member(&joining).await.unwrap();
    assert_eq!(
        store.get_member("member").await.unwrap(),
        Some(joining.clone())
    );
    let mut up = joining.clone();
    up.status = MemberStatus::Up;
    up.revision = Revision::new(2).unwrap();
    store.compare_and_put_member(&joining, &up).await.unwrap();
    assert!(matches!(
        store.compare_and_delete_member(&joining).await,
        Err(StorageError::CompareFailed)
    ));
    store.compare_and_delete_member(&up).await.unwrap();

    let key = PlacementSlotKey::Shard {
        entity_type: EntityType::new("etcd-acceptance").unwrap(),
        shard_id: ShardId::new(1),
    };
    let owner = node("owner", 2, 29002);
    let slot = PlacementSlot {
        key: key.clone(),
        config_fingerprint: ConfigFingerprint::new([8; 32]),
        owner: Some(owner.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        coordinator_term: CoordinatorTerm::new(1).unwrap(),
        revision: Revision::new(1).unwrap(),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    store
        .compare_and_put_slot(None, slot.clone())
        .await
        .unwrap();
    assert!(matches!(
        store.compare_and_put_slot(None, slot).await,
        Err(StorageError::CompareFailed)
    ));

    let claim_lease = store.grant_lease(Duration::from_secs(2)).await.unwrap();
    let claim = ClaimGrant {
        slot: key.clone(),
        owner,
        coordinator_term: CoordinatorTerm::new(1).unwrap(),
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        grant_sequence: GrantSequence::new(1).unwrap(),
        ttl: Duration::from_secs(2),
    };
    store.put_claim(&claim, claim_lease).await.unwrap();
    let mut stale = claim.clone();
    stale.grant_sequence = GrantSequence::new(2).unwrap();
    assert!(matches!(
        store.delete_claim(&stale).await,
        Err(StorageError::CompareFailed)
    ));
    store.delete_claim(&claim).await.unwrap();
    assert!(store.get_claim(&key).await.unwrap().is_none());

    store.put_claim(&claim, claim_lease).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if store.get_claim(&key).await.unwrap().is_none() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leased claim did not expire"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mismatch_prefix = format!("{prefix}-mismatch");
    let mut raw = Client::connect(endpoints.clone(), None).await.unwrap();
    raw.put(format!("{mismatch_prefix}/schema_generation"), "1", None)
        .await
        .unwrap();
    let mismatch = EtcdPlacementStore::connect(EtcdPlacementConfig {
        endpoints,
        cluster_prefix: mismatch_prefix,
        maximum_list_records: 8,
        connect_options: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        mismatch.ensure_schema_generation().await,
        Err(StorageError::SchemaGenerationMismatch)
    ));
}
