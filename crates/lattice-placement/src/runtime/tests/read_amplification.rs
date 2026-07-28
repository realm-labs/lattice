use super::*;

const SHARDS: u32 = 24;

struct AmplificationFixture {
    leader: PlacementDomainLeader<InMemoryPlacementStore>,
    store: Arc<InMemoryPlacementStore>,
    entity_type: EntityType,
    source: NodeKey,
}

/// Brings up one domain with `hosts` live members and a shard count large enough that an unbounded
/// scan is distinguishable from a bounded page.
async fn fixture(cluster: &str, port_base: u16, page_size: usize) -> AmplificationFixture {
    let cluster_id = ClusterId::new(cluster).unwrap();
    let (coordinator_node, _) = node(&cluster_id, "coordinator", port_base, u128::from(port_base));
    let (source, _) = node(
        &cluster_id,
        "source",
        port_base + 1,
        u128::from(port_base) + 1,
    );
    let (target, _) = node(
        &cluster_id,
        "target",
        port_base + 2,
        u128::from(port_base) + 2,
    );
    let associations = Arc::new(
        AssociationManager::new(
            coordinator_node.address.clone(),
            coordinator_node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let source_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &source,
        u128::from(port_base) * 100,
    );
    let target_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &target,
        u128::from(port_base) * 100 + 10,
    );
    let entity_type = EntityType::new("amplified").unwrap();
    let protocol_id = ProtocolId::new(91).unwrap();
    let entity_config = EntityConfig::new(
        domain(),
        entity_type.clone(),
        protocol_id,
        SHARDS,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let descriptor = ProtocolDescriptor {
        protocol_id,
        fingerprint: ProtocolFingerprint::new([13; 32]),
    };
    let store = Arc::new(InMemoryPlacementStore::new(128, 16).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator_node,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig {
            reconciliation_page_size: page_size,
            ..PlacementDomainLeaderConfig::default()
        },
    )
    .await
    .unwrap();
    let hello = |node: NodeKey| {
        test_hello(
            node,
            TestHelloSpec {
                capacity_units: 64,
                hosted_entity_types: [entity_type.clone()].into_iter().collect(),
                protocols: vec![descriptor.clone()],
                entity_configs: vec![entity_config.clone()],
                ..TestHelloSpec::default()
            },
        )
    };
    register_up(&mut leader, hello(source.clone()), source_key).await;
    register_up(&mut leader, hello(target.clone()), target_key).await;
    AmplificationFixture {
        leader,
        store,
        entity_type,
        source,
    }
}

impl AmplificationFixture {
    fn shard_key(&self, shard: u32) -> PlacementSlotKey {
        PlacementSlotKey::Shard {
            domain: domain(),
            entity_type: self.entity_type.clone(),
            shard_id: ShardId::new(shard),
        }
    }

    async fn allocate(&mut self, shard: u32) {
        self.leader
            .ensure_shard_allocated(self.entity_type.clone(), ShardId::new(shard))
            .await
            .unwrap();
    }

    async fn allocate_running(&mut self, shard: u32) {
        self.allocate(shard).await;
        let key = self.shard_key(shard);
        let slot = self.store.get_slot(&key).await.unwrap().unwrap();
        let owner = slot.owner.clone().unwrap();
        self.leader
            .complete_initial_ready(&key, &owner, slot.assignment_generation)
            .await
            .unwrap();
    }
}

/// Allocation used to rebuild the placement view from a full `list_slots`, so a cold start that
/// resolved N shards paid a scan that grew with the shards it had already placed.
#[tokio::test]
async fn allocating_every_shard_never_rescans_the_slots_it_already_placed() {
    let mut fixture = fixture("amplify-allocate", 26600, 8).await;
    fixture.store.reset_read_counts();
    for shard in 0..SHARDS {
        fixture.allocate(shard).await;
    }
    let counts = fixture.store.read_counts();
    assert_eq!(counts.list_slots, 0);
    assert_eq!(counts.list_slots_page, 0);
    // One existence probe per resolution, and nothing that grows with the placed inventory.
    assert_eq!(counts.get_slot, u64::from(SHARDS));
    assert_eq!(counts.slot_records, u64::from(SHARDS));
    assert_eq!(fixture.leader.slots.len(), usize::try_from(SHARDS).unwrap());
}

/// The bounded pass is the periodic sweep, so its per-tick read cost has to be a function of the
/// page size alone. Two domains that differ only in how many slots they hold must cost the same.
#[tokio::test]
async fn one_bounded_pass_costs_the_same_at_any_inventory_size() {
    let small = fixture("amplify-small", 26620, 4).await;
    let large = fixture("amplify-large", 26640, 4).await;
    for (fixture, shards) in [(small, 4_u32), (large, SHARDS)] {
        let mut fixture = fixture;
        for shard in 0..shards {
            fixture.allocate(shard).await;
        }
        fixture.store.reset_read_counts();
        fixture.leader.reconcile_bounded_pass().await.unwrap();
        let counts = fixture.store.read_counts();
        assert_eq!(counts.list_slots, 0, "shards={shards}");
        assert_eq!(counts.list_slots_page, 1, "shards={shards}");
        assert_eq!(counts.slot_records, 4, "shards={shards}");
        assert_eq!(counts.get_claim, 4, "shards={shards}");
        assert_eq!(
            fixture.leader.reconciliation.backlog,
            usize::try_from(shards).unwrap() - 4,
            "shards={shards}"
        );
    }
}

/// A sweep that resumes from a key cursor still visits every slot exactly once, so the bounded
/// per-pass cost above does not buy progress that silently skips records.
#[tokio::test]
async fn repeated_bounded_passes_visit_every_slot_exactly_once() {
    let mut fixture = fixture("amplify-sweep", 26660, 4).await;
    for shard in 0..SHARDS {
        fixture.allocate(shard).await;
    }
    fixture.store.reset_read_counts();
    let passes = usize::try_from(SHARDS).unwrap() / 4;
    for _ in 0..passes {
        fixture.leader.reconcile_bounded_pass().await.unwrap();
    }
    let counts = fixture.store.read_counts();
    assert_eq!(counts.slot_records, u64::from(SHARDS));
    assert_eq!(counts.get_claim, u64::from(SHARDS));
    assert_eq!(fixture.leader.reconciliation.backlog, 0);
    assert!(fixture.leader.reconciliation.cursor.is_none());
    assert!(fixture.leader.reconciliation.quarantined.is_empty());
}

/// Drain readiness is a release decision, so it keeps reading durable truth. What it must not do is
/// repeat that read on every renewal tick while the member demonstrably still owns slots.
#[tokio::test]
async fn a_stalled_drain_stops_rescanning_while_it_still_owns_slots() {
    let mut fixture = fixture("amplify-drain", 26680, 8).await;
    for shard in 0..4 {
        fixture.allocate_running(shard).await;
    }
    let incarnation = fixture.source.incarnation;
    fixture
        .leader
        .begin_member_drain(incarnation, "drain-amplified".to_owned(), incarnation)
        .await
        .unwrap();
    fixture.store.reset_read_counts();
    for _ in 0..8 {
        fixture
            .leader
            .maybe_send_drain_ready(incarnation)
            .await
            .unwrap();
    }
    let counts = fixture.store.read_counts();
    assert_eq!(counts.list_slots, 0);
    assert_eq!(counts.list_slots_page, 0);
    assert_eq!(counts.slot_records, 0);
}

/// The mirror is only ever an input to a proposal, but a proposal built from a stale mirror wastes
/// a round, so the commit paths have to keep it converged on what the store actually holds.
#[tokio::test]
async fn the_slot_mirror_matches_durable_truth_after_a_full_handoff() {
    let mut fixture = fixture("amplify-mirror", 26700, 8).await;
    for shard in 0..4 {
        fixture.allocate_running(shard).await;
    }
    let incarnation = fixture.source.incarnation;
    fixture
        .leader
        .begin_member_drain(incarnation, "drain-mirror".to_owned(), incarnation)
        .await
        .unwrap();
    let durable = fixture
        .store
        .list_slots(&domain())
        .await
        .unwrap()
        .into_iter()
        .map(|slot| (slot.key.clone(), slot))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(fixture.leader.slots, durable);
}
