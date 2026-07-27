use lattice_remoting::{
    association::Association,
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
    wire::{Frame, FrameKind},
};

use super::*;
use crate::{coordinator::NodeLoadReport, runtime::membership::slot_record_key};

struct DomainFixture {
    leader: PlacementDomainLeader<InMemoryPlacementStore>,
    store: Arc<InMemoryPlacementStore>,
    entity_type: EntityType,
    host: NodeKey,
    host_key: AssociationKey,
    proxy: NodeKey,
    proxy_key: AssociationKey,
    host_hello: TestHello,
    proxy_hello: TestHello,
}

async fn domain_fixture(
    cluster: &str,
    port_base: u16,
    incarnation_base: u128,
    config: PlacementDomainLeaderConfig,
) -> DomainFixture {
    let cluster_id = ClusterId::new(cluster).unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", port_base, incarnation_base);
    let (host, _) = node(&cluster_id, "host", port_base + 1, incarnation_base + 1);
    let (proxy, _) = node(&cluster_id, "proxy", port_base + 2, incarnation_base + 2);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let host_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator.incarnation,
        &host,
        u128::from(port_base) * 10,
    );
    let proxy_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator.incarnation,
        &proxy,
        u128::from(port_base) * 10 + 5,
    );
    let store = Arc::new(InMemoryPlacementStore::new(32, 32).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store.clone(),
        associations,
        coordinator,
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        config,
    )
    .await
    .unwrap();
    let entity_type = EntityType::new("resilience-entity").unwrap();
    let protocol_id = ProtocolId::new(71).unwrap();
    let entity_config = EntityConfig::new(
        domain(),
        entity_type.clone(),
        protocol_id,
        8,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let descriptor = ProtocolDescriptor {
        protocol_id,
        fingerprint: ProtocolFingerprint::new([11; 32]),
    };
    let host_hello = test_hello(
        host.clone(),
        TestHelloSpec {
            capacity_units: 4,
            hosted_entity_types: [entity_type.clone()].into_iter().collect(),
            protocols: vec![descriptor.clone()],
            entity_configs: vec![entity_config.clone()],
            ..TestHelloSpec::default()
        },
    );
    let proxy_hello = test_hello(
        proxy.clone(),
        TestHelloSpec {
            capacity_units: 1,
            proxied_entity_types: [entity_type.clone()].into_iter().collect(),
            protocols: vec![descriptor],
            ..TestHelloSpec::default()
        },
    );
    register_up(&mut leader, host_hello.clone(), host_key.clone()).await;
    register_up(&mut leader, proxy_hello.clone(), proxy_key.clone()).await;
    DomainFixture {
        leader,
        store,
        entity_type,
        host,
        host_key,
        proxy,
        proxy_key,
        host_hello,
        proxy_hello,
    }
}

fn drained_control(association: &Association) -> mpsc::Receiver<Frame> {
    let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
    while control.try_recv().is_ok() {}
    control
}

fn collected_commands(control: &mut mpsc::Receiver<Frame>) -> Vec<PlacementControlCommand> {
    let mut commands = Vec::new();
    while let Ok(frame) = control.try_recv() {
        let payload = match frame.kind {
            FrameKind::CoordinatorEvent => frame.payload().to_vec(),
            FrameKind::ControlEnvelope => decode_control_envelope(&frame).unwrap().payload.to_vec(),
            _ => continue,
        };
        if let Ok(scoped) = decode_control_command(&payload, DEFAULT_MAX_CONTROL_PAYLOAD) {
            commands.push(scoped.command);
        }
    }
    commands
}

fn resolve_shard(
    association: &AssociationKey,
    entity_type: &EntityType,
    shard_id: ShardId,
    request_id: u128,
) -> PlacementControlEventKind {
    PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
        association: association.clone(),
        command_id: CommandId::generate(),
        scope: CoordinatorScope::Placement(domain()),
        coordinator_term: None,
        command: PlacementControlCommand::ResolveShard {
            request_id,
            domain: domain(),
            entity_type: entity_type.clone(),
            shard_id,
        },
    }))
}

#[tokio::test]
async fn transient_claim_lease_failure_focuses_one_slot_instead_of_ending_leadership() {
    let mut fixture = domain_fixture(
        "claim-keepalive-resilience",
        26700,
        700,
        PlacementDomainLeaderConfig::default(),
    )
    .await;
    fixture
        .leader
        .handle_control(resolve_shard(
            &fixture.proxy_key,
            &fixture.entity_type,
            ShardId::new(2),
            1,
        ))
        .await
        .unwrap();
    let shard_key = PlacementSlotKey::Shard {
        domain: domain(),
        entity_type: fixture.entity_type.clone(),
        shard_id: ShardId::new(2),
    };
    let lease_id = fixture.leader.claims.get(&shard_key).unwrap().lease_id;
    fixture.store.revoke_lease(lease_id).await.unwrap();

    fixture.leader.renew().await.unwrap();

    assert!(fixture.leader.reconciliation.focus.contains(&shard_key));
    assert!(fixture.leader.claims.contains_key(&shard_key));
    assert_eq!(
        fixture
            .store
            .get_slot(&shard_key)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementSlotState::Allocating
    );

    fixture.leader.reconcile_bounded_pass().await.unwrap();
    assert!(fixture.leader.reconciliation.focus.is_empty());
    let repaired = fixture.store.get_slot(&shard_key).await.unwrap().unwrap();
    assert_eq!(repaired.assignment_generation.get(), 2);
    assert_eq!(repaired.owner.as_ref(), Some(&fixture.host));
    assert!(fixture.store.get_claim(&shard_key).await.unwrap().is_some());
}

#[tokio::test]
async fn logic_session_replacement_and_member_removal_clear_the_bounded_load_entry() {
    let mut fixture = domain_fixture(
        "load-table-eviction",
        26720,
        720,
        PlacementDomainLeaderConfig {
            maximum_node_loads: 1,
            ..PlacementDomainLeaderConfig::default()
        },
    )
    .await;
    let cluster_id = fixture.host_key.cluster_id.clone();
    let local_incarnation = fixture.host_key.local_incarnation;
    let report = |node: NodeKey| {
        PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
            association: AssociationKey {
                cluster_id: cluster_id.clone(),
                local_incarnation,
                remote_address: node.address.clone(),
                remote_incarnation: node.incarnation,
            },
            command_id: CommandId::generate(),
            scope: CoordinatorScope::Placement(domain()),
            coordinator_term: None,
            command: PlacementControlCommand::NodeLoad(NodeLoadReport {
                node,
                sequence: 1,
                observed_at: MonotonicTime::from_millis(1),
                total_weight: 7,
            }),
        }))
    };
    fixture
        .leader
        .handle_control(report(fixture.host.clone()))
        .await
        .unwrap();
    assert!(
        fixture
            .leader
            .loads
            .node(fixture.host.incarnation)
            .is_some()
    );

    fixture
        .leader
        .register(fixture.host_hello.domain.clone(), fixture.host_key.clone())
        .await
        .unwrap();
    assert!(
        fixture
            .leader
            .loads
            .node(fixture.host.incarnation)
            .is_none(),
        "a replacement logic session must not inherit a stale sequence"
    );
    fixture
        .leader
        .handle_control(report(fixture.host.clone()))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .leader
            .loads
            .node(fixture.host.incarnation)
            .unwrap()
            .sequence,
        1
    );

    let record = fixture
        .leader
        .sessions
        .get(&fixture.host.incarnation)
        .unwrap()
        .record
        .clone();
    fixture
        .leader
        .remove_member(record, MemberRemovalReason::GracefulLeave)
        .await
        .unwrap();

    assert!(
        fixture
            .leader
            .loads
            .node(fixture.host.incarnation)
            .is_none()
    );
    assert!(
        !fixture
            .leader
            .node_load_received
            .contains_key(&fixture.host.incarnation)
    );
    fixture
        .leader
        .handle_control(report(fixture.proxy.clone()))
        .await
        .unwrap();
    assert!(
        fixture
            .leader
            .loads
            .node(fixture.proxy.incarnation)
            .is_some()
    );
}

#[tokio::test]
async fn allocating_resolution_publishes_a_slot_delta_without_a_full_snapshot() {
    let mut fixture = domain_fixture(
        "resolution-delta",
        26740,
        740,
        PlacementDomainLeaderConfig::default(),
    )
    .await;
    let proxy_association = fixture.leader.associations.get(&fixture.proxy_key).unwrap();
    let mut control = drained_control(&proxy_association);

    fixture
        .leader
        .handle_control(resolve_shard(
            &fixture.proxy_key,
            &fixture.entity_type,
            ShardId::new(5),
            9,
        ))
        .await
        .unwrap();

    let commands = collected_commands(&mut control);
    assert!(
        !commands
            .iter()
            .any(|command| matches!(command, PlacementControlCommand::SnapshotBegin(_))),
        "resolution must not re-encode the whole domain snapshot"
    );
    let slot_key = slot_record_key(&PlacementSlotKey::Shard {
        domain: domain(),
        entity_type: fixture.entity_type.clone(),
        shard_id: ShardId::new(5),
    });
    assert!(commands.iter().any(|command| matches!(
        command,
        PlacementControlCommand::StateDelta(delta)
            if delta.records.iter().any(|record| record.key == slot_key)
    )));
}

#[tokio::test]
async fn a_member_reaching_the_leader_revision_reconciles_its_claims_once() {
    let mut fixture = domain_fixture(
        "claim-reconciliation-once",
        26760,
        760,
        PlacementDomainLeaderConfig::default(),
    )
    .await;
    let shard_key = PlacementSlotKey::Shard {
        domain: domain(),
        entity_type: fixture.entity_type.clone(),
        shard_id: ShardId::new(4),
    };
    let config = fixture
        .leader
        .entity_configs
        .get(&fixture.entity_type)
        .cloned()
        .unwrap();
    seed_running_slot(
        &mut fixture.leader,
        PlacementSlot {
            key: shard_key.clone(),
            config_fingerprint: config.fingerprint(),
            owner: Some(fixture.host.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(1).unwrap(),
            version: PlacementVersion::new(
                domain(),
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(1).unwrap(),
            ),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        },
        Some(&fixture.host_hello),
    )
    .await;
    fixture.leader.claims.remove(&shard_key);
    let host_association = fixture.leader.associations.get(&fixture.host_key).unwrap();
    let mut control = drained_control(&host_association);
    let applied = PlacementControlEventKind::Command(Box::new(InboundPlacementControl {
        association: fixture.host_key.clone(),
        command_id: CommandId::generate(),
        scope: CoordinatorScope::Placement(domain()),
        coordinator_term: None,
        command: PlacementControlCommand::AppliedRevision(fixture.leader.version.clone()),
    }));

    fixture
        .leader
        .handle_control(applied.clone())
        .await
        .unwrap();
    let first = collected_commands(&mut control)
        .into_iter()
        .filter(|command| matches!(command, PlacementControlCommand::ClaimGranted(_)))
        .count();
    assert_eq!(first, 1);

    fixture.leader.handle_control(applied).await.unwrap();
    let second = collected_commands(&mut control)
        .into_iter()
        .filter(|command| matches!(command, PlacementControlCommand::ClaimGranted(_)))
        .count();
    assert_eq!(
        second, 0,
        "a repeated acknowledgement must not rescan slots"
    );
    assert!(
        fixture
            .leader
            .sessions
            .get(&fixture.host.incarnation)
            .unwrap()
            .claims_reconciled
    );
    assert_eq!(fixture.proxy_hello.domain.node, fixture.proxy);
}

#[tokio::test]
async fn singleton_kinds_spread_across_one_eligibility_set() {
    let cluster_id = ClusterId::new("singleton-spread").unwrap();
    let (coordinator, _) = node(&cluster_id, "coordinator", 26780, 780);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator.address.clone(),
            coordinator.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let store = Arc::new(InMemoryPlacementStore::new(64, 64).unwrap());
    let mut leader = PlacementDomainLeader::elect(
        store,
        associations.clone(),
        coordinator.clone(),
        CoordinatorScope::Placement(domain()),
        CoordinatorTerm::new(1).unwrap(),
        PlacementDomainLeaderConfig::default(),
    )
    .await
    .unwrap();
    let protocol_id = ProtocolId::new(73).unwrap();
    let descriptor = ProtocolDescriptor {
        protocol_id,
        fingerprint: ProtocolFingerprint::new([12; 32]),
    };
    let kinds = (0..6)
        .map(|index| SingletonKind::new(format!("kind-{index}")).unwrap())
        .collect::<Vec<_>>();
    let configs = kinds
        .iter()
        .map(|kind| SingletonConfig::new(domain(), kind.clone(), protocol_id))
        .collect::<Vec<_>>();
    for (kind, config) in kinds.iter().zip(&configs) {
        leader
            .singleton_configs
            .insert(kind.clone(), config.clone());
    }
    for index in 0..4_u128 {
        let (host, _) = node(
            &cluster_id,
            &format!("singleton-host-{index}"),
            26790 + index as u16,
            790 + index,
        );
        let key = attach_test_session(
            &associations,
            &cluster_id,
            coordinator.incarnation,
            &host,
            8000 + index * 10,
        );
        register_up(
            &mut leader,
            test_hello(
                host,
                TestHelloSpec {
                    capacity_units: 4,
                    singleton_eligibility: kinds.iter().cloned().collect(),
                    protocols: vec![descriptor.clone()],
                    singleton_configs: configs.clone(),
                    ..TestHelloSpec::default()
                },
            ),
            key,
        )
        .await;
    }

    let targets = kinds
        .iter()
        .zip(&configs)
        .map(|(kind, config)| {
            leader
                .select_singleton_target(kind, config, None)
                .unwrap()
                .node_id
        })
        .collect::<BTreeSet<_>>();
    assert!(
        targets.len() > 1,
        "one eligibility set must not host every singleton kind on one node"
    );
    for (kind, config) in kinds.iter().zip(&configs) {
        assert_eq!(
            leader.select_singleton_target(kind, config, None).unwrap(),
            leader.select_singleton_target(kind, config, None).unwrap()
        );
    }
}

#[tokio::test]
async fn member_removal_survives_a_surviving_session_whose_association_is_gone() {
    let mut fixture = domain_fixture(
        "removal-association-loss",
        26760,
        760,
        PlacementDomainLeaderConfig::default(),
    )
    .await;
    let proxy_association = fixture.leader.associations.get(&fixture.proxy_key).unwrap();
    assert!(
        fixture
            .leader
            .associations
            .remove(&fixture.proxy_key, proxy_association.id())
    );

    let record = fixture
        .leader
        .sessions
        .get(&fixture.host.incarnation)
        .unwrap()
        .record
        .clone();
    fixture
        .leader
        .remove_member(record, MemberRemovalReason::FailureDetected)
        .await
        .expect("an unreachable surviving session must not end the domain leader");

    assert!(
        !fixture
            .leader
            .sessions
            .contains_key(&fixture.host.incarnation)
    );
    assert!(
        fixture
            .leader
            .sessions
            .contains_key(&fixture.proxy.incarnation)
    );
}
