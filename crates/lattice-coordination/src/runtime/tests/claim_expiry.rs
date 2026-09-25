use crate::candidate_fixture::elect_group;
use lattice_model::actor::ProtocolId;
use lattice_remoting::{
    association::Association,
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
    wire::{Frame, FrameKind},
};

use super::*;

struct ClaimFixture {
    leader: GroupCoordinator<InMemoryCoordinationStore>,
    store: Arc<InMemoryCoordinationStore>,
    associations: Arc<AssociationManager>,
    host: NodeKey,
    host_key: AssociationKey,
    spare: NodeKey,
    spare_key: AssociationKey,
    shard_key: PlacementSlotKey,
    hello: Box<dyn Fn(NodeKey) -> TestHello>,
}

async fn claim_fixture(
    cluster: &str,
    port_base: u16,
    incarnation_base: u128,
    config: GroupCoordinatorConfig,
) -> ClaimFixture {
    let cluster_id = ClusterId::new(cluster).unwrap();
    let (coordinator_node, _) = node(&cluster_id, "coordinator", port_base, incarnation_base);
    let (host, _) = node(&cluster_id, "host", port_base + 1, incarnation_base + 1);
    let (spare, _) = node(&cluster_id, "spare", port_base + 2, incarnation_base + 2);
    let associations = Arc::new(
        AssociationManager::new(
            coordinator_node.address.clone(),
            coordinator_node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let host_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &host,
        u128::from(port_base) * 10,
    );
    let spare_key = attach_test_session(
        &associations,
        &cluster_id,
        coordinator_node.incarnation,
        &spare,
        u128::from(port_base) * 10 + 5,
    );
    let store = Arc::new(InMemoryCoordinationStore::new(16, 16).unwrap());
    let entity_type = EntityType::new("claim-entity").unwrap();
    let protocol_id = ProtocolId::new(61).unwrap();
    let entity_config = EntityConfig::new(
        group(),
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
        fingerprint: ProtocolFingerprint::new([3; 32]),
    };
    let mut leader = elect_group(
        store.clone(),
        associations.clone(),
        coordinator_node,
        CoordinatorScope::Group(group()),
        CoordinatorTerm::new(1).unwrap(),
        config,
    )
    .await
    .unwrap();
    let shard_key = PlacementSlotKey::Shard {
        group: group(),
        entity_type: entity_type.clone(),
        shard_id: ShardId::new(3),
    };
    let fingerprint = entity_config.fingerprint();
    let hello = move |node: NodeKey| {
        test_hello(
            node,
            TestHelloSpec {
                capacity_units: 4,
                hosted_entity_types: [entity_type.clone()].into_iter().collect(),
                protocols: vec![descriptor.clone()],
                entity_configs: vec![entity_config.clone()],
                ..TestHelloSpec::default()
            },
        )
    };
    register_up(&mut leader, hello(host.clone()), host_key.clone()).await;
    seed_running_slot(
        &mut leader,
        PlacementSlot {
            key: shard_key.clone(),
            config_fingerprint: fingerprint,
            owner: Some(host.clone()),
            target: None,
            assignment_generation: AssignmentGeneration::new(1).unwrap(),
            version: PlacementVersion::new(
                group(),
                CoordinatorTerm::new(1).unwrap(),
                Revision::new(1).unwrap(),
            ),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        },
        Some(&hello(host.clone())),
    )
    .await;
    ClaimFixture {
        leader,
        store,
        associations,
        host,
        host_key,
        spare,
        spare_key,
        shard_key,
        hello: Box::new(hello),
    }
}

fn drain_control(association: &Association) -> mpsc::Receiver<Frame> {
    let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
    while control.try_recv().is_ok() {}
    control
}

fn collected_grants(control: &mut mpsc::Receiver<Frame>) -> (usize, usize) {
    let mut ephemeral = 0;
    let mut reliable = 0;
    while let Ok(frame) = control.try_recv() {
        let payload = match frame.kind {
            FrameKind::CoordinatorEvent => frame.payload().to_vec(),
            FrameKind::ControlEnvelope => decode_control_envelope(&frame).unwrap().payload.to_vec(),
            _ => continue,
        };
        let Ok(scoped) = decode_control_command(&payload, DEFAULT_MAX_CONTROL_PAYLOAD) else {
            continue;
        };
        if matches!(scoped.command, PlacementControlCommand::ClaimGranted(_)) {
            match frame.kind {
                FrameKind::CoordinatorEvent => ephemeral += 1,
                _ => reliable += 1,
            }
        }
    }
    (ephemeral, reliable)
}

#[tokio::test]
async fn correlated_issuance_is_guarded_and_persisted_before_response() {
    let mut fixture = claim_fixture(
        "fresh-grant",
        34800,
        34800,
        GroupCoordinatorConfig::default(),
    )
    .await;
    let association = fixture.associations.get(&fixture.host_key).unwrap();
    let mut outgoing = drain_control(&association);
    let original = fixture
        .store
        .get_claim(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();
    fixture
        .leader
        .issue_requested_claim(
            &fixture.host_key,
            123,
            fixture.shard_key.clone(),
            original.grant.assignment_generation,
        )
        .await
        .unwrap();
    let committed = fixture
        .store
        .get_claim(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.grant.request_id, 123);
    assert!(committed.grant.grant_sequence > original.grant.grant_sequence);
    assert_eq!(committed.lease_id, original.lease_id);
    let frame = outgoing.recv().await.unwrap();
    let command = decode_control_command(frame.payload(), DEFAULT_MAX_CONTROL_PAYLOAD)
        .unwrap()
        .command;
    assert_eq!(
        command,
        PlacementControlCommand::ClaimGranted(committed.grant.clone())
    );
    fixture
        .store
        .revoke_lease(fixture.leader.leader.candidate_lease_id)
        .await
        .unwrap();
    assert!(
        fixture
            .leader
            .issue_requested_claim(
                &fixture.host_key,
                124,
                fixture.shard_key.clone(),
                original.grant.assignment_generation
            )
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .store
            .get_claim(&fixture.shard_key)
            .await
            .unwrap()
            .unwrap()
            .grant,
        committed.grant
    );
    assert!(outgoing.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn unreconnected_group_admission_expires_without_claiming_the_owner_stopped() {
    let mut fixture = claim_fixture(
        "group-reconnect-grace",
        34820,
        34820,
        GroupCoordinatorConfig::default(),
    )
    .await;
    fixture.leader.sessions.clear();
    fixture.leader.reconcile_initial_inventory().await.unwrap();
    assert!(
        fixture
            .store
            .get_group_member(&group(), &fixture.host.node_id)
            .await
            .unwrap()
            .is_some()
    );
    fixture.leader.reconcile_bounded_pass().await.unwrap();
    assert!(
        fixture
            .store
            .get_group_member(&group(), &fixture.host.node_id)
            .await
            .unwrap()
            .is_some()
    );
    tokio::time::advance(fixture.leader.config.member_heartbeat_timeout + Duration::from_secs(1))
        .await;
    fixture.leader.reconcile_bounded_pass().await.unwrap();
    assert!(
        fixture
            .store
            .get_group_member(&group(), &fixture.host.node_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .store
            .get_member(&fixture.host.node_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        fixture
            .store
            .get_claim(&fixture.shard_key)
            .await
            .unwrap()
            .is_some(),
        "removing group admission is not permission to revoke outstanding grants"
    );
}

#[tokio::test(start_paused = true)]
async fn missing_claim_cannot_reinstall_before_full_protocol_holdoff() {
    let mut fixture = claim_fixture(
        "replacement-wait",
        34810,
        34810,
        GroupCoordinatorConfig::default(),
    )
    .await;
    let claim = fixture
        .store
        .get_claim(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();
    fixture.store.revoke_lease(claim.lease_id).await.unwrap();
    let slot = fixture
        .store
        .get_slot(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();
    fixture.leader.fence_missing_claim(slot).await.unwrap();
    let fenced = fixture
        .store
        .get_slot(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !fixture
            .leader
            .reinstall_fenced_authority(fenced.clone())
            .await
            .unwrap()
    );
    tokio::time::advance(Duration::from_secs(15)).await;
    assert!(
        !fixture
            .leader
            .reinstall_fenced_authority(fenced.clone())
            .await
            .unwrap()
    );
    // Losing the process-local observation starts the whole conservative wait again.
    fixture.leader.reconciliation.replacement_waits.clear();
    assert!(
        !fixture
            .leader
            .reinstall_fenced_authority(fenced.clone())
            .await
            .unwrap()
    );
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(
        fixture
            .leader
            .reinstall_fenced_authority(fenced)
            .await
            .unwrap()
    );
}

fn fenced_config() -> GroupCoordinatorConfig {
    GroupCoordinatorConfig {
        renewal_interval: Duration::from_millis(10),
        claim_ttl: Duration::from_millis(50),
        member_heartbeat_interval: Duration::from_millis(10),
        member_heartbeat_timeout: Duration::from_millis(100),
        ..GroupCoordinatorConfig::default()
    }
}

#[tokio::test(start_paused = true)]
async fn heartbeat_timeout_defers_fencing_until_the_claim_lease_expires() {
    let mut fixture = claim_fixture("claim-expiry-test", 26500, 500, fenced_config()).await;
    let leased = fixture
        .store
        .get_claim(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();

    fixture
        .leader
        .sessions
        .get_mut(&fixture.host.incarnation)
        .unwrap()
        .last_heartbeat = Instant::now() - Duration::from_secs(1);
    fixture.leader.renew().await.unwrap();

    assert!(
        !fixture
            .leader
            .sessions
            .contains_key(&fixture.host.incarnation)
    );
    let retained = fixture
        .store
        .get_slot(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.state, PlacementSlotState::Running);
    assert_eq!(retained.owner.as_ref(), Some(&fixture.host));
    assert_eq!(
        fixture
            .store
            .get_claim(&fixture.shard_key)
            .await
            .unwrap()
            .map(|claim| claim.lease_id),
        Some(leased.lease_id)
    );
    assert!(!fixture.leader.claims.contains_key(&fixture.shard_key));

    fixture.leader.reconcile_bounded_pass().await.unwrap();
    assert_eq!(
        fixture
            .store
            .get_slot(&fixture.shard_key)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementSlotState::Running
    );
    assert!(!fixture.leader.claims.contains_key(&fixture.shard_key));

    fixture.store.revoke_lease(leased.lease_id).await.unwrap();
    fixture.leader.reconcile_bounded_pass().await.unwrap();
    assert_eq!(
        fixture
            .store
            .get_slot(&fixture.shard_key)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementSlotState::Fenced
    );

    let spare_hello = (fixture.hello)(fixture.spare.clone());
    register_up(&mut fixture.leader, spare_hello, fixture.spare_key.clone()).await;
    fixture.leader.reconcile_bounded_pass().await.unwrap();
    assert_eq!(
        fixture
            .store
            .get_slot(&fixture.shard_key)
            .await
            .unwrap()
            .unwrap()
            .state,
        PlacementSlotState::Fenced
    );
    tokio::time::advance(Duration::from_secs(16)).await;
    fixture.leader.reconcile_bounded_pass().await.unwrap();
    let reinstalled = fixture
        .store
        .get_slot(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reinstalled.owner.as_ref(), Some(&fixture.spare));
    assert_eq!(reinstalled.state, PlacementSlotState::Allocating);
    assert_eq!(reinstalled.assignment_generation.get(), 2);
    assert_eq!(
        fixture
            .store
            .get_claim(&fixture.shard_key)
            .await
            .unwrap()
            .unwrap()
            .grant
            .owner,
        fixture.spare
    );
}

#[tokio::test]
async fn force_removal_leaves_the_claim_lease_to_expire_while_graceful_leave_revokes_it() {
    let mut fixture = claim_fixture(
        "claim-force-removal-test",
        26520,
        520,
        GroupCoordinatorConfig::default(),
    )
    .await;
    let leased = fixture
        .store
        .get_claim(&fixture.shard_key)
        .await
        .unwrap()
        .unwrap();
    let member = fixture
        .store
        .get_member(&fixture.host.node_id)
        .await
        .unwrap()
        .unwrap();

    fixture
        .leader
        .remove_member(member.clone(), MemberRemovalReason::ForceRemoved)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .store
            .get_claim(&fixture.shard_key)
            .await
            .unwrap()
            .map(|claim| claim.lease_id),
        Some(leased.lease_id)
    );
    assert!(!fixture.leader.claims.contains_key(&fixture.shard_key));

    fixture.leader.claims.insert(
        fixture.shard_key.clone(),
        ClaimLease {
            lease_id: leased.lease_id,
            grant: leased.grant.clone(),
        },
    );
    fixture
        .leader
        .finish_member_removal(member, MemberRemovalReason::GracefulLeave)
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .get_claim(&fixture.shard_key)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn claim_grants_are_delivered_outside_the_reliable_control_outbox() {
    let mut fixture = claim_fixture(
        "claim-grant-outbox-test",
        26540,
        540,
        GroupCoordinatorConfig::default(),
    )
    .await;
    let association = fixture.associations.get(&fixture.host_key).unwrap();
    let mut control = drain_control(&association);

    fixture.leader.renew().await.unwrap();

    assert_eq!(collected_grants(&mut control), (1, 0));
    let replayed = association
        .replay_control_frames()
        .into_iter()
        .filter_map(|frame| decode_control_envelope(&frame).ok())
        .filter_map(|envelope| {
            decode_control_command(&envelope.payload, DEFAULT_MAX_CONTROL_PAYLOAD).ok()
        })
        .filter(|scoped| matches!(scoped.command, PlacementControlCommand::ClaimGranted(_)))
        .count();
    assert_eq!(replayed, 0);
}

#[tokio::test]
async fn a_stale_member_heartbeat_stops_grant_renewal_before_the_claim_lease_expires() {
    let mut fixture = claim_fixture(
        "claim-grant-window-test",
        26560,
        560,
        GroupCoordinatorConfig::default(),
    )
    .await;
    let association = fixture.associations.get(&fixture.host_key).unwrap();
    let mut control = drain_control(&association);
    let window = fixture.leader.config.member_heartbeat_timeout - fixture.leader.config.claim_ttl;

    fixture
        .leader
        .sessions
        .get_mut(&fixture.host.incarnation)
        .unwrap()
        .last_heartbeat = Instant::now() - window - Duration::from_secs(1);
    fixture.leader.renew().await.unwrap();
    assert!(
        fixture
            .leader
            .sessions
            .contains_key(&fixture.host.incarnation)
    );
    assert!(fixture.leader.claims.contains_key(&fixture.shard_key));
    assert_eq!(collected_grants(&mut control), (0, 0));

    fixture
        .leader
        .sessions
        .get_mut(&fixture.host.incarnation)
        .unwrap()
        .last_heartbeat = Instant::now();
    fixture.leader.renew().await.unwrap();
    assert_eq!(collected_grants(&mut control), (1, 0));
}

#[test]
fn leader_config_rejects_a_claim_ttl_that_outlives_the_member_heartbeat_timeout() {
    let base = GroupCoordinatorConfig::default();
    base.validate().unwrap();
    let floor = base.claim_ttl + base.member_heartbeat_interval;
    let error = GroupCoordinatorConfig {
        member_heartbeat_timeout: floor,
        ..base.clone()
    }
    .validate()
    .unwrap_err();
    assert!(matches!(
        error,
        CoordinatorRuntimeError::InvalidConfig { .. }
    ));
    assert_eq!(
        error.to_string(),
        "Coordinator runtime configuration is invalid: member_heartbeat_timeout=20s must be \
         greater than claim_ttl=15s plus member_heartbeat_interval=5s (20s)"
    );
    GroupCoordinatorConfig {
        member_heartbeat_timeout: floor + Duration::from_millis(1),
        ..base.clone()
    }
    .validate()
    .unwrap();
    assert!(matches!(
        GroupCoordinatorConfig {
            member_heartbeat_interval: Duration::ZERO,
            ..base
        }
        .validate(),
        Err(CoordinatorRuntimeError::InvalidConfig { .. })
    ));
}
