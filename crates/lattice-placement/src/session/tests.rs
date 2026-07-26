use lattice_core::actor_ref::{
    ClusterId, ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation,
};
use lattice_remoting::{
    association::{LaneAttachment, LaneKind},
    config::RemotingConfig,
};

use super::*;
use crate::authority::AuthorityEvent;
use crate::types::{
    AssignmentGeneration, ClaimGrant, CoordinatorTerm, GrantSequence, PlacementVersion, Revision,
    ShardId,
};

/// A frozen process keeps whatever admission flag it cached and never runs the fencing tick, so
/// the only thing that can stop it answering after it is thawed is the deadline the grant it
/// holds was installed with.
#[tokio::test(start_paused = true)]
async fn admission_closes_on_the_installed_deadline_even_though_no_tick_ran() {
    let domain = PlacementDomainId::new("frozen-owner").unwrap();
    let local = NodeKey {
        node_id: "owner".to_owned(),
        address: NodeAddress::new("127.0.0.1", 34200).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let key = PlacementSlotKey::Shard {
        domain: domain.clone(),
        entity_type: EntityType::new("frozen-entity").unwrap(),
        shard_id: ShardId::new(0),
    };
    let slot = PlacementSlot {
        key: key.clone(),
        config_fingerprint: ConfigFingerprint::new([3; 32]),
        owner: Some(local.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(
            domain.clone(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let origin = Instant::now();
    let mut authority = PlacementAuthority::new(local.clone(), Duration::from_secs(2)).unwrap();
    authority
        .transition(AuthorityEvent::ReconcileSlot(slot.clone()))
        .unwrap();
    authority
        .transition(AuthorityEvent::InstallGrant {
            grant: ClaimGrant {
                domain: domain.clone(),
                slot: key.clone(),
                owner: local.clone(),
                coordinator_term: slot.version.term,
                assignment_generation: slot.assignment_generation,
                grant_sequence: GrantSequence::new(1).unwrap(),
                ttl: Duration::from_secs(15),
            },
            now: monotonic_since(origin),
        })
        .unwrap();
    let state = LogicPlacementState {
        local_node: local,
        coordinator_term: 1,
        session: PlacementDomainState::new(domain),
        slots: [(key.clone(), slot)].into_iter().collect(),
        authorities: [(key.clone(), authority)].into_iter().collect(),
        resolution_failures: BTreeMap::new(),
        domain_up: true,
        origin,
        changed: Arc::new(Notify::new()),
    };

    assert!(state.admission_open(&key));
    tokio::time::advance(Duration::from_secs(12)).await;
    assert!(state.admission_open(&key));
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(!state.admission_open(&key));
    tokio::time::advance(Duration::from_secs(26)).await;
    assert!(!state.admission_open(&key));
}

#[tokio::test(start_paused = true)]
async fn an_unacknowledged_drain_completion_gives_up_instead_of_polling_forever() {
    let cluster_id = ClusterId::new("drain-timeout").unwrap();
    let local = NodeKey {
        node_id: "logic".to_owned(),
        address: NodeAddress::new("127.0.0.1", 34100).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let remote_address = NodeAddress::new("127.0.0.1", 34101).unwrap();
    let remote_incarnation = NodeIncarnation::new(2).unwrap();
    let associations = Arc::new(
        AssociationManager::new(
            local.address.clone(),
            local.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let association = associations
        .get_or_create(
            cluster_id.clone(),
            remote_address.clone(),
            remote_incarnation,
        )
        .unwrap();
    let coordinator = AssociationKey {
        cluster_id,
        local_incarnation: local.incarnation,
        remote_address,
        remote_incarnation,
    };
    for (lane, nonce) in [
        (LaneKind::Control, 1_u128),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: coordinator.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    let config = LogicCoordinatorConfig {
        drain_acknowledgement_timeout: Duration::from_secs(4),
        ..LogicCoordinatorConfig::default()
    };
    let (session, _effects) = PlacementDomainSession::new(
        PlacementDomainHello::builder(local, PlacementDomainId::new("drain-timeout").unwrap(), 1)
            .build(),
        coordinator,
        associations,
        config.clone(),
        8,
        1,
    )
    .unwrap();

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        config.drain_acknowledgement_timeout * 4,
        session
            .control_handle()
            .complete_member_drain("drain-operation".to_owned()),
    )
    .await
    .expect("an unacknowledged drain must not poll forever");

    assert!(matches!(
        outcome,
        Err(LogicSessionError::DrainNotAcknowledged)
    ));
    assert!(started.elapsed() >= config.drain_acknowledgement_timeout);
    assert!(started.elapsed() < config.drain_acknowledgement_timeout * 2);
}
