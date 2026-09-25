use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use lattice_coordination::{
    cluster_session::ClusterSession,
    control::{
        PlacementControlCommand, PlacementControlRouter, control_stream_id, decode_control_command,
        encode_control_command_for_term,
    },
    coordinator::{ActorGroupHello, MemberHello},
    session::{GroupSession, GroupSessionConfig, GroupSessionError},
    types::NodeKey,
};
use lattice_model::{
    cluster::CoordinatorScope,
    cluster::{ActorGroupId, ClusterId, NodeEndpoint, NodeIncarnation},
};
use lattice_remoting::{
    association::{Association, AssociationManager},
    config::RemotingConfig,
    control::{ControlAck, ControlDispatch, decode_control_envelope},
};
use tokio::sync::watch;

fn setup() -> (NodeKey, Arc<AssociationManager>, Arc<Association>) {
    let node = NodeKey {
        node_id: "departing".to_owned(),
        address: NodeEndpoint::new("127.0.0.1", 34501).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let associations = Arc::new(
        AssociationManager::new(
            node.address.clone(),
            node.incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let association = associations
        .get_or_create(
            ClusterId::new("drain-commit").unwrap(),
            NodeEndpoint::new("127.0.0.1", 34502).unwrap(),
            NodeIncarnation::new(2).unwrap(),
        )
        .unwrap();
    (node, associations, association)
}

fn hello(node: NodeKey) -> MemberHello {
    MemberHello {
        node,
        roles: BTreeSet::new(),
        failure_domains: BTreeMap::new(),
        protocols: Vec::new(),
        remoting_capabilities: BTreeSet::new(),
    }
}

fn acknowledge(association: &Association) {
    let frame = association
        .replay_control_frames()
        .pop()
        .expect("drain request queued");
    let envelope = decode_control_envelope(&frame).unwrap();
    association
        .acknowledge_control(ControlAck {
            association_epoch: envelope.association_epoch,
            stream_id: envelope.stream_id,
            cumulative_sequence: envelope.sequence,
        })
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn membership_drain_transport_ack_is_not_a_commit_confirmation() {
    let (node, associations, association) = setup();
    let (_session, handle, _effects) = ClusterSession::new(
        hello(node),
        association.key().clone(),
        associations,
        GroupSessionConfig::default(),
        8,
        1,
    )
    .unwrap();
    let mut operation = Box::pin(handle.complete_drain("leave-1".to_owned()));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), operation.as_mut())
            .await
            .is_err()
    );
    acknowledge(&association);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), operation.as_mut())
            .await
            .is_err(),
        "a rejected command receives the same transport ACK as a committed drain"
    );
}

#[tokio::test(start_paused = true)]
async fn placement_drain_transport_ack_is_not_a_commit_confirmation() {
    let (node, associations, association) = setup();
    let (session, _effects) = GroupSession::new(
        ActorGroupHello::builder(node, ActorGroupId::new("group").unwrap(), 1).build(),
        association.key().clone(),
        associations,
        GroupSessionConfig::default(),
        8,
        1,
    )
    .unwrap();
    let handle = session.control_handle();
    let mut operation = Box::pin(handle.complete_member_drain("leave-1".to_owned()));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), operation.as_mut())
            .await
            .is_err()
    );
    acknowledge(&association);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), operation.as_mut())
            .await
            .is_err(),
        "a rejected command receives the same transport ACK as a committed drain"
    );
}

#[tokio::test(start_paused = true)]
async fn membership_drain_wait_obeys_its_configured_timeout() {
    let (node, associations, association) = setup();
    let config = GroupSessionConfig {
        drain_acknowledgement_timeout: Duration::from_secs(2),
        ..GroupSessionConfig::default()
    };
    let (_session, handle, _effects) = ClusterSession::new(
        hello(node),
        association.key().clone(),
        associations,
        config,
        8,
        1,
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        handle.complete_drain("leave-1".to_owned()),
    )
    .await
    .expect("membership drain must be bounded");
    assert!(matches!(
        result,
        Err(GroupSessionError::DrainNotAcknowledged)
    ));
}

#[tokio::test]
async fn placement_redrive_resends_completion_after_its_transport_ack() {
    let (node, associations, association) = setup();
    let (session, _effects) = GroupSession::new(
        ActorGroupHello::builder(node, ActorGroupId::new("group").unwrap(), 1).build(),
        association.key().clone(),
        associations,
        GroupSessionConfig::default(),
        8,
        1,
    )
    .unwrap();
    let handle = session.control_handle();
    handle.request_member_drain("leave-1").await.unwrap();
    acknowledge(&association);
    handle.begin_drain("leave-1".to_owned()).unwrap();
    let frame = association.replay_control_frames().pop().unwrap();
    let envelope = decode_control_envelope(&frame).unwrap();
    let command = decode_control_command(&envelope.payload, 16 * 1024).unwrap();
    assert!(matches!(command.command,
        PlacementControlCommand::DrainComplete { operation_id, .. } if operation_id == "leave-1"));
}

async fn confirm(
    sender: &PlacementControlRouter,
    association: &Association,
    scope: CoordinatorScope,
    operation_id: &str,
) {
    let payload = encode_control_command_for_term(
        &scope,
        1,
        &PlacementControlCommand::DrainCommitted {
            operation_id: operation_id.to_owned(),
            expected_incarnation: NodeIncarnation::new(1).unwrap(),
        },
        16 * 1024,
    )
    .unwrap();
    sender
        .apply(
            association.key().clone(),
            control_stream_id(&scope),
            lattice_remoting::control::CommandId::generate(),
            payload,
        )
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn membership_requires_and_accepts_its_application_commit_response() {
    let (node, associations, association) = setup();
    let (session, handle, _effects) = ClusterSession::new(
        hello(node),
        association.key().clone(),
        associations,
        GroupSessionConfig::default(),
        8,
        1,
    )
    .unwrap();
    let (controls, receiver) = PlacementControlRouter::bounded(8, 16 * 1024).unwrap();
    let (shutdown, stopped) = watch::channel(false);
    let task = tokio::spawn(session.run_recoverable(receiver, stopped));
    let mut operation = Box::pin(handle.complete_drain("leave-1".to_owned()));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), operation.as_mut())
            .await
            .is_err()
    );
    confirm(
        &controls,
        &association,
        CoordinatorScope::Cluster,
        "leave-1",
    )
    .await;
    operation.await.unwrap();
    shutdown.send(true).unwrap();
    task.await.unwrap().0.unwrap();
    assert_eq!(handle.committed_drain().as_deref(), Some("leave-1"));
    handle.complete_drain("leave-1".to_owned()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn placement_requires_and_accepts_its_application_commit_response() {
    let (node, associations, association) = setup();
    let group = ActorGroupId::new("group").unwrap();
    let (session, mut effects) = GroupSession::new(
        ActorGroupHello::builder(node, group.clone(), 1).build(),
        association.key().clone(),
        associations,
        GroupSessionConfig::default(),
        8,
        1,
    )
    .unwrap();
    let handle = session.control_handle();
    let (controls, receiver) = PlacementControlRouter::bounded(8, 16 * 1024).unwrap();
    let (shutdown, stopped) = watch::channel(false);
    let task = tokio::spawn(session.run_recoverable(receiver, stopped));
    let mut operation = Box::pin(handle.complete_member_drain("leave-1".to_owned()));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), operation.as_mut())
            .await
            .is_err()
    );
    confirm(
        &controls,
        &association,
        CoordinatorScope::Group(group),
        "leave-1",
    )
    .await;
    operation.await.unwrap();
    assert!(
        matches!(effects.recv().await, Some(lattice_coordination::session::LogicPlacementEffect::DrainCommitted { operation_id, .. }) if operation_id == "leave-1")
    );
    shutdown.send(true).unwrap();
    task.await.unwrap().0.unwrap();
    handle
        .complete_member_drain("leave-1".to_owned())
        .await
        .unwrap();
}
