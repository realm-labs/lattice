use std::io::{Error, ErrorKind};

use tokio::net::TcpListener;

use super::*;
use crate::{association::AssociationState, lane::LaneError, messaging::outbound::OutboundMessage};

#[test]
fn classifies_normal_peer_disconnects_without_hiding_protocol_failures() {
    let disconnected = EndpointError::Lane(LaneError::Wire(WireError::Io(Error::from(
        ErrorKind::UnexpectedEof,
    ))));
    assert!(is_peer_disconnect(&disconnected));
    assert!(!is_peer_disconnect(&EndpointError::WrongDialDirection));
}
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
};

use crate::{
    association::AssociationKey,
    control::{CommandId, ControlDispatchError, ControlGap, RejectControlDispatch},
    messaging::{
        error::RemoteMessageError,
        target::{ExactActorTarget, SenderIdentity},
    },
    protocol::ProtocolFingerprint,
};

struct EchoDispatch;

#[derive(Default)]
struct RecordingControl {
    applied: Mutex<Vec<Bytes>>,
}

#[derive(Default)]
struct RejectInvalidControl {
    rejected: Mutex<bool>,
    applied: Mutex<Vec<Bytes>>,
}

#[derive(Default)]
struct BlockingControl {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[derive(Default)]
struct RecoveringControl {
    old_attempts: std::sync::atomic::AtomicUsize,
    applied: Mutex<Vec<Bytes>>,
}

#[derive(Default)]
struct RetryingWithEphemeralControl {
    retry_started: tokio::sync::Notify,
    ephemeral_applied: tokio::sync::Notify,
}

#[derive(Default)]
struct StreamIsolatingControl {
    blocked_started: tokio::sync::Notify,
    independent_applied: tokio::sync::Notify,
}

#[async_trait]
impl ControlDispatch for RecordingControl {
    async fn apply(
        &self,
        _association: AssociationKey,
        _stream_id: crate::control::ControlStreamId,
        _command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        self.applied
            .lock()
            .expect("recording control poisoned")
            .push(payload);
        Ok(())
    }

    async fn reconcile(
        &self,
        _association: AssociationKey,
        _gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        Ok(())
    }
}

#[async_trait]
impl ControlDispatch for RejectInvalidControl {
    async fn apply(
        &self,
        _association: AssociationKey,
        _stream_id: crate::control::ControlStreamId,
        _command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        if payload == Bytes::from_static(b"invalid") {
            *self.rejected.lock().expect("rejected flag poisoned") = true;
            return Err(ControlDispatchError::InvalidCommand);
        }
        self.applied
            .lock()
            .expect("recording control poisoned")
            .push(payload);
        Ok(())
    }

    async fn reconcile(
        &self,
        _association: AssociationKey,
        _gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        Ok(())
    }
}

#[async_trait]
impl ControlDispatch for BlockingControl {
    async fn apply(
        &self,
        _association: AssociationKey,
        _stream_id: crate::control::ControlStreamId,
        _command_id: CommandId,
        _payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        self.started.notify_waiters();
        self.release.notified().await;
        Ok(())
    }

    async fn reconcile(
        &self,
        _association: AssociationKey,
        _gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        Ok(())
    }
}

#[async_trait]
impl ControlDispatch for RecoveringControl {
    async fn apply(
        &self,
        _association: AssociationKey,
        _stream_id: crate::control::ControlStreamId,
        _command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        if payload == Bytes::from_static(b"term-28-heartbeat") {
            if self
                .old_attempts
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                == 0
            {
                return Err(ControlDispatchError::RetryLater(
                    crate::control::ControlRetryReason::ConsumerBusy,
                ));
            }
            return Err(ControlDispatchError::InvalidCommand);
        }
        self.applied
            .lock()
            .expect("recovering control poisoned")
            .push(payload);
        Ok(())
    }

    async fn reconcile(
        &self,
        _association: AssociationKey,
        _gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        Ok(())
    }
}

#[async_trait]
impl ControlDispatch for RetryingWithEphemeralControl {
    async fn apply(
        &self,
        _association: AssociationKey,
        _stream_id: crate::control::ControlStreamId,
        _command_id: CommandId,
        _payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        self.retry_started.notify_waiters();
        Err(ControlDispatchError::RetryLater(
            crate::control::ControlRetryReason::ConsumerBusy,
        ))
    }

    async fn apply_ephemeral(
        &self,
        _association: AssociationKey,
        _command_id: CommandId,
        _payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        self.ephemeral_applied.notify_waiters();
        Ok(())
    }

    async fn reconcile(
        &self,
        _association: AssociationKey,
        _gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        Ok(())
    }
}

#[async_trait]
impl ControlDispatch for StreamIsolatingControl {
    async fn apply(
        &self,
        _association: AssociationKey,
        stream_id: crate::control::ControlStreamId,
        _command_id: CommandId,
        _payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        if stream_id == crate::control::ControlStreamId::DEFAULT {
            self.blocked_started.notify_waiters();
            return Err(ControlDispatchError::RetryLater(
                crate::control::ControlRetryReason::ConsumerBusy,
            ));
        }
        self.independent_applied.notify_waiters();
        Ok(())
    }

    async fn reconcile(
        &self,
        _association: AssociationKey,
        _gap: Option<ControlGap>,
    ) -> Result<(), ControlDispatchError> {
        Ok(())
    }
}

#[async_trait]
impl InboundDispatch for EchoDispatch {
    async fn tell(
        &self,
        _sender: Option<ActorRef>,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Ok(())
    }

    async fn ask(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if Instant::now() >= deadline {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        Ok(payload)
    }
}

fn endpoint(identity: NodeIdentity, protocol: ProtocolDescriptor) -> Arc<RemotingEndpoint> {
    endpoint_with_control(identity, protocol, Arc::new(RejectControlDispatch))
}

fn endpoint_with_control(
    identity: NodeIdentity,
    protocol: ProtocolDescriptor,
    control: Arc<dyn ControlDispatch>,
) -> Arc<RemotingEndpoint> {
    let config = RemotingConfig {
        heartbeat_interval: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    endpoint_with_config(identity, protocol, control, config)
}

fn endpoint_with_config(
    identity: NodeIdentity,
    protocol: ProtocolDescriptor,
    control: Arc<dyn ControlDispatch>,
    config: RemotingConfig,
) -> Arc<RemotingEndpoint> {
    let manager = Arc::new(
        AssociationManager::new(
            identity.address.clone(),
            identity.incarnation,
            config.clone(),
        )
        .unwrap(),
    );
    Arc::new(
        RemotingEndpoint::builder(
            identity,
            config,
            manager,
            Arc::new(OutboundMessaging::new(32).unwrap()),
            Arc::new(EchoDispatch),
        )
        .control_dispatch(control)
        .catalogue(vec![protocol])
        .build()
        .unwrap(),
    )
}

#[test]
fn classifies_accept_failures_by_recoverability() {
    assert_eq!(
        classify_accept_failure(ErrorKind::ConnectionAborted),
        AcceptRecovery::Immediate
    );
    assert_eq!(
        classify_accept_failure(ErrorKind::Interrupted),
        AcceptRecovery::Immediate
    );
    assert_eq!(
        classify_accept_failure(Error::from_raw_os_error(24).kind()),
        AcceptRecovery::Delayed
    );
    assert_eq!(
        classify_accept_failure(ErrorKind::OutOfMemory),
        AcceptRecovery::Delayed
    );
    assert_eq!(
        classify_accept_failure(ErrorKind::InvalidInput),
        AcceptRecovery::Fatal
    );
}

#[tokio::test]
async fn accept_loop_sheds_connections_at_the_cap_and_keeps_accepting() {
    use tokio::{io::AsyncReadExt, net::TcpStream};

    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_address = probe.local_addr().unwrap();
    drop(probe);
    let config = RemotingConfig {
        max_associations: 1,
        heartbeat_interval: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let limit = config.required_socket_budget() - 1;
    let server_identity = NodeIdentity {
        cluster_id: ClusterId::new("accept-cap-test").unwrap(),
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_address.port()).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(7).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"accept-cap-test/v1"),
    };
    let server = endpoint_with_config(
        server_identity,
        descriptor,
        Arc::new(RejectControlDispatch),
        config,
    );
    server.bind().await.unwrap();

    let mut held = Vec::new();
    for _ in 0..limit {
        held.push(TcpStream::connect(server_address).await.unwrap());
    }
    wait_until(|| server.open_connection_count() == limit).await;

    let mut shed = TcpStream::connect(server_address).await.unwrap();
    wait_until(|| server.shed_connection_count() == 1).await;
    let mut byte = [0_u8; 1];
    assert_eq!(shed.read(&mut byte).await.unwrap(), 0);

    held.pop();
    wait_until(|| server.open_connection_count() == limit - 1).await;
    let _resumed = TcpStream::connect(server_address).await.unwrap();
    wait_until(|| server.open_connection_count() == limit).await;
    assert_eq!(server.shed_connection_count(), 1);
    assert_eq!(server.accept_failure_count(), 0);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_stalled_peer_dial_does_not_block_other_peers() {
    use tokio::net::TcpStream;

    let blackhole = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let blackhole_port = blackhole.local_addr().unwrap().port();
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = probe.local_addr().unwrap().port();
    drop(probe);
    let client_port = server_port.min(blackhole_port).saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("connect-fairness-test").unwrap();
    let identity = |node_id: &str, port: u16, incarnation: u128| NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: node_id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    };
    let client_identity = identity("client", client_port, 1);
    let blackhole_identity = identity("blackhole", blackhole_port, 2);
    let server_identity = identity("server", server_port, 3);
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(7).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"connect-fairness-test/v1"),
    };
    let config = RemotingConfig {
        connect_timeout: Duration::from_secs(2),
        heartbeat_interval: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let client = endpoint_with_config(
        client_identity,
        descriptor.clone(),
        Arc::new(RejectControlDispatch),
        config.clone(),
    );
    let server = endpoint_with_config(
        server_identity.clone(),
        descriptor,
        Arc::new(RejectControlDispatch),
        config,
    );
    server.bind().await.unwrap();
    let stalled = {
        let client = client.clone();
        tokio::spawn(async move { client.connect_peer(blackhole_identity).await })
    };
    let _stalled_socket: (TcpStream, _) = blackhole.accept().await.unwrap();

    let association = tokio::time::timeout(
        Duration::from_millis(750),
        client.connect_peer(server_identity),
    )
    .await
    .expect("a stalled peer dial must not block an unrelated peer")
    .unwrap();

    assert_eq!(association.state(), AssociationState::Active);
    assert!(!stalled.is_finished());
    stalled.abort();
    let _ = stalled.await;
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_association_that_never_activates_is_abandoned_and_releases_its_permit() {
    use crate::transport::negotiate_inbound;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = listener.local_addr().unwrap().port();
    let client_port = server_port.saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("abandon-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
        incarnation: NodeIncarnation::new(2).unwrap(),
    };
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(7).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"abandon-test/v1"),
    };
    let config = RemotingConfig {
        connect_timeout: Duration::from_millis(300),
        establishing_timeout: Duration::from_millis(100),
        reconnect_backoff_min: Duration::from_millis(20),
        reconnect_backoff_max: Duration::from_millis(40),
        heartbeat_interval: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let validator = HandshakeValidator::new(
        server_identity.clone(),
        config.max_frame_size,
        config.bulk_stripes,
    )
    .unwrap();
    let max_frame_size = config.max_frame_size;
    let server_catalogue = vec![descriptor.clone()];
    let control_only_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut connection =
            FramedConnection::new(stream, FrameCodec::new(max_frame_size).unwrap());
        let _ = negotiate_inbound(&mut connection, &validator, &server_catalogue, 8).await;
    });
    let client = endpoint_with_config(
        client_identity,
        descriptor,
        Arc::new(RejectControlDispatch),
        config,
    );

    assert!(client.connect_peer(server_identity.clone()).await.is_err());
    control_only_server.await.unwrap();
    wait_until(|| {
        client
            .associations
            .get_exact(
                &cluster_id,
                &server_identity.address,
                server_identity.incarnation,
            )
            .is_none()
    })
    .await;

    assert_eq!(client.open_connection_count(), 0);
    assert_eq!(client.connect_lock_count(), 0);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_accepting_peer_admits_a_new_association_after_the_dialer_dropped_its_own() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = probe.local_addr().unwrap().port();
    drop(probe);
    let client_port = server_port.saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("stale-association-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
        incarnation: NodeIncarnation::new(2).unwrap(),
    };
    assert!(
        (&client_identity.address, client_identity.incarnation.get())
            < (&server_identity.address, server_identity.incarnation.get())
    );
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(7).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"stale-association-test/v1"),
    };
    let config = RemotingConfig {
        connect_timeout: Duration::from_secs(2),
        establishing_timeout: Duration::from_secs(5),
        reconnect_backoff_min: Duration::from_millis(20),
        reconnect_backoff_max: Duration::from_millis(40),
        heartbeat_interval: Duration::from_millis(100),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let client = endpoint_with_config(
        client_identity.clone(),
        descriptor.clone(),
        Arc::new(RejectControlDispatch),
        config.clone(),
    );
    let server = endpoint_with_config(
        server_identity.clone(),
        descriptor,
        Arc::new(RejectControlDispatch),
        config,
    );
    server.bind().await.unwrap();
    let stale = client.connect_peer(server_identity.clone()).await.unwrap();
    assert_eq!(stale.state(), AssociationState::Active);
    wait_until(|| server.associations.len() == 1).await;

    stale.begin_close();
    stale.finish_close();
    assert!(client.associations.remove(stale.key(), stale.id()));
    wait_until(|| {
        let _ = client.disconnect_association(stale.id());
        server.associations.attached_lane_count() == 0
    })
    .await;
    let accepted_id = || {
        server
            .associations
            .get_exact(
                &cluster_id,
                &client_identity.address,
                client_identity.incarnation,
            )
            .map(|association| association.id())
    };
    assert_eq!(accepted_id(), Some(stale.id()));

    let reconnected = client.connect_peer(server_identity).await.unwrap();

    assert_ne!(reconnected.id(), stale.id());
    assert_eq!(reconnected.state(), AssociationState::Active);
    assert_eq!(accepted_id(), Some(reconnected.id()));
    assert_eq!(server.associations.len(), 1);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

/// A peer that is frozen or blackholed and then recovers keeps its `NodeIncarnation`, so
/// the association key is byte-identical on both sides and the accepting node can only
/// tell the rejoin apart by the `AssociationId`. Meanwhile every dial the peer made while
/// it was unreachable is still sitting in the accept backlog, so the recovering node sees
/// a burst of overlapping inbound connections for one lane. Losing one of those must not
/// leave the association looking permanently live, or the rejoin is fenced forever.
#[tokio::test]
async fn backlogged_duplicate_dials_do_not_fence_a_same_incarnation_rejoin() {
    use crate::transport::negotiate_outbound;
    use tokio::net::TcpStream;

    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = probe.local_addr().unwrap().port();
    drop(probe);
    let client_port = server_port.saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("frozen-peer-rejoin-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
        incarnation: NodeIncarnation::new(2).unwrap(),
    };
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(7).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"frozen-peer-rejoin-test/v1"),
    };
    let config = RemotingConfig {
        heartbeat_interval: Duration::from_secs(30),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let server = endpoint_with_config(
        server_identity.clone(),
        descriptor.clone(),
        Arc::new(RejectControlDispatch),
        config.clone(),
    );
    server.bind().await.unwrap();

    let dial_control_lane = |association_id: AssociationId, connection_nonce: u128| {
        let client_identity = client_identity.clone();
        let server_identity = server_identity.clone();
        let descriptor = descriptor.clone();
        let max_frame_size = config.max_frame_size;
        async move {
            let stream = TcpStream::connect(("127.0.0.1", server_port))
                .await
                .unwrap();
            let mut connection =
                FramedConnection::new(stream, FrameCodec::new(max_frame_size).unwrap());
            negotiate_outbound(
                &mut connection,
                &Handshake {
                    source: client_identity,
                    expected_remote: server_identity,
                    association_id,
                    lane: LaneKind::Control,
                    connection_nonce,
                    maximum_frame_size: max_frame_size,
                    features: FeatureBits::REQUIRED_V3,
                },
                &[descriptor],
                8,
            )
            .await
            .unwrap();
            connection
        }
    };
    let accepted_id = || {
        server
            .associations
            .get_exact(
                &cluster_id,
                &client_identity.address,
                client_identity.incarnation,
            )
            .map(|association| association.id())
    };

    // The generation the peer owned before it went away, plus the backlogged retries it
    // issued while unreachable. Descending nonces make every retry win the duplicate
    // tie-break against the connection that is actually running the lane.
    let frozen_id = AssociationId::generate();
    let running = dial_control_lane(frozen_id, 1_000).await;
    wait_until(|| server.associations.attached_lane_count() == 1).await;
    let mut backlog = Vec::new();
    for nonce in (1..=3).rev() {
        backlog.push(dial_control_lane(frozen_id, nonce).await);
    }
    wait_until(|| accepted_id() == Some(frozen_id)).await;

    // The peer gave up on that generation: it dropped the association and every socket.
    drop(backlog);
    drop(running);
    let stale = server
        .associations
        .get_exact(
            &cluster_id,
            &client_identity.address,
            client_identity.incarnation,
        )
        .unwrap();
    wait_until(|| !stale.has_live_connection()).await;
    assert_eq!(stale.attached_lane_count(), 0);

    // It rejoins under the same incarnation with a fresh association generation.
    let rejoined_id = AssociationId::generate();
    let _rejoined = dial_control_lane(rejoined_id, 500).await;

    wait_until(|| accepted_id() == Some(rejoined_id)).await;
    assert_eq!(server.associations.len(), 1);
    assert_eq!(stale.state(), AssociationState::Closed);
    server.shutdown().await.unwrap();
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("endpoint did not reach the expected accept state");
}

#[tokio::test]
async fn real_tcp_endpoint_establishes_all_lanes_and_delivers_ask() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = probe.local_addr().unwrap().port();
    drop(probe);
    let client_port = server_port.saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("endpoint-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
        incarnation: NodeIncarnation::new(2).unwrap(),
    };
    assert!(
        (&client_identity.address, client_identity.incarnation.get())
            < (&server_identity.address, server_identity.incarnation.get())
    );
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"endpoint-test/v1");
    let descriptor = ProtocolDescriptor {
        protocol_id,
        fingerprint,
    };
    let control = Arc::new(RecordingControl::default());
    let client = endpoint(client_identity.clone(), descriptor.clone());
    let server =
        endpoint_with_control(server_identity.clone(), descriptor.clone(), control.clone());
    server.bind().await.unwrap();
    let association = client.connect_peer(server_identity.clone()).await.unwrap();
    assert_eq!(association.state(), AssociationState::Active);
    let target = ActorRef::new(
        cluster_id,
        server_identity.address.clone(),
        server_identity.incarnation,
        ActorPath::user(["user", "echo"]).unwrap(),
        ActivationId::new(server_identity.incarnation, 1).unwrap(),
        protocol_id,
    )
    .unwrap();
    let reply = client
        .messaging
        .ask(
            &association,
            &SenderIdentity::Process(9),
            &target,
            OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"hello")),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(reply, Bytes::from_static(b"hello"));
    association
        .admit_control_command(Bytes::from_static(b"before-reconnect"))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while association.control_outbox_len() != 0
            || control
                .applied
                .lock()
                .expect("recording control poisoned")
                .len()
                != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.disconnect_association(association.id()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while association.state() != AssociationState::Reconnecting {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while association.state() != AssociationState::Active {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let reply = client
        .messaging
        .ask(
            &association,
            &SenderIdentity::Process(9),
            &target,
            OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"again")),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(reply, Bytes::from_static(b"again"));
    association
        .admit_control_command(Bytes::from_static(b"after-reconnect"))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while association.control_outbox_len() != 0
            || control
                .applied
                .lock()
                .expect("recording control poisoned")
                .len()
                != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

mod reliable_control;
