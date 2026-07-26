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

#[async_trait]
impl ControlDispatch for RecordingControl {
    async fn apply(
        &self,
        _association: AssociationKey,
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
        _command_id: CommandId,
        payload: Bytes,
    ) -> Result<(), ControlDispatchError> {
        if payload == Bytes::from_static(b"term-28-heartbeat") {
            if self
                .old_attempts
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                == 0
            {
                return Err(ControlDispatchError::Unavailable);
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
