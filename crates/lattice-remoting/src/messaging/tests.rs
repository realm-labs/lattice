use super::codec::*;
use super::error::*;
use super::inbound::*;
use super::outbound::*;
use super::target::*;

use super::*;
use crate::association::{AssociationKey, AssociationState, LaneAttachment, LaneKind};
use crate::config::RemotingConfig;
use crate::protocol::ProtocolDescriptor;
use crate::transport::FramedConnection;
use crate::wire::FrameCodec;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

fn active_association(
    protocol_id: ProtocolId,
    fingerprint: ProtocolFingerprint,
) -> Arc<Association> {
    let key = AssociationKey {
        cluster_id: ClusterId::new("test").unwrap(),
        local_incarnation: NodeIncarnation::new(1).unwrap(),
        remote_address: NodeAddress::new("remote", 25520).unwrap(),
        remote_incarnation: NodeIncarnation::new(2).unwrap(),
    };
    let association = Arc::new(Association::new(key.clone(), RemotingConfig::default()).unwrap());
    for (lane, nonce) in [
        (LaneKind::Control, 1),
        (LaneKind::Interactive, 2),
        (LaneKind::Bulk(0), 3),
    ] {
        association
            .attach(LaneAttachment {
                association_id: association.id(),
                key: key.clone(),
                lane,
                connection_nonce: nonce,
            })
            .unwrap();
    }
    association
        .install_peer_catalogue([ProtocolDescriptor {
            protocol_id,
            fingerprint,
        }])
        .unwrap();
    assert_eq!(association.state(), AssociationState::Active);
    association
}

fn target(protocol_id: ProtocolId) -> ActorRef<()> {
    let node = NodeIncarnation::new(2).unwrap();
    ActorRef::new(
        ClusterId::new("test").unwrap(),
        NodeAddress::new("remote", 25520).unwrap(),
        node,
        ActorPath::user(["user", "target"]).unwrap(),
        ActivationId::new(node, 1).unwrap(),
        protocol_id,
    )
    .unwrap()
}

struct RecordingDispatch {
    activation: ActivationId,
    tells: AtomicUsize,
}

#[async_trait]
impl InboundDispatch for RecordingDispatch {
    async fn tell(
        &self,
        target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        if target.activation_id != self.activation {
            return Err(RemoteMessageError::StaleActivation);
        }
        self.tells.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(())
    }

    async fn ask(
        &self,
        target: ExactActorTarget,
        _message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if target.activation_id != self.activation {
            return Err(RemoteMessageError::StaleActivation);
        }
        if Instant::now() >= deadline {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        Ok(payload)
    }
}

#[tokio::test]
async fn real_tcp_tell_and_ask_dispatch_exact_activation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let protocol_id = ProtocolId::new(7).unwrap();
    let actor_ref = target(protocol_id);
    let dispatch = Arc::new(RecordingDispatch {
        activation: actor_ref.activation_id(),
        tells: AtomicUsize::new(0),
    });
    let server_dispatch = dispatch.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        serve_inbound_connection(
            FramedConnection::new(stream, FrameCodec::new(4096).unwrap()),
            server_dispatch,
            None,
        )
        .await
        .unwrap();
    });
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut client = FramedConnection::new(stream, FrameCodec::new(4096).unwrap());
    let exact = ExactActorTarget::from(&actor_ref);
    client
        .write_frame(&Frame::encode_message(
            FrameKind::Tell,
            &TellWire {
                sender: 9_u128.to_be_bytes().to_vec(),
                target: Some(target_to_wire(&exact)),
                message_id: 1,
                payload: b"tell".to_vec(),
            },
        ))
        .await
        .unwrap();
    let correlation = CorrelationId::new(9, 1).unwrap();
    client
        .write_frame(&Frame::encode_message(
            FrameKind::Ask,
            &AskWire {
                sender: 9_u128.to_be_bytes().to_vec(),
                target: Some(target_to_wire(&exact)),
                correlation_id: correlation.to_bytes().to_vec(),
                timeout_nanos: Duration::from_secs(1).as_nanos() as u64,
                message_id: 2,
                payload: b"ask".to_vec(),
            },
        ))
        .await
        .unwrap();
    let reply = client.read_frame().await.unwrap();
    assert_eq!(
        decode_reply(&reply).unwrap(),
        (correlation, Bytes::from_static(b"ask"))
    );
    client
        .write_frame(&Frame {
            kind: FrameKind::Close,
            payload: Bytes::new(),
        })
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!(dispatch.tells.load(AtomicOrdering::SeqCst), 1);
}

#[tokio::test]
async fn disconnect_result_changes_only_at_socket_write_boundary() {
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"test/v1");
    for (committed, expected) in [
        (false, AskError::AssociationLostBeforeWrite),
        (true, AskError::UnknownResult),
    ] {
        let association = active_association(protocol_id, fingerprint);
        let messaging = Arc::new(OutboundMessaging::new(4).unwrap());
        let task_messaging = messaging.clone();
        let task_association = association.clone();
        let actor_ref = target(protocol_id);
        let task = tokio::spawn(async move {
            task_messaging
                .ask(
                    &task_association,
                    &SenderIdentity::Process(9),
                    &actor_ref,
                    fingerprint,
                    1,
                    Bytes::new(),
                    Instant::now() + Duration::from_secs(5),
                )
                .await
        });
        tokio::task::yield_now().await;
        let correlation = messaging.pending_correlations()[0];
        if committed {
            assert!(messaging.mark_socket_write_started(correlation));
        }
        assert_eq!(messaging.fail_association(association.id()), 1);
        assert_eq!(task.await.unwrap().unwrap_err(), expected);
    }
}

#[tokio::test]
async fn expired_queued_ask_is_dropped_before_socket_write() {
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"test/v1");
    let association = active_association(protocol_id, fingerprint);
    let mut interactive = association.take_receivers().unwrap().interactive;
    let messaging = Arc::new(OutboundMessaging::new(4).unwrap());
    let task_messaging = messaging.clone();
    let task_association = association.clone();
    let actor_ref = target(protocol_id);
    let task = tokio::spawn(async move {
        task_messaging
            .ask(
                &task_association,
                &SenderIdentity::Process(9),
                &actor_ref,
                fingerprint,
                1,
                Bytes::new(),
                Instant::now() + Duration::from_millis(10),
            )
            .await
    });
    let mut frame = interactive.recv().await.unwrap();
    assert_eq!(task.await.unwrap().unwrap_err(), AskError::DeadlineExceeded);
    assert!(!messaging.prepare_ask_for_socket_write(&mut frame));
    assert_eq!(messaging.pending_count(), 0);
}

#[test]
fn one_protocol_mismatch_does_not_close_the_association() {
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"test/v1");
    let association = active_association(protocol_id, fingerprint);
    let messaging = OutboundMessaging::new(4).unwrap();
    let actor_ref = target(protocol_id);
    let mismatch = messaging.tell(
        &association,
        &SenderIdentity::Process(9),
        &actor_ref,
        ProtocolFingerprint::digest(b"other"),
        1,
        Bytes::new(),
    );
    assert!(matches!(
        mismatch,
        Err(TellError::Protocol(
            RemoteMessageError::ProtocolFingerprintMismatch
        ))
    ));
    assert_eq!(association.state(), AssociationState::Active);
    assert!(
        messaging
            .tell(
                &association,
                &SenderIdentity::Process(9),
                &actor_ref,
                fingerprint,
                1,
                Bytes::new(),
            )
            .is_ok()
    );
}
