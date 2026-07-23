use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use tokio::net::{TcpListener, TcpStream};

use super::{codec::*, error::*, inbound::*, outbound::*, target::*, *};
use super::{target_cache::ExactTargetCache, target_dictionary::ExactTargetDictionary};
use crate::{
    association::{AssociationKey, AssociationState, LaneAttachment, LaneKind},
    config::RemotingConfig,
    protocol::ProtocolDescriptor,
    transport::FramedConnection,
    wire::FrameCodec,
};

#[test]
fn actor_panicked_maps_to_the_dedicated_remote_failure_code() {
    assert_eq!(
        failure_code(&RemoteMessageError::ActorPanicked),
        RemoteFailureCode::ActorPanicked
    );
}

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

fn target(protocol_id: ProtocolId) -> ActorRef {
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
        _sender: Option<ActorRef>,
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
        if payload == Bytes::from_static(b"panic") {
            return Err(RemoteMessageError::ActorPanicked);
        }
        Ok(payload)
    }
}

#[tokio::test]
async fn real_tcp_tell_and_ask_dispatch_exact_activation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
    let stream = TcpStream::connect(address).await.unwrap();
    let mut client = FramedConnection::new(stream, FrameCodec::new(4096).unwrap());
    client
        .write_frame(&Frame::encode_message(
            FrameKind::Tell,
            &TellWire {
                target: Some(target_to_wire(&actor_ref)),
                message_id: 1,
                payload: Bytes::from_static(b"tell"),
                sender_actor: None,
                target_id: 0,
            },
        ))
        .await
        .unwrap();
    let correlation = CorrelationId::new(9, 1).unwrap();
    client
        .write_frame(&Frame::encode_message(
            FrameKind::Ask,
            &AskWire {
                target: Some(target_to_wire(&actor_ref)),
                correlation_id: Bytes::copy_from_slice(&correlation.to_bytes()),
                timeout_nanos: Duration::from_secs(1).as_nanos() as u64,
                message_id: 2,
                payload: Bytes::from_static(b"ask"),
            },
        ))
        .await
        .unwrap();
    let reply = client.read_frame().await.unwrap();
    assert_eq!(
        decode_reply(&reply).unwrap(),
        (correlation, Bytes::from_static(b"ask"))
    );
    let panic_correlation = CorrelationId::new(9, 2).unwrap();
    client
        .write_frame(&Frame::encode_message(
            FrameKind::Ask,
            &AskWire {
                target: Some(target_to_wire(&actor_ref)),
                correlation_id: Bytes::copy_from_slice(&panic_correlation.to_bytes()),
                timeout_nanos: Duration::from_secs(1).as_nanos() as u64,
                message_id: 2,
                payload: Bytes::from_static(b"panic"),
            },
        ))
        .await
        .unwrap();
    let failure = decode_failure(&client.read_frame().await.unwrap()).unwrap();
    assert_eq!(failure.correlation_id, panic_correlation);
    assert_eq!(failure.code, RemoteFailureCode::ActorPanicked);
    client
        .write_frame(&Frame::new(FrameKind::Close, Bytes::new()))
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!(dispatch.tells.load(AtomicOrdering::SeqCst), 1);
}

#[tokio::test]
async fn outbound_tell_preserves_an_exact_actor_sender() {
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"test/v1");
    let association = active_association(protocol_id, fingerprint);
    let mut receivers = association.take_receivers().unwrap();
    let messaging = OutboundMessaging::new(4).unwrap();
    let recipient = target(protocol_id);
    let sender: ActorRef = ActorRef::new(
        ClusterId::new("test").unwrap(),
        NodeAddress::new("sender", 25521).unwrap(),
        NodeIncarnation::new(3).unwrap(),
        ActorPath::user(["user", "sender"]).unwrap(),
        ActivationId::new(NodeIncarnation::new(3).unwrap(), 9).unwrap(),
        protocol_id,
    )
    .unwrap();
    let sender_identity = SenderIdentity::from(&sender);
    let stripe = messaging
        .tell(
            &association,
            &sender_identity,
            &recipient,
            OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"tell")),
        )
        .unwrap();

    let frame = receivers.bulk[stripe].recv().await.unwrap();
    let decoded = decode_tell(&frame).unwrap();
    assert!(
        decoded
            .sender
            .as_ref()
            .is_some_and(|actual| actual.same_activation(&sender))
    );
}

#[tokio::test]
async fn prepared_exact_tell_preserves_sender_and_is_bound_to_association() {
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"test/v1");
    let association = active_association(protocol_id, fingerprint);
    let mut receivers = association.take_receivers().unwrap();
    let messaging = OutboundMessaging::new(4).unwrap();
    let recipient = target(protocol_id);
    let sender: ActorRef = ActorRef::new(
        ClusterId::new("test").unwrap(),
        NodeAddress::new("sender", 25521).unwrap(),
        NodeIncarnation::new(3).unwrap(),
        ActorPath::user(["user", "sender"]).unwrap(),
        ActivationId::new(NodeIncarnation::new(3).unwrap(), 9).unwrap(),
        protocol_id,
    )
    .unwrap();
    let route = messaging
        .prepare_exact_tell_route(
            association.clone(),
            &SenderIdentity::from(&sender),
            &recipient,
            fingerprint,
        )
        .unwrap();

    let stripe = route.tell(1, Bytes::from_static(b"tell")).unwrap();
    route.tell(1, Bytes::from_static(b"tell")).unwrap();
    let registration = receivers.bulk[stripe].recv().await.unwrap();
    let compact = receivers.bulk[stripe].recv().await.unwrap();
    assert!(compact.payload_len() < registration.payload_len());
    let mut cache = ExactTargetCache::new(8);
    let mut dictionary = ExactTargetDictionary::new();
    let decoded = decode_tell_cached(&registration, &mut cache, &mut dictionary).unwrap();
    let compact_decoded = decode_tell_cached(&compact, &mut cache, &mut dictionary).unwrap();
    let decoded_target: ActorRef = decoded.target.actor_ref().unwrap();
    let compact_target: ActorRef = compact_decoded.target.actor_ref().unwrap();
    assert!(decoded_target.same_activation(&recipient));
    assert!(compact_target.same_activation(&recipient));
    assert!(
        decoded
            .sender
            .as_ref()
            .is_some_and(|actual| actual.same_activation(&sender))
    );

    association.begin_close();
    assert!(matches!(
        route.tell(2, Bytes::new()),
        Err(TellError::Association(AssociationError::NotActive))
    ));
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
                    OutboundMessage::new(fingerprint, 1, Bytes::new()),
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
                OutboundMessage::new(fingerprint, 1, Bytes::new()),
                Instant::now() + Duration::from_millis(10),
            )
            .await
    });
    let mut frame = interactive.recv().await.unwrap();
    assert_eq!(task.await.unwrap().unwrap_err(), AskError::DeadlineExceeded);
    assert!(!messaging.prepare_ask_for_socket_write(&mut frame));
    assert_eq!(messaging.pending_count(), 0);
}

#[tokio::test]
async fn cancelling_an_ask_removes_it_from_the_shared_deadline_driver() {
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"test/v1");
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
                OutboundMessage::new(fingerprint, 1, Bytes::new()),
                Instant::now() + Duration::from_secs(30),
            )
            .await
    });
    tokio::task::yield_now().await;
    assert_eq!(messaging.pending_count(), 1);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(messaging.pending_count(), 0);
}

#[tokio::test]
async fn an_earlier_ask_wakes_the_shared_deadline_driver() {
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"test/v1");
    let association = active_association(protocol_id, fingerprint);
    let messaging = Arc::new(OutboundMessaging::new(4).unwrap());
    let long_messaging = messaging.clone();
    let long_association = association.clone();
    let long_target = target(protocol_id);
    let long = tokio::spawn(async move {
        long_messaging
            .ask(
                &long_association,
                &SenderIdentity::Process(9),
                &long_target,
                OutboundMessage::new(fingerprint, 1, Bytes::new()),
                Instant::now() + Duration::from_secs(30),
            )
            .await
    });
    tokio::task::yield_now().await;

    let short_messaging = messaging.clone();
    let short_association = association.clone();
    let short_target = target(protocol_id);
    let short = tokio::spawn(async move {
        short_messaging
            .ask(
                &short_association,
                &SenderIdentity::Process(9),
                &short_target,
                OutboundMessage::new(fingerprint, 2, Bytes::new()),
                Instant::now() + Duration::from_millis(10),
            )
            .await
    });

    let result = tokio::time::timeout(Duration::from_millis(200), short)
        .await
        .expect("short deadline driver wake timed out")
        .unwrap();
    assert_eq!(result.unwrap_err(), AskError::DeadlineExceeded);
    long.abort();
    assert!(long.await.unwrap_err().is_cancelled());
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
        OutboundMessage::new(ProtocolFingerprint::digest(b"other"), 1, Bytes::new()),
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
                OutboundMessage::new(fingerprint, 1, Bytes::new()),
            )
            .is_ok()
    );
}
