use async_trait::async_trait;
use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
};
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::{
    association::{AssociationKey, LaneAttachment},
    config::RemotingConfig,
    control::RejectControlDispatch,
    messaging::{
        codec::reply_frame,
        error::RemoteMessageError,
        outbound::OutboundMessage,
        target::{CorrelationId, ExactActorTarget},
    },
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
    transport::FramedConnection,
};

struct EchoDispatch {
    delay: Duration,
}

#[derive(Default)]
struct RecordingDispatch {
    tells: std::sync::Mutex<Vec<ExactActorTarget>>,
}

#[async_trait]
impl InboundDispatch for RecordingDispatch {
    async fn tell(
        &self,
        target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        self.tells
            .lock()
            .expect("recording dispatch poisoned")
            .push(target);
        Ok(())
    }

    async fn ask(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Ok(payload)
    }
}

#[async_trait]
impl InboundDispatch for EchoDispatch {
    async fn tell(
        &self,
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
        tokio::time::sleep(self.delay).await;
        if Instant::now() >= deadline {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        Ok(payload)
    }
}

fn active_association(
    local: NodeIncarnation,
    remote: NodeIncarnation,
    remote_address: NodeAddress,
    protocol_id: ProtocolId,
    fingerprint: ProtocolFingerprint,
) -> Arc<Association> {
    let key = AssociationKey {
        cluster_id: ClusterId::new("lane-test").unwrap(),
        local_incarnation: local,
        remote_address,
        remote_incarnation: remote,
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
    association
}

#[tokio::test]
async fn interactive_lane_stays_awake_while_ask_is_in_flight() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = listener.local_addr().unwrap();
    let client_incarnation = NodeIncarnation::new(1).unwrap();
    let server_incarnation = NodeIncarnation::new(2).unwrap();
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
    let server_address = NodeAddress::new("127.0.0.1", socket.port()).unwrap();
    let client_address = NodeAddress::new("127.0.0.1", 25549).unwrap();
    let client_association = active_association(
        client_incarnation,
        server_incarnation,
        server_address.clone(),
        protocol_id,
        fingerprint,
    );
    let server_association = active_association(
        server_incarnation,
        client_incarnation,
        client_address,
        protocol_id,
        fingerprint,
    );
    let mut client_receiver = client_association.take_receivers().unwrap().interactive;
    let mut server_receiver = server_association.take_receivers().unwrap().interactive;
    let client_messaging = Arc::new(OutboundMessaging::new(8).unwrap());
    let server_messaging = Arc::new(OutboundMessaging::new(8).unwrap());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server_shutdown = shutdown_rx.clone();
    let server_lane = {
        let association = server_association.clone();
        let messaging = server_messaging.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            BidirectionalLane::new(
                association,
                LaneKind::Interactive,
                2,
                LaneServices::new(
                    messaging,
                    Arc::new(EchoDispatch {
                        delay: Duration::from_millis(125),
                    }),
                    Arc::new(RejectControlDispatch),
                ),
                BidirectionalLaneConfig {
                    maximum_frame_size: 4096,
                    maximum_concurrent_inbound_asks: 8,
                    heartbeat_interval: Duration::from_millis(100),
                    heartbeat_miss_limit: 10,
                    control_apply_retry_timeout: Duration::from_secs(30),
                    idle_data_connection_timeout: Duration::from_millis(25),
                    maximum_cached_exact_targets: 8,
                    socket_read_ahead_bytes: 1024,
                    maximum_ready_write_batch_frames: 8,
                    maximum_ready_read_batch_frames: 8,
                    maximum_coalesced_write_batch_bytes: 4096,
                    maximum_pending_control_applies: 8,
                },
            )
            .run(&mut server_receiver, stream, &mut server_shutdown)
            .await
        })
    };
    let stream = TcpStream::connect(socket).await.unwrap();
    let mut client_shutdown = shutdown_rx;
    let client_lane = {
        let association = client_association.clone();
        let messaging = client_messaging.clone();
        tokio::spawn(async move {
            BidirectionalLane::new(
                association,
                LaneKind::Interactive,
                2,
                LaneServices::new(
                    messaging,
                    Arc::new(EchoDispatch {
                        delay: Duration::from_millis(125),
                    }),
                    Arc::new(RejectControlDispatch),
                ),
                BidirectionalLaneConfig {
                    maximum_frame_size: 4096,
                    maximum_concurrent_inbound_asks: 8,
                    heartbeat_interval: Duration::from_millis(100),
                    heartbeat_miss_limit: 10,
                    control_apply_retry_timeout: Duration::from_secs(30),
                    idle_data_connection_timeout: Duration::from_millis(25),
                    maximum_cached_exact_targets: 8,
                    socket_read_ahead_bytes: 1024,
                    maximum_ready_write_batch_frames: 8,
                    maximum_ready_read_batch_frames: 8,
                    maximum_coalesced_write_batch_bytes: 4096,
                    maximum_pending_control_applies: 8,
                },
            )
            .run(&mut client_receiver, stream, &mut client_shutdown)
            .await
        })
    };
    let target = ActorRef::new(
        ClusterId::new("lane-test").unwrap(),
        server_address,
        server_incarnation,
        ActorPath::user(["user", "echo"]).unwrap(),
        ActivationId::new(server_incarnation, 1).unwrap(),
        protocol_id,
    )
    .unwrap();
    let mut pending = JoinSet::new();
    for index in 0_u8..8 {
        let messaging = client_messaging.clone();
        let association = client_association.clone();
        let target = target.clone();
        pending.spawn(async move {
            let expected = Bytes::from(vec![index]);
            let reply = messaging
                .ask(
                    &association,
                    &target,
                    OutboundMessage::new(fingerprint, u64::from(index) + 1, expected.clone()),
                    Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap();
            (reply, expected)
        });
    }
    while let Some(completed) = pending.join_next().await {
        let (reply, expected) = completed.unwrap();
        assert_eq!(reply, expected);
    }
    shutdown_tx.send(true).unwrap();
    assert_eq!(client_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
    assert_eq!(server_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
}

fn duplex_lane_config() -> BidirectionalLaneConfig {
    BidirectionalLaneConfig {
        maximum_frame_size: 4096,
        maximum_concurrent_inbound_asks: 8,
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_miss_limit: 2,
        control_apply_retry_timeout: Duration::from_secs(2),
        idle_data_connection_timeout: Duration::from_secs(5),
        maximum_cached_exact_targets: 8,
        socket_read_ahead_bytes: 1024,
        maximum_ready_write_batch_frames: 8,
        maximum_ready_read_batch_frames: 8,
        maximum_coalesced_write_batch_bytes: 4096,
        maximum_pending_control_applies: 8,
    }
}

fn lane_services(
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
) -> LaneServices {
    LaneServices::new(messaging, dispatch, Arc::new(RejectControlDispatch))
}

fn lane_target(
    address: &NodeAddress,
    incarnation: NodeIncarnation,
    protocol_id: ProtocolId,
) -> ActorRef {
    ActorRef::new(
        ClusterId::new("lane-test").unwrap(),
        address.clone(),
        incarnation,
        ActorPath::user(["user", "echo"]).unwrap(),
        ActivationId::new(incarnation, 1).unwrap(),
        protocol_id,
    )
    .unwrap()
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("lane did not reach the expected state");
}

#[tokio::test]
async fn a_bulk_stripe_failure_keeps_interactive_asks_pending() {
    let client_incarnation = NodeIncarnation::new(1).unwrap();
    let server_incarnation = NodeIncarnation::new(2).unwrap();
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
    let server_address = NodeAddress::new("127.0.0.1", 25551).unwrap();
    let association = active_association(
        client_incarnation,
        server_incarnation,
        server_address.clone(),
        protocol_id,
        fingerprint,
    );
    let messaging = Arc::new(OutboundMessaging::new(8).unwrap());
    let target = lane_target(&server_address, server_incarnation, protocol_id);
    let ask = {
        let messaging = messaging.clone();
        let association = association.clone();
        tokio::spawn(async move {
            messaging
                .ask(
                    &association,
                    &target,
                    OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"ask")),
                    Instant::now() + Duration::from_secs(5),
                )
                .await
        })
    };
    wait_for(|| messaging.pending_count() == 1).await;
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let mut bulk = association.take_lane_receiver(LaneKind::Bulk(0)).unwrap();
    let (peer, lane_io) = tokio::io::duplex(1024);
    drop(peer);
    let bulk_result = BidirectionalLane::new(
        association.clone(),
        LaneKind::Bulk(0),
        3,
        lane_services(messaging.clone(), Arc::new(RecordingDispatch::default())),
        duplex_lane_config(),
    )
    .run(&mut bulk, lane_io, &mut shutdown_rx)
    .await;

    assert!(bulk_result.is_err());
    assert_eq!(messaging.pending_count(), 1);
    assert!(!ask.is_finished());

    let mut interactive = association
        .take_lane_receiver(LaneKind::Interactive)
        .unwrap();
    let (peer, lane_io) = tokio::io::duplex(1024);
    drop(peer);
    let interactive_result = BidirectionalLane::new(
        association.clone(),
        LaneKind::Interactive,
        2,
        lane_services(messaging.clone(), Arc::new(RecordingDispatch::default())),
        duplex_lane_config(),
    )
    .run(&mut interactive, lane_io, &mut shutdown_rx)
    .await;

    assert!(interactive_result.is_err());
    assert_eq!(messaging.pending_count(), 0);
    assert_eq!(
        ask.await.unwrap().unwrap_err(),
        AskError::AssociationLostBeforeWrite
    );
}

#[tokio::test]
async fn a_blocked_control_write_fails_within_the_heartbeat_window() {
    let association = active_association(
        NodeIncarnation::new(1).unwrap(),
        NodeIncarnation::new(2).unwrap(),
        NodeAddress::new("127.0.0.1", 25554).unwrap(),
        ProtocolId::new(7).unwrap(),
        ProtocolFingerprint::digest(b"lane-test/v1"),
    );
    let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
    let (_peer, lane_io) = tokio::io::duplex(16);
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        BidirectionalLane::new(
            association.clone(),
            LaneKind::Control,
            1,
            lane_services(
                Arc::new(OutboundMessaging::new(8).unwrap()),
                Arc::new(RecordingDispatch::default()),
            ),
            duplex_lane_config(),
        )
        .run(&mut control, lane_io, &mut shutdown_rx),
    )
    .await
    .expect("a control lane write must not outlive its heartbeat window");

    assert!(matches!(result, Err(LaneError::WriteTimeout)));
}

#[tokio::test]
async fn a_queued_compact_tell_is_expanded_after_the_stripe_reconnects() {
    let client_incarnation = NodeIncarnation::new(1).unwrap();
    let server_incarnation = NodeIncarnation::new(2).unwrap();
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
    let server_address = NodeAddress::new("127.0.0.1", 25555).unwrap();
    let client_address = NodeAddress::new("127.0.0.1", 25556).unwrap();
    let client_association = active_association(
        client_incarnation,
        server_incarnation,
        server_address.clone(),
        protocol_id,
        fingerprint,
    );
    let server_association = active_association(
        server_incarnation,
        client_incarnation,
        client_address,
        protocol_id,
        fingerprint,
    );
    let messaging = Arc::new(OutboundMessaging::new(8).unwrap());
    let target = lane_target(&server_address, server_incarnation, protocol_id);
    let route = messaging
        .prepare_exact_tell_route(client_association.clone(), &target, fingerprint)
        .unwrap();
    let mut client_bulk = client_association
        .take_lane_receiver(LaneKind::Bulk(0))
        .unwrap();
    route.tell(1, Bytes::from_static(b"registration")).unwrap();
    let registration = client_bulk.recv().await.unwrap();
    route.tell(1, Bytes::from_static(b"compact")).unwrap();
    client_association.release_queued_bytes(registration.payload_len());

    client_association.detach(LaneKind::Bulk(0), 3);
    client_association
        .attach(LaneAttachment {
            association_id: client_association.id(),
            key: client_association.key().clone(),
            lane: LaneKind::Bulk(0),
            connection_nonce: 4,
        })
        .unwrap();

    let mut server_bulk = server_association
        .take_lane_receiver(LaneKind::Bulk(0))
        .unwrap();
    let dispatch = Arc::new(RecordingDispatch::default());
    let (client_io, server_io) = tokio::io::duplex(4096);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut client_shutdown = shutdown_rx.clone();
    let mut server_shutdown = shutdown_rx;
    let server_lane = {
        let association = server_association.clone();
        let dispatch = dispatch.clone();
        let messaging = messaging.clone();
        tokio::spawn(async move {
            BidirectionalLane::new(
                association,
                LaneKind::Bulk(0),
                3,
                lane_services(messaging, dispatch),
                duplex_lane_config(),
            )
            .run(&mut server_bulk, server_io, &mut server_shutdown)
            .await
        })
    };
    let client_lane = {
        let association = client_association.clone();
        let messaging = messaging.clone();
        tokio::spawn(async move {
            BidirectionalLane::new(
                association,
                LaneKind::Bulk(0),
                4,
                lane_services(messaging, Arc::new(RecordingDispatch::default())),
                duplex_lane_config(),
            )
            .run(&mut client_bulk, client_io, &mut client_shutdown)
            .await
        })
    };

    wait_for(|| {
        dispatch
            .tells
            .lock()
            .expect("recording dispatch poisoned")
            .len()
            == 1
    })
    .await;
    shutdown_tx.send(true).unwrap();
    assert_eq!(client_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
    assert_eq!(server_lane.await.unwrap().unwrap(), LaneExit::Shutdown);
    let dispatched = dispatch.tells.lock().expect("recording dispatch poisoned");
    let dispatched: ActorRef = dispatched[0].actor_ref().unwrap();
    assert!(dispatched.same_activation(&target));
    assert_eq!(server_association.metrics().dropped_inbound_frames, 0);
}

#[tokio::test]
async fn an_unregistered_dictionary_id_drops_one_frame_and_keeps_the_lane() {
    let client_incarnation = NodeIncarnation::new(1).unwrap();
    let server_incarnation = NodeIncarnation::new(2).unwrap();
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
    let server_address = NodeAddress::new("127.0.0.1", 25557).unwrap();
    let client_address = NodeAddress::new("127.0.0.1", 25558).unwrap();
    let client_association = active_association(
        client_incarnation,
        server_incarnation,
        server_address.clone(),
        protocol_id,
        fingerprint,
    );
    let server_association = active_association(
        server_incarnation,
        client_incarnation,
        client_address,
        protocol_id,
        fingerprint,
    );
    let messaging = Arc::new(OutboundMessaging::new(8).unwrap());
    let target = lane_target(&server_address, server_incarnation, protocol_id);
    let route = messaging
        .prepare_exact_tell_route(client_association.clone(), &target, fingerprint)
        .unwrap();
    route.tell(1, Bytes::from_static(b"registration")).unwrap();
    route.tell(1, Bytes::from_static(b"compact")).unwrap();
    let mut client_bulk = client_association
        .take_lane_receiver(LaneKind::Bulk(0))
        .unwrap();
    let registration = client_bulk.recv().await.unwrap();
    let compact = client_bulk.recv().await.unwrap();
    assert!(compact.payload_len() < registration.payload_len());

    let mut server_bulk = server_association
        .take_lane_receiver(LaneKind::Bulk(0))
        .unwrap();
    let dispatch = Arc::new(RecordingDispatch::default());
    let (peer, lane_io) = tokio::io::duplex(4096);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut lane_shutdown = shutdown_rx;
    let lane = {
        let association = server_association.clone();
        let dispatch = dispatch.clone();
        let messaging = messaging.clone();
        tokio::spawn(async move {
            BidirectionalLane::new(
                association,
                LaneKind::Bulk(0),
                3,
                lane_services(messaging, dispatch),
                duplex_lane_config(),
            )
            .run(&mut server_bulk, lane_io, &mut lane_shutdown)
            .await
        })
    };
    let mut peer = FramedConnection::new(peer, FrameCodec::new(4096).unwrap());
    peer.write_frame(&compact).await.unwrap();
    peer.write_frame(&registration).await.unwrap();

    wait_for(|| {
        dispatch
            .tells
            .lock()
            .expect("recording dispatch poisoned")
            .len()
            == 1
    })
    .await;
    assert_eq!(server_association.metrics().dropped_inbound_frames, 1);
    shutdown_tx.send(true).unwrap();
    assert_eq!(lane.await.unwrap().unwrap(), LaneExit::Shutdown);
}

#[tokio::test]
async fn a_reply_without_a_pending_ask_is_discarded_and_counted() {
    let association = active_association(
        NodeIncarnation::new(1).unwrap(),
        NodeIncarnation::new(2).unwrap(),
        NodeAddress::new("127.0.0.1", 25559).unwrap(),
        ProtocolId::new(7).unwrap(),
        ProtocolFingerprint::digest(b"lane-test/v1"),
    );
    let mut interactive = association
        .take_lane_receiver(LaneKind::Interactive)
        .unwrap();
    let (peer, lane_io) = tokio::io::duplex(4096);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut lane_shutdown = shutdown_rx;
    let lane = {
        let association = association.clone();
        tokio::spawn(async move {
            BidirectionalLane::new(
                association,
                LaneKind::Interactive,
                2,
                lane_services(
                    Arc::new(OutboundMessaging::new(8).unwrap()),
                    Arc::new(RecordingDispatch::default()),
                ),
                duplex_lane_config(),
            )
            .run(&mut interactive, lane_io, &mut lane_shutdown)
            .await
        })
    };
    let mut peer = FramedConnection::new(peer, FrameCodec::new(4096).unwrap());
    peer.write_frame(&reply_frame(
        CorrelationId::new(9, 1).unwrap(),
        Bytes::from_static(b"late"),
    ))
    .await
    .unwrap();

    wait_for(|| association.metrics().discarded_replies == 1).await;
    shutdown_tx.send(true).unwrap();
    assert_eq!(lane.await.unwrap().unwrap(), LaneExit::Shutdown);
}

#[tokio::test]
async fn the_control_lane_announces_close_before_shutdown() {
    let association = active_association(
        NodeIncarnation::new(1).unwrap(),
        NodeIncarnation::new(2).unwrap(),
        NodeAddress::new("127.0.0.1", 25560).unwrap(),
        ProtocolId::new(7).unwrap(),
        ProtocolFingerprint::digest(b"lane-test/v1"),
    );
    let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
    let (peer, lane_io) = tokio::io::duplex(4096);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut lane_shutdown = shutdown_rx;
    let lane = {
        let association = association.clone();
        tokio::spawn(async move {
            BidirectionalLane::new(
                association,
                LaneKind::Control,
                1,
                lane_services(
                    Arc::new(OutboundMessaging::new(8).unwrap()),
                    Arc::new(RecordingDispatch::default()),
                ),
                duplex_lane_config(),
            )
            .run(&mut control, lane_io, &mut lane_shutdown)
            .await
        })
    };
    let mut peer = FramedConnection::new(peer, FrameCodec::new(4096).unwrap());
    assert_eq!(peer.read_frame().await.unwrap().kind, FrameKind::Heartbeat);
    shutdown_tx.send(true).unwrap();

    let announced = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if peer.read_frame().await.unwrap().kind == FrameKind::Close {
                return;
            }
        }
    })
    .await;

    assert!(announced.is_ok());
    assert_eq!(lane.await.unwrap().unwrap(), LaneExit::Shutdown);
}

#[tokio::test]
async fn a_remote_close_completes_pending_asks_by_dispatch_knowledge() {
    let client_incarnation = NodeIncarnation::new(1).unwrap();
    let server_incarnation = NodeIncarnation::new(2).unwrap();
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"lane-test/v1");
    let server_address = NodeAddress::new("127.0.0.1", 25561).unwrap();
    let association = active_association(
        client_incarnation,
        server_incarnation,
        server_address.clone(),
        protocol_id,
        fingerprint,
    );
    let messaging = Arc::new(OutboundMessaging::new(8).unwrap());
    let target = lane_target(&server_address, server_incarnation, protocol_id);
    let ask = {
        let messaging = messaging.clone();
        let association = association.clone();
        tokio::spawn(async move {
            messaging
                .ask(
                    &association,
                    &target,
                    OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"ask")),
                    Instant::now() + Duration::from_secs(5),
                )
                .await
        })
    };
    wait_for(|| messaging.pending_count() == 1).await;

    let mut control = association.take_lane_receiver(LaneKind::Control).unwrap();
    let (peer, lane_io) = tokio::io::duplex(4096);
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let mut peer = FramedConnection::new(peer, FrameCodec::new(4096).unwrap());
    peer.write_frame(&Frame::new(FrameKind::Close, Bytes::new()))
        .await
        .unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        BidirectionalLane::new(
            association.clone(),
            LaneKind::Control,
            1,
            lane_services(messaging.clone(), Arc::new(RecordingDispatch::default())),
            duplex_lane_config(),
        )
        .run(&mut control, lane_io, &mut shutdown_rx),
    )
    .await
    .expect("a remote close must exit the control lane");

    assert_eq!(result.unwrap(), LaneExit::RemoteClose);
    assert_eq!(
        ask.await.unwrap().unwrap_err(),
        AskError::AssociationLostBeforeWrite
    );
}
