use crate::endpoint_pool::*;

use std::collections::BTreeSet;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicUsize;

use lattice_core::actor_ref::ActorRef;
use lattice_core::direct_link::ids::DirectLinkMessageId;
use lattice_core::direct_link::messages::LinkMessageFlags;
use lattice_core::direct_link::options::{DirectLinkMode, DirectLinkOptions};
use lattice_core::direct_link::stream::{DirectLinkMessageDescriptor, DirectLinkStreamDescriptor};
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::{actor_kind, service_kind};

#[derive(Debug, Default)]
struct RecordingLifecycle {
    direction_closed: StdMutex<Vec<LinkDirectionClosed>>,
    link_closed: StdMutex<Vec<LinkClosed>>,
}

impl DirectLinkEndpointPoolLifecycle for RecordingLifecycle {
    fn deliver_direction_closed(
        &self,
        _actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), LinkError> {
        self.direction_closed.lock().unwrap().push(event);
        Ok(())
    }

    fn deliver_link_closed(
        &self,
        _actor_ref: &ActorRef,
        event: LinkClosed,
    ) -> Result<(), LinkError> {
        self.link_closed.lock().unwrap().push(event);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct FakeTransport {
    connects: Arc<StdMutex<Vec<DirectLinkEndpoint>>>,
    frames: Arc<StdMutex<Vec<DirectLinkFrame>>>,
    fail_message_writes: bool,
    protocol_error_on_read: Option<usize>,
    read_count: Arc<AtomicUsize>,
}

#[async_trait]
impl DirectLinkTransport for FakeTransport {
    type Listener = ();
    type Connection = FakeConnection;

    async fn bind(
        &self,
        _config: crate::transport::DirectLinkListenConfig,
    ) -> Result<Self::Listener, LinkError> {
        Ok(())
    }

    async fn connect_physical(
        &self,
        endpoint: DirectLinkEndpoint,
        _max_frame_size: usize,
    ) -> Result<Self::Connection, LinkError> {
        self.connects.lock().unwrap().push(endpoint);
        let frame_start_index = self.frames.lock().unwrap().len();
        Ok(FakeConnection {
            frames: self.frames.clone(),
            fail_message_writes: self.fail_message_writes,
            protocol_error_on_read: self.protocol_error_on_read,
            read_count: self.read_count.clone(),
            acknowledged_opens: 0,
            frame_start_index,
        })
    }
}

#[derive(Debug)]
struct FakeConnection {
    frames: Arc<StdMutex<Vec<DirectLinkFrame>>>,
    fail_message_writes: bool,
    protocol_error_on_read: Option<usize>,
    read_count: Arc<AtomicUsize>,
    acknowledged_opens: usize,
    frame_start_index: usize,
}

#[async_trait]
impl DirectLinkConnection for FakeConnection {
    async fn read_frame(&mut self) -> Result<DirectLinkFrame, LinkError> {
        let open = loop {
            let open_frames = self
                .frames
                .lock()
                .unwrap()
                .iter()
                .skip(self.frame_start_index)
                .filter(|frame| frame.kind == DirectLinkFrameKind::OpenLink)
                .cloned()
                .collect::<Vec<_>>();
            if open_frames.len() > self.acknowledged_opens {
                let frame = open_frames[self.acknowledged_opens].clone();
                self.acknowledged_opens += 1;
                break frame.decode_open_link().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        let read = self.read_count.fetch_add(1, Ordering::Relaxed) + 1;
        if self.protocol_error_on_read == Some(read) {
            return Ok(DirectLinkFrame {
                kind: DirectLinkFrameKind::ProtocolError,
                link_id: open.link_id,
                sequence: LinkSequence(0),
                message_id: None,
                flags: Default::default(),
                header: Vec::new(),
                payload: b"connection fatal".to_vec(),
            });
        }
        let ack = crate::session::OpenLinkAck {
            link_id: open.link_id.clone(),
            source_to_target: crate::session::NegotiatedDirection {
                direction: LinkDirection::SourceToTarget,
                stream_name: open.source_to_target.stream_name,
                accepted_message_type_ids: open.source_to_target.supported_message_type_ids,
                next_receive_sequence: LinkSequence(1),
                backpressure: open.options.backpressure,
                closed: false,
            },
            target_to_source: None,
        };
        DirectLinkFrame::open_link_ack(&ack).map_err(|error| LinkError::Protocol(error.to_string()))
    }

    async fn write_frame(&mut self, frame: DirectLinkFrame) -> Result<(), LinkError> {
        if self.fail_message_writes && frame.kind == DirectLinkFrameKind::Message {
            return Err(LinkError::Protocol("simulated connection loss".to_string()));
        }
        self.frames.lock().unwrap().push(frame);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), LinkError> {
        Ok(())
    }
}

fn endpoint() -> DirectLinkEndpoint {
    DirectLinkEndpoint::new("tcp://127.0.0.1:9001".parse().unwrap())
}

fn stream() -> DirectLinkStreamDescriptor {
    DirectLinkStreamDescriptor {
        stream_name: "movement".to_string(),
        messages: vec![DirectLinkMessageDescriptor {
            message_id: DirectLinkMessageId(7),
            proto_full_name: "game.Position".to_string(),
            rust_type_name: "Position".to_string(),
        }],
    }
}

fn actor_ref(actor_id: u64) -> ActorRef {
    ActorRef::direct(
        service_kind!("Battle"),
        actor_kind!("BattleActor"),
        ActorId::U64(actor_id),
        InstanceId::new("battle-1"),
        "http://127.0.0.1:18080".parse().unwrap(),
        None,
    )
}

fn request(link_id: &str) -> DirectLinkOpenRequest {
    let endpoint = endpoint();
    DirectLinkOpenRequest {
        link_id: LinkId::new(link_id),
        source: actor_ref(1),
        target: LinkTarget::Endpoint {
            endpoint,
            target: actor_ref(2),
        },
        mode: DirectLinkMode::Unidirectional,
        source_to_target: stream(),
        target_to_source: None,
        options: DirectLinkOptions::default(),
        trace: Default::default(),
    }
}

#[tokio::test]
async fn endpoint_pool_reuses_one_physical_connection_for_multiple_links() {
    let transport = FakeTransport::default();
    let pool = PooledDirectLinkEndpointPool::new(
        transport.clone(),
        DirectLinkEndpointPoolConfig::default(),
    );

    let first = pool.open_link(request("link-1")).await.unwrap();
    let second = pool.open_link(request("link-2")).await.unwrap();

    assert_eq!(first.connection_id, second.connection_id);
    assert_eq!(transport.connects.lock().unwrap().len(), 1);
    assert_eq!(pool.active_links_for_endpoint(&endpoint()).await, 2);
    let metrics = pool.metrics_snapshot();
    assert_eq!(metrics.physical_connections_opened, 1);
    assert_eq!(metrics.logical_links_opened, 2);
    assert_eq!(metrics.active_logical_links, 2);
    assert_eq!(
        metrics.links_per_connection,
        BTreeMap::from([(first.connection_id, 2)])
    );
    assert_eq!(
        metrics.frames_per_connection,
        BTreeMap::from([(first.connection_id, 2)])
    );
}

#[tokio::test]
async fn node_drain_closes_logical_links_before_physical_connection() {
    let transport = FakeTransport::default();
    let pool = PooledDirectLinkEndpointPool::new(
        transport.clone(),
        DirectLinkEndpointPoolConfig::default(),
    );
    let first = pool.open_link(request("link-1")).await.unwrap();
    let second = pool.open_link(request("link-2")).await.unwrap();
    assert_eq!(first.connection_id, second.connection_id);

    let closed = pool
        .close_all_logical_links(LinkCloseReason::NodeDraining)
        .await
        .unwrap();

    assert_eq!(closed, 2);
    assert_eq!(pool.active_links_for_endpoint(&endpoint()).await, 0);
    let metrics = pool.metrics_snapshot();
    assert_eq!(metrics.active_logical_links, 0);
    assert_eq!(metrics.logical_links_closed, 2);
    assert_eq!(metrics.active_physical_connections, 1);
    assert_eq!(
        metrics.links_per_connection,
        BTreeMap::from([(first.connection_id, 0)])
    );
    let close_frames = transport
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|frame| frame.kind == DirectLinkFrameKind::Close)
        .count();
    assert_eq!(close_frames, 2);
}

#[tokio::test]
async fn node_drain_delivers_source_link_closed_events() {
    let lifecycle = Arc::new(RecordingLifecycle::default());
    let pool = PooledDirectLinkEndpointPool::new_with_lifecycle(
        FakeTransport::default(),
        DirectLinkEndpointPoolConfig::default(),
        Some(lifecycle.clone()),
    );
    pool.open_link(request("link-1")).await.unwrap();
    pool.open_link(request("link-2")).await.unwrap();

    let closed = pool
        .close_all_logical_links(LinkCloseReason::NodeDraining)
        .await
        .unwrap();

    assert_eq!(closed, 2);
    let link_closed = lifecycle.link_closed.lock().unwrap().clone();
    assert_eq!(link_closed.len(), 2);
    assert!(link_closed.iter().all(|event| {
        event.reason == LinkCloseReason::NodeDraining
            && event.closed_directions == BTreeSet::from([LinkDirection::SourceToTarget])
    }));
    assert!(lifecycle.direction_closed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn peer_connection_loss_closes_every_multiplexed_logical_link() {
    let lifecycle = Arc::new(RecordingLifecycle::default());
    let pool = PooledDirectLinkEndpointPool::new_with_lifecycle(
        FakeTransport {
            fail_message_writes: true,
            ..FakeTransport::default()
        },
        DirectLinkEndpointPoolConfig::default(),
        Some(lifecycle.clone()),
    );
    let first = pool.open_link(request("link-1")).await.unwrap();
    let second = pool.open_link(request("link-2")).await.unwrap();
    assert_eq!(first.connection_id, second.connection_id);
    let send_error = first
        .session
        .sender
        .tell(message("link-1"))
        .await
        .unwrap_err();

    assert!(matches!(send_error, LinkSendError::Protocol(_)));
    let closed = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let closed = pool.closed_link_reasons().await;
            if closed.len() == 2 {
                break closed;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        closed,
        BTreeMap::from([
            (LinkId::new("link-1"), LinkCloseReason::ConnectionLost),
            (LinkId::new("link-2"), LinkCloseReason::ConnectionLost),
        ])
    );
    assert_eq!(pool.active_links_for_endpoint(&endpoint()).await, 0);
    assert_eq!(
        pool.metrics_snapshot(),
        DirectLinkEndpointPoolMetricsSnapshot {
            physical_connections_opened: 1,
            physical_connections_closed: 1,
            active_physical_connections: 0,
            logical_links_opened: 2,
            logical_links_closed: 2,
            active_logical_links: 0,
            frames_written: 2,
            reconnects: 0,
            pool_rejections: 0,
            pool_queue_backpressure_events: 0,
            links_per_connection: BTreeMap::new(),
            frames_per_connection: BTreeMap::from([(first.connection_id, 2)]),
        }
    );
    let link_closed = lifecycle.link_closed.lock().unwrap().clone();
    assert_eq!(link_closed.len(), 2);
    assert!(
        link_closed
            .iter()
            .all(|event| event.reason == LinkCloseReason::ConnectionLost)
    );

    fn message(link_id: &str) -> OutboundDirectLinkMessage {
        OutboundDirectLinkMessage {
            link_id: LinkId::new(link_id),
            direction: LinkDirection::SourceToTarget,
            message_id: DirectLinkMessageId(7),
            proto_full_name: "game.Position",
            metadata: Vec::new(),
            payload: b"abc".to_vec(),
            flags: LinkMessageFlags::EMPTY,
        }
    }
}

#[tokio::test]
async fn connection_level_protocol_error_closes_connection_and_multiplexed_links() {
    let pool = PooledDirectLinkEndpointPool::new(
        FakeTransport {
            protocol_error_on_read: Some(2),
            ..FakeTransport::default()
        },
        DirectLinkEndpointPoolConfig::default(),
    );
    let first = pool.open_link(request("link-1")).await.unwrap();
    let error = pool.open_link(request("link-2")).await.unwrap_err();

    assert!(matches!(error, LinkError::Protocol(ref reason) if reason == "connection fatal"));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let closed = pool.closed_link_reasons().await;
            if closed.len() == 1 {
                assert!(matches!(
                    closed.get(&first.session.link_id),
                    Some(LinkCloseReason::ProtocolError(reason)) if reason == "connection fatal"
                ));
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(pool.active_links_for_endpoint(&endpoint()).await, 0);
    assert_eq!(pool.metrics_snapshot().physical_connections_closed, 1);
}

#[tokio::test]
async fn link_level_protocol_error_closes_only_affected_logical_link() {
    let transport = FakeTransport::default();
    let pool = PooledDirectLinkEndpointPool::new(
        transport.clone(),
        DirectLinkEndpointPoolConfig::default(),
    );
    let first = pool.open_link(request("link-1")).await.unwrap();
    let second = pool.open_link(request("link-2")).await.unwrap();

    pool.process_protocol_error_frame(DirectLinkFrame {
        kind: DirectLinkFrameKind::ProtocolError,
        link_id: first.session.link_id.clone(),
        sequence: LinkSequence(0),
        message_id: None,
        flags: Default::default(),
        header: Vec::new(),
        payload: b"bad message on link-1".to_vec(),
    })
    .await
    .unwrap();
    second.session.sender.tell(message("link-2")).await.unwrap();

    assert_eq!(pool.active_links_for_endpoint(&endpoint()).await, 1);
    assert_eq!(
        pool.closed_link_reasons().await,
        BTreeMap::from([(
            LinkId::new("link-1"),
            LinkCloseReason::ProtocolError("bad message on link-1".to_string())
        )])
    );
    let metrics = pool.metrics_snapshot();
    assert_eq!(metrics.logical_links_closed, 1);
    assert_eq!(metrics.active_logical_links, 1);
    assert_eq!(metrics.active_physical_connections, 1);
    assert_eq!(
        metrics.links_per_connection,
        BTreeMap::from([(first.connection_id, 1)])
    );
    assert!(transport.frames.lock().unwrap().iter().any(|frame| {
        frame.kind == DirectLinkFrameKind::Message && frame.link_id == LinkId::new("link-2")
    }));

    fn message(link_id: &str) -> OutboundDirectLinkMessage {
        OutboundDirectLinkMessage {
            link_id: LinkId::new(link_id),
            direction: LinkDirection::SourceToTarget,
            message_id: DirectLinkMessageId(7),
            proto_full_name: "game.Position",
            metadata: Vec::new(),
            payload: b"abc".to_vec(),
            flags: LinkMessageFlags::EMPTY,
        }
    }
}

#[tokio::test]
async fn pool_queue_backpressure_is_recorded_for_try_tell_enqueue_failure() {
    let pool = PooledDirectLinkEndpointPool::new(FakeTransport::default(), Default::default());
    let session = pool.open_link(request("link-1")).await.unwrap();
    let state_guard = pool.inner.state.lock().await;

    let error = session.session.sender.try_tell(OutboundDirectLinkMessage {
        link_id: LinkId::new("link-1"),
        direction: LinkDirection::SourceToTarget,
        message_id: DirectLinkMessageId(7),
        proto_full_name: "game.Position",
        metadata: Vec::new(),
        payload: b"abc".to_vec(),
        flags: LinkMessageFlags::EMPTY,
    });

    assert!(matches!(error, Err(LinkSendError::BackpressureFull)));
    assert_eq!(pool.metrics_snapshot().pool_queue_backpressure_events, 1);
    drop(state_guard);

    session
        .session
        .sender
        .tell(OutboundDirectLinkMessage {
            link_id: LinkId::new("link-1"),
            direction: LinkDirection::SourceToTarget,
            message_id: DirectLinkMessageId(7),
            proto_full_name: "game.Position",
            metadata: Vec::new(),
            payload: b"abc".to_vec(),
            flags: LinkMessageFlags::EMPTY,
        })
        .await
        .unwrap();

    let frames = pool.inner.transport.frames.lock().unwrap();
    let message_frame = frames
        .iter()
        .find(|frame| frame.kind == DirectLinkFrameKind::Message)
        .expect("message frame was written after retry");
    assert_eq!(message_frame.sequence, LinkSequence(1));
}

#[tokio::test]
async fn pooled_sender_rejects_messages_for_other_logical_links() {
    let pool = PooledDirectLinkEndpointPool::new(FakeTransport::default(), Default::default());
    let session = pool.open_link(request("link-1")).await.unwrap();

    let error = session
        .session
        .sender
        .tell(OutboundDirectLinkMessage {
            link_id: LinkId::new("link-2"),
            direction: LinkDirection::SourceToTarget,
            message_id: DirectLinkMessageId(7),
            proto_full_name: "game.Position",
            metadata: Vec::new(),
            payload: b"abc".to_vec(),
            flags: LinkMessageFlags::EMPTY,
        })
        .await
        .unwrap_err();

    assert!(
        matches!(error, LinkSendError::Protocol(message) if message.contains("cannot send frame"))
    );
}

#[tokio::test]
async fn endpoint_pool_honors_stable_stripe_selection() {
    let config = DirectLinkEndpointPoolConfig {
        connections_per_endpoint: NonZeroUsize::new(4).unwrap(),
        ..DirectLinkEndpointPoolConfig::default()
    };
    let link_id = LinkId::new("stable-link");

    assert_eq!(
        config.stripe_index_for_link(&link_id),
        config.stripe_index_for_link(&link_id)
    );
    assert!(config.stripe_index_for_link(&link_id) < 4);
}

#[tokio::test]
async fn endpoint_pool_honors_connections_per_endpoint() {
    let transport = FakeTransport::default();
    let config = DirectLinkEndpointPoolConfig {
        connections_per_endpoint: NonZeroUsize::new(2).unwrap(),
        ..DirectLinkEndpointPoolConfig::default()
    };
    let first = LinkId::new("striped-link-0");
    let second = (1..100)
        .map(|index| LinkId::new(format!("striped-link-{index}")))
        .find(|candidate| {
            config.stripe_index_for_link(candidate) != config.stripe_index_for_link(&first)
        })
        .expect("test should find link ids for both stripes");
    let pool = PooledDirectLinkEndpointPool::new(transport.clone(), config);

    let first = pool.open_link(request(first.as_str())).await.unwrap();
    let second = pool.open_link(request(second.as_str())).await.unwrap();

    assert_ne!(first.connection_id, second.connection_id);
    assert_eq!(transport.connects.lock().unwrap().len(), 2);
    let metrics = pool.metrics_snapshot();
    assert_eq!(metrics.physical_connections_opened, 2);
    assert_eq!(metrics.logical_links_opened, 2);
}

#[tokio::test]
async fn max_links_per_connection_rejects_before_openlink() {
    let transport = FakeTransport::default();
    let config = DirectLinkEndpointPoolConfig {
        max_links_per_connection: 1,
        ..DirectLinkEndpointPoolConfig::default()
    };
    let pool = PooledDirectLinkEndpointPool::new(transport.clone(), config);

    pool.open_link(request("link-1")).await.unwrap();
    let error = pool.open_link(request("link-2")).await.unwrap_err();

    assert!(matches!(error, LinkError::Overloaded));
    assert_eq!(transport.connects.lock().unwrap().len(), 1);
    let open_frames = transport
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|frame| frame.kind == DirectLinkFrameKind::OpenLink)
        .count();
    assert_eq!(open_frames, 1);
    assert_eq!(pool.metrics_snapshot().pool_rejections, 1);
}

#[tokio::test]
async fn max_links_per_endpoint_rejects_before_openlink() {
    let transport = FakeTransport::default();
    let config = DirectLinkEndpointPoolConfig {
        max_links_per_endpoint: 1,
        connections_per_endpoint: NonZeroUsize::new(2).unwrap(),
        ..DirectLinkEndpointPoolConfig::default()
    };
    let pool = PooledDirectLinkEndpointPool::new(transport.clone(), config);

    pool.open_link(request("link-1")).await.unwrap();
    let error = pool.open_link(request("link-2")).await.unwrap_err();

    assert!(matches!(error, LinkError::Overloaded));
    let open_frames = transport
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|frame| frame.kind == DirectLinkFrameKind::OpenLink)
        .count();
    assert_eq!(open_frames, 1);
    assert_eq!(pool.metrics_snapshot().pool_rejections, 1);
}

#[tokio::test]
async fn pooled_sender_writes_message_frames_through_selected_connection() {
    let transport = FakeTransport::default();
    let pool = PooledDirectLinkEndpointPool::new(
        transport.clone(),
        DirectLinkEndpointPoolConfig::default(),
    );
    let session = pool.open_link(request("link-1")).await.unwrap();
    let sender = session.session.sender.clone();

    sender
        .tell(OutboundDirectLinkMessage {
            link_id: LinkId::new("link-1"),
            direction: LinkDirection::SourceToTarget,
            message_id: DirectLinkMessageId(7),
            proto_full_name: "game.Position",
            metadata: Vec::new(),
            payload: b"abc".to_vec(),
            flags: LinkMessageFlags::EMPTY,
        })
        .await
        .unwrap();

    let frames = transport.frames.lock().unwrap();
    assert!(frames.iter().any(|frame| {
        frame.kind == DirectLinkFrameKind::Message
            && frame.link_id == LinkId::new("link-1")
            && frame.message_id == Some(DirectLinkMessageId(7))
    }));
}
