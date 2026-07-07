use super::*;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lattice_actor::{
    ActorContext, ActorRuntime, ActorSpawnOptions, ActorTellError, Handler, MailboxConfig,
};
use lattice_core::{
    ActorId, ActorKind, ActorRef, BackpressurePolicy, DirectLinkMessage, DirectLinkMode,
    DirectLinkOptions, DirectLinkRuntime, DirectLinkRuntimeHandle, DirectLinkSender,
    DirectLinkSession, DirectLinkStreamDescriptor, DirectLinkStreamType, InstanceId,
    LinkBackpressure, LinkCloseReason, LinkClosed, LinkDirection, LinkDirectionClosed, LinkError,
    LinkId, LinkOpened, LinkSendError, LinkSequence, Linked, OutboundDirectLinkMessage,
    ServiceContext, ServiceKind, actor_kind, service_kind,
};
use prost::Message as _;
use std::time::Instant;

use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

use crate::protocol::DirectLinkFrame;
use crate::session::{
    DIRECT_LINK_PROTOCOL_VERSION, DirectLinkActorPolicy, OpenLinkDirection, OpenLinkRejectReason,
    OpenLinkRequest, OpenLinkValidationPolicy,
};
use crate::stream::DirectLinkStream;

#[derive(Clone, PartialEq, prost::Message)]
struct PositionUpdate {
    #[prost(uint64, tag = "1")]
    tick: u64,
}

impl DirectLinkMessage for PositionUpdate {
    const PROTO_FULL_NAME: &'static str = "game.PositionUpdate";
}

#[derive(Clone, PartialEq, prost::Message)]
struct InputCommand {
    #[prost(uint64, tag = "1")]
    command_id: u64,
}

impl DirectLinkMessage for InputCommand {
    const PROTO_FULL_NAME: &'static str = "game.InputCommand";
}

struct BattleActor {
    received: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl lattice_actor::Actor for BattleActor {
    type Error = Infallible;
}

#[async_trait]
impl Handler<Linked<PositionUpdate>> for BattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Linked<PositionUpdate>,
    ) -> Result<(), Self::Error> {
        self.received
            .lock()
            .expect("received mutex poisoned")
            .push(msg.payload.tick);
        Ok(())
    }
}

#[async_trait]
impl Handler<Linked<InputCommand>> for BattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Linked<InputCommand>,
    ) -> Result<(), Self::Error> {
        self.received
            .lock()
            .expect("received mutex poisoned")
            .push(msg.payload.command_id);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for BattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkOpened,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for BattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkDirectionClosed,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for BattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkClosed,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for BattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkBackpressure,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct GatewayActor {
    received: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl lattice_actor::Actor for GatewayActor {
    type Error = Infallible;
}

#[async_trait]
impl Handler<Linked<PositionUpdate>> for GatewayActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Linked<PositionUpdate>,
    ) -> Result<(), Self::Error> {
        self.received
            .lock()
            .expect("received mutex poisoned")
            .push(msg.payload.tick);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for GatewayActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkOpened,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for GatewayActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkDirectionClosed,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for GatewayActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkClosed,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for GatewayActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkBackpressure,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordingLinkRuntime {
    outbound_requests: Mutex<Vec<(LinkId, DirectLinkStreamDescriptor)>>,
    sender: Arc<RecordingLinkSender>,
}

#[async_trait]
impl DirectLinkRuntime for RecordingLinkRuntime {
    async fn open_link(
        &self,
        _request: lattice_core::DirectLinkOpenRequest,
    ) -> Result<DirectLinkSession, LinkError> {
        Err(LinkError::Protocol(
            "open_link is not used by this test".to_string(),
        ))
    }

    async fn get_outbound(
        &self,
        link_id: LinkId,
        stream: DirectLinkStreamDescriptor,
    ) -> Result<DirectLinkSession, LinkError> {
        self.outbound_requests
            .lock()
            .expect("outbound requests mutex poisoned")
            .push((link_id.clone(), stream.clone()));
        Ok(DirectLinkSession {
            link_id,
            direction: LinkDirection::TargetToSource,
            accepted_message_ids: stream.accepted_message_ids(),
            stream,
            sender: self.sender.clone(),
        })
    }

    async fn close_all(
        &self,
        _link_id: LinkId,
        _reason: lattice_core::LinkCloseReason,
    ) -> Result<(), LinkError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordingLinkSender {
    sent: Mutex<Vec<OutboundDirectLinkMessage>>,
}

#[async_trait]
impl DirectLinkSender for RecordingLinkSender {
    async fn tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
        self.try_tell(message)
    }

    fn try_tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
        self.sent
            .lock()
            .expect("sent messages mutex poisoned")
            .push(message);
        Ok(())
    }

    async fn close(&self, _reason: lattice_core::LinkCloseReason) -> Result<(), LinkSendError> {
        Ok(())
    }
}

#[derive(Clone)]
struct BattleUpdateStream;

impl DirectLinkStreamType for BattleUpdateStream {
    type Metadata = ();

    fn descriptor() -> DirectLinkStreamDescriptor {
        DirectLinkStream::new("battle-update")
            .message::<PositionUpdate>()
            .descriptor()
    }
}

struct OpeningBattleActor {
    opened: Arc<Mutex<Vec<LinkOpened>>>,
    outbound: Arc<Mutex<Option<(LinkDirection, String)>>>,
}

#[async_trait]
impl lattice_actor::Actor for OpeningBattleActor {
    type Error = Infallible;
}

#[async_trait]
impl Handler<Linked<InputCommand>> for OpeningBattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: Linked<InputCommand>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for OpeningBattleActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        msg: LinkOpened,
    ) -> Result<(), Self::Error> {
        let outbound = ctx
            .links()
            .get::<BattleUpdateStream>(msg.link_id.clone())
            .await
            .expect("target-to-source link should be available");
        *self
            .outbound
            .lock()
            .expect("outbound handle mutex poisoned") =
            Some((outbound.direction(), outbound.stream().stream_name.clone()));
        self.opened
            .lock()
            .expect("opened messages mutex poisoned")
            .push(msg);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for OpeningBattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkDirectionClosed,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for OpeningBattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkClosed,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for OpeningBattleActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkBackpressure,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct ClosingActor {
    direction_closed: Arc<Mutex<Vec<LinkDirectionClosed>>>,
    link_closed: Arc<Mutex<Vec<LinkClosed>>>,
}

#[async_trait]
impl lattice_actor::Actor for ClosingActor {
    type Error = Infallible;
}

#[async_trait]
impl Handler<Linked<InputCommand>> for ClosingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: Linked<InputCommand>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<Linked<PositionUpdate>> for ClosingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: Linked<PositionUpdate>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for ClosingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkOpened,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for ClosingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: LinkDirectionClosed,
    ) -> Result<(), Self::Error> {
        self.direction_closed
            .lock()
            .expect("direction closed mutex poisoned")
            .push(msg);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for ClosingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: LinkClosed,
    ) -> Result<(), Self::Error> {
        self.link_closed
            .lock()
            .expect("link closed mutex poisoned")
            .push(msg);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for ClosingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkBackpressure,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct BackpressureActor {
    received: Arc<Mutex<Vec<u64>>>,
    backpressure: Arc<Mutex<Vec<LinkBackpressure>>>,
    direction_closed: Arc<Mutex<Vec<LinkDirectionClosed>>>,
    link_closed: Arc<Mutex<Vec<LinkClosed>>>,
}

#[async_trait]
impl lattice_actor::Actor for BackpressureActor {
    type Error = Infallible;
}

#[async_trait]
impl Handler<Linked<InputCommand>> for BackpressureActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Linked<InputCommand>,
    ) -> Result<(), Self::Error> {
        self.received
            .lock()
            .expect("received mutex poisoned")
            .push(msg.payload.command_id);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for BackpressureActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkOpened,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for BackpressureActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: LinkDirectionClosed,
    ) -> Result<(), Self::Error> {
        self.direction_closed
            .lock()
            .expect("direction closed mutex poisoned")
            .push(msg);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for BackpressureActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: LinkClosed,
    ) -> Result<(), Self::Error> {
        self.link_closed
            .lock()
            .expect("link closed mutex poisoned")
            .push(msg);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for BackpressureActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: LinkBackpressure,
    ) -> Result<(), Self::Error> {
        self.backpressure
            .lock()
            .expect("backpressure mutex poisoned")
            .push(msg);
        Ok(())
    }
}

struct BlockingActor {
    received: Arc<Mutex<Vec<u64>>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl lattice_actor::Actor for BlockingActor {
    type Error = Infallible;
}

#[async_trait]
impl Handler<Linked<InputCommand>> for BlockingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Linked<InputCommand>,
    ) -> Result<(), Self::Error> {
        if msg.payload.command_id == 100 {
            self.entered.notify_waiters();
            self.release.notified().await;
        }
        self.received
            .lock()
            .expect("received mutex poisoned")
            .push(msg.payload.command_id);
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkOpened> for BlockingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkOpened,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkDirectionClosed> for BlockingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkDirectionClosed,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkClosed> for BlockingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkClosed,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<LinkBackpressure> for BlockingActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: LinkBackpressure,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn inbound_router_delivers_message_frame_to_target_actor_mailbox() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            BattleActor {
                received: received.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("movement").message::<PositionUpdate>();
    let descriptor = stream.descriptor();
    let binding = stream.for_actor::<BattleActor>(actor_kind!("Battle"));
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-inbound");
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(binding, move |_| Some(handle.clone()))
        .build();
    let message_id = descriptor.message_id_for::<PositionUpdate>().unwrap();
    let frame = DirectLinkFrame::message(
        link_id,
        LinkSequence(1),
        message_id,
        PositionUpdate { tick: 99 }.encode_to_vec(),
    );

    router.deliver_frame(frame).unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            if !received.lock().expect("received mutex poisoned").is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*received.lock().expect("received mutex poisoned"), vec![99]);
}

#[tokio::test]
async fn inbound_router_does_not_advance_sequence_when_mailbox_is_full() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handle = ActorRuntime::default()
        .spawn_actor(
            BlockingActor {
                received: received.clone(),
                entered: entered.clone(),
                release: release.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle.try_tell(linked_command(100)).unwrap();
    timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    handle.try_tell(linked_command(101)).unwrap();

    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-mailbox-full-sequence");
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap();
    let handle_for_router = handle.clone();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            stream.for_actor::<BlockingActor>(actor_kind!("Battle")),
            move |_| Some(handle_for_router.clone()),
        )
        .build();
    let message_id = descriptor.message_id_for::<InputCommand>().unwrap();
    let frame = || {
        DirectLinkFrame::message(
            link_id.clone(),
            LinkSequence(1),
            message_id,
            InputCommand { command_id: 11 }.encode_to_vec(),
        )
    };

    assert!(matches!(
        router.deliver_frame(frame()),
        Err(InboundDeliveryError::Delivery(
            DirectLinkDeliveryError::Mailbox(ActorTellError::MailboxFull)
        ))
    ));

    release.notify_waiters();
    wait_for_len(&received, 2).await;

    router.deliver_frame(frame()).unwrap();
    wait_for_len(&received, 3).await;
    assert_eq!(
        *received.lock().expect("received mutex poisoned"),
        vec![100, 101, 11]
    );
}

#[tokio::test]
async fn inbound_router_delivers_bidirectional_frames_to_each_direction_actor() {
    let battle_received = Arc::new(Mutex::new(Vec::new()));
    let gateway_received = Arc::new(Mutex::new(Vec::new()));
    let runtime = ActorRuntime::default();
    let battle_handle = runtime
        .spawn_actor(
            BattleActor {
                received: battle_received.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let gateway_handle = runtime
        .spawn_actor(
            GatewayActor {
                received: gateway_received.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
    let input_descriptor = input_stream.descriptor();
    let update_descriptor = update_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-bidirectional");
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Bidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: Some(OpenLinkDirection::from_stream(
                link_id.clone(),
                &update_descriptor,
            )),
            options: DirectLinkOptions::bidirectional(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager.clone())
        .bind_actor(
            input_stream.for_actor::<BattleActor>(actor_kind!("Battle")),
            move |_| Some(battle_handle.clone()),
        )
        .bind_actor(
            update_stream.for_actor::<GatewayActor>(actor_kind!("GatewaySession")),
            move |_| Some(gateway_handle.clone()),
        )
        .build();

    router
        .deliver_frame(DirectLinkFrame::message(
            link_id.clone(),
            LinkSequence(1),
            input_descriptor.message_id_for::<InputCommand>().unwrap(),
            InputCommand { command_id: 11 }.encode_to_vec(),
        ))
        .unwrap();
    router
        .deliver_frame(DirectLinkFrame::directed_message(
            link_id.clone(),
            LinkDirection::TargetToSource,
            LinkSequence(1),
            update_descriptor
                .message_id_for::<PositionUpdate>()
                .unwrap(),
            PositionUpdate { tick: 22 }.encode_to_vec(),
        ))
        .unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            let battle_done = !battle_received
                .lock()
                .expect("received mutex poisoned")
                .is_empty();
            let gateway_done = !gateway_received
                .lock()
                .expect("received mutex poisoned")
                .is_empty();
            if battle_done && gateway_done {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *battle_received.lock().expect("received mutex poisoned"),
        vec![11]
    );
    assert_eq!(
        *gateway_received.lock().expect("received mutex poisoned"),
        vec![22]
    );
    assert_eq!(
        manager.link_snapshot(&link_id).unwrap().directions,
        [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
            .into_iter()
            .collect()
    );
}

#[tokio::test]
async fn inbound_router_delivers_link_opened_and_actor_gets_target_to_source_handle() {
    let opened = Arc::new(Mutex::new(Vec::new()));
    let outbound = Arc::new(Mutex::new(None));
    let runtime = Arc::new(RecordingLinkRuntime::default());
    let mut service = ServiceContext::builder(service_kind!("Battle"), InstanceId::new("battle-1"));
    service
        .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
        .unwrap();
    let link_id = LinkId::new("link-opened");
    let target_ref = actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9);
    let handle = ActorRuntime::default()
        .spawn_actor(
            OpeningBattleActor {
                opened: opened.clone(),
                outbound: outbound.clone(),
            },
            ActorSpawnOptions {
                self_ref: Some(target_ref.clone()),
                service: service.build(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
    let input_descriptor = input_stream.descriptor();
    let update_descriptor = update_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
        .unwrap();
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: target_ref,
            mode: DirectLinkMode::Bidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: Some(OpenLinkDirection::from_stream(
                link_id.clone(),
                &update_descriptor,
            )),
            options: DirectLinkOptions::bidirectional(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<OpeningBattleActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    router.deliver_link_opened_to_target(&link_id).unwrap();

    timeout(Duration::from_secs(1), async {
        loop {
            if outbound
                .lock()
                .expect("outbound handle mutex poisoned")
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let opened = opened.lock().expect("opened messages mutex poisoned");
    assert_eq!(opened.len(), 1);
    assert_eq!(opened[0].mode, DirectLinkMode::Bidirectional);
    assert_eq!(opened[0].inbound_stream, "gateway-input");
    assert_eq!(opened[0].outbound_stream.as_deref(), Some("battle-update"));
    assert_eq!(
        *outbound.lock().expect("outbound handle mutex poisoned"),
        Some((LinkDirection::TargetToSource, "battle-update".to_string()))
    );
    assert_eq!(
        runtime
            .outbound_requests
            .lock()
            .expect("outbound requests mutex poisoned")
            .as_slice(),
        &[(link_id, BattleUpdateStream::descriptor())]
    );
}

#[tokio::test]
async fn process_open_link_frame_returns_ack_and_delivers_link_opened() {
    let opened = Arc::new(Mutex::new(Vec::new()));
    let outbound = Arc::new(Mutex::new(None));
    let runtime = Arc::new(RecordingLinkRuntime::default());
    let mut service = ServiceContext::builder(service_kind!("Battle"), InstanceId::new("battle-1"));
    service
        .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
        .unwrap();
    let link_id = LinkId::new("link-open-frame");
    let target_ref = actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9);
    let handle = ActorRuntime::default()
        .spawn_actor(
            OpeningBattleActor {
                opened: opened.clone(),
                outbound: outbound.clone(),
            },
            ActorSpawnOptions {
                self_ref: Some(target_ref.clone()),
                service: service.build(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
    let input_descriptor = input_stream.descriptor();
    let update_descriptor = update_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
        .unwrap();
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")])
            .require_peer_identity("lattice.test"),
    );
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<OpeningBattleActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    let response = router
        .process_open_link_frame(
            DirectLinkFrame::open_link_with_peer_identity(
                &OpenLinkRequest {
                    protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                    link_id: link_id.clone(),
                    source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                    target: target_ref,
                    mode: DirectLinkMode::Bidirectional,
                    source_to_target: OpenLinkDirection::from_stream(
                        link_id.clone(),
                        &input_descriptor,
                    ),
                    target_to_source: Some(OpenLinkDirection::from_stream(
                        link_id.clone(),
                        &update_descriptor,
                    )),
                    options: DirectLinkOptions::bidirectional(),
                },
                DirectLinkPeerIdentity::new(
                    service_kind!("Gateway"),
                    InstanceId::new("instance-7"),
                    "spiffe://lattice.test/svc/gateway/instance/instance-7",
                ),
            )
            .unwrap(),
            None,
        )
        .unwrap();

    assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkAck);
    let ack = response.decode_open_link_ack().unwrap();
    assert_eq!(ack.link_id, link_id);
    assert_eq!(ack.source_to_target.stream_name, "gateway-input");
    assert_eq!(
        ack.target_to_source
            .as_ref()
            .expect("target-to-source negotiation")
            .stream_name,
        "battle-update"
    );
    wait_for_len(&opened, 1).await;
    let opened = opened.lock().expect("opened messages mutex poisoned");
    assert_eq!(opened[0].link_id, link_id);
    assert_eq!(opened[0].mode, DirectLinkMode::Bidirectional);
    assert_eq!(opened[0].inbound_stream, "gateway-input");
    assert_eq!(opened[0].outbound_stream.as_deref(), Some("battle-update"));
    assert_eq!(
        *outbound.lock().expect("outbound handle mutex poisoned"),
        Some((LinkDirection::TargetToSource, "battle-update".to_string()))
    );
}

#[tokio::test]
async fn process_open_link_frame_rejects_missing_required_peer_identity() {
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::lazy(None));
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")])
            .require_peer_identity("lattice.test"),
    );
    let link_id = LinkId::new("link-open-missing-identity");
    let router = DirectLinkInboundRouter::builder(manager).build();

    let response = router
        .process_open_link_frame(
            DirectLinkFrame::open_link(&OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap(),
            None,
        )
        .unwrap();

    assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkReject);
    let reject = response.decode_open_link_reject().unwrap();
    assert_eq!(reject.reason, OpenLinkRejectReason::Unauthorized);
}

#[tokio::test]
async fn process_open_link_frame_rejects_when_link_open_delivery_fails() {
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    manager.register_actor(actor_kind!("Battle"), DirectLinkActorPolicy::lazy(None));
    manager.set_validation_policy(
        OpenLinkValidationPolicy::hosted(service_kind!("Battle"))
            .authorize_sources([service_kind!("Gateway")]),
    );
    let link_id = LinkId::new("link-open-delivery-fails");
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            stream.for_actor::<BattleActor>(actor_kind!("Battle")),
            |_| None,
        )
        .build();

    let response = router
        .process_open_link_frame(
            DirectLinkFrame::open_link(&OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Unidirectional,
                source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
                target_to_source: None,
                options: DirectLinkOptions::default(),
            })
            .unwrap(),
            None,
        )
        .unwrap();

    assert_eq!(response.kind, DirectLinkFrameKind::OpenLinkReject);
    let reject = response.decode_open_link_reject().unwrap();
    assert_eq!(reject.link_id, link_id);
    assert_eq!(reject.reason, OpenLinkRejectReason::ActorUnavailable);
}

#[tokio::test]
async fn inbound_router_emits_direction_and_link_closed_once_per_transition() {
    let target_direction_closed = Arc::new(Mutex::new(Vec::new()));
    let source_direction_closed = Arc::new(Mutex::new(Vec::new()));
    let target_link_closed = Arc::new(Mutex::new(Vec::new()));
    let source_link_closed = Arc::new(Mutex::new(Vec::new()));
    let runtime = ActorRuntime::default();
    let target_handle = runtime
        .spawn_actor(
            ClosingActor {
                direction_closed: target_direction_closed.clone(),
                link_closed: target_link_closed.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let source_handle = runtime
        .spawn_actor(
            ClosingActor {
                direction_closed: source_direction_closed.clone(),
                link_closed: source_link_closed.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
    let input_descriptor = input_stream.descriptor();
    let update_descriptor = update_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-close-events");
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Bidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: Some(OpenLinkDirection::from_stream(
                link_id.clone(),
                &update_descriptor,
            )),
            options: DirectLinkOptions::bidirectional(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
            move |_| Some(target_handle.clone()),
        )
        .bind_actor(
            update_stream.for_actor::<ClosingActor>(actor_kind!("GatewaySession")),
            move |_| Some(source_handle.clone()),
        )
        .build();

    router
        .close_direction(
            &link_id,
            LinkDirection::SourceToTarget,
            LinkCloseReason::Done,
        )
        .unwrap();
    router
        .close_direction(
            &link_id,
            LinkDirection::SourceToTarget,
            LinkCloseReason::Done,
        )
        .unwrap();
    wait_for_len(&target_direction_closed, 1).await;
    assert_eq!(
        target_direction_closed
            .lock()
            .expect("direction closed mutex poisoned")
            .len(),
        1
    );

    router
        .close_direction(
            &link_id,
            LinkDirection::TargetToSource,
            LinkCloseReason::Done,
        )
        .unwrap();
    router
        .close_direction(
            &link_id,
            LinkDirection::TargetToSource,
            LinkCloseReason::Done,
        )
        .unwrap();
    wait_for_len(&source_direction_closed, 1).await;
    wait_for_len(&target_link_closed, 1).await;
    wait_for_len(&source_link_closed, 1).await;

    assert_eq!(
        source_direction_closed
            .lock()
            .expect("direction closed mutex poisoned")
            .len(),
        1
    );
    assert_eq!(
        target_link_closed
            .lock()
            .expect("link closed mutex poisoned")
            .len(),
        1
    );
    assert_eq!(
        source_link_closed
            .lock()
            .expect("link closed mutex poisoned")
            .len(),
        1
    );
    assert_eq!(
        target_direction_closed
            .lock()
            .expect("direction closed mutex poisoned")[0]
            .stream,
        "gateway-input"
    );
    assert_eq!(
        source_direction_closed
            .lock()
            .expect("direction closed mutex poisoned")[0]
            .stream,
        "battle-update"
    );
}

#[tokio::test]
async fn inbound_router_close_all_emits_structured_reasons_once() {
    for reason in [
        LinkCloseReason::HeartbeatTimeout,
        LinkCloseReason::ProtocolError("invalid sequence".to_string()),
        LinkCloseReason::TargetPassivated,
        LinkCloseReason::TargetMigrating,
        LinkCloseReason::NodeDraining,
        LinkCloseReason::ConnectionLost,
    ] {
        let target_direction_closed = Arc::new(Mutex::new(Vec::new()));
        let source_direction_closed = Arc::new(Mutex::new(Vec::new()));
        let target_link_closed = Arc::new(Mutex::new(Vec::new()));
        let source_link_closed = Arc::new(Mutex::new(Vec::new()));
        let runtime = ActorRuntime::default();
        let target_handle = runtime
            .spawn_actor(
                ClosingActor {
                    direction_closed: target_direction_closed.clone(),
                    link_closed: target_link_closed.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let source_handle = runtime
            .spawn_actor(
                ClosingActor {
                    direction_closed: source_direction_closed.clone(),
                    link_closed: source_link_closed.clone(),
                },
                Default::default(),
            )
            .await
            .unwrap();
        let manager = Arc::new(DirectLinkSessionManager::new());
        let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
        let update_stream = DirectLinkStream::new("battle-update").message::<PositionUpdate>();
        let input_descriptor = input_stream.descriptor();
        let update_descriptor = update_stream.descriptor();
        manager
            .register_binding(actor_kind!("Battle"), input_descriptor.clone())
            .unwrap();
        manager
            .register_binding(actor_kind!("GatewaySession"), update_descriptor.clone())
            .unwrap();
        let link_id = LinkId::new(format!("link-close-all-{reason:?}"));
        manager
            .open_link(OpenLinkRequest {
                protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
                link_id: link_id.clone(),
                source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
                target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
                mode: DirectLinkMode::Bidirectional,
                source_to_target: OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &input_descriptor,
                ),
                target_to_source: Some(OpenLinkDirection::from_stream(
                    link_id.clone(),
                    &update_descriptor,
                )),
                options: DirectLinkOptions::bidirectional(),
            })
            .unwrap();
        let router = DirectLinkInboundRouter::builder(manager)
            .bind_actor(
                input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
                move |_| Some(target_handle.clone()),
            )
            .bind_actor(
                update_stream.for_actor::<ClosingActor>(actor_kind!("GatewaySession")),
                move |_| Some(source_handle.clone()),
            )
            .build();

        router.close_all(&link_id, reason.clone()).unwrap();
        router.close_all(&link_id, reason.clone()).unwrap();

        wait_for_len(&target_direction_closed, 1).await;
        wait_for_len(&source_direction_closed, 1).await;
        wait_for_len(&target_link_closed, 1).await;
        wait_for_len(&source_link_closed, 1).await;
        assert_eq!(
            target_direction_closed
                .lock()
                .expect("direction closed mutex poisoned")
                .len(),
            1
        );
        assert_eq!(
            source_direction_closed
                .lock()
                .expect("direction closed mutex poisoned")
                .len(),
            1
        );
        assert_eq!(
            target_link_closed
                .lock()
                .expect("link closed mutex poisoned")
                .as_slice(),
            &[LinkClosed {
                link_id: link_id.clone(),
                reason: reason.clone(),
                closed_directions: [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
                    .into_iter()
                    .collect(),
                last_sequence_seen: None,
            }]
        );
        assert_eq!(
            source_link_closed
                .lock()
                .expect("link closed mutex poisoned")
                .as_slice(),
            &[LinkClosed {
                link_id,
                reason,
                closed_directions: [LinkDirection::SourceToTarget, LinkDirection::TargetToSource]
                    .into_iter()
                    .collect(),
                last_sequence_seen: None,
            }]
        );
    }
}

#[tokio::test]
async fn heartbeat_and_ack_refresh_liveness_before_idle_timeout_close() {
    let direction_closed = Arc::new(Mutex::new(Vec::new()));
    let link_closed = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            ClosingActor {
                direction_closed: direction_closed.clone(),
                link_closed: link_closed.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let input_descriptor = input_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-heartbeat");
    let mut options = DirectLinkOptions::unidirectional();
    options.idle_timeout = Duration::from_secs(30);
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: None,
            options,
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<ClosingActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();
    let heartbeat_at = Instant::now() + Duration::from_secs(10);

    router
        .process_frame_at(DirectLinkFrame::heartbeat(link_id.clone()), heartbeat_at)
        .unwrap();
    assert_eq!(
        router
            .close_idle_links_at(heartbeat_at + Duration::from_secs(29))
            .unwrap(),
        0
    );
    router
        .process_frame_at(
            DirectLinkFrame::heartbeat_ack(link_id.clone()),
            heartbeat_at + Duration::from_secs(29),
        )
        .unwrap();
    assert_eq!(
        router
            .close_idle_links_at(heartbeat_at + Duration::from_secs(58))
            .unwrap(),
        0
    );
    assert_eq!(
        router
            .close_idle_links_at(heartbeat_at + Duration::from_secs(59))
            .unwrap(),
        1
    );

    wait_for_len(&direction_closed, 1).await;
    wait_for_len(&link_closed, 1).await;
    assert_eq!(
        direction_closed
            .lock()
            .expect("direction closed mutex poisoned")[0]
            .reason,
        LinkCloseReason::HeartbeatTimeout
    );
    assert_eq!(
        link_closed.lock().expect("link closed mutex poisoned")[0].reason,
        LinkCloseReason::HeartbeatTimeout
    );
}

#[tokio::test]
async fn process_frame_closes_invalid_message_frames_with_protocol_error() {
    for (name, frame) in [
        ("wrong direction", ProtocolErrorFrame::WrongDirection),
        (
            "unsupported message type",
            ProtocolErrorFrame::UnsupportedMessageType,
        ),
        ("decode error", ProtocolErrorFrame::DecodeError),
    ] {
        let link_id = LinkId::new(format!("link-protocol-error-{name}"));
        let (router, descriptor, received, link_closed) =
            protocol_error_test_router(link_id.clone()).await;
        let message_id = descriptor.message_id_for::<InputCommand>().unwrap();
        let frame = match frame {
            ProtocolErrorFrame::WrongDirection => DirectLinkFrame::directed_message(
                link_id.clone(),
                LinkDirection::TargetToSource,
                LinkSequence(1),
                message_id,
                InputCommand { command_id: 11 }.encode_to_vec(),
            ),
            ProtocolErrorFrame::UnsupportedMessageType => DirectLinkFrame::message(
                link_id.clone(),
                LinkSequence(1),
                DirectLinkMessageId(999),
                InputCommand { command_id: 11 }.encode_to_vec(),
            ),
            ProtocolErrorFrame::DecodeError => DirectLinkFrame::message(
                link_id.clone(),
                LinkSequence(1),
                message_id,
                b"not protobuf".to_vec(),
            ),
        };

        assert!(router.process_frame(frame).is_err());
        wait_for_len(&link_closed, 1).await;
        assert!(received.lock().expect("received mutex poisoned").is_empty());
        let event = link_closed.lock().expect("link closed mutex poisoned")[0].clone();
        assert_eq!(event.link_id, link_id);
        assert!(matches!(
            event.reason,
            LinkCloseReason::ProtocolError(ref reason) if reason.contains(name)
        ));
    }
}

#[tokio::test]
async fn process_frame_closes_remote_protocol_error_frame() {
    let link_id = LinkId::new("link-remote-protocol-error");
    let (router, _descriptor, _received, link_closed) =
        protocol_error_test_router(link_id.clone()).await;
    let frame = DirectLinkFrame {
        kind: DirectLinkFrameKind::ProtocolError,
        link_id: link_id.clone(),
        sequence: LinkSequence(0),
        message_id: None,
        flags: LinkMessageFlags::EMPTY,
        header: Vec::new(),
        payload: b"remote invalid sequence".to_vec(),
    };

    router.process_frame(frame).unwrap();
    wait_for_len(&link_closed, 1).await;
    let event = link_closed.lock().expect("link closed mutex poisoned")[0].clone();
    assert_eq!(event.link_id, link_id);
    assert_eq!(
        event.reason,
        LinkCloseReason::ProtocolError("remote invalid sequence".to_string())
    );
}

#[tokio::test]
async fn inbound_backpressure_drop_newest_emits_event_without_mailbox_delivery() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let backpressure = Arc::new(Mutex::new(Vec::new()));
    let direction_closed = Arc::new(Mutex::new(Vec::new()));
    let link_closed = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            BackpressureActor {
                received: received.clone(),
                backpressure: backpressure.clone(),
                direction_closed,
                link_closed,
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let input_descriptor = input_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-inbound-drop-newest");
    let mut options = DirectLinkOptions::unidirectional();
    options.backpressure = BackpressurePolicy::DropNewest { max_pending: 0 };
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: None,
            options,
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager.clone())
        .bind_actor(
            input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    router
        .deliver_frame(DirectLinkFrame::message(
            link_id.clone(),
            LinkSequence(1),
            input_descriptor.message_id_for::<InputCommand>().unwrap(),
            InputCommand { command_id: 11 }.encode_to_vec(),
        ))
        .unwrap();

    wait_for_len(&backpressure, 1).await;
    assert!(received.lock().expect("received mutex poisoned").is_empty());
    let events = backpressure.lock().expect("backpressure mutex poisoned");
    assert_eq!(events[0].link_id, link_id);
    assert_eq!(
        events[0].policy,
        BackpressurePolicy::DropNewest { max_pending: 0 }
    );
    assert_eq!(events[0].pending, 0);
    assert_eq!(events[0].dropped, 1);
    assert_eq!(manager.metrics().snapshot().dropped, 1);
    assert_eq!(manager.metrics().snapshot().backpressure_events, 1);
}

#[tokio::test]
async fn inbound_backpressure_disconnect_closes_link_with_event() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let backpressure = Arc::new(Mutex::new(Vec::new()));
    let direction_closed = Arc::new(Mutex::new(Vec::new()));
    let link_closed = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            BackpressureActor {
                received: received.clone(),
                backpressure: backpressure.clone(),
                direction_closed: direction_closed.clone(),
                link_closed: link_closed.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let input_descriptor = input_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-inbound-disconnect");
    let mut options = DirectLinkOptions::unidirectional();
    options.backpressure = BackpressurePolicy::Disconnect { max_pending: 0 };
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &input_descriptor),
            target_to_source: None,
            options,
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager.clone())
        .bind_actor(
            input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    assert!(matches!(
        router.deliver_frame(DirectLinkFrame::message(
            link_id.clone(),
            LinkSequence(1),
            input_descriptor.message_id_for::<InputCommand>().unwrap(),
            InputCommand { command_id: 11 }.encode_to_vec(),
        )),
        Err(InboundDeliveryError::BackpressureExceeded)
    ));

    wait_for_len(&backpressure, 1).await;
    wait_for_len(&direction_closed, 1).await;
    wait_for_len(&link_closed, 1).await;
    assert!(received.lock().expect("received mutex poisoned").is_empty());
    assert_eq!(
        direction_closed
            .lock()
            .expect("direction closed mutex poisoned")[0]
            .reason,
        LinkCloseReason::BackpressureExceeded
    );
    assert_eq!(
        link_closed.lock().expect("link closed mutex poisoned")[0].reason,
        LinkCloseReason::BackpressureExceeded
    );
    assert_eq!(manager.metrics().snapshot().closed, 1);
    assert_eq!(manager.metrics().snapshot().backpressure_events, 1);
}

#[test]
fn inbound_router_rejects_unbound_actor_kind() {
    let manager = Arc::new(DirectLinkSessionManager::new());
    let stream = DirectLinkStream::new("movement").message::<PositionUpdate>();
    let descriptor = stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), descriptor.clone())
        .unwrap();
    let link_id = LinkId::new("link-unbound");
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id.clone(), &descriptor),
            target_to_source: None,
            options: DirectLinkOptions::default(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager).build();
    let frame = DirectLinkFrame::message(
        link_id,
        LinkSequence(1),
        descriptor.message_id_for::<PositionUpdate>().unwrap(),
        PositionUpdate { tick: 1 }.encode_to_vec(),
    );

    assert!(matches!(
        router.deliver_frame(frame),
        Err(InboundDeliveryError::UnboundActorKind { .. })
    ));
}

enum ProtocolErrorFrame {
    WrongDirection,
    UnsupportedMessageType,
    DecodeError,
}

async fn protocol_error_test_router(
    link_id: LinkId,
) -> (
    DirectLinkInboundRouter,
    DirectLinkStreamDescriptor,
    Arc<Mutex<Vec<u64>>>,
    Arc<Mutex<Vec<LinkClosed>>>,
) {
    let received = Arc::new(Mutex::new(Vec::new()));
    let backpressure = Arc::new(Mutex::new(Vec::new()));
    let direction_closed = Arc::new(Mutex::new(Vec::new()));
    let link_closed = Arc::new(Mutex::new(Vec::new()));
    let handle = ActorRuntime::default()
        .spawn_actor(
            BackpressureActor {
                received: received.clone(),
                backpressure,
                direction_closed,
                link_closed: link_closed.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let manager = Arc::new(DirectLinkSessionManager::new());
    let input_stream = DirectLinkStream::new("gateway-input").message::<InputCommand>();
    let input_descriptor = input_stream.descriptor();
    manager
        .register_binding(actor_kind!("Battle"), input_descriptor.clone())
        .unwrap();
    manager
        .open_link(OpenLinkRequest {
            protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
            link_id: link_id.clone(),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            mode: DirectLinkMode::Unidirectional,
            source_to_target: OpenLinkDirection::from_stream(link_id, &input_descriptor),
            target_to_source: None,
            options: DirectLinkOptions::unidirectional(),
        })
        .unwrap();
    let router = DirectLinkInboundRouter::builder(manager)
        .bind_actor(
            input_stream.for_actor::<BackpressureActor>(actor_kind!("Battle")),
            move |_| Some(handle.clone()),
        )
        .build();

    (router, input_descriptor, received, link_closed)
}

fn actor_ref(service_kind: ServiceKind, actor_kind: ActorKind, id: u64) -> ActorRef {
    ActorRef::direct(
        service_kind,
        actor_kind,
        ActorId::U64(id),
        InstanceId::new(format!("instance-{id}")),
        "http://127.0.0.1:10000".parse().unwrap(),
        None,
    )
}

fn linked_command(command_id: u64) -> Linked<InputCommand> {
    Linked {
        payload: InputCommand { command_id },
        metadata: (),
        context: LinkMessageContext {
            link_id: LinkId::new("prefill"),
            source: actor_ref(service_kind!("Gateway"), actor_kind!("GatewaySession"), 7),
            target: actor_ref(service_kind!("Battle"), actor_kind!("Battle"), 9),
            sequence: 0,
            received_at: Instant::now(),
            flags: LinkMessageFlags::EMPTY,
        },
    }
}

async fn wait_for_len<T>(items: &Arc<Mutex<Vec<T>>>, expected: usize) {
    timeout(Duration::from_secs(1), async {
        loop {
            if items.lock().expect("items mutex poisoned").len() >= expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
