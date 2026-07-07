use crate::inbound::*;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorTellError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::traits::{Actor, Handler};
use lattice_core::actor_ref::ActorRef;
use lattice_core::direct_link::errors::{LinkError, LinkSendError};
use lattice_core::direct_link::ids::{DirectLinkMessageId, LinkId, LinkSequence};
use lattice_core::direct_link::messages::{
    LinkBackpressure, LinkClosed, LinkDirectionClosed, LinkMessageContext, LinkMessageFlags,
    LinkOpened, Linked,
};
use lattice_core::direct_link::options::{
    BackpressurePolicy, DirectLinkMode, DirectLinkOptions, LinkCloseReason, LinkDirection,
};
use lattice_core::direct_link::runtime::{
    DirectLinkOpenRequest, DirectLinkRuntime, DirectLinkRuntimeHandle, DirectLinkSender,
    DirectLinkSession, OutboundDirectLinkMessage,
};
use lattice_core::direct_link::stream::{
    DirectLinkMessage, DirectLinkStreamDescriptor, DirectLinkStreamType,
};
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_core::service_context::ServiceContext;
use lattice_core::{actor_kind, service_kind};
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
impl Actor for BattleActor {
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
impl Actor for GatewayActor {
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
        _request: DirectLinkOpenRequest,
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

    async fn close_all(&self, _link_id: LinkId, _reason: LinkCloseReason) -> Result<(), LinkError> {
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

    async fn close(&self, _reason: LinkCloseReason) -> Result<(), LinkSendError> {
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
impl Actor for OpeningBattleActor {
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
impl Actor for ClosingActor {
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
impl Actor for BackpressureActor {
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
impl Actor for BlockingActor {
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

mod backpressure_and_helpers;
mod delivery;
mod lifecycle;
