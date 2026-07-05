use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use lattice_actor::{Actor, ActorHandle, Handler};
use lattice_core::{
    ActorKind, ActorRef, DirectLinkMessageId, LinkCloseReason, LinkClosed, LinkDirection,
    LinkDirectionClosed, LinkId, LinkMessageContext, LinkMessageFlags, LinkOpened,
};
use thiserror::Error;

use crate::codec::{DirectLinkFrame, DirectLinkFrameKind};
use crate::delivery::{DirectLinkDeliveryError, DirectLinkDispatch};
use crate::session::{
    CloseTransition, DirectLinkSessionManager, ManagedLinkSnapshot, MessageFrameError,
    SessionManagerError,
};
use crate::stream::DirectLinkActorBinding;

#[derive(Debug, Error)]
pub enum InboundDeliveryError {
    #[error("direct-link frame kind is not a message")]
    NotMessageFrame,
    #[error("direct-link message frame is missing a message id")]
    MissingMessageId,
    #[error("direct-link actor kind is not bound: {actor_kind:?}")]
    UnboundActorKind { actor_kind: ActorKind },
    #[error("direct-link target actor is unavailable")]
    ActorUnavailable,
    #[error("direct-link open event is unavailable")]
    LinkOpenUnavailable,
    #[error(transparent)]
    Frame(#[from] MessageFrameError),
    #[error(transparent)]
    Session(#[from] SessionManagerError),
    #[error(transparent)]
    Delivery(#[from] DirectLinkDeliveryError),
}

pub struct DirectLinkInboundRouter {
    session_manager: Arc<DirectLinkSessionManager>,
    bindings: HashMap<ActorKind, Box<dyn ErasedInboundBinding>>,
}

impl fmt::Debug for DirectLinkInboundRouter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectLinkInboundRouter")
            .field("binding_count", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

impl DirectLinkInboundRouter {
    pub fn builder(
        session_manager: Arc<DirectLinkSessionManager>,
    ) -> DirectLinkInboundRouterBuilder {
        DirectLinkInboundRouterBuilder {
            session_manager,
            bindings: HashMap::new(),
        }
    }

    pub fn deliver_frame(&self, frame: DirectLinkFrame) -> Result<(), InboundDeliveryError> {
        if frame.kind != DirectLinkFrameKind::Message {
            return Err(InboundDeliveryError::NotMessageFrame);
        }
        let direction = frame.direction();
        let message_id = frame
            .message_id
            .ok_or(InboundDeliveryError::MissingMessageId)?;
        self.session_manager.validate_message_frame(
            &frame.link_id,
            direction,
            message_id,
            frame.sequence,
        )?;
        let snapshot = self
            .session_manager
            .link_snapshot(&frame.link_id)
            .ok_or(MessageFrameError::UnknownLink)?;
        let actor_ref = actor_for_direction(&snapshot, direction).clone();
        let binding = self.bindings.get(&actor_ref.actor_kind).ok_or_else(|| {
            InboundDeliveryError::UnboundActorKind {
                actor_kind: actor_ref.actor_kind.clone(),
            }
        })?;
        let context = LinkMessageContext {
            link_id: frame.link_id,
            source: snapshot.source,
            target: snapshot.target,
            sequence: frame.sequence.0,
            received_at: std::time::Instant::now(),
            flags: LinkMessageFlags::from_bits(frame.flags.bits()),
        };
        binding.deliver(&actor_ref, message_id, &frame.payload, context)
    }

    pub fn deliver_link_opened_to_target(
        &self,
        link_id: &LinkId,
    ) -> Result<(), InboundDeliveryError> {
        let snapshot = self
            .session_manager
            .link_snapshot(link_id)
            .ok_or(InboundDeliveryError::LinkOpenUnavailable)?;
        let opened = self
            .session_manager
            .link_opened_for_actor(link_id, &snapshot.target)
            .ok_or(InboundDeliveryError::LinkOpenUnavailable)?;
        let binding = self
            .bindings
            .get(&snapshot.target.actor_kind)
            .ok_or_else(|| InboundDeliveryError::UnboundActorKind {
                actor_kind: snapshot.target.actor_kind.clone(),
            })?;
        binding.deliver_link_opened(&snapshot.target, opened)
    }

    pub fn close_direction(
        &self,
        link_id: &LinkId,
        direction: LinkDirection,
        reason: LinkCloseReason,
    ) -> Result<(), InboundDeliveryError> {
        let snapshot = self
            .session_manager
            .link_snapshot(link_id)
            .ok_or(InboundDeliveryError::LinkOpenUnavailable)?;
        match self
            .session_manager
            .close_direction(link_id, direction, reason)?
        {
            CloseTransition::AlreadyClosed => Ok(()),
            CloseTransition::DirectionClosed(event) => {
                let actor_ref = actor_for_direction(&snapshot, event.direction);
                self.deliver_direction_closed(actor_ref, event)
            }
            CloseTransition::LinkClosed {
                direction_closed,
                link_closed,
            } => {
                let actor_ref = actor_for_direction(&snapshot, direction_closed.direction);
                self.deliver_direction_closed(actor_ref, direction_closed)?;
                self.deliver_link_closed_to_bound_actors(&snapshot, link_closed)
            }
        }
    }

    fn deliver_direction_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), InboundDeliveryError> {
        let binding = self.bindings.get(&actor_ref.actor_kind).ok_or_else(|| {
            InboundDeliveryError::UnboundActorKind {
                actor_kind: actor_ref.actor_kind.clone(),
            }
        })?;
        binding.deliver_direction_closed(actor_ref, event)
    }

    fn deliver_link_closed_to_bound_actors(
        &self,
        snapshot: &ManagedLinkSnapshot,
        event: LinkClosed,
    ) -> Result<(), InboundDeliveryError> {
        for actor_ref in [&snapshot.source, &snapshot.target] {
            if let Some(binding) = self.bindings.get(&actor_ref.actor_kind) {
                binding.deliver_link_closed(actor_ref, event.clone())?;
            }
        }
        Ok(())
    }
}

pub struct DirectLinkInboundRouterBuilder {
    session_manager: Arc<DirectLinkSessionManager>,
    bindings: HashMap<ActorKind, Box<dyn ErasedInboundBinding>>,
}

impl DirectLinkInboundRouterBuilder {
    pub fn bind_actor<A, Messages, F>(
        mut self,
        binding: DirectLinkActorBinding<A, Messages>,
        resolver: F,
    ) -> Self
    where
        A: Actor,
        Messages: DirectLinkDispatch<A>,
        A: Handler<LinkOpened> + Handler<LinkDirectionClosed> + Handler<LinkClosed>,
        F: Fn(&ActorRef) -> Option<ActorHandle<A>> + Send + Sync + 'static,
    {
        self.bindings.insert(
            binding.actor_kind().clone(),
            Box::new(TypedInboundBinding {
                binding,
                resolver: Arc::new(resolver),
                _actor: PhantomData,
            }),
        );
        self
    }

    pub fn build(self) -> DirectLinkInboundRouter {
        DirectLinkInboundRouter {
            session_manager: self.session_manager,
            bindings: self.bindings,
        }
    }
}

trait ErasedInboundBinding: Send + Sync + 'static {
    fn deliver(
        &self,
        actor_ref: &ActorRef,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        context: LinkMessageContext,
    ) -> Result<(), InboundDeliveryError>;

    fn deliver_link_opened(
        &self,
        actor_ref: &ActorRef,
        opened: LinkOpened,
    ) -> Result<(), InboundDeliveryError>;

    fn deliver_direction_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), InboundDeliveryError>;

    fn deliver_link_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkClosed,
    ) -> Result<(), InboundDeliveryError>;
}

type ActorResolver<A> = dyn Fn(&ActorRef) -> Option<ActorHandle<A>> + Send + Sync;

struct TypedInboundBinding<A, Messages>
where
    A: Actor,
{
    binding: DirectLinkActorBinding<A, Messages>,
    resolver: Arc<ActorResolver<A>>,
    _actor: PhantomData<fn() -> A>,
}

impl<A, Messages> ErasedInboundBinding for TypedInboundBinding<A, Messages>
where
    A: Actor + Handler<LinkOpened> + Handler<LinkDirectionClosed> + Handler<LinkClosed>,
    Messages: DirectLinkDispatch<A>,
{
    fn deliver(
        &self,
        actor_ref: &ActorRef,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        context: LinkMessageContext,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        self.binding
            .try_deliver(&handle, message_id, payload, context)
            .map_err(Into::into)
    }

    fn deliver_link_opened(
        &self,
        actor_ref: &ActorRef,
        opened: LinkOpened,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        handle
            .try_tell(opened)
            .map_err(DirectLinkDeliveryError::from)
            .map_err(Into::into)
    }

    fn deliver_direction_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkDirectionClosed,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        handle
            .try_tell(event)
            .map_err(DirectLinkDeliveryError::from)
            .map_err(Into::into)
    }

    fn deliver_link_closed(
        &self,
        actor_ref: &ActorRef,
        event: LinkClosed,
    ) -> Result<(), InboundDeliveryError> {
        let handle = (self.resolver)(actor_ref).ok_or(InboundDeliveryError::ActorUnavailable)?;
        handle
            .try_tell(event)
            .map_err(DirectLinkDeliveryError::from)
            .map_err(Into::into)
    }
}

fn actor_for_direction(snapshot: &ManagedLinkSnapshot, direction: LinkDirection) -> &ActorRef {
    match direction {
        LinkDirection::SourceToTarget => &snapshot.target,
        LinkDirection::TargetToSource => &snapshot.source,
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use lattice_actor::{ActorContext, ActorRuntime, ActorSpawnOptions, Handler};
    use lattice_core::{
        ActorId, ActorKind, ActorRef, DirectLinkMessage, DirectLinkMode, DirectLinkOptions,
        DirectLinkRuntime, DirectLinkRuntimeHandle, DirectLinkSender, DirectLinkSession,
        DirectLinkStreamDescriptor, DirectLinkStreamType, InstanceId, LinkCloseReason, LinkClosed,
        LinkDirection, LinkDirectionClosed, LinkError, LinkId, LinkOpened, LinkSendError,
        LinkSequence, Linked, OutboundDirectLinkMessage, ServiceContext, ServiceKind, actor_kind,
        service_kind,
    };
    use prost::Message as _;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::codec::DirectLinkFrame;
    use crate::session::{DIRECT_LINK_PROTOCOL_VERSION, OpenLinkDirection, OpenLinkRequest};
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
        let mut service =
            ServiceContext::builder(service_kind!("Battle"), InstanceId::new("battle-1"));
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
}
