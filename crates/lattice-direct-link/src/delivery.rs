use lattice_actor::{Actor, ActorHandle, ActorTellError, Handler};
use lattice_core::{
    DirectLinkMessage, DirectLinkMessageId, DirectLinkStreamDescriptor, LinkMessageContext, Linked,
};
use thiserror::Error;

pub fn try_deliver_linked<A, T>(
    handle: &ActorHandle<A>,
    payload: T,
    context: LinkMessageContext,
) -> Result<(), ActorTellError>
where
    A: Actor + Handler<Linked<T>>,
    T: Send + 'static,
{
    handle.try_tell(Linked { payload, context })
}

#[derive(Debug, Error)]
pub enum DirectLinkDeliveryError {
    #[error("direct-link message type is not supported by this actor binding")]
    UnsupportedMessageType,
    #[error("failed to decode direct-link message payload: {0}")]
    Decode(String),
    #[error(transparent)]
    Mailbox(#[from] ActorTellError),
}

pub trait DirectLinkDispatch<A>: Send + Sync + 'static
where
    A: Actor,
{
    fn try_dispatch(
        handle: &ActorHandle<A>,
        stream: &DirectLinkStreamDescriptor,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        context: LinkMessageContext,
    ) -> Result<(), DirectLinkDeliveryError>;
}

impl<A> DirectLinkDispatch<A> for ()
where
    A: Actor,
{
    fn try_dispatch(
        _handle: &ActorHandle<A>,
        _stream: &DirectLinkStreamDescriptor,
        _message_id: DirectLinkMessageId,
        _payload: &[u8],
        _context: LinkMessageContext,
    ) -> Result<(), DirectLinkDeliveryError> {
        Err(DirectLinkDeliveryError::UnsupportedMessageType)
    }
}

impl<A, Head, Tail> DirectLinkDispatch<A> for (Head, Tail)
where
    A: Actor + Handler<Linked<Head>>,
    Head: DirectLinkMessage,
    Tail: DirectLinkDispatch<A>,
{
    fn try_dispatch(
        handle: &ActorHandle<A>,
        stream: &DirectLinkStreamDescriptor,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        context: LinkMessageContext,
    ) -> Result<(), DirectLinkDeliveryError> {
        if stream.message_id_for::<Head>() == Some(message_id) {
            let payload = Head::decode(payload)
                .map_err(|error| DirectLinkDeliveryError::Decode(error.to_string()))?;
            return try_deliver_linked(handle, payload, context).map_err(Into::into);
        }
        Tail::try_dispatch(handle, stream, message_id, payload, context)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use async_trait::async_trait;
    use http::Uri;
    use lattice_actor::{ActorContext, ActorRuntime};
    use lattice_core::{
        ActorId, ActorKind, ActorRef, DirectLinkMessage, DirectLinkMessageId, InstanceId, LinkId,
        LinkMessageFlags, LinkSequence, ServiceKind,
    };
    use prost::Message as _;
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::stream::DirectLinkStream;

    #[derive(Clone, PartialEq, prost::Message)]
    struct PositionUpdate {
        #[prost(uint64, tag = "1")]
        tick: u64,
    }

    impl DirectLinkMessage for PositionUpdate {
        const PROTO_FULL_NAME: &'static str = "game.PositionUpdate";
    }

    struct LinkActor {
        started: Arc<Notify>,
        release: Arc<Notify>,
        received: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl lattice_actor::Actor for LinkActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<PositionUpdate>> for LinkActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Linked<PositionUpdate>,
        ) -> Result<(), Self::Error> {
            self.started.notify_waiters();
            self.release.notified().await;
            self.received
                .lock()
                .expect("received mutex poisoned")
                .push(msg.payload.tick);
            Ok(())
        }
    }

    #[tokio::test]
    async fn direct_link_delivery_enqueues_linked_message_without_waiting_for_handler() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let received = Arc::new(Mutex::new(Vec::new()));
        let actor = LinkActor {
            started: started.clone(),
            release: release.clone(),
            received: received.clone(),
        };
        let handle = ActorRuntime::default()
            .spawn_actor(actor, Default::default())
            .await
            .unwrap();

        try_deliver_linked(
            &handle,
            PositionUpdate { tick: 42 },
            link_context(LinkId::new("link-1")),
        )
        .unwrap();
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        assert!(received.lock().expect("received mutex poisoned").is_empty());

        release.notify_waiters();
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

        assert_eq!(*received.lock().expect("received mutex poisoned"), vec![42]);
    }

    #[tokio::test]
    async fn actor_binding_dispatch_decodes_by_message_id_and_enqueues() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let received = Arc::new(Mutex::new(Vec::new()));
        let actor = LinkActor {
            started: started.clone(),
            release: release.clone(),
            received: received.clone(),
        };
        let handle = ActorRuntime::default()
            .spawn_actor(actor, Default::default())
            .await
            .unwrap();
        let stream = DirectLinkStream::new("movement").message::<PositionUpdate>();
        let binding = stream.for_actor::<LinkActor>(ActorKind::from_static("Battle"));
        let descriptor = stream.descriptor();
        let message_id = descriptor.message_id_for::<PositionUpdate>().unwrap();

        assert!(matches!(
            binding.try_deliver(
                &handle,
                DirectLinkMessageId(999),
                &PositionUpdate { tick: 7 }.encode_to_vec(),
                link_context(LinkId::new("unsupported")),
            ),
            Err(DirectLinkDeliveryError::UnsupportedMessageType)
        ));
        assert!(matches!(
            binding.try_deliver(
                &handle,
                message_id,
                b"not protobuf",
                link_context(LinkId::new("bad-decode")),
            ),
            Err(DirectLinkDeliveryError::Decode(_))
        ));

        binding
            .try_deliver(
                &handle,
                message_id,
                &PositionUpdate { tick: 7 }.encode_to_vec(),
                link_context(LinkId::new("link-dispatch")),
            )
            .unwrap();
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        release.notify_waiters();
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

        assert_eq!(*received.lock().expect("received mutex poisoned"), vec![7]);
    }

    fn link_context(link_id: LinkId) -> LinkMessageContext {
        let source = actor_ref("Gateway", "GatewaySession", 7);
        let target = actor_ref("Battle", "Battle", 9);
        LinkMessageContext {
            link_id,
            source,
            target,
            sequence: LinkSequence(1).0,
            received_at: Instant::now(),
            flags: LinkMessageFlags::EMPTY,
        }
    }

    fn actor_ref(service: &'static str, actor: &'static str, id: u64) -> ActorRef {
        ActorRef::direct(
            ServiceKind::from_static(service),
            ActorKind::from_static(actor),
            ActorId::U64(id),
            InstanceId::new(format!("instance-{id}")),
            "http://127.0.0.1:10000".parse::<Uri>().unwrap(),
            None,
        )
    }
}
