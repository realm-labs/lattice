use lattice_actor::{Actor, ActorHandle, ActorTellError, Handler};
use lattice_core::{
    DirectLinkMessage, DirectLinkMessageId, DirectLinkMetadata, DirectLinkStreamDescriptor,
    LinkMessageContext, Linked,
};
use thiserror::Error;

pub fn try_deliver_linked<A, T, M>(
    handle: &ActorHandle<A>,
    payload: T,
    metadata: M,
    context: LinkMessageContext,
) -> Result<(), ActorTellError>
where
    A: Actor + Handler<Linked<T, M>>,
    T: Send + 'static,
    M: Send + 'static,
{
    handle.try_tell(Linked {
        payload,
        metadata,
        context,
    })
}

#[derive(Debug, Error)]
pub enum DirectLinkDeliveryError {
    #[error("direct-link message type is not supported by this actor binding")]
    UnsupportedMessageType,
    #[error("failed to decode direct-link message payload: {0}")]
    Decode(String),
    #[error("failed to decode direct-link message metadata: {0}")]
    DecodeMetadata(String),
    #[error(transparent)]
    Mailbox(#[from] ActorTellError),
}

pub trait DirectLinkDispatch<A, M>: Send + Sync + 'static
where
    A: Actor,
    M: DirectLinkMetadata,
{
    fn try_dispatch(
        handle: &ActorHandle<A>,
        stream: &DirectLinkStreamDescriptor,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        metadata: M,
        context: LinkMessageContext,
    ) -> Result<(), DirectLinkDeliveryError>;
}

impl<A, M> DirectLinkDispatch<A, M> for ()
where
    A: Actor,
    M: DirectLinkMetadata,
{
    fn try_dispatch(
        _handle: &ActorHandle<A>,
        _stream: &DirectLinkStreamDescriptor,
        _message_id: DirectLinkMessageId,
        _payload: &[u8],
        _metadata: M,
        _context: LinkMessageContext,
    ) -> Result<(), DirectLinkDeliveryError> {
        Err(DirectLinkDeliveryError::UnsupportedMessageType)
    }
}

impl<A, M, Head, Tail> DirectLinkDispatch<A, M> for (Head, Tail)
where
    A: Actor + Handler<Linked<Head, M>>,
    M: DirectLinkMetadata,
    Head: DirectLinkMessage,
    Tail: DirectLinkDispatch<A, M>,
{
    fn try_dispatch(
        handle: &ActorHandle<A>,
        stream: &DirectLinkStreamDescriptor,
        message_id: DirectLinkMessageId,
        payload: &[u8],
        metadata: M,
        context: LinkMessageContext,
    ) -> Result<(), DirectLinkDeliveryError> {
        if stream.message_id_for::<Head>() == Some(message_id) {
            let payload = Head::decode(payload)
                .map_err(|error| DirectLinkDeliveryError::Decode(error.to_string()))?;
            return try_deliver_linked(handle, payload, metadata, context).map_err(Into::into);
        }
        Tail::try_dispatch(handle, stream, message_id, payload, metadata, context)
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
        ActorId, ActorKind, ActorRef, DirectLinkMessage, DirectLinkMessageId, DirectLinkMetadata,
        InstanceId, LinkId, LinkMessageFlags, LinkMetadataError, LinkSequence, ServiceKind,
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

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestMetadata {
        package_index: u32,
    }

    impl lattice_core::DirectLinkMetadata for TestMetadata {
        fn encode_metadata(&self) -> Result<Vec<u8>, LinkMetadataError> {
            Ok(self.package_index.to_be_bytes().to_vec())
        }

        fn decode_metadata(bytes: &[u8]) -> Result<Self, LinkMetadataError> {
            let bytes: [u8; 4] = bytes
                .try_into()
                .map_err(|_| LinkMetadataError::Decode("expected u32 metadata".to_string()))?;
            Ok(Self {
                package_index: u32::from_be_bytes(bytes),
            })
        }
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

    struct MetadataActor {
        received: Arc<Mutex<Vec<(u64, u32)>>>,
    }

    #[async_trait]
    impl lattice_actor::Actor for MetadataActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<PositionUpdate, TestMetadata>> for MetadataActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Linked<PositionUpdate, TestMetadata>,
        ) -> Result<(), Self::Error> {
            self.received
                .lock()
                .expect("received mutex poisoned")
                .push((msg.payload.tick, msg.metadata.package_index));
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
            (),
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
                &[],
                link_context(LinkId::new("unsupported")),
            ),
            Err(DirectLinkDeliveryError::UnsupportedMessageType)
        ));
        assert!(matches!(
            binding.try_deliver(
                &handle,
                message_id,
                b"not protobuf",
                &[],
                link_context(LinkId::new("bad-decode")),
            ),
            Err(DirectLinkDeliveryError::Decode(_))
        ));

        binding
            .try_deliver(
                &handle,
                message_id,
                &PositionUpdate { tick: 7 }.encode_to_vec(),
                &[],
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

    #[tokio::test]
    async fn actor_binding_dispatch_decodes_metadata_and_enqueues() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let actor = MetadataActor {
            received: received.clone(),
        };
        let handle = ActorRuntime::default()
            .spawn_actor(actor, Default::default())
            .await
            .unwrap();
        let stream = DirectLinkStream::new("movement")
            .metadata::<TestMetadata>()
            .message::<PositionUpdate>();
        let binding = stream.for_actor::<MetadataActor>(ActorKind::from_static("Battle"));
        let descriptor = stream.descriptor();
        let message_id = descriptor.message_id_for::<PositionUpdate>().unwrap();

        binding
            .try_deliver(
                &handle,
                message_id,
                &PositionUpdate { tick: 7 }.encode_to_vec(),
                &TestMetadata { package_index: 99 }
                    .encode_metadata()
                    .unwrap(),
                link_context(LinkId::new("link-dispatch-metadata")),
            )
            .unwrap();
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

        assert_eq!(
            *received.lock().expect("received mutex poisoned"),
            vec![(7, 99)]
        );
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
