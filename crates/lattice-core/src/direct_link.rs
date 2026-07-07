pub mod errors;
pub mod ids;
pub mod messages;
pub mod options;
pub mod runtime;
pub mod stream;
pub mod target;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use crate::actor_ref::ActorRef;
    use crate::direct_link::errors::*;
    use crate::direct_link::ids::*;
    use crate::direct_link::options::*;
    use crate::direct_link::runtime::*;
    use crate::direct_link::stream::*;
    use crate::direct_link::target::*;
    use crate::id::ActorId;
    use crate::instance::InstanceId;
    use crate::kind::{ActorKind, ServiceKind};
    use crate::service_context::ServiceContext;

    #[derive(Clone, PartialEq, prost::Message)]
    struct InputCommand {
        #[prost(uint64, tag = "1")]
        command_id: u64,
    }

    impl DirectLinkMessage for InputCommand {
        const PROTO_FULL_NAME: &'static str = "game.InputCommand";
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct StateDelta {
        #[prost(uint64, tag = "1")]
        tick: u64,
    }

    impl DirectLinkMessage for StateDelta {
        const PROTO_FULL_NAME: &'static str = "game.StateDelta";
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestMetadata {
        package_index: u32,
    }

    impl DirectLinkMetadata for TestMetadata {
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

    #[derive(Clone)]
    struct SourceToTargetStream;

    impl DirectLinkStreamType for SourceToTargetStream {
        type Metadata = ();

        fn descriptor() -> DirectLinkStreamDescriptor {
            stream("gateway-input", &[message::<InputCommand>("gateway-input")])
        }
    }

    #[derive(Clone)]
    struct TargetToSourceStream;

    impl DirectLinkStreamType for TargetToSourceStream {
        type Metadata = ();

        fn descriptor() -> DirectLinkStreamDescriptor {
            stream("battle-update", &[message::<StateDelta>("battle-update")])
        }
    }

    #[derive(Clone)]
    struct MetadataStream;

    impl DirectLinkStreamType for MetadataStream {
        type Metadata = TestMetadata;

        fn descriptor() -> DirectLinkStreamDescriptor {
            stream(
                "metadata-input",
                &[message::<InputCommand>("metadata-input")],
            )
        }
    }

    #[derive(Debug, Default)]
    struct RecordingRuntime {
        requests: Mutex<Vec<DirectLinkOpenRequest>>,
        sender: Arc<RecordingSender>,
    }

    #[async_trait]
    impl DirectLinkRuntime for RecordingRuntime {
        async fn open_link(
            &self,
            request: DirectLinkOpenRequest,
        ) -> Result<DirectLinkSession, LinkError> {
            let session = DirectLinkSession {
                link_id: request.link_id.clone(),
                direction: LinkDirection::SourceToTarget,
                stream: request.source_to_target.clone(),
                accepted_message_ids: request.source_to_target.accepted_message_ids(),
                sender: self.sender.clone(),
            };
            self.requests
                .lock()
                .expect("open requests mutex poisoned")
                .push(request);
            Ok(session)
        }

        async fn get_outbound(
            &self,
            link_id: LinkId,
            stream: DirectLinkStreamDescriptor,
        ) -> Result<DirectLinkSession, LinkError> {
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
            _reason: LinkCloseReason,
        ) -> Result<(), LinkError> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingSender {
        messages: Mutex<Vec<OutboundDirectLinkMessage>>,
    }

    #[async_trait]
    impl DirectLinkSender for RecordingSender {
        async fn tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
            self.try_tell(message)
        }

        fn try_tell(&self, message: OutboundDirectLinkMessage) -> Result<(), LinkSendError> {
            self.messages
                .lock()
                .expect("sent messages mutex poisoned")
                .push(message);
            Ok(())
        }

        async fn close(&self, _reason: LinkCloseReason) -> Result<(), LinkSendError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn connect_bidirectional_returns_source_to_target_send_handle() {
        let runtime = Arc::new(RecordingRuntime::default());
        let mut context = ServiceContext::builder(
            ServiceKind::from_static("Gateway"),
            InstanceId::new("gateway-1"),
        );
        context
            .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
            .unwrap();
        let source = actor_ref("Gateway", "GatewaySession", 7);
        let target = actor_ref("Battle", "Battle", 9);
        let manager = DirectLinkManager::new(context.build(), Some(source.clone()));

        let link = manager
            .connect_bidirectional(
                target.clone(),
                SourceToTargetStream,
                TargetToSourceStream,
                DirectLinkOptions::default(),
            )
            .await
            .unwrap();
        link.try_tell(InputCommand { command_id: 42 }).unwrap();

        assert_eq!(link.direction(), LinkDirection::SourceToTarget);
        assert_eq!(
            link.stream(),
            &<SourceToTargetStream as DirectLinkStreamType>::descriptor()
        );
        let requests = runtime
            .requests
            .lock()
            .expect("open requests mutex poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].source, source);
        assert_eq!(requests[0].target, LinkTarget::Actor(target));
        assert_eq!(requests[0].mode, DirectLinkMode::Bidirectional);
        assert_eq!(requests[0].options.mode, DirectLinkMode::Bidirectional);
        assert_eq!(
            requests[0].source_to_target,
            <SourceToTargetStream as DirectLinkStreamType>::descriptor()
        );
        assert_eq!(
            requests[0].target_to_source,
            Some(<TargetToSourceStream as DirectLinkStreamType>::descriptor())
        );

        let messages = runtime
            .sender
            .messages
            .lock()
            .expect("sent messages mutex poisoned");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].link_id, *link.id());
        assert_eq!(messages[0].direction, LinkDirection::SourceToTarget);
        assert_eq!(
            messages[0].message_id,
            DirectLinkMessageId::for_proto("gateway-input", InputCommand::PROTO_FULL_NAME)
        );
        assert_eq!(messages[0].proto_full_name, InputCommand::PROTO_FULL_NAME);
    }

    #[tokio::test]
    async fn tell_with_metadata_encodes_stream_metadata() {
        let runtime = Arc::new(RecordingRuntime::default());
        let mut context = ServiceContext::builder(
            ServiceKind::from_static("Gateway"),
            InstanceId::new("gateway-1"),
        );
        context
            .insert_extension(DirectLinkRuntimeHandle::new(runtime.clone()))
            .unwrap();
        let source = actor_ref("Gateway", "GatewaySession", 7);
        let target = actor_ref("Player", "Player", 9);
        let manager = DirectLinkManager::new(context.build(), Some(source));

        let link = manager
            .connect(target, MetadataStream, DirectLinkOptions::unidirectional())
            .await
            .unwrap();
        link.try_tell_with_metadata(
            InputCommand { command_id: 42 },
            TestMetadata { package_index: 88 },
        )
        .unwrap();

        let messages = runtime
            .sender
            .messages
            .lock()
            .expect("sent messages mutex poisoned");
        assert_eq!(messages[0].metadata, 88u32.to_be_bytes());
    }

    fn stream(
        stream_name: impl Into<String>,
        messages: &[DirectLinkMessageDescriptor],
    ) -> DirectLinkStreamDescriptor {
        DirectLinkStreamDescriptor {
            stream_name: stream_name.into(),
            messages: messages.to_vec(),
        }
    }

    fn message<T>(stream_name: &str) -> DirectLinkMessageDescriptor
    where
        T: DirectLinkMessage,
    {
        DirectLinkMessageDescriptor {
            message_id: DirectLinkMessageId::for_proto(stream_name, T::PROTO_FULL_NAME),
            proto_full_name: T::PROTO_FULL_NAME.to_string(),
            rust_type_name: std::any::type_name::<T>().to_string(),
        }
    }

    fn actor_ref(service_kind: &'static str, actor_kind: &'static str, id: u64) -> ActorRef {
        ActorRef::direct(
            ServiceKind::from_static(service_kind),
            ActorKind::from_static(actor_kind),
            ActorId::U64(id),
            InstanceId::new(format!("{service_kind}-{id}")),
            "http://127.0.0.1:10000".parse().unwrap(),
            None,
        )
    }

    #[test]
    fn stream_descriptors_have_expected_message_ids() {
        assert_eq!(
            <SourceToTargetStream as DirectLinkStreamType>::descriptor().accepted_message_ids(),
            BTreeSet::from([DirectLinkMessageId::for_proto(
                "gateway-input",
                InputCommand::PROTO_FULL_NAME
            )])
        );
        assert_eq!(
            <TargetToSourceStream as DirectLinkStreamType>::descriptor().accepted_message_ids(),
            BTreeSet::from([DirectLinkMessageId::for_proto(
                "battle-update",
                StateDelta::PROTO_FULL_NAME
            )])
        );
    }
}
