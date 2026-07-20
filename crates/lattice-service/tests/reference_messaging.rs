use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use lattice_actor::actor_protocol;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::protocol::{
    ActorProtocolBinding, CodecDescriptor, DecodeError, EncodeError, Protocol, WireCodec,
};
use lattice_actor::registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig};
use lattice_actor::traits::{Actor, Handler, StopReason};
use lattice_core::actor_kind;
use lattice_core::actor_ref::{ActorRef, ClusterId, NodeAddress, NodeIncarnation};
use lattice_core::id::ActorId;
use lattice_core::kind::ActorKind;
use lattice_remoting::config::RemotingConfig;
use lattice_service::builder::LatticeService;
use lattice_service::config::NodeConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

const SOURCE_PROTOCOL_ID: u64 = 0x7265_665f_7372_6301;
const SINK_PROTOCOL_ID: u64 = 0x7265_665f_736e_6b01;
type SenderObserver = Arc<Mutex<Option<oneshot::Sender<Option<ActorRef>>>>>;

#[derive(Debug, Serialize, Deserialize, lattice_actor::Message)]
#[serde(bound = "")]
struct SendTo {
    target: ActorRef<SinkProtocol>,
}

#[derive(Debug, lattice_actor::Message)]
struct Delivered;

#[derive(Clone, Copy)]
struct SendToCodec;

impl WireCodec<SendTo> for SendToCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(1, 1);

    fn encode(&self, value: &SendTo, output: &mut BytesMut) -> Result<(), EncodeError> {
        let encoded =
            serde_json::to_vec(value).map_err(|error| EncodeError::new(error.to_string()))?;
        output.extend_from_slice(&encoded);
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<SendTo, DecodeError> {
        serde_json::from_slice(input).map_err(|error| DecodeError::new(error.to_string()))
    }
}

#[derive(Clone, Copy)]
struct DeliveredCodec;

impl WireCodec<Delivered> for DeliveredCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(2, 1);

    fn encode(&self, _value: &Delivered, _output: &mut BytesMut) -> Result<(), EncodeError> {
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<Delivered, DecodeError> {
        if input.is_empty() {
            Ok(Delivered)
        } else {
            Err(DecodeError::new("Delivered payload must be empty"))
        }
    }
}

struct SourceActor;

impl Actor for SourceActor {
    type Error = ActorError;
}

impl Handler<SendTo> for SourceActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        message: SendTo,
    ) -> Result<(), ActorError> {
        ctx.tell(&message.target, Delivered).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct SinkActor {
    observed: SenderObserver,
}

impl Actor for SinkActor {
    type Error = ActorError;
}

impl Handler<Delivered> for SinkActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _message: Delivered,
    ) -> Result<(), ActorError> {
        if let Some(observed) = self.observed.lock().expect("observer poisoned").take() {
            let _ = observed.send(ctx.sender().cloned());
        }
        Ok(())
    }
}

actor_protocol! {
    SourceProtocol {
        protocol_id: SOURCE_PROTOCOL_ID;
        name: "reference/source/v1";
        tell 1 => SendTo {
            schema_version: 1,
            codec: SendToCodec,
        }
    }
}

actor_protocol! {
    SinkProtocol {
        protocol_id: SINK_PROTOCOL_ID;
        name: "reference/sink/v1";
        tell 1 => Delivered {
            schema_version: 1,
            codec: DeliveredCodec,
        }
    }
}

fn registry<A: Actor, P: Protocol>(
    kind: ActorKind,
    cluster_id: &ClusterId,
    address: &NodeAddress,
    incarnation: NodeIncarnation,
    protocol: &ActorProtocolBinding<A, P>,
) -> Arc<ActorRegistry<A>> {
    Arc::new(ActorRegistry::new_bound(
        kind,
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: address.clone(),
                node_incarnation: incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        protocol,
    ))
}

#[tokio::test]
async fn deserialized_actor_ref_sends_without_binding() {
    let cluster_id = ClusterId::new("reference-messaging").unwrap();
    let address = NodeAddress::new("127.0.0.1", 25261).unwrap();
    let incarnation = NodeIncarnation::new(1).unwrap();
    let source_protocol = Arc::new(SourceProtocol::bind::<SourceActor>().unwrap());
    let sink_protocol = Arc::new(SinkProtocol::bind::<SinkActor>().unwrap());
    let source_registry = registry(
        actor_kind!("ReferenceSource"),
        &cluster_id,
        &address,
        incarnation,
        source_protocol.as_ref(),
    );
    let sink_registry = registry(
        actor_kind!("ReferenceSink"),
        &cluster_id,
        &address,
        incarnation,
        sink_protocol.as_ref(),
    );
    let (observed_tx, observed_rx) = oneshot::channel();
    let sink_handle = sink_registry
        .start(
            ActorId::U64(1),
            SinkActor {
                observed: Arc::new(Mutex::new(Some(observed_tx))),
            },
        )
        .await
        .unwrap();
    let source_handle = source_registry
        .start(ActorId::U64(1), SourceActor)
        .await
        .unwrap();
    let sink_ref: ActorRef<SinkProtocol> = sink_handle.typed_actor_ref().unwrap().unwrap();
    let source_ref: ActorRef<SourceProtocol> = source_handle.typed_actor_ref().unwrap().unwrap();
    let decoded_sink: ActorRef<SinkProtocol> =
        serde_json::from_slice(&serde_json::to_vec(&sink_ref).unwrap()).unwrap();

    let service = LatticeService::builder(NodeConfig {
        cluster_id,
        node_id: "reference-node".to_owned(),
        address,
        incarnation,
        roles: BTreeSet::new(),
        remoting: RemotingConfig::default(),
        maximum_actor_protocols: 8,
        maximum_watches: 8,
        maximum_supervised_tasks: 8,
        shutdown_timeout: Duration::from_secs(1),
    })
    .unwrap()
    .register_actor(source_registry, source_protocol)
    .unwrap()
    .register_actor(sink_registry, sink_protocol)
    .unwrap()
    .build()
    .unwrap();
    service.start().await.unwrap();

    service
        .tell(
            &source_ref,
            SendTo {
                target: decoded_sink,
            },
        )
        .await
        .unwrap();

    let sender = observed_rx.await.unwrap().expect("actor sender missing");
    assert!(sender.same_activation(&source_ref));

    source_handle.stop(StopReason::Requested).await.unwrap();
    sink_handle.stop(StopReason::Requested).await.unwrap();
    service.shutdown().await.unwrap();
}
