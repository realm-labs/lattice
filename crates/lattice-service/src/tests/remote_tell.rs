use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use lattice_actor::{
    actor_protocol,
    context::HandlerContext,
    error::ActorError,
    mailbox::MailboxConfig,
    protocol::{CodecDescriptor, DecodeError, EncodeError, WireCodec},
    registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig},
    traits::{Actor, Handler},
};
use lattice_core::{
    actor_kind,
    actor_ref::{ActorRef, ClusterId, NodeIncarnation},
    id::ActorId,
};
use lattice_remoting::handshake::NodeIdentity;
use tokio::sync::Notify;

use super::{PROTOCOL_ID, node_config, unused_address};
use crate::builder::LatticeService;

#[derive(Debug, Clone, lattice_actor::Message)]
struct FloodTell(u64);

#[derive(Clone, Copy)]
struct FloodTellCodec;

impl WireCodec<FloodTell> for FloodTellCodec {
    const DESCRIPTOR: CodecDescriptor = CodecDescriptor::new(2, 1);

    fn encode(&self, value: &FloodTell, output: &mut BytesMut) -> Result<(), EncodeError> {
        output.extend_from_slice(&value.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<FloodTell, DecodeError> {
        Ok(FloodTell(u64::from_be_bytes(input.try_into().map_err(
            |_| DecodeError::new("FloodTell requires eight bytes"),
        )?)))
    }
}

struct FloodActor {
    processed: Arc<AtomicUsize>,
    completed: Arc<Notify>,
}

impl Actor for FloodActor {
    type Error = ActorError;
    type Behavior = lattice_actor::state_machine::Stateless;
}

impl Handler<FloodTell> for FloodActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        message: FloodTell,
    ) -> Result<(), ActorError> {
        self.processed.fetch_add(1, Ordering::Relaxed);
        if message.0 == 0 {
            self.completed.notify_waiters();
        }
        Ok(())
    }
}

actor_protocol! {
    FloodProtocol {
        protocol_id: PROTOCOL_ID + 2;
        name: "service-test/flood/v1";
        tell 1 => FloodTell {
            schema_version: 1,
            codec: FloodTellCodec,
        }
    }
}

#[tokio::test]
async fn remote_tell_waits_for_mailbox_capacity_without_losing_messages() {
    const MESSAGES: usize = 4_096;

    let cluster_id = ClusterId::new("service-tell-backpressure").unwrap();
    let first_address = unused_address().await;
    let second_address = unused_address().await;
    let (client_address, server_address) = if first_address < second_address {
        (first_address, second_address)
    } else {
        (second_address, first_address)
    };
    let client_incarnation = NodeIncarnation::new(11).unwrap();
    let server_incarnation = NodeIncarnation::new(12).unwrap();
    let binding = Arc::new(FloodProtocol::bind::<FloodActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Flood"),
        ActorRegistryConfig {
            mailbox: MailboxConfig::bounded(1),
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: server_address.clone(),
                node_incarnation: server_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        binding.as_ref(),
    ));
    let processed = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(Notify::new());
    let handle = registry
        .start(
            ActorId::U64(1),
            FloodActor {
                processed: processed.clone(),
                completed: completed.clone(),
            },
        )
        .await
        .unwrap();
    let target: ActorRef<FloodProtocol> = handle.typed_actor_ref().unwrap().unwrap();
    let server = LatticeService::builder(node_config(
        cluster_id.clone(),
        "server",
        server_address.clone(),
        server_incarnation,
    ))
    .unwrap()
    .register_actor(registry, binding)
    .unwrap()
    .build()
    .unwrap();
    let client = LatticeService::builder(node_config(
        cluster_id.clone(),
        "client",
        client_address,
        client_incarnation,
    ))
    .unwrap()
    .use_protocol::<FloodProtocol>()
    .unwrap()
    .build()
    .unwrap();
    server.start().await.unwrap();
    client.start().await.unwrap();
    client
        .connect_peer(NodeIdentity {
            cluster_id,
            node_id: "server".to_owned(),
            address: server_address,
            incarnation: server_incarnation,
        })
        .await
        .unwrap();

    for sequence in (0..MESSAGES).rev() {
        client
            .tell(&target, FloodTell(sequence as u64))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while processed.load(Ordering::Acquire) != MESSAGES {
            let notified = completed.notified();
            if processed.load(Ordering::Acquire) == MESSAGES {
                return;
            }
            notified.await;
        }
    })
    .await
    .unwrap();

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
