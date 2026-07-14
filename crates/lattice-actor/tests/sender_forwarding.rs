use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::handle::ActorHandle;
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions};
use lattice_actor::traits::{Actor, Handler};
use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
};
use tokio::sync::mpsc;

struct TargetActor {
    observed: mpsc::UnboundedSender<ActorRef>,
}

struct ForwarderActor {
    target: ActorHandle<TargetActor>,
}

struct SourceActor {
    forwarder: ActorHandle<ForwarderActor>,
}

#[async_trait]
impl Actor for TargetActor {
    type Error = ActorError;
}

#[async_trait]
impl Actor for ForwarderActor {
    type Error = ActorError;
}

#[async_trait]
impl Actor for SourceActor {
    type Error = ActorError;
}

#[derive(lattice_actor::Message)]
struct Start;

#[derive(lattice_actor::Message)]
struct Relay {
    preserve_sender: bool,
}

#[derive(lattice_actor::Message)]
struct Delivered;

#[async_trait]
impl Handler<Start> for SourceActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _message: Start,
    ) -> Result<(), ActorError> {
        ctx.tell_local(
            &self.forwarder,
            Relay {
                preserve_sender: true,
            },
        )?;
        ctx.tell_local(
            &self.forwarder,
            Relay {
                preserve_sender: false,
            },
        )?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Relay> for ForwarderActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        message: Relay,
    ) -> Result<(), ActorError> {
        if message.preserve_sender {
            ctx.forward_local(&self.target, Delivered)?;
        } else {
            ctx.tell_local(&self.target, Delivered)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Delivered> for TargetActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _message: Delivered,
    ) -> Result<(), ActorError> {
        let sender = ctx
            .sender()
            .cloned()
            .ok_or_else(|| ActorError::new("delivered message has no actor sender"))?;
        self.observed
            .send(sender)
            .map_err(|_| ActorError::new("sender observer was dropped"))
    }
}

fn actor_ref(name: &str, sequence: u64) -> ActorRef {
    let incarnation = NodeIncarnation::new(7).unwrap();
    ActorRef::new(
        ClusterId::new("sender-forwarding").unwrap(),
        NodeAddress::new("127.0.0.1", 25520).unwrap(),
        incarnation,
        ActorPath::user(["user", name]).unwrap(),
        ActivationId::new(incarnation, sequence).unwrap(),
        ProtocolId::new(1).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn tell_uses_self_and_forward_preserves_the_original_sender() {
    let runtime = ActorRuntime::default();
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let target = runtime
        .spawn_actor(
            TargetActor {
                observed: observed_tx,
            },
            ActorSpawnOptions::default(),
        )
        .await
        .unwrap();

    let forwarder_ref = actor_ref("forwarder", 2);
    let forwarder = runtime
        .spawn_actor(
            ForwarderActor { target },
            ActorSpawnOptions {
                self_ref: Some(forwarder_ref.clone()),
                ..ActorSpawnOptions::default()
            },
        )
        .await
        .unwrap();

    let source_ref = actor_ref("source", 3);
    let source = runtime
        .spawn_actor(
            SourceActor { forwarder },
            ActorSpawnOptions {
                self_ref: Some(source_ref.clone()),
                ..ActorSpawnOptions::default()
            },
        )
        .await
        .unwrap();

    source.tell(Start).await.unwrap();

    let forwarded_sender = observed_rx.recv().await.unwrap();
    let told_sender = observed_rx.recv().await.unwrap();
    assert!(forwarded_sender.same_activation(&source_ref));
    assert!(told_sender.same_activation(&forwarder_ref));
}
