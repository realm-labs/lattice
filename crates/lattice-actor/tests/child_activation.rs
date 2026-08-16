#![cfg(feature = "distributed")]

use std::time::Duration;

use lattice_actor::{
    context::ActorContext,
    directory::ActivationDirectory,
    error::ActorError,
    mailbox::MailboxConfig,
    runtime::{ActorRuntime, ActorSpawnOptions},
    traits::{Actor, ChildActorKey, ChildActorOptions, ChildSupervision, StopReason},
};
use lattice_core::{
    actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    },
    instance::InstanceId,
    kind::ServiceKind,
    service_context::ServiceContext,
};
use tokio::sync::mpsc;

const TIMEOUT: Duration = Duration::from_secs(2);

struct ChildActor {
    activations: mpsc::UnboundedSender<ActorRef>,
}

impl Actor for ChildActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(reference) = ctx.self_ref() {
            let _ = self.activations.send(reference.clone());
        }
        Ok(())
    }
}

struct ParentActor {
    activations: mpsc::UnboundedSender<ActorRef>,
    supervision: ChildSupervision,
}

impl Actor for ParentActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let activations = self.activations.clone();
        ctx.spawn_child_with_factory(
            ChildActorKey::new("worker"),
            move || ChildActor {
                activations: activations.clone(),
            },
            ChildActorOptions {
                mailbox: MailboxConfig::bounded(4),
                supervision: self.supervision,
                protocol_id: Some(ProtocolId::new(41).expect("protocol ID")),
                ..ChildActorOptions::default()
            },
        )?;
        Ok(())
    }
}

fn service_with_directory(capacity: usize) -> ServiceContext {
    let mut service = ServiceContext::builder(
        ServiceKind::from_static("test"),
        InstanceId::new("child-activation"),
    );
    service
        .insert_extension(ActivationDirectory::new(capacity).expect("activation directory"))
        .expect("directory extension");
    service.build()
}

fn parent_ref() -> ActorRef {
    let node_incarnation = NodeIncarnation::new(3).expect("node incarnation");
    ActorRef::new(
        ClusterId::new("test").expect("cluster ID"),
        NodeAddress::new("127.0.0.1", 19099).expect("node address"),
        node_incarnation,
        ActorPath::user(["parent"]).expect("actor path"),
        ActivationId::new(node_incarnation, 1).expect("activation ID"),
        ProtocolId::new(40).expect("protocol ID"),
    )
    .expect("parent reference")
}

async fn spawn_parent(
    service: &ServiceContext,
    supervision: ChildSupervision,
) -> (
    lattice_actor::handle::ActorHandle<ParentActor>,
    mpsc::UnboundedReceiver<ActorRef>,
) {
    let (activations, receiver) = mpsc::unbounded_channel();
    let handle = ActorRuntime::default()
        .spawn_actor(
            ParentActor {
                activations,
                supervision,
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(4),
                self_ref: Some(parent_ref()),
                service: service.clone(),
                ..ActorSpawnOptions::default()
            },
        )
        .expect("parent spawns");
    (handle, receiver)
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(TIMEOUT, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition should hold within the timeout");
}

#[tokio::test]
async fn restarted_child_takes_a_new_activation_identity() {
    let service = service_with_directory(8);
    let directory = service
        .extension::<ActivationDirectory>()
        .expect("directory extension");
    let (_parent, mut activations) = spawn_parent(&service, ChildSupervision::RestartChild).await;

    let first = tokio::time::timeout(TIMEOUT, activations.recv())
        .await
        .expect("first activation is reported")
        .expect("child reports its reference");
    wait_until(|| directory.resolve::<ChildActor, _>(&first).is_some()).await;
    directory
        .resolve::<ChildActor, _>(&first)
        .expect("first activation resolves")
        .stop(StopReason::Requested)
        .await
        .expect("child accepts stop");

    let second = tokio::time::timeout(TIMEOUT, activations.recv())
        .await
        .expect("replacement activation is reported")
        .expect("replacement reports its reference");
    wait_until(|| directory.resolve::<ChildActor, _>(&second).is_some()).await;

    assert_eq!(second.actor_path(), first.actor_path());
    assert_ne!(second.activation_id(), first.activation_id());
    assert!(
        directory.resolve::<ChildActor, _>(&first).is_none(),
        "a reference to the dead child must never resolve to its replacement"
    );
}

#[tokio::test]
async fn terminated_child_releases_its_directory_entry() {
    let service = service_with_directory(1);
    let directory = service
        .extension::<ActivationDirectory>()
        .expect("directory extension");
    let (_parent, mut activations) = spawn_parent(&service, ChildSupervision::StopChild).await;

    let child_ref = tokio::time::timeout(TIMEOUT, activations.recv())
        .await
        .expect("activation is reported")
        .expect("child reports its reference");
    wait_until(|| directory.resolve::<ChildActor, _>(&child_ref).is_some()).await;
    directory
        .resolve::<ChildActor, _>(&child_ref)
        .expect("child resolves")
        .stop(StopReason::Requested)
        .await
        .expect("child accepts stop");

    wait_until(|| directory.is_empty()).await;
}
