use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use lattice_actor::{
    context::HandlerContext,
    error::ActorError,
    handle::ActorHandle,
    runtime::{ActorRuntime, ActorSpawnOptions},
    traits::{Actor, Handler},
};
use tokio::sync::Notify;

struct TargetActor {
    deliveries: Arc<AtomicUsize>,
    delivered: Arc<Notify>,
}

struct SourceActor {
    target: ActorHandle<TargetActor>,
}

impl Actor for TargetActor {
    type Error = ActorError;
    type Behavior = lattice_actor::state_machine::Stateless;
}

impl Actor for SourceActor {
    type Error = ActorError;
    type Behavior = lattice_actor::state_machine::Stateless;
}

#[derive(lattice_actor::Message)]
struct Start;

#[derive(lattice_actor::Message)]
struct Delivered;

impl Handler<Start> for SourceActor {
    async fn handle(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        _message: Start,
    ) -> Result<(), ActorError> {
        ctx.tell(&self.target, Delivered).await?;
        Ok(())
    }
}

impl Handler<Delivered> for TargetActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: Delivered,
    ) -> Result<(), ActorError> {
        self.deliveries.fetch_add(1, Ordering::Relaxed);
        self.delivered.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn context_tell_accepts_a_local_handle() {
    let runtime = ActorRuntime::default();
    let deliveries = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(Notify::new());
    let target = runtime
        .spawn_actor(
            TargetActor {
                deliveries: deliveries.clone(),
                delivered: delivered.clone(),
            },
            ActorSpawnOptions::default(),
        )
        .unwrap();
    let source = runtime
        .spawn_actor(SourceActor { target }, ActorSpawnOptions::default())
        .unwrap();

    source.tell(Start).await.unwrap();
    delivered.notified().await;
    assert_eq!(deliveries.load(Ordering::Relaxed), 1);
}
