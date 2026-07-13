use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorCallError, ActorError, ActorStopError};
use lattice_actor::registry::ActorRegistry;
use lattice_actor::registry::ActorRegistryConfig;
use lattice_actor::reply::ReplyTo;
use lattice_actor::traits::{
    Actor, ActorLifecycleState, PassivationReason, Request, Responder, StopReason,
};
use lattice_core::actor_kind;
use lattice_core::id::ActorId;
use tokio::sync::Semaphore;
use tokio::time::timeout;

struct PassivatingActor {
    stop_entered: Arc<Semaphore>,
    release_stop: Arc<Semaphore>,
    handled_pings: Arc<AtomicUsize>,
}

#[async_trait]
impl Actor for PassivatingActor {
    type Error = ActorError;

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        self.stop_entered.add_permits(1);
        self.release_stop.acquire().await.unwrap().forget();
        Ok(())
    }
}

struct BeginPassivation;

impl Request for BeginPassivation {
    type Response = ();
}

#[async_trait]
impl Responder<BeginPassivation> for PassivatingActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _request: BeginPassivation,
        reply_to: ReplyTo<()>,
    ) -> Result<(), ActorError> {
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        let _ = reply_to.send(());
        Ok(())
    }
}

struct Ping;

impl Request for Ping {
    type Response = ();
}

#[async_trait]
impl Responder<Ping> for PassivatingActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: Ping,
        reply_to: ReplyTo<()>,
    ) -> Result<(), ActorError> {
        self.handled_pings.fetch_add(1, Ordering::SeqCst);
        let _ = reply_to.send(());
        Ok(())
    }
}

#[tokio::test]
async fn request_arriving_while_actor_is_passivating_is_not_processed_by_old_incarnation() {
    let registry = ActorRegistry::<PassivatingActor>::new(
        actor_kind!("Passivating"),
        ActorRegistryConfig::default(),
    );
    let stop_entered = Arc::new(Semaphore::new(0));
    let release_stop = Arc::new(Semaphore::new(0));
    let handled_pings = Arc::new(AtomicUsize::new(0));
    let handle = registry
        .start(
            ActorId::U64(7),
            PassivatingActor {
                stop_entered: stop_entered.clone(),
                release_stop: release_stop.clone(),
                handled_pings: handled_pings.clone(),
            },
        )
        .await
        .unwrap();
    let mut lifecycle = handle.subscribe_lifecycle();

    handle.ask(BeginPassivation).await.unwrap();
    stop_entered.acquire().await.unwrap().forget();
    while *lifecycle.borrow() != ActorLifecycleState::Passivating {
        lifecycle.changed().await.unwrap();
    }

    let mut blocked_call = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ask(Ping).await }
    });
    assert!(
        timeout(Duration::from_millis(10), &mut blocked_call)
            .await
            .is_err()
    );

    release_stop.add_permits(1);
    let result = blocked_call.await.unwrap();

    assert!(matches!(result, Err(ActorCallError::MailboxClosed)));
    assert_eq!(handled_pings.load(Ordering::SeqCst), 0);
}
