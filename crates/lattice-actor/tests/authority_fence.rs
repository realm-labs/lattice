use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_actor::{
    context::{ActorContext, HandlerContext},
    error::{ActorCallError, ActorFailure, ActorStopError},
    mailbox::MailboxConfig,
    reply::ReplyTo,
    runtime::{ActorRuntime, ActorSpawnOptions},
    traits::{Actor, ActorLifecycleState, Handler, Message, Request, Responder, StopReason},
};
use tokio::sync::Semaphore;

#[derive(Default)]
struct Observed {
    started: AtomicUsize,
    handled: AtomicUsize,
    stopped: AtomicUsize,
    dropped: AtomicUsize,
    persistence_available: AtomicBool,
}

struct FencedActor {
    observed: Arc<Observed>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl Actor for FencedActor {
    type Error = ActorFailure;
    type Behavior = lattice_actor::state_machine::Stateless;

    async fn started(&mut self, _: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        self.observed.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn stopping(
        &mut self,
        _: &mut ActorContext<Self>,
        _: StopReason,
    ) -> Result<(), ActorStopError> {
        self.observed.stopped.fetch_add(1, Ordering::SeqCst);
        if self.observed.persistence_available.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(ActorStopError::new("store unavailable"))
        }
    }
}

impl Drop for FencedActor {
    fn drop(&mut self) {
        self.observed.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct Work(bool);

impl Message for Work {}

impl Handler<Work> for FencedActor {
    async fn handle(
        &mut self,
        _: &mut HandlerContext<'_, Self>,
        work: Work,
    ) -> Result<(), Self::Error> {
        self.observed.handled.fetch_add(1, Ordering::SeqCst);
        if work.0 {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        Ok(())
    }
}

struct Probe;

impl Request for Probe {
    type Response = ();
}

impl Responder<Probe> for FencedActor {
    async fn respond(
        &mut self,
        _: &mut HandlerContext<'_, Self>,
        _: Probe,
        reply_to: ReplyTo<()>,
    ) -> Result<(), Self::Error> {
        let _ = reply_to.send(());
        Ok(())
    }
}

#[tokio::test]
async fn fence_before_first_poll_skips_startup_and_still_persists() {
    let observed = Arc::new(Observed::default());
    observed.persistence_available.store(true, Ordering::SeqCst);
    let runtime = ActorRuntime::default();
    let handle = runtime
        .spawn_actor(
            FencedActor {
                observed: observed.clone(),
                entered: Arc::new(Semaphore::new(0)),
                release: Arc::new(Semaphore::new(0)),
            },
            ActorSpawnOptions::default(),
        )
        .unwrap();
    handle.fence_business_admission();
    assert!(handle.tell(Work(false)).await.is_err());
    tokio::time::timeout(Duration::from_secs(1), handle.subscribe_terminated().recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.started.load(Ordering::SeqCst), 0);
    assert_eq!(observed.handled.load(Ordering::SeqCst), 0);
    assert_eq!(observed.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(observed.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fence_rejects_prefetched_work_even_when_system_mailbox_is_full() {
    let observed = Arc::new(Observed::default());
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let runtime = ActorRuntime::default();
    let handle = runtime
        .spawn_actor(
            FencedActor {
                observed: observed.clone(),
                entered: entered.clone(),
                release: release.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::with_lanes(8, 1),
                ..ActorSpawnOptions::default()
            },
        )
        .unwrap();
    handle.try_tell(Work(true)).unwrap();
    handle.try_tell(Work(false)).unwrap();
    entered.acquire().await.unwrap().forget();
    handle.stop(StopReason::Requested).unwrap();
    assert!(handle.stop(StopReason::Requested).is_err());
    handle.fence_business_admission();
    assert!(handle.try_tell(Work(false)).is_err());
    assert_eq!(
        handle.ask(Probe, Duration::from_secs(1)).await,
        Err(ActorCallError::LifecycleUnavailable {
            state: ActorLifecycleState::Stopping,
        })
    );
    release.add_permits(1);
    let mut lifecycle = handle.subscribe_lifecycle();
    tokio::time::timeout(Duration::from_secs(1), async {
        while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(observed.handled.load(Ordering::SeqCst), 1);
    assert_eq!(observed.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(observed.dropped.load(Ordering::SeqCst), 0);
    observed.persistence_available.store(true, Ordering::SeqCst);
    handle.retry_stop().await.unwrap();
    assert_eq!(observed.dropped.load(Ordering::SeqCst), 1);
    assert!(handle.business_admission_fenced());
}

#[tokio::test]
async fn fence_wakes_an_idle_actor_without_a_mailbox_command() {
    let observed = Arc::new(Observed::default());
    observed.persistence_available.store(true, Ordering::SeqCst);
    let runtime = ActorRuntime::default();
    let handle = runtime
        .spawn_actor(
            FencedActor {
                observed,
                entered: Arc::new(Semaphore::new(0)),
                release: Arc::new(Semaphore::new(0)),
            },
            ActorSpawnOptions::default(),
        )
        .unwrap();
    let mut lifecycle = handle.subscribe_lifecycle();
    while *lifecycle.borrow() != ActorLifecycleState::Running {
        lifecycle.changed().await.unwrap();
    }
    handle.fence_business_admission();
    tokio::time::timeout(Duration::from_secs(1), handle.subscribe_terminated().recv())
        .await
        .unwrap()
        .unwrap();
}
