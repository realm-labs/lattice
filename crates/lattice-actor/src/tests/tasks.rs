//! Actor-scoped background work: timers and scoped task cancellation.

use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Mutex, oneshot};

use super::{Tick, spawn_actor};
use crate::{
    context::{ActorContext, HandlerContext},
    error::{ActorFailure, ActorStopError},
    mailbox::MailboxConfig,
    state_machine::Stateless,
    traits::{Actor, Handler, StopReason},
    watch::TerminatedReason,
};

#[tokio::test]
async fn local_timer_delivers_message_to_actor() {
    struct TimerActor {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Actor for TimerActor {
        type Error = ActorFailure;
        type Behavior = ::lattice_actor::state_machine::Stateless;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorFailure> {
            ctx.notify_after(Duration::from_millis(5), Tick);
            Ok(())
        }
    }

    impl Handler<Tick> for TimerActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            _msg: Tick,
        ) -> Result<(), ActorFailure> {
            self.events.lock().await.push("tick");
            Ok(())
        }
    }

    let events = Arc::new(Mutex::new(Vec::new()));
    let _handle = spawn_actor(
        TimerActor {
            events: events.clone(),
        },
        MailboxConfig::bounded(8),
    );

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(*events.lock().await, vec!["tick"]);
}

#[tokio::test]
async fn scoped_task_is_cancelled_when_actor_stops() {
    struct TaskActor {
        dropped_tx: Option<oneshot::Sender<()>>,
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    impl Actor for TaskActor {
        type Error = ActorFailure;
        type Behavior = ::lattice_actor::state_machine::Stateless;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorFailure> {
            let signal = DropSignal(self.dropped_tx.take());
            ctx.spawn_scoped(async move {
                let _signal = signal;
                std::future::pending::<()>().await;
            });
            Ok(())
        }
    }

    let (dropped_tx, dropped_rx) = oneshot::channel();
    let handle = spawn_actor(
        TaskActor {
            dropped_tx: Some(dropped_tx),
        },
        MailboxConfig::bounded(8),
    );

    handle.stop(StopReason::Requested).unwrap();

    tokio::time::timeout(Duration::from_millis(100), dropped_rx)
        .await
        .unwrap()
        .unwrap();
}

struct CleanupActor {
    released: Arc<AtomicUsize>,
    started: Option<oneshot::Sender<()>>,
    stopping: Option<oneshot::Sender<usize>>,
}

struct CountRelease(Arc<AtomicUsize>);

impl Drop for CountRelease {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

impl Actor for CleanupActor {
    type Error = ActorFailure;
    type Behavior = Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let scoped = CountRelease(self.released.clone());
        ctx.spawn_scoped(async move {
            pending::<()>().await;
            drop(scoped);
        });
        let deferred = CountRelease(self.released.clone());
        ctx.pipe_to_self(
            async move {
                pending::<()>().await;
                drop(deferred);
                Tick
            },
            |message| message,
        )?;
        let _ = self.started.take().unwrap().send(());
        Ok(())
    }

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        let _ = self
            .stopping
            .take()
            .unwrap()
            .send(self.released.load(Ordering::Acquire));
        Ok(())
    }
}

impl Handler<Tick> for CleanupActor {
    async fn handle(
        &mut self,
        _: &mut HandlerContext<'_, Self>,
        _: Tick,
    ) -> Result<(), Self::Error> {
        panic!("injected actor failure with outstanding off-turn resources");
    }
}

#[tokio::test]
async fn stopping_hook_runs_after_scoped_and_deferred_resources_have_been_released() {
    let (started, ready) = oneshot::channel();
    let (stopping, stopped) = oneshot::channel();
    let handle = spawn_actor(
        CleanupActor {
            released: Arc::new(AtomicUsize::new(0)),
            started: Some(started),
            stopping: Some(stopping),
        },
        MailboxConfig::bounded(8),
    );
    ready.await.unwrap();
    handle.stop(StopReason::Requested).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stopped)
            .await
            .unwrap()
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn panic_termination_is_published_after_off_turn_resources_have_been_released() {
    let released = Arc::new(AtomicUsize::new(0));
    let (started, ready) = oneshot::channel();
    let (stopping, _stopped) = oneshot::channel();
    let handle = spawn_actor(
        CleanupActor {
            released: released.clone(),
            started: Some(started),
            stopping: Some(stopping),
        },
        MailboxConfig::bounded(8),
    );
    let mut terminated = handle.subscribe_terminated();
    ready.await.unwrap();
    handle.tell(Tick).await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), terminated.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.reason, TerminatedReason::Panicked);
    assert_eq!(released.load(Ordering::Acquire), 2);
}
