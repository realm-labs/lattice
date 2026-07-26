//! Actor-scoped background work: timers and scoped task cancellation.

use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, oneshot};

use super::Tick;
use crate::{
    context::{ActorContext, HandlerContext},
    error::ActorError,
    mailbox::MailboxConfig,
    runtime::spawn_actor,
    traits::{Actor, Handler, StopReason},
};

#[tokio::test]
async fn local_timer_delivers_message_to_actor() {
    struct TimerActor {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Actor for TimerActor {
        type Error = ActorError;
        type Behavior = ::lattice_actor::state_machine::Stateless;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            ctx.notify_after(Duration::from_millis(5), Tick);
            Ok(())
        }
    }

    impl Handler<Tick> for TimerActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            _msg: Tick,
        ) -> Result<(), ActorError> {
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
        type Error = ActorError;
        type Behavior = ::lattice_actor::state_machine::Stateless;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
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

    handle.stop(StopReason::Requested).await.unwrap();

    tokio::time::timeout(Duration::from_millis(100), dropped_rx)
        .await
        .unwrap()
        .unwrap();
}
