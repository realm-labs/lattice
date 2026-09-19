//! Stop and passivation behaviour, including idle detection around long-running handlers.

use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, Semaphore};

use super::{ASK_TIMEOUT, Ping, Record, StopAfterReply, TestActor, spawn_actor};
use crate::{
    context::{ActorContext, HandlerContext},
    error::{ActorCallError, ActorFailure, ActorStopError, ActorTellError},
    mailbox::MailboxConfig,
    runtime::{ActorRuntime, ActorSpawnOptions, PassivationPolicy},
    traits::{Actor, ActorLifecycleState, Handler, PassivationReason, StopReason},
};

#[tokio::test]
async fn stop_uses_system_lane_and_closes_actor() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(Semaphore::new(0));
    let actor = TestActor {
        events,
        start_gate: None,
        stopped: Some(stopped.clone()),
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));
    let mut terminated = handle.subscribe_terminated();

    handle.stop(StopReason::Requested).unwrap();
    stopped.acquire().await.unwrap().forget();
    terminated.recv().await.unwrap();

    let result = handle.ask(Ping("after-stop"), ASK_TIMEOUT).await;
    assert!(matches!(
        result,
        Err(ActorCallError::LifecycleUnavailable {
            state: ActorLifecycleState::Stopped
        })
    ));
}

#[tokio::test]
async fn business_passivation_happens_after_handler_response() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(Semaphore::new(0));
    let actor = TestActor {
        events: events.clone(),
        start_gate: None,
        stopped: Some(stopped.clone()),
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));
    let mut terminated = handle.subscribe_terminated();

    let reply = handle.ask(StopAfterReply, ASK_TIMEOUT).await.unwrap();
    stopped.acquire().await.unwrap().forget();
    terminated.recv().await.unwrap();
    let after_stop = handle.tell(Record::new("after-stop")).await;

    assert_eq!(reply, "reply-before-stop");
    assert_eq!(*events.lock().await, vec!["handled"]);
    let returned = match after_stop {
        Err(ActorTellError::LifecycleUnavailable {
            state: ActorLifecycleState::Stopped,
            message,
        }) => message,
        other => panic!("expected stopped lifecycle rejection, got {other:?}"),
    };
    assert_eq!(returned.value, "after-stop");
}

#[tokio::test]
async fn idle_passivation_waits_until_a_long_handler_finishes() {
    struct IdleActor {
        stopped: Arc<Semaphore>,
    }

    impl Actor for IdleActor {
        type Error = ActorFailure;
        type Behavior = crate::state_machine::Stateless;

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            reason: StopReason,
        ) -> Result<(), ActorStopError> {
            assert_eq!(
                reason,
                StopReason::Passivated(PassivationReason::IdleTimeout)
            );
            self.stopped.add_permits(1);
            Ok(())
        }
    }

    #[derive(Debug, crate::Message)]
    struct Park {
        entered: Arc<Semaphore>,
        release: Arc<Semaphore>,
        completed: Arc<Semaphore>,
    }

    #[derive(Debug, crate::Message)]
    struct Observe {
        processed: Arc<Semaphore>,
    }

    impl Handler<Park> for IdleActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            msg: Park,
        ) -> Result<(), ActorFailure> {
            msg.entered.add_permits(1);
            msg.release.acquire().await.unwrap().forget();
            msg.completed.add_permits(1);
            Ok(())
        }
    }

    impl Handler<Observe> for IdleActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            msg: Observe,
        ) -> Result<(), ActorFailure> {
            msg.processed.add_permits(1);
            Ok(())
        }
    }

    let stopped = Arc::new(Semaphore::new(0));
    let runtime = ActorRuntime::default();
    let handle = runtime
        .spawn_actor(
            IdleActor {
                stopped: stopped.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                passivation: PassivationPolicy::IdleTimeout(Duration::from_millis(50)),
                ..ActorSpawnOptions::default()
            },
        )
        .unwrap();

    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let completed = Arc::new(Semaphore::new(0));
    handle
        .try_tell_for_test(Park {
            entered: entered.clone(),
            release: release.clone(),
            completed: completed.clone(),
        })
        .unwrap();
    entered.acquire().await.unwrap().forget();

    // The handler remains busy for longer than the configured idle timeout.
    tokio::time::sleep(Duration::from_millis(150)).await;
    // Work queued behind it must run before a fresh idle period can passivate
    // the Actor.
    let processed = Arc::new(Semaphore::new(0));
    handle
        .tell(Observe {
            processed: processed.clone(),
        })
        .await
        .unwrap();
    release.add_permits(1);
    completed.acquire().await.unwrap().forget();
    tokio::time::timeout(Duration::from_millis(200), processed.acquire())
        .await
        .expect("message after long handler is processed before passivation")
        .unwrap()
        .forget();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stopped.acquire())
            .await
            .is_err(),
        "idle timeout must restart after the queued message completes"
    );

    tokio::time::timeout(Duration::from_secs(5), stopped.acquire())
        .await
        .expect("Actor passivates after becoming idle")
        .unwrap()
        .forget();
}
