//! Stop and passivation behaviour, including retries behind a saturated system lane.

use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, Semaphore};

use super::{ASK_TIMEOUT, Ping, Record, StopAfterReply, TestActor};
use crate::{
    context::{ActorContext, HandlerContext},
    error::{ActorCallError, ActorError, ActorStopError, ActorTellError},
    mailbox::MailboxConfig,
    runtime::{ActorRuntime, ActorSpawnOptions, PassivationPolicy, spawn_actor},
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

    handle.stop(StopReason::Requested).await.unwrap();
    stopped.acquire().await.unwrap().forget();

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

    let reply = handle.ask(StopAfterReply, ASK_TIMEOUT).await.unwrap();
    stopped.acquire().await.unwrap().forget();
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
async fn idle_passivation_retries_after_a_full_system_lane() {
    struct IdleActor {
        stopped: Arc<Semaphore>,
    }

    impl Actor for IdleActor {
        type Error = ActorError;
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
    }

    #[derive(Debug, crate::Message)]
    struct OccupySystemLane;

    impl Handler<Park> for IdleActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            msg: Park,
        ) -> Result<(), ActorError> {
            msg.entered.add_permits(1);
            msg.release.acquire().await.unwrap().forget();
            Ok(())
        }
    }

    impl Handler<OccupySystemLane> for IdleActor {
        async fn handle(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            _msg: OccupySystemLane,
        ) -> Result<(), ActorError> {
            Ok(())
        }
    }

    let stopped = Arc::new(Semaphore::new(0));
    let handle = ActorRuntime::default()
        .spawn_actor(
            IdleActor {
                stopped: stopped.clone(),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::with_lanes(8, 1),
                passivation: PassivationPolicy::IdleTimeout(Duration::from_millis(20)),
                ..ActorSpawnOptions::default()
            },
        )
        .await
        .unwrap();

    // Park the Actor, then occupy the only system slot so the idle stop is rejected once.
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    handle
        .try_tell_for_test(Park {
            entered: entered.clone(),
            release: release.clone(),
        })
        .unwrap();
    entered.acquire().await.unwrap().forget();
    handle.try_tell_system_for_test(OccupySystemLane).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    release.add_permits(1);

    tokio::time::timeout(Duration::from_secs(5), stopped.acquire())
        .await
        .expect("passivation monitor retries after backpressure")
        .unwrap()
        .forget();
}
