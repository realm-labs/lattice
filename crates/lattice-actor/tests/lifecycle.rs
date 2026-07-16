use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorCallError, ActorError, ActorStopError};
use lattice_actor::handle::ActorHandle;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::{ActorRuntime, ActorSpawnOptions, PassivationPolicy, spawn_actor};
use lattice_actor::traits::{
    Actor, ActorLifecycleState, ChildActorKey, ChildActorOptions, ChildSupervision, Handler,
    PassivationReason, Responder, StopReason,
};
use lattice_actor::watch::{ActorTerminated, TerminatedReason};
use lattice_core::service_context::ServiceContext;
use tokio::sync::{Mutex, Semaphore};

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn local_actor_watch_sends_typed_termination_notification() {
    struct TargetActor;

    #[async_trait]
    impl Actor for TargetActor {
        type Error = ActorError;
    }

    struct WatcherActor {
        target: ActorHandle<TargetActor>,
        events: Arc<Mutex<Vec<TerminatedReason>>>,
        notified: Arc<Semaphore>,
    }

    #[async_trait]
    impl Actor for WatcherActor {
        type Error = ActorError;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            ctx.watch(&self.target)?;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<ActorTerminated> for WatcherActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: ActorTerminated,
        ) -> Result<(), ActorError> {
            self.events.lock().await.push(msg.reason);
            self.notified.add_permits(1);
            Ok(())
        }
    }

    let target = spawn_actor(TargetActor, MailboxConfig::bounded(8));
    let events = Arc::new(Mutex::new(Vec::new()));
    let notified = Arc::new(Semaphore::new(0));
    let _watcher = spawn_actor(
        WatcherActor {
            target: target.clone(),
            events: events.clone(),
            notified: notified.clone(),
        },
        MailboxConfig::bounded(8),
    );

    tokio::time::sleep(Duration::from_millis(10)).await;
    target.stop(StopReason::Requested).await.unwrap();
    notified.acquire().await.unwrap().forget();

    assert_eq!(*events.lock().await, vec![TerminatedReason::Stopped]);
}

#[tokio::test]
async fn watcher_stop_auto_unwatches_local_target() {
    struct TargetActor;

    #[async_trait]
    impl Actor for TargetActor {
        type Error = ActorError;
    }

    struct WatcherActor {
        target: ActorHandle<TargetActor>,
        events: Arc<Mutex<Vec<TerminatedReason>>>,
    }

    #[async_trait]
    impl Actor for WatcherActor {
        type Error = ActorError;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            ctx.watch(&self.target)?;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<ActorTerminated> for WatcherActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: ActorTerminated,
        ) -> Result<(), ActorError> {
            self.events.lock().await.push(msg.reason);
            Ok(())
        }
    }

    let target = spawn_actor(TargetActor, MailboxConfig::bounded(8));
    let events = Arc::new(Mutex::new(Vec::new()));
    let watcher = spawn_actor(
        WatcherActor {
            target: target.clone(),
            events: events.clone(),
        },
        MailboxConfig::bounded(8),
    );

    tokio::time::sleep(Duration::from_millis(10)).await;
    watcher.stop(StopReason::Requested).await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    target.stop(StopReason::Requested).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert!(events.lock().await.is_empty());
}

#[tokio::test]
async fn local_child_actor_stops_with_parent_lifecycle() {
    struct ChildActor {
        stopped: Option<Arc<Semaphore>>,
    }

    #[async_trait]
    impl Actor for ChildActor {
        type Error = ActorError;
        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            if let Some(stopped) = self.stopped.take() {
                stopped.add_permits(1);
            }
            Ok(())
        }
    }

    struct ParentActor {
        child_stopped: Arc<Semaphore>,
    }

    #[async_trait]
    impl Actor for ParentActor {
        type Error = ActorError;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            ctx.spawn_child(
                ChildActorKey::new("child"),
                ChildActor {
                    stopped: Some(self.child_stopped.clone()),
                },
                ChildActorOptions::default(),
            )?;
            Ok(())
        }
    }

    let child_stopped = Arc::new(Semaphore::new(0));
    let parent = spawn_actor(
        ParentActor {
            child_stopped: child_stopped.clone(),
        },
        MailboxConfig::bounded(8),
    );

    tokio::time::sleep(Duration::from_millis(10)).await;
    parent.stop(StopReason::Requested).await.unwrap();

    tokio::time::timeout(Duration::from_millis(100), child_stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[tokio::test]
async fn local_child_actor_duplicate_key_is_rejected() {
    struct ChildActor;

    #[async_trait]
    impl Actor for ChildActor {
        type Error = ActorError;
    }

    struct ParentActor {
        duplicate_rejected: Arc<Semaphore>,
    }

    #[async_trait]
    impl Actor for ParentActor {
        type Error = ActorError;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            let key = ChildActorKey::new("child");
            ctx.spawn_child(key.clone(), ChildActor, ChildActorOptions::default())?;
            if ctx
                .spawn_child(key, ChildActor, ChildActorOptions::default())
                .is_err()
            {
                self.duplicate_rejected.add_permits(1);
            }
            Ok(())
        }
    }

    let duplicate_rejected = Arc::new(Semaphore::new(0));
    let _parent = spawn_actor(
        ParentActor {
            duplicate_rejected: duplicate_rejected.clone(),
        },
        MailboxConfig::bounded(8),
    );

    tokio::time::timeout(Duration::from_millis(100), duplicate_rejected.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[tokio::test]
async fn child_supervision_stop_parent_stops_parent_when_child_stops() {
    struct ChildActor;

    #[async_trait]
    impl Actor for ChildActor {
        type Error = ActorError;
    }

    #[derive(Debug, lattice_actor::Message)]
    struct StopChild;

    struct ParentActor {
        child: Option<ActorHandle<ChildActor>>,
        stopped: Option<Arc<Semaphore>>,
    }

    #[async_trait]
    impl Actor for ParentActor {
        type Error = ActorError;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            self.child = Some(ctx.spawn_child(
                ChildActorKey::new("child"),
                ChildActor,
                ChildActorOptions {
                    protocol_id: None,
                    mailbox: MailboxConfig::bounded(8),
                    supervision: ChildSupervision::StopParent,
                },
            )?);
            Ok(())
        }

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            if let Some(stopped) = self.stopped.take() {
                stopped.add_permits(1);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<StopChild> for ParentActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: StopChild,
        ) -> Result<(), ActorError> {
            self.child
                .as_ref()
                .expect("child should be available")
                .stop(StopReason::Requested)
                .await
                .map_err(|error| ActorError::new(error.to_string()))?;
            Ok(())
        }
    }

    let stopped = Arc::new(Semaphore::new(0));
    let parent = spawn_actor(
        ParentActor {
            child: None,
            stopped: Some(stopped.clone()),
        },
        MailboxConfig::bounded(8),
    );

    parent.tell(StopChild).await.unwrap();
    tokio::time::timeout(Duration::from_millis(100), stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[tokio::test]
async fn child_supervision_restart_child_recreates_child_from_factory() {
    struct ChildActor;

    #[async_trait]
    impl Actor for ChildActor {
        type Error = ActorError;
    }

    #[derive(Debug, lattice_actor::Message)]
    struct StopChild;

    struct ParentActor {
        child: Option<ActorHandle<ChildActor>>,
        child_started: Arc<Semaphore>,
    }

    #[async_trait]
    impl Actor for ParentActor {
        type Error = ActorError;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            let child_started = self.child_started.clone();
            self.child = Some(ctx.spawn_child_with_factory(
                ChildActorKey::new("child"),
                move || {
                    child_started.add_permits(1);
                    ChildActor
                },
                ChildActorOptions {
                    protocol_id: None,
                    mailbox: MailboxConfig::bounded(8),
                    supervision: ChildSupervision::RestartChild,
                },
            )?);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<StopChild> for ParentActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: StopChild,
        ) -> Result<(), ActorError> {
            self.child
                .as_ref()
                .expect("child should be available")
                .stop(StopReason::Requested)
                .await
                .map_err(|error| ActorError::new(error.to_string()))?;
            Ok(())
        }
    }

    let child_started = Arc::new(Semaphore::new(0));
    let parent = spawn_actor(
        ParentActor {
            child: None,
            child_started: child_started.clone(),
        },
        MailboxConfig::bounded(8),
    );

    child_started.acquire().await.unwrap().forget();
    parent.tell(StopChild).await.unwrap();
    tokio::time::timeout(Duration::from_millis(100), child_started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[tokio::test]
async fn handler_error_returns_to_caller_and_actor_remains_running() {
    #[derive(Debug, lattice_actor::Request)]
    #[request(response = String)]
    struct Ping(&'static str);

    #[derive(Debug, lattice_actor::Request)]
    #[request(response = ())]
    struct Fail;

    struct TestActor;

    #[async_trait]
    impl Actor for TestActor {
        type Error = ActorError;
    }

    #[async_trait]
    impl Responder<Ping> for TestActor {
        async fn respond(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            request: Ping,
            reply_to: ReplyTo<String>,
        ) -> Result<(), ActorError> {
            let _ = reply_to.send(format!("pong:{}", request.0));
            Ok(())
        }
    }

    #[async_trait]
    impl Responder<Fail> for TestActor {
        async fn respond(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _request: Fail,
            _reply_to: ReplyTo<()>,
        ) -> Result<(), ActorError> {
            Err(ActorError::new("handler failed"))
        }
    }

    let handle = spawn_actor(TestActor, MailboxConfig::bounded(8));

    let error = handle.ask(Fail, ASK_TIMEOUT).await;
    let reply = handle.ask(Ping("after-error"), ASK_TIMEOUT).await.unwrap();

    assert!(matches!(error, Err(ActorCallError::Handler(_))));
    assert_eq!(reply, "pong:after-error");
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Running);
}

#[tokio::test]
async fn stopping_failure_enters_stop_failed_state() {
    struct FailingStopActor;

    #[async_trait]
    impl Actor for FailingStopActor {
        type Error = ActorError;
        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            Err(ActorStopError::new("save failed"))
        }
    }

    let handle = spawn_actor(FailingStopActor, MailboxConfig::bounded(8));
    let mut lifecycle = handle.subscribe_lifecycle();

    handle.stop(StopReason::Requested).await.unwrap();
    tokio::time::timeout(Duration::from_millis(100), async {
        loop {
            lifecycle.changed().await.unwrap();
            if *lifecycle.borrow() == ActorLifecycleState::StopFailed {
                break;
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::StopFailed);
}

#[tokio::test]
async fn stopping_failure_retains_actor_state_and_retry_terminates_once() {
    struct RetainedActor {
        state: usize,
        persistence_available: Arc<AtomicBool>,
        persisted_state: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for RetainedActor {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Actor for RetainedActor {
        type Error = ActorError;

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            self.state += 1;
            if !self.persistence_available.load(Ordering::SeqCst) {
                return Err(ActorStopError::new("persistence unavailable"));
            }
            self.persisted_state.store(self.state, Ordering::SeqCst);
            Ok(())
        }
    }

    struct RetainedWatcher {
        target: ActorHandle<RetainedActor>,
        ready: Arc<Semaphore>,
        notifications: Arc<Mutex<Vec<TerminatedReason>>>,
        notified: Arc<Semaphore>,
    }

    #[async_trait]
    impl Actor for RetainedWatcher {
        type Error = ActorError;

        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            ctx.watch(&self.target)?;
            self.ready.add_permits(1);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<ActorTerminated> for RetainedWatcher {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            notification: ActorTerminated,
        ) -> Result<(), ActorError> {
            self.notifications.lock().await.push(notification.reason);
            self.notified.add_permits(1);
            Ok(())
        }
    }

    let persistence_available = Arc::new(AtomicBool::new(false));
    let persisted_state = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let handle = spawn_actor(
        RetainedActor {
            state: 40,
            persistence_available: persistence_available.clone(),
            persisted_state: persisted_state.clone(),
            dropped: dropped.clone(),
        },
        MailboxConfig::bounded(8),
    );
    let mut lifecycle = handle.subscribe_lifecycle();
    let mut terminated = handle.subscribe_terminated();
    let watcher_ready = Arc::new(Semaphore::new(0));
    let watcher_notifications = Arc::new(Mutex::new(Vec::new()));
    let watcher_notified = Arc::new(Semaphore::new(0));
    let _watcher = spawn_actor(
        RetainedWatcher {
            target: handle.clone(),
            ready: watcher_ready.clone(),
            notifications: watcher_notifications.clone(),
            notified: watcher_notified.clone(),
        },
        MailboxConfig::bounded(8),
    );
    watcher_ready.acquire().await.unwrap().forget();

    handle.stop(StopReason::Requested).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(terminated.try_recv().is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), watcher_notified.acquire())
            .await
            .is_err(),
        "DeathWatch must remain pending while the exact actor is retained"
    );
    let failure = handle.inspect_stop_failure().expect("failure record");
    assert_eq!(failure.reason, StopReason::Requested);
    assert_eq!(failure.attempt_count, 1);

    persistence_available.store(true, Ordering::SeqCst);
    handle.retry_stop().await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), terminated.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.reason, TerminatedReason::Stopped);
    assert_eq!(persisted_state.load(Ordering::SeqCst), 42);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(terminated.try_recv().is_err());
    watcher_notified.acquire().await.unwrap().forget();
    assert_eq!(
        *watcher_notifications.lock().await,
        vec![TerminatedReason::Stopped]
    );
    let mut late_termination = handle.subscribe_terminated();
    let late_event = tokio::time::timeout(Duration::from_secs(1), late_termination.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(late_event, event);
    assert!(late_termination.try_recv().is_err());
}

#[tokio::test]
async fn stop_failed_rejects_business_traffic_but_accepts_force_stop() {
    #[derive(lattice_actor::Message)]
    struct BusinessMessage;

    struct RetainedActor;

    #[async_trait]
    impl Actor for RetainedActor {
        type Error = ActorError;

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            Err(ActorStopError::new("save failed"))
        }
    }

    #[async_trait]
    impl Handler<BusinessMessage> for RetainedActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _msg: BusinessMessage,
        ) -> Result<(), ActorError> {
            panic!("business message must not reach a retained actor")
        }
    }

    let handle = spawn_actor(RetainedActor, MailboxConfig::bounded(8));
    let mut lifecycle = handle.subscribe_lifecycle();
    let mut data_loss = handle.subscribe_forced_data_loss();
    handle.stop(StopReason::Requested).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    assert!(matches!(
        handle.tell(BusinessMessage).await,
        Err(lattice_actor::error::ActorTellError::LifecycleUnavailable {
            state: ActorLifecycleState::StopFailed
        })
    ));

    handle
        .force_stop("approved state discard", "INC-1234")
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), data_loss.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.reason, "approved state discard");
    assert_eq!(event.ticket, "INC-1234");
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
}

#[tokio::test]
async fn passivation_policy_idle_timeout_stops_idle_actor() {
    struct IdleActor {
        stopped: Option<Arc<Semaphore>>,
    }

    #[async_trait]
    impl Actor for IdleActor {
        type Error = ActorError;
        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            reason: StopReason,
        ) -> Result<(), ActorStopError> {
            assert_eq!(
                reason,
                StopReason::Passivated(PassivationReason::IdleTimeout)
            );
            if let Some(stopped) = self.stopped.take() {
                stopped.add_permits(1);
            }
            Ok(())
        }
    }

    let runtime = ActorRuntime::default();
    let stopped = Arc::new(Semaphore::new(0));
    let handle = runtime
        .spawn_actor(
            IdleActor {
                stopped: Some(stopped.clone()),
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: None,
                scheduler_key: None,
                passivation: PassivationPolicy::IdleTimeout(Duration::from_millis(10)),
                self_ref: None,
                service: ServiceContext::empty(),
            },
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_millis(100), stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
}
