use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore, oneshot};

use crate::context::ActorContext;
use crate::error::{ActorActivationError, ActorCallError, ActorError, ActorTellError};
use crate::handle::ActorHandle;
use crate::mailbox::MailboxConfig;
use crate::registry::{ActorRegistry, ActorRegistryConfig};
use crate::runtime::{
    ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions, PassivationPolicy,
    spawn_actor,
};
use crate::traits::{
    Actor, ActorLifecycleState, ChildActorKey, ChildActorOptions, ChildSupervision, Handler,
    HandlerErrorAction, Message, PassivationReason, StopReason,
};
use crate::watch::{ActorTerminated, TerminatedReason};
use lattice_core::{ActorId, actor_kind};

#[derive(Debug)]
struct Ping(&'static str);

impl Message for Ping {
    type Reply = String;
}

#[derive(Debug)]
struct Record {
    value: &'static str,
    processed: Option<Arc<Semaphore>>,
}

impl Record {
    fn new(value: &'static str) -> Self {
        Self {
            value,
            processed: None,
        }
    }

    fn with_processed_signal(value: &'static str, processed: Arc<Semaphore>) -> Self {
        Self {
            value,
            processed: Some(processed),
        }
    }
}

impl Message for Record {
    type Reply = ();
}

#[derive(Debug)]
struct StopAfterReply;

impl Message for StopAfterReply {
    type Reply = &'static str;
}

#[derive(Debug)]
struct Tick;

impl Message for Tick {
    type Reply = ();
}

struct TestActor {
    events: Arc<Mutex<Vec<&'static str>>>,
    start_gate: Option<Arc<Semaphore>>,
    stopped: Option<Arc<Semaphore>>,
}

#[derive(Debug)]
struct Fail;

impl Message for Fail {
    type Reply = ();
}

#[async_trait]
impl Actor for TestActor {
    type Error = ActorError;
    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(gate) = self.start_gate.take() {
            let permit = gate
                .acquire()
                .await
                .map_err(|_| ActorError::new("start gate was closed"))?;
            permit.forget();
        }
        Ok(())
    }

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), crate::ActorStopError> {
        if let Some(stopped) = self.stopped.take() {
            stopped.add_permits(1);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Ping> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Ping,
    ) -> Result<String, ActorError> {
        self.events.lock().await.push(msg.0);
        Ok(format!("pong:{}", msg.0))
    }
}

#[async_trait]
impl Handler<Record> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Record,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push(msg.value);
        if let Some(processed) = msg.processed {
            processed.add_permits(1);
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<StopAfterReply> for TestActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _msg: StopAfterReply,
    ) -> Result<&'static str, ActorError> {
        self.events.lock().await.push("handled");
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        Ok("reply-before-stop")
    }
}

#[async_trait]
impl Handler<Tick> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: Tick,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push("tick");
        Ok(())
    }
}

#[async_trait]
impl Handler<Fail> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: Fail,
    ) -> Result<(), ActorError> {
        Err(ActorError::new("handler failed"))
    }
}

fn assert_handler_bound<A, M>()
where
    A: Handler<M>,
    M: Message,
{
}

#[derive(Debug, Error)]
enum BusinessActorError {
    #[error("business store is unavailable")]
    StoreUnavailable,
    #[error(transparent)]
    Framework(#[from] ActorError),
}

struct BusinessErrorActor {
    observed_errors: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl Actor for BusinessErrorActor {
    type Error = BusinessActorError;

    async fn on_error<M>(&mut self, _ctx: &mut ActorContext<Self>, error: &BusinessActorError)
    where
        M: Message,
    {
        let label = match error {
            BusinessActorError::StoreUnavailable => "store_unavailable",
            BusinessActorError::Framework(_) => "framework",
        };
        self.observed_errors.lock().await.push(label);
    }
}

struct LoadBusinessState;

impl Message for LoadBusinessState {
    type Reply = ();
}

struct RecoverBusinessState;

impl Message for RecoverBusinessState {
    type Reply = &'static str;
}

fn load_business_state() -> Result<(), BusinessActorError> {
    Err(BusinessActorError::StoreUnavailable)
}

#[async_trait]
impl Handler<LoadBusinessState> for BusinessErrorActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _msg: LoadBusinessState,
    ) -> Result<(), BusinessActorError> {
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        load_business_state()?;
        Ok(())
    }
}

#[async_trait]
impl Handler<RecoverBusinessState> for BusinessErrorActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _msg: RecoverBusinessState,
    ) -> Result<&'static str, BusinessActorError> {
        load_business_state()?;
        Ok("loaded")
    }

    async fn handle_error(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        error: BusinessActorError,
    ) -> HandlerErrorAction<&'static str, BusinessActorError> {
        match error {
            BusinessActorError::StoreUnavailable => HandlerErrorAction::Reply("fallback"),
            other => HandlerErrorAction::Propagate(other),
        }
    }
}

#[test]
fn handler_compile_time_bounds_are_typed() {
    assert_handler_bound::<TestActor, Ping>();
    assert_handler_bound::<TestActor, Record>();
}

#[tokio::test]
async fn actor_handler_can_use_business_error_with_question_mark() {
    let handle = spawn_actor(
        BusinessErrorActor {
            observed_errors: Arc::new(Mutex::new(Vec::new())),
        },
        MailboxConfig::default(),
    );

    let error = handle.call(LoadBusinessState).await.unwrap_err();

    match error {
        ActorCallError::Handler(error) => {
            assert_eq!(error.message(), "business store is unavailable");
        }
        other => panic!("expected business handler error, got {other:?}"),
    }
}

#[tokio::test]
async fn actor_handler_error_hook_can_recover_reply() {
    let observed_errors = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_actor(
        BusinessErrorActor {
            observed_errors: observed_errors.clone(),
        },
        MailboxConfig::default(),
    );

    let reply = handle.call(RecoverBusinessState).await.unwrap();

    assert_eq!(reply, "fallback");
    assert_eq!(*observed_errors.lock().await, vec!["store_unavailable"]);
}

#[tokio::test]
async fn actor_handle_call_and_tell_deliver_typed_messages() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let actor = TestActor {
        events: events.clone(),
        start_gate: None,
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));

    let reply = handle.call(Ping("one")).await.unwrap();
    handle.tell(Record::new("two")).await.unwrap();

    assert_eq!(reply, "pong:one");
    assert_eq!(*events.lock().await, vec!["one", "two"]);
}

#[test]
fn actor_execution_policy_defaults_to_task_per_actor() {
    assert_eq!(
        ActorRuntimeConfig::default().default_execution,
        ActorExecutionPolicy::TaskPerActor
    );
    assert_eq!(ActorSpawnOptions::default().execution, None);
}

#[tokio::test]
async fn actor_runtime_spawns_task_per_actor() {
    let runtime = ActorRuntime::new(ActorRuntimeConfig::default());
    let events = Arc::new(Mutex::new(Vec::new()));
    let actor = TestActor {
        events: events.clone(),
        start_gate: None,
        stopped: None,
    };

    let handle = runtime
        .spawn_actor(actor, ActorSpawnOptions::default())
        .await
        .unwrap();
    let reply = handle.call(Ping("runtime")).await.unwrap();

    assert_eq!(reply, "pong:runtime");
    assert_eq!(*events.lock().await, vec!["runtime"]);
}

#[tokio::test]
async fn keyed_worker_pool_system_mailbox_keeps_priority_over_normal_mailbox() {
    let runtime = ActorRuntime::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let start_gate = Arc::new(Semaphore::new(0));
    let processed = Arc::new(Semaphore::new(0));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: events.clone(),
                start_gate: Some(start_gate.clone()),
                stopped: None,
            },
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(8),
                execution: Some(ActorExecutionPolicy::KeyedWorkerPool { worker_count: 2 }),
                scheduler_key: None,
                passivation: PassivationPolicy::Disabled,
                self_ref: None,
            },
        )
        .await
        .unwrap();

    handle
        .try_tell_for_test(Record::with_processed_signal("normal", processed.clone()))
        .unwrap();
    handle
        .try_tell_system_for_test(Record::with_processed_signal("system", processed.clone()))
        .unwrap();
    start_gate.add_permits(1);
    processed.acquire_many(2).await.unwrap().forget();

    assert_eq!(*events.lock().await, vec!["system", "normal"]);
}

#[tokio::test]
async fn system_mailbox_has_priority_over_normal_mailbox() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let start_gate = Arc::new(Semaphore::new(0));
    let processed = Arc::new(Semaphore::new(0));
    let actor = TestActor {
        events: events.clone(),
        start_gate: Some(start_gate.clone()),
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));

    handle
        .try_tell_for_test(Record::with_processed_signal("normal", processed.clone()))
        .unwrap();
    handle
        .try_tell_system_for_test(Record::with_processed_signal("system", processed.clone()))
        .unwrap();
    start_gate.add_permits(1);
    processed.acquire_many(2).await.unwrap().forget();

    assert_eq!(*events.lock().await, vec!["system", "normal"]);
}

#[tokio::test]
async fn mailbox_full_returns_explicit_error() {
    let start_gate = Arc::new(Semaphore::new(0));
    let actor = TestActor {
        events: Arc::new(Mutex::new(Vec::new())),
        start_gate: Some(start_gate.clone()),
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(1));

    handle.try_tell_for_test(Record::new("first")).unwrap();
    let second = handle.try_tell_for_test(Record::new("second"));

    assert!(matches!(second, Err(ActorTellError::MailboxFull)));
    start_gate.add_permits(1);
}

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

    let result = handle.call(Ping("after-stop")).await;
    assert!(matches!(result, Err(ActorCallError::MailboxClosed)));
}

#[tokio::test]
async fn local_timer_delivers_message_to_actor() {
    struct TimerActor {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl Actor for TimerActor {
        type Error = ActorError;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            ctx.notify_after(std::time::Duration::from_millis(5), Tick);
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<Tick> for TimerActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
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

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
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

    #[async_trait]
    impl Actor for TaskActor {
        type Error = ActorError;
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

    tokio::time::timeout(std::time::Duration::from_millis(100), dropped_rx)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn business_passivation_happens_after_handler_reply() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(Semaphore::new(0));
    let actor = TestActor {
        events: events.clone(),
        start_gate: None,
        stopped: Some(stopped.clone()),
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));

    let reply = handle.call(StopAfterReply).await.unwrap();
    stopped.acquire().await.unwrap().forget();
    let after_stop = handle.tell(Record::new("after-stop")).await;

    assert_eq!(reply, "reply-before-stop");
    assert_eq!(*events.lock().await, vec!["handled"]);
    assert!(matches!(after_stop, Err(ActorTellError::MailboxClosed)));
}

#[tokio::test]
async fn actor_registry_prevents_duplicate_start() {
    let registry =
        ActorRegistry::<TestActor>::new(actor_kind!("Test"), ActorRegistryConfig::default());
    let actor_id = ActorId::U64(1);
    let actor = TestActor {
        events: Arc::new(Mutex::new(Vec::new())),
        start_gate: None,
        stopped: None,
    };

    registry.start(actor_id.clone(), actor).await.unwrap();
    let duplicate = registry
        .start(
            actor_id,
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            },
        )
        .await;

    assert!(matches!(
        duplicate,
        Err(ActorActivationError::AlreadyExists)
    ));
}

#[tokio::test]
async fn actor_registry_activation_waiters_share_single_activation() {
    let registry = Arc::new(ActorRegistry::<TestActor>::new(
        actor_kind!("Test"),
        ActorRegistryConfig::default(),
    ));
    let actor_id = ActorId::U64(2);
    let activations = Arc::new(AtomicUsize::new(0));
    let activation_entered = Arc::new(Semaphore::new(0));
    let start_gate = Arc::new(Semaphore::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));

    let first = {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        let activations = activations.clone();
        let activation_entered = activation_entered.clone();
        let start_gate = start_gate.clone();
        let events = events.clone();
        tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async move {
                    activations.fetch_add(1, Ordering::SeqCst);
                    activation_entered.add_permits(1);
                    let permit = start_gate.acquire().await.unwrap();
                    permit.forget();
                    Ok(TestActor {
                        events,
                        start_gate: None,
                        stopped: None,
                    })
                })
                .await
        })
    };
    activation_entered.acquire().await.unwrap().forget();

    let mut tasks = vec![first];
    for _ in 0..3 {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        tasks.push(tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async {
                    panic!("waiter must not run activation")
                })
                .await
        }));
    }

    start_gate.add_permits(1);

    for task in tasks {
        task.await.unwrap().unwrap();
    }

    assert_eq!(activations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn actor_registry_bounds_and_times_out_activation_waiters() {
    let registry = Arc::new(ActorRegistry::<TestActor>::new(
        actor_kind!("Test"),
        ActorRegistryConfig {
            mailbox: MailboxConfig::bounded(8),
            passivation: Default::default(),
            waiter_capacity: 0,
            waiter_timeout: std::time::Duration::from_millis(20),
            actor_ref: None,
        },
    ));
    let actor_id = ActorId::U64(3);
    let start_gate = Arc::new(Semaphore::new(0));

    let first = tokio::spawn({
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        let start_gate = start_gate.clone();
        async move {
            registry
                .get_or_activate(actor_id, || async move {
                    let permit = start_gate.acquire().await.unwrap();
                    permit.forget();
                    Ok(TestActor {
                        events: Arc::new(Mutex::new(Vec::new())),
                        start_gate: None,
                        stopped: None,
                    })
                })
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let second = registry
        .get_or_activate(actor_id, || async {
            Ok(TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            })
        })
        .await;

    assert!(matches!(
        second,
        Err(ActorActivationError::WaiterCapacityExceeded)
    ));
    start_gate.add_permits(1);
    first.await.unwrap().unwrap();
}

#[tokio::test]
async fn actor_registry_activation_failure_wakes_waiters_and_allows_retry() {
    let registry = Arc::new(ActorRegistry::<TestActor>::new(
        actor_kind!("Test"),
        ActorRegistryConfig::default(),
    ));
    let actor_id = ActorId::U64(4);
    let activation_entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));

    let first = {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        let activation_entered = activation_entered.clone();
        let release = release.clone();
        tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async move {
                    activation_entered.add_permits(1);
                    let permit = release.acquire().await.unwrap();
                    permit.forget();
                    Err(ActorError::new("load failed"))
                })
                .await
        })
    };

    activation_entered.acquire().await.unwrap().forget();
    let waiter = {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async {
                    panic!("waiter must not run activation")
                })
                .await
        })
    };

    release.add_permits(1);
    assert!(matches!(
        first.await.unwrap(),
        Err(ActorActivationError::ActivationFailed(_))
    ));
    assert!(matches!(
        waiter.await.unwrap(),
        Err(ActorActivationError::ActivationFailed(_))
    ));

    let retry = registry
        .get_or_activate(actor_id, || async {
            Ok(TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            })
        })
        .await;

    assert!(retry.is_ok());
}

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

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
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

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    watcher.stop(StopReason::Requested).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    target.stop(StopReason::Requested).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

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
        ) -> Result<(), crate::ActorStopError> {
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

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    parent.stop(StopReason::Requested).await.unwrap();

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        child_stopped.acquire(),
    )
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

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        duplicate_rejected.acquire(),
    )
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

    #[derive(Debug)]
    struct StopChild;

    impl Message for StopChild {
        type Reply = ();
    }

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
        ) -> Result<(), crate::ActorStopError> {
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
    tokio::time::timeout(std::time::Duration::from_millis(100), stopped.acquire())
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

    #[derive(Debug)]
    struct StopChild;

    impl Message for StopChild {
        type Reply = ();
    }

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
    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        child_started.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
}

#[tokio::test]
async fn handler_error_returns_to_caller_and_actor_remains_running() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let actor = TestActor {
        events: events.clone(),
        start_gate: None,
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));

    let error = handle.call(Fail).await;
    let reply = handle.call(Ping("after-error")).await.unwrap();

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
        ) -> Result<(), crate::ActorStopError> {
            Err(crate::ActorStopError::new("save failed"))
        }
    }

    let handle = spawn_actor(FailingStopActor, MailboxConfig::bounded(8));
    let mut lifecycle = handle.subscribe_lifecycle();

    handle.stop(StopReason::Requested).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
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
        ) -> Result<(), crate::ActorStopError> {
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
                passivation: PassivationPolicy::IdleTimeout(std::time::Duration::from_millis(10)),
                self_ref: None,
            },
        )
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_millis(100), stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
}
