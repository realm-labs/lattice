use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore, oneshot};

use crate::context::ActorContext;
use crate::error::{
    ActorActivationError, ActorCallError, ActorError, ActorStopError, ActorTellError,
};
use crate::mailbox::MailboxConfig;
use crate::registry::{ActorRegistry, ActorRegistryConfig};
use crate::runtime::{
    ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions, PassivationPolicy,
    spawn_actor,
};
use crate::traits::{
    Actor, ChildActorKey, ChildActorOptions, Handler, HandlerErrorAction, Message,
    PassivationReason, StopReason,
};
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::service_context::ServiceContext;
use lattice_core::{actor_kind, service_kind};

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

#[derive(Debug)]
struct ReadContextInstance;

impl Message for ReadContextInstance {
    type Reply = InstanceId;
}

#[derive(Debug)]
struct SpawnContextChild;

impl Message for SpawnContextChild {
    type Reply = InstanceId;
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
    ) -> Result<(), ActorStopError> {
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
impl Handler<ReadContextInstance> for TestActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _msg: ReadContextInstance,
    ) -> Result<InstanceId, ActorError> {
        Ok(ctx.service().instance_id().clone())
    }
}

#[async_trait]
impl Handler<SpawnContextChild> for TestActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _msg: SpawnContextChild,
    ) -> Result<InstanceId, ActorError> {
        let child = TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: None,
            stopped: None,
        };
        let handle = ctx.spawn_child(
            ChildActorKey::new("context-child"),
            child,
            ChildActorOptions::default(),
        )?;
        handle
            .call(ReadContextInstance)
            .await
            .map_err(|error| ActorError::new(error.to_string()))
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
    let barrier = handle.call(Ping("barrier")).await.unwrap();

    assert_eq!(reply, "pong:one");
    assert_eq!(barrier, "pong:barrier");
    assert_eq!(*events.lock().await, vec!["one", "two", "barrier"]);
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
async fn standalone_actor_receives_empty_service_context() {
    let handle = spawn_actor(
        TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: None,
            stopped: None,
        },
        MailboxConfig::default(),
    );

    let instance = handle.call(ReadContextInstance).await.unwrap();

    assert_eq!(instance, InstanceId::new("local"));
}

#[tokio::test]
async fn actor_spawn_options_pass_service_context_to_handler_and_child() {
    let runtime = ActorRuntime::default();
    let service = ServiceContext::new(service_kind!("World"), InstanceId::new("world-service"));
    let handle = runtime
        .spawn_actor(
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            },
            ActorSpawnOptions {
                service: service.clone(),
                ..ActorSpawnOptions::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        handle.call(ReadContextInstance).await.unwrap(),
        InstanceId::new("world-service")
    );
    assert_eq!(
        handle.call(SpawnContextChild).await.unwrap(),
        InstanceId::new("world-service")
    );
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
                service: ServiceContext::empty(),
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
            shard_migration: Default::default(),
            waiter_capacity: 0,
            waiter_timeout: std::time::Duration::from_millis(20),
            actor_ref: None,
            service: ServiceContext::empty(),
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
