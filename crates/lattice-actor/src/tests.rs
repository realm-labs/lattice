use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_core::{
    actor_kind, id::ActorId, instance::InstanceId, service_context::ServiceContext, service_kind,
};
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore, oneshot};

use crate::{
    context::ActorContext,
    error::{
        ActorActivationError, ActorCallError, ActorError, ActorStopError, ActorTellError,
        PipeToSelfError,
    },
    mailbox::MailboxConfig,
    registry::{ActorRegistry, ActorRegistryConfig},
    reply::ReplyTo,
    runtime::{
        ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions,
        PassivationPolicy, spawn_actor,
    },
    traits::{
        Actor, ActorLifecycleState, ChildActorKey, ChildActorOptions, Handler, Message,
        MessageMetadata, PassivationReason, Request, Responder, ResponderErrorAction, StopReason,
    },
};

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, crate::Request)]
#[request(response = String)]
struct Ping(&'static str);

#[derive(Debug, crate::Message)]
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

#[derive(Debug, crate::Request)]
#[request(response = &'static str)]
struct StopAfterReply;

#[derive(Debug, crate::Message)]
struct Tick;

#[derive(Debug, crate::Request)]
#[request(response = &'static str)]
struct DeferredReply {
    gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
}

#[derive(crate::Message)]
struct DeferredReady {
    reply_to: ReplyTo<&'static str>,
}

#[derive(crate::Message)]
struct PipeRecord {
    gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
    processed: Arc<Semaphore>,
}

#[derive(crate::Request)]
#[request(response = bool)]
struct ProbePipeCapacity {
    gate: Arc<Semaphore>,
}

#[derive(Debug, crate::Request)]
#[request(response = InstanceId)]
struct ReadContextInstance;

#[derive(Debug, crate::Request)]
#[request(response = InstanceId)]
struct SpawnContextChild;

#[derive(crate::Message)]
struct ContextChildResolved {
    result: Result<InstanceId, ActorCallError>,
    reply_to: ReplyTo<InstanceId>,
}

struct TestActor {
    events: Arc<Mutex<Vec<&'static str>>>,
    start_gate: Option<Arc<Semaphore>>,
    stopped: Option<Arc<Semaphore>>,
}

#[derive(Debug, crate::Message)]
struct Fail;

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

impl Responder<Ping> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: Ping,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push(request.0);
        assert!(ctx.sender().is_none());
        let _ = reply_to.send(format!("pong:{}", request.0));
        Ok(())
    }
}

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

impl Handler<PipeRecord> for TestActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        message: PipeRecord,
    ) -> Result<(), ActorError> {
        message.entered.add_permits(1);
        let gate = message.gate;
        let processed = message.processed;
        ctx.pipe_to_self(
            async move {
                if let Ok(permit) = gate.acquire_owned().await {
                    permit.forget();
                }
                "piped"
            },
            move |value| Record::with_processed_signal(value, processed),
        )?;
        Ok(())
    }
}

impl Responder<ProbePipeCapacity> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: ProbePipeCapacity,
        reply_to: ReplyTo<bool>,
    ) -> Result<(), ActorError> {
        let gate = request.gate;
        ctx.pipe_to_self(
            async move {
                if let Ok(permit) = gate.acquire_owned().await {
                    permit.forget();
                }
            },
            |()| Tick,
        )?;
        let rejected = matches!(
            ctx.pipe_to_self(async {}, |()| Tick),
            Err(PipeToSelfError::Capacity { capacity: 1 })
        );
        reply_to.send(rejected)?;
        Ok(())
    }
}

impl Responder<StopAfterReply> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _request: StopAfterReply,
        reply_to: ReplyTo<&'static str>,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push("handled");
        let _ = reply_to.send("reply-before-stop");
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        Ok(())
    }
}

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

impl Responder<ReadContextInstance> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _request: ReadContextInstance,
        reply_to: ReplyTo<InstanceId>,
    ) -> Result<(), ActorError> {
        let _ = reply_to.send(ctx.service().instance_id().clone());
        Ok(())
    }
}

impl Responder<SpawnContextChild> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _request: SpawnContextChild,
        reply_to: ReplyTo<InstanceId>,
    ) -> Result<(), ActorError> {
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
        ctx.defer_reply(
            reply_to,
            async move { handle.ask(ReadContextInstance, ASK_TIMEOUT).await },
            |result, reply_to| ContextChildResolved { result, reply_to },
        )?;
        Ok(())
    }
}

impl Handler<ContextChildResolved> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        message: ContextChildResolved,
    ) -> Result<(), ActorError> {
        match message.result {
            Ok(instance) => message.reply_to.send(instance)?,
            Err(error) => message.reply_to.fail_with(error)?,
        }
        Ok(())
    }
}

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

impl Responder<DeferredReply> for TestActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: DeferredReply,
        reply_to: ReplyTo<&'static str>,
    ) -> Result<(), ActorError> {
        request.entered.add_permits(1);
        ctx.defer_reply(
            reply_to,
            async move {
                if let Ok(permit) = request.gate.acquire().await {
                    permit.forget();
                }
            },
            |(), reply_to| DeferredReady { reply_to },
        )?;
        Ok(())
    }
}

impl Handler<DeferredReady> for TestActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        message: DeferredReady,
    ) -> Result<(), ActorError> {
        self.events.lock().await.push("deferred-ready");
        let _ = message.reply_to.send("done");
        Ok(())
    }
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

impl Actor for BusinessErrorActor {
    type Error = BusinessActorError;

    async fn on_error<M>(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        error: &BusinessActorError,
    ) where
        M: Send + 'static,
    {
        let label = match error {
            BusinessActorError::StoreUnavailable => "store_unavailable",
            BusinessActorError::Framework(_) => "framework",
        };
        self.observed_errors.lock().await.push(label);
    }
}

#[derive(crate::Request)]
#[request(response = ())]
struct LoadBusinessState;

#[derive(crate::Request)]
#[request(response = &'static str)]
struct RecoverBusinessState;

fn load_business_state() -> Result<(), BusinessActorError> {
    Err(BusinessActorError::StoreUnavailable)
}

impl Responder<LoadBusinessState> for BusinessErrorActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _request: LoadBusinessState,
        _reply_to: ReplyTo<()>,
    ) -> Result<(), BusinessActorError> {
        ctx.request_passivation(PassivationReason::BusinessIdle)?;
        load_business_state()?;
        Ok(())
    }
}

impl Responder<RecoverBusinessState> for BusinessErrorActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: RecoverBusinessState,
        reply_to: ReplyTo<&'static str>,
    ) -> Result<(), BusinessActorError> {
        load_business_state()?;
        let _ = reply_to.send("loaded");
        Ok(())
    }

    async fn respond_error(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        error: BusinessActorError,
    ) -> ResponderErrorAction<&'static str, BusinessActorError> {
        match error {
            BusinessActorError::StoreUnavailable => ResponderErrorAction::Respond("fallback"),
            other => ResponderErrorAction::Propagate(other),
        }
    }
}

#[test]
fn handler_compile_time_bounds_are_typed() {
    assert_handler_bound::<TestActor, Record>();
    fn assert_responder_bound<A, R>()
    where
        A: Responder<R>,
        R: Request,
    {
    }
    assert_responder_bound::<TestActor, Ping>();
}

#[tokio::test]
async fn actor_handler_can_use_business_error_with_question_mark() {
    let handle = spawn_actor(
        BusinessErrorActor {
            observed_errors: Arc::new(Mutex::new(Vec::new())),
        },
        MailboxConfig::default(),
    );

    let error = handle
        .ask(LoadBusinessState, ASK_TIMEOUT)
        .await
        .unwrap_err();

    match error {
        ActorCallError::Handler(error) => {
            assert_eq!(error.message(), "business store is unavailable");
        }
        other => panic!("expected business handler error, got {other:?}"),
    }
}

#[tokio::test]
async fn actor_handler_error_hook_can_recover_response() {
    let observed_errors = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_actor(
        BusinessErrorActor {
            observed_errors: observed_errors.clone(),
        },
        MailboxConfig::default(),
    );

    let reply = handle.ask(RecoverBusinessState, ASK_TIMEOUT).await.unwrap();

    assert_eq!(reply, "fallback");
    assert_eq!(*observed_errors.lock().await, vec!["store_unavailable"]);
}

#[tokio::test]
async fn actor_handle_ask_and_tell_deliver_typed_messages() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let actor = TestActor {
        events: events.clone(),
        start_gate: None,
        stopped: None,
    };
    let handle = spawn_actor(actor, MailboxConfig::bounded(8));

    let reply = handle.ask(Ping("one"), ASK_TIMEOUT).await.unwrap();
    handle.tell(Record::new("two")).await.unwrap();
    let barrier = handle.ask(Ping("barrier"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(reply, "pong:one");
    assert_eq!(barrier, "pong:barrier");
    assert_eq!(*events.lock().await, vec!["one", "two", "barrier"]);
}

#[tokio::test]
async fn deferred_reply_does_not_block_the_actor_mailbox() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_actor(
        TestActor {
            events,
            start_gate: None,
            stopped: None,
        },
        MailboxConfig::default(),
    );
    let reply_gate = Arc::new(Semaphore::new(0));
    let deferred_gate = reply_gate.clone();
    let entered = Arc::new(Semaphore::new(0));
    let deferred_entered = entered.clone();
    let ask_handle = handle.clone();
    let ask = tokio::spawn(async move {
        ask_handle
            .ask(
                DeferredReply {
                    gate: deferred_gate,
                    entered: deferred_entered,
                },
                ASK_TIMEOUT,
            )
            .await
    });

    entered.acquire().await.unwrap().forget();
    let processed = Arc::new(Semaphore::new(0));
    handle
        .tell(Record::with_processed_signal(
            "after-deferred",
            processed.clone(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), processed.acquire())
        .await
        .expect("mailbox should accept the next message while the reply is pending")
        .unwrap()
        .forget();
    assert!(!ask.is_finished());

    // The deferred task owns the ask sender and can answer after the handler returned.
    reply_gate.add_permits(1);
    assert_eq!(ask.await.unwrap().unwrap(), "done");
}

#[tokio::test]
async fn pipe_to_self_posts_async_results_from_a_regular_handler() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let handle = spawn_actor(
        TestActor {
            events: events.clone(),
            start_gate: None,
            stopped: None,
        },
        MailboxConfig::default(),
    );
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let pipe_processed = Arc::new(Semaphore::new(0));
    handle
        .tell(PipeRecord {
            gate: gate.clone(),
            entered: entered.clone(),
            processed: pipe_processed.clone(),
        })
        .await
        .unwrap();
    entered.acquire().await.unwrap().forget();

    let interleaved = Arc::new(Semaphore::new(0));
    handle
        .tell(Record::with_processed_signal(
            "while-pipe-pending",
            interleaved.clone(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), interleaved.acquire())
        .await
        .expect("mailbox should continue while pipe-to-self work is pending")
        .unwrap()
        .forget();

    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), pipe_processed.acquire())
        .await
        .expect("pipe-to-self result should return through the mailbox")
        .unwrap()
        .forget();
    handle.ask(Ping("barrier"), ASK_TIMEOUT).await.unwrap();

    assert_eq!(
        *events.lock().await,
        vec!["while-pipe-pending", "piped", "barrier"]
    );
}

#[tokio::test]
async fn pipe_to_self_enforces_deferred_operation_capacity() {
    let handle = spawn_actor(
        TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: None,
            stopped: None,
        },
        MailboxConfig::bounded(8).with_deferred_capacity(1),
    );
    let gate = Arc::new(Semaphore::new(0));

    assert!(
        handle
            .ask(ProbePipeCapacity { gate: gate.clone() }, ASK_TIMEOUT)
            .await
            .unwrap()
    );
    gate.add_permits(1);
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
    let reply = handle.ask(Ping("runtime"), ASK_TIMEOUT).await.unwrap();

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

    let instance = handle.ask(ReadContextInstance, ASK_TIMEOUT).await.unwrap();

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
        handle.ask(ReadContextInstance, ASK_TIMEOUT).await.unwrap(),
        InstanceId::new("world-service")
    );
    assert_eq!(
        handle.ask(SpawnContextChild, ASK_TIMEOUT).await.unwrap(),
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

    let result = handle.ask(Ping("after-stop"), ASK_TIMEOUT).await;
    assert!(matches!(
        result,
        Err(ActorCallError::LifecycleUnavailable {
            state: ActorLifecycleState::Stopped
        })
    ));
}

#[tokio::test]
async fn local_timer_delivers_message_to_actor() {
    struct TimerActor {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Actor for TimerActor {
        type Error = ActorError;
        async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            ctx.notify_after(Duration::from_millis(5), Tick);
            Ok(())
        }
    }

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
    assert!(matches!(
        after_stop,
        Err(ActorTellError::LifecycleUnavailable {
            state: ActorLifecycleState::Stopped
        })
    ));
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
            waiter_timeout: Duration::from_millis(20),
            quarantine_capacity: 8,
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
    tokio::time::sleep(Duration::from_millis(10)).await;

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
