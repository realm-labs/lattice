use std::any::type_name;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::{ActorCallError, ActorError, ActorStopError};
use lattice_actor::handle::ActorHandle;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::observation::{
    ActorLifecycleEvent, ActorMetadata, ActorObserver, ActorObserverHandle, RequestCompletion,
};
use lattice_actor::registry::{ActorRegistry, ActorRegistryConfig};
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::{
    ActorExecutionPolicy, ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions,
};
use lattice_actor::traits::{
    Actor, ActorLifecycleState, ChildActorKey, ChildActorOptions, ChildSupervision, Handler,
    MessageMetadata, MessageOutcome, Responder, StopReason,
};
use lattice_actor::watch::TerminatedReason;
use lattice_core::actor_kind;
use lattice_core::id::ActorId;
use tokio::sync::Semaphore;

const TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, PartialEq, Eq)]
enum ObserverEvent {
    MessageEnqueued(&'static str),
    MessageFinished(&'static str, MessageOutcome),
    RequestCompleted(&'static str, RequestCompletion),
    Lifecycle(ActorLifecycleEvent),
}

#[derive(Clone, Default)]
struct RecordingObserver {
    events: Arc<Mutex<Vec<ObserverEvent>>>,
}

impl RecordingObserver {
    fn snapshot(&self) -> Vec<ObserverEvent> {
        self.events.lock().expect("observer mutex poisoned").clone()
    }
}

impl ActorObserver for RecordingObserver {
    fn message_enqueued(
        &self,
        _actor: &ActorMetadata,
        message: &MessageMetadata,
        _queue_depth: usize,
    ) {
        self.events
            .lock()
            .expect("observer mutex poisoned")
            .push(ObserverEvent::MessageEnqueued(message.type_name()));
    }

    fn message_finished(
        &self,
        _actor: &ActorMetadata,
        message: &MessageMetadata,
        outcome: MessageOutcome,
        _processing_time: Duration,
    ) {
        self.events
            .lock()
            .expect("observer mutex poisoned")
            .push(ObserverEvent::MessageFinished(message.type_name(), outcome));
    }

    fn request_completed(
        &self,
        _actor: &ActorMetadata,
        message: &MessageMetadata,
        completion: RequestCompletion,
        _total_time: Duration,
    ) {
        self.events
            .lock()
            .expect("observer mutex poisoned")
            .push(ObserverEvent::RequestCompleted(
                message.type_name(),
                completion,
            ));
    }

    fn lifecycle(&self, _actor: &ActorMetadata, event: ActorLifecycleEvent) {
        self.events
            .lock()
            .expect("observer mutex poisoned")
            .push(ObserverEvent::Lifecycle(event));
    }
}

#[derive(lattice_actor::Message)]
struct Crash;

#[derive(lattice_actor::Request)]
#[request(response = u32)]
struct Ping;

struct PolicyActor {
    stopping_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Actor for PolicyActor {
    type Error = ActorError;

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        self.stopping_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Handler<Crash> for PolicyActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message: Crash,
    ) -> Result<(), ActorError> {
        panic!("policy actor crashed")
    }
}

#[async_trait]
impl Responder<Ping> for PolicyActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: Ping,
        reply_to: ReplyTo<u32>,
    ) -> Result<(), ActorError> {
        reply_to.send(1)?;
        Ok(())
    }
}

#[tokio::test]
async fn callback_panic_terminates_actor_under_every_execution_policy() {
    for policy in [
        ActorExecutionPolicy::TaskPerActor,
        ActorExecutionPolicy::KeyedWorkerPool { worker_count: 1 },
        ActorExecutionPolicy::DedicatedThreadPool { worker_count: 1 },
    ] {
        let runtime = ActorRuntime::default();
        let stopping_calls = Arc::new(AtomicUsize::new(0));
        let handle = runtime
            .spawn_actor(
                PolicyActor {
                    stopping_calls: stopping_calls.clone(),
                },
                options(policy),
            )
            .await
            .unwrap();
        let mut terminated = handle.subscribe_terminated();

        handle.tell(Crash).await.unwrap();
        let event = tokio::time::timeout(TIMEOUT, terminated.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.reason, TerminatedReason::Panicked);
        assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
        assert_eq!(stopping_calls.load(Ordering::SeqCst), 0);

        let replacement = runtime
            .spawn_actor(
                PolicyActor {
                    stopping_calls: Arc::new(AtomicUsize::new(0)),
                },
                options(policy),
            )
            .await
            .unwrap();
        assert_eq!(replacement.ask(Ping, TIMEOUT).await.unwrap(), 1);
    }
}

fn options(policy: ActorExecutionPolicy) -> ActorSpawnOptions {
    ActorSpawnOptions {
        mailbox: MailboxConfig::bounded(8),
        execution: Some(policy),
        scheduler_key: Some(ActorId::U64(7)),
        ..ActorSpawnOptions::default()
    }
}

#[derive(lattice_actor::Request)]
#[request(response = u32)]
struct CrashRequest;

struct RequestPanicActor;

#[async_trait]
impl Actor for RequestPanicActor {
    type Error = ActorError;
}

#[async_trait]
impl Responder<CrashRequest> for RequestPanicActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: CrashRequest,
        _reply_to: ReplyTo<u32>,
    ) -> Result<(), ActorError> {
        panic!("request crashed")
    }
}

#[tokio::test]
async fn request_panic_completes_ask_and_observation_once() {
    let observer = RecordingObserver::default();
    let runtime = ActorRuntime::new(ActorRuntimeConfig {
        default_execution: ActorExecutionPolicy::TaskPerActor,
        observer: ActorObserverHandle::new(observer.clone()),
    });
    let handle = runtime
        .spawn_actor(RequestPanicActor, ActorSpawnOptions::default())
        .await
        .unwrap();
    let mut terminated = handle.subscribe_terminated();

    assert_eq!(
        handle.ask(CrashRequest, TIMEOUT).await,
        Err(ActorCallError::ActorPanicked)
    );
    assert_eq!(
        tokio::time::timeout(TIMEOUT, terminated.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        TerminatedReason::Panicked
    );

    let events = observer.snapshot();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                **event
                    == ObserverEvent::RequestCompleted(
                        type_name::<CrashRequest>(),
                        RequestCompletion::ActorPanicked,
                    )
            })
            .count(),
        1
    );
    assert!(events.contains(&ObserverEvent::MessageFinished(
        type_name::<CrashRequest>(),
        MessageOutcome::Panicked,
    )));
    assert!(events.contains(&ObserverEvent::Lifecycle(ActorLifecycleEvent::Panicked)));
}

struct BeforeHookPanicActor;

#[async_trait]
impl Actor for BeforeHookPanicActor {
    type Error = ActorError;

    fn before_message(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message: lattice_actor::traits::MessageView<'_>,
    ) {
        panic!("before hook crashed")
    }
}

#[async_trait]
impl Responder<CrashRequest> for BeforeHookPanicActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: CrashRequest,
        reply_to: ReplyTo<u32>,
    ) -> Result<(), ActorError> {
        reply_to.send(1)?;
        Ok(())
    }
}

#[tokio::test]
async fn panic_before_reply_control_registration_returns_actor_panicked() {
    let handle =
        lattice_actor::runtime::spawn_actor(BeforeHookPanicActor, MailboxConfig::bounded(8));
    assert_eq!(
        handle.ask(CrashRequest, TIMEOUT).await,
        Err(ActorCallError::ActorPanicked)
    );
}

struct AfterHookPanicActor;

#[async_trait]
impl Actor for AfterHookPanicActor {
    type Error = ActorError;

    fn after_message(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _metadata: &MessageMetadata,
        _outcome: MessageOutcome,
    ) {
        panic!("after hook crashed")
    }
}

#[async_trait]
impl Responder<Ping> for AfterHookPanicActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: Ping,
        reply_to: ReplyTo<u32>,
    ) -> Result<(), ActorError> {
        reply_to.send(7)?;
        Ok(())
    }
}

#[tokio::test]
async fn panic_after_successful_reply_does_not_overwrite_the_reply() {
    let handle =
        lattice_actor::runtime::spawn_actor(AfterHookPanicActor, MailboxConfig::bounded(8));
    let mut terminated = handle.subscribe_terminated();
    assert_eq!(handle.ask(Ping, TIMEOUT).await.unwrap(), 7);
    assert_eq!(
        tokio::time::timeout(TIMEOUT, terminated.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        TerminatedReason::Panicked
    );
}

#[derive(lattice_actor::Message)]
struct BlockingCrash {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[derive(lattice_actor::Request)]
#[request(response = u32)]
struct QueuedRequest;

struct QueueActor;

#[async_trait]
impl Actor for QueueActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<BlockingCrash> for QueueActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        message: BlockingCrash,
    ) -> Result<(), ActorError> {
        message.entered.add_permits(1);
        message.release.acquire().await.unwrap().forget();
        panic!("blocked handler crashed")
    }
}

#[async_trait]
impl Responder<QueuedRequest> for QueueActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: QueuedRequest,
        reply_to: ReplyTo<u32>,
    ) -> Result<(), ActorError> {
        reply_to.send(1)?;
        Ok(())
    }
}

#[tokio::test]
async fn queued_ask_is_rejected_with_actor_panicked() {
    let observer = RecordingObserver::default();
    let runtime = ActorRuntime::new(ActorRuntimeConfig {
        default_execution: ActorExecutionPolicy::TaskPerActor,
        observer: ActorObserverHandle::new(observer.clone()),
    });
    let handle = runtime
        .spawn_actor(QueueActor, ActorSpawnOptions::default())
        .await
        .unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    handle
        .tell(BlockingCrash {
            entered: entered.clone(),
            release: release.clone(),
        })
        .await
        .unwrap();
    entered.acquire().await.unwrap().forget();

    let ask = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ask(QueuedRequest, TIMEOUT).await }
    });
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if observer
                .snapshot()
                .contains(&ObserverEvent::MessageEnqueued(type_name::<QueuedRequest>()))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    release.add_permits(1);

    assert_eq!(ask.await.unwrap(), Err(ActorCallError::ActorPanicked));
    assert_eq!(
        observer
            .snapshot()
            .iter()
            .filter(|event| {
                **event
                    == ObserverEvent::RequestCompleted(
                        type_name::<QueuedRequest>(),
                        RequestCompletion::ActorPanicked,
                    )
            })
            .count(),
        1
    );
}

#[derive(lattice_actor::Request)]
#[request(response = u32)]
struct HeldRequest {
    held: Arc<Semaphore>,
}

struct DeferredPanicActor {
    reply: Option<ReplyTo<u32>>,
}

#[async_trait]
impl Actor for DeferredPanicActor {
    type Error = ActorError;
}

#[async_trait]
impl Responder<HeldRequest> for DeferredPanicActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        request: HeldRequest,
        reply_to: ReplyTo<u32>,
    ) -> Result<(), ActorError> {
        self.reply = Some(reply_to);
        request.held.add_permits(1);
        Ok(())
    }
}

#[async_trait]
impl Handler<Crash> for DeferredPanicActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message: Crash,
    ) -> Result<(), ActorError> {
        panic!("deferred actor crashed")
    }
}

#[tokio::test]
async fn deferred_ask_is_cancelled_with_actor_panicked() {
    let handle = lattice_actor::runtime::spawn_actor(
        DeferredPanicActor { reply: None },
        MailboxConfig::bounded(8),
    );
    let held = Arc::new(Semaphore::new(0));
    let ask = tokio::spawn({
        let handle = handle.clone();
        let held = held.clone();
        async move { handle.ask(HeldRequest { held }, TIMEOUT).await }
    });
    held.acquire().await.unwrap().forget();
    handle.tell(Crash).await.unwrap();
    assert_eq!(ask.await.unwrap(), Err(ActorCallError::ActorPanicked));
}

#[derive(lattice_actor::Message)]
struct LaunchPanickingTask {
    ran: Arc<Semaphore>,
}

struct ScopedTaskActor;

#[async_trait]
impl Actor for ScopedTaskActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<LaunchPanickingTask> for ScopedTaskActor {
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        message: LaunchPanickingTask,
    ) -> Result<(), ActorError> {
        ctx.spawn_scoped(async move {
            message.ran.add_permits(1);
            panic!("scoped task crashed")
        });
        Ok(())
    }
}

#[async_trait]
impl Responder<Ping> for ScopedTaskActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: Ping,
        reply_to: ReplyTo<u32>,
    ) -> Result<(), ActorError> {
        reply_to.send(3)?;
        Ok(())
    }
}

#[tokio::test]
async fn scoped_task_panic_remains_isolated_from_the_actor() {
    let handle = lattice_actor::runtime::spawn_actor(ScopedTaskActor, MailboxConfig::bounded(8));
    let ran = Arc::new(Semaphore::new(0));
    handle
        .tell(LaunchPanickingTask { ran: ran.clone() })
        .await
        .unwrap();
    ran.acquire().await.unwrap().forget();
    tokio::task::yield_now().await;

    assert_eq!(handle.ask(Ping, TIMEOUT).await.unwrap(), 3);
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Running);
}

struct StartActor {
    panic_on_start: bool,
}

#[async_trait]
impl Actor for StartActor {
    type Error = ActorError;

    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        assert!(!self.panic_on_start, "start crashed");
        Ok(())
    }
}

#[tokio::test]
async fn start_panic_releases_registry_activation_for_replacement() {
    let registry =
        ActorRegistry::<StartActor>::new(actor_kind!("PanicStart"), ActorRegistryConfig::default());
    let actor_id = ActorId::U64(99);
    let first = registry
        .start(
            actor_id.clone(),
            StartActor {
                panic_on_start: true,
            },
        )
        .await
        .unwrap();
    let mut terminated = first.subscribe_terminated();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, terminated.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        TerminatedReason::Panicked
    );

    let replacement = registry
        .start(
            actor_id,
            StartActor {
                panic_on_start: false,
            },
        )
        .await
        .unwrap();
    assert_ne!(first.local_ref(), replacement.local_ref());
}

struct StoppingPanicActor;

#[async_trait]
impl Actor for StoppingPanicActor {
    type Error = ActorError;

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        panic!("stopping crashed")
    }
}

#[tokio::test]
async fn stopping_panic_terminates_without_entering_stop_failed() {
    let handle = lattice_actor::runtime::spawn_actor(StoppingPanicActor, MailboxConfig::bounded(8));
    let mut terminated = handle.subscribe_terminated();
    handle.stop(StopReason::Requested).await.unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, terminated.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        TerminatedReason::Panicked
    );
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
    assert!(handle.inspect_stop_failure().is_none());
}

struct DropPanicActor;

#[async_trait]
impl Actor for DropPanicActor {
    type Error = ActorError;
}

impl Drop for DropPanicActor {
    fn drop(&mut self) {
        panic!("drop crashed")
    }
}

#[tokio::test]
async fn actor_drop_panic_still_publishes_termination() {
    let handle = lattice_actor::runtime::spawn_actor(DropPanicActor, MailboxConfig::bounded(8));
    let mut terminated = handle.subscribe_terminated();
    handle.stop(StopReason::Requested).await.unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, terminated.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        TerminatedReason::Panicked
    );
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
}

#[derive(lattice_actor::Message)]
struct CrashChild;

struct PanicChild {
    started: Arc<Semaphore>,
}

#[async_trait]
impl Actor for PanicChild {
    type Error = ActorError;

    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        self.started.add_permits(1);
        Ok(())
    }
}

#[async_trait]
impl Handler<Crash> for PanicChild {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message: Crash,
    ) -> Result<(), ActorError> {
        panic!("child crashed")
    }
}

struct SupervisingParent {
    child: Option<ActorHandle<PanicChild>>,
    child_started: Arc<Semaphore>,
    supervision: ChildSupervision,
}

#[async_trait]
impl Actor for SupervisingParent {
    type Error = ActorError;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let child_started = self.child_started.clone();
        self.child = Some(ctx.spawn_child_with_factory(
            ChildActorKey::new("panic-child"),
            move || PanicChild {
                started: child_started.clone(),
            },
            ChildActorOptions {
                mailbox: MailboxConfig::bounded(8),
                supervision: self.supervision,
                protocol_id: None,
            },
        )?);
        Ok(())
    }
}

#[async_trait]
impl Handler<CrashChild> for SupervisingParent {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _message: CrashChild,
    ) -> Result<(), ActorError> {
        self.child
            .as_ref()
            .expect("child should be running")
            .tell(Crash)
            .await?;
        Ok(())
    }
}

#[tokio::test]
async fn restart_child_supervision_replaces_panicked_child() {
    let child_started = Arc::new(Semaphore::new(0));
    let parent = lattice_actor::runtime::spawn_actor(
        SupervisingParent {
            child: None,
            child_started: child_started.clone(),
            supervision: ChildSupervision::RestartChild,
        },
        MailboxConfig::bounded(8),
    );
    child_started.acquire().await.unwrap().forget();

    parent.tell(CrashChild).await.unwrap();
    tokio::time::timeout(TIMEOUT, child_started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(parent.lifecycle_state(), ActorLifecycleState::Running);
}

#[tokio::test]
async fn stop_parent_supervision_observes_panicked_child() {
    let child_started = Arc::new(Semaphore::new(0));
    let parent = lattice_actor::runtime::spawn_actor(
        SupervisingParent {
            child: None,
            child_started: child_started.clone(),
            supervision: ChildSupervision::StopParent,
        },
        MailboxConfig::bounded(8),
    );
    let mut terminated = parent.subscribe_terminated();
    child_started.acquire().await.unwrap().forget();

    parent.tell(CrashChild).await.unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, terminated.recv())
            .await
            .unwrap()
            .unwrap()
            .reason,
        TerminatedReason::Stopped
    );
}
