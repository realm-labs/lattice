use lattice_actor::context::HandlerContext;
use std::{
    any::type_name,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use lattice_actor::{
    actor_protocol,
    error::{ActorCallError, ActorError, ActorTellError},
    mailbox::MailboxConfig,
    observation::{
        ActorLifecycleEvent, ActorMetadata, ActorObserver, ActorObserverHandle, MailboxRejection,
        ProtocolFailure, RequestCompletion,
    },
    protocol::{DispatchError, DispatchMode, ProstCodec},
    reply::ReplyTo,
    runtime::{ActorRuntime, ActorRuntimeConfig, ActorSpawnOptions},
    traits::{Actor, Handler, MessageKind, MessageMetadata, MessageOutcome, Responder, StopReason},
};
use tokio::sync::Semaphore;

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, PartialEq, prost::Message, lattice_actor::Message)]
struct WireTell {}

#[derive(lattice_actor::Request)]
#[request(response = &'static str)]
struct DeferredRequest {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[derive(lattice_actor::Message)]
struct BlockingTell {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[derive(lattice_actor::Message)]
struct QueuedTell;

#[derive(lattice_actor::Request)]
#[request(response = ())]
struct QueuedRequest;

struct ObservedActor;

impl Actor for ObservedActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Handler<WireTell> for ObservedActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: WireTell,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

impl Responder<DeferredRequest> for ObservedActor {
    async fn respond(
        &mut self,
        ctx: &mut HandlerContext<'_, Self>,
        request: DeferredRequest,
        reply_to: ReplyTo<&'static str>,
    ) -> Result<(), ActorError> {
        request.entered.add_permits(1);
        ctx.spawn_scoped(async move {
            if let Ok(permit) = request.release.acquire().await {
                permit.forget();
                let _ = reply_to.send("done");
            }
        });
        Ok(())
    }
}

impl Handler<BlockingTell> for ObservedActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        message: BlockingTell,
    ) -> Result<(), ActorError> {
        message.entered.add_permits(1);
        if let Ok(permit) = message.release.acquire().await {
            permit.forget();
        }
        Ok(())
    }
}

impl Handler<QueuedTell> for ObservedActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: QueuedTell,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

impl Responder<QueuedRequest> for ObservedActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _request: QueuedRequest,
        reply_to: ReplyTo<()>,
    ) -> Result<(), ActorError> {
        reply_to.send(())?;
        Ok(())
    }
}

actor_protocol! {
    ObserverProtocol {
        protocol_id: 991;
        name: "observer/v1";
        tell 1 => WireTell {
            schema_version: 1,
            codec: ProstCodec,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ObserverEvent {
    Enqueued(&'static str),
    MailboxRejected(&'static str, MailboxRejection),
    Finished(&'static str, MessageOutcome),
    RequestCompleted(&'static str, RequestCompletion),
    Lifecycle(ActorLifecycleEvent),
    ProtocolFailed(u64, MessageKind, ProtocolFailure),
}

#[derive(Clone)]
struct RecordingObserver {
    events: Arc<Mutex<Vec<ObserverEvent>>>,
    signal: Arc<Semaphore>,
}

impl RecordingObserver {
    fn record(&self, event: ObserverEvent) {
        self.events
            .lock()
            .expect("observer events mutex is not poisoned")
            .push(event);
        self.signal.add_permits(1);
    }
}

impl ActorObserver for RecordingObserver {
    fn message_enqueued(
        &self,
        _actor: &ActorMetadata,
        message: &MessageMetadata,
        _queue_depth: usize,
    ) {
        self.record(ObserverEvent::Enqueued(message.type_name()));
    }

    fn mailbox_rejected(
        &self,
        _actor: &ActorMetadata,
        message: &MessageMetadata,
        reason: MailboxRejection,
    ) {
        self.record(ObserverEvent::MailboxRejected(message.type_name(), reason));
    }

    fn message_finished(
        &self,
        _actor: &ActorMetadata,
        message: &MessageMetadata,
        outcome: MessageOutcome,
        _processing_time: Duration,
    ) {
        self.record(ObserverEvent::Finished(message.type_name(), outcome));
    }

    fn request_completed(
        &self,
        _actor: &ActorMetadata,
        message: &MessageMetadata,
        completion: RequestCompletion,
    ) {
        self.record(ObserverEvent::RequestCompleted(
            message.type_name(),
            completion,
        ));
    }

    fn lifecycle(&self, _actor: &ActorMetadata, event: ActorLifecycleEvent) {
        self.record(ObserverEvent::Lifecycle(event));
    }

    fn protocol_failed(
        &self,
        _actor: &ActorMetadata,
        message_id: u64,
        kind: MessageKind,
        _payload_size: usize,
        failure: ProtocolFailure,
    ) {
        self.record(ObserverEvent::ProtocolFailed(message_id, kind, failure));
    }
}

fn observed_runtime() -> (ActorRuntime, Arc<Mutex<Vec<ObserverEvent>>>, Arc<Semaphore>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let signal = Arc::new(Semaphore::new(0));
    let observer = ActorObserverHandle::new(RecordingObserver {
        events: events.clone(),
        signal: signal.clone(),
    });
    let runtime = ActorRuntime::new(ActorRuntimeConfig {
        observer,
        ..ActorRuntimeConfig::default()
    });
    (runtime, events, signal)
}

async fn wait_for_event(
    events: &Arc<Mutex<Vec<ObserverEvent>>>,
    signal: &Arc<Semaphore>,
    predicate: impl Fn(&ObserverEvent) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if events
                .lock()
                .expect("observer events mutex is not poisoned")
                .iter()
                .any(&predicate)
            {
                return;
            }
            signal
                .acquire()
                .await
                .expect("observer signal remains open")
                .forget();
        }
    })
    .await
    .expect("expected observer event should arrive");
}

#[tokio::test]
async fn runtime_observer_reports_deferred_completion_lifecycle_and_protocol_failure() {
    let (runtime, events, signal) = observed_runtime();
    let handle = runtime
        .spawn_actor(ObservedActor, ActorSpawnOptions::default())
        .await
        .unwrap();
    wait_for_event(&events, &signal, |event| {
        *event == ObserverEvent::Lifecycle(ActorLifecycleEvent::Started)
    })
    .await;

    handle.tell(WireTell {}).await.unwrap();
    wait_for_event(&events, &signal, |event| {
        *event == ObserverEvent::Finished(type_name::<WireTell>(), MessageOutcome::Handled)
    })
    .await;

    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let ask_handle = handle.clone();
    let ask_entered = entered.clone();
    let ask_release = release.clone();
    let ask = tokio::spawn(async move {
        ask_handle
            .ask(
                DeferredRequest {
                    entered: ask_entered,
                    release: ask_release,
                },
                ASK_TIMEOUT,
            )
            .await
    });
    entered.acquire().await.unwrap().forget();
    wait_for_event(&events, &signal, |event| {
        *event == ObserverEvent::Finished(type_name::<DeferredRequest>(), MessageOutcome::Handled)
    })
    .await;
    assert!(!events
        .lock()
        .expect("observer events mutex is not poisoned")
        .iter()
        .any(|event| matches!(event, ObserverEvent::RequestCompleted(name, _) if *name == type_name::<DeferredRequest>())));

    release.add_permits(1);
    assert_eq!(ask.await.unwrap().unwrap(), "done");
    wait_for_event(&events, &signal, |event| {
        *event
            == ObserverEvent::RequestCompleted(
                type_name::<DeferredRequest>(),
                RequestCompletion::ReplyDelivered,
            )
    })
    .await;

    let protocol = ObserverProtocol::bind::<ObservedActor>().unwrap();
    let error = protocol
        .dispatch(handle.clone(), 999, DispatchMode::Tell, Bytes::new(), None)
        .await
        .unwrap_err();
    assert!(matches!(error, DispatchError::UnknownMessage(999)));
    wait_for_event(&events, &signal, |event| {
        *event
            == ObserverEvent::ProtocolFailed(
                999,
                MessageKind::Tell,
                ProtocolFailure::UnknownMessage,
            )
    })
    .await;

    handle.stop(StopReason::Requested).await.unwrap();
    wait_for_event(&events, &signal, |event| {
        *event == ObserverEvent::Lifecycle(ActorLifecycleEvent::Stopped(StopReason::Requested))
    })
    .await;

    let deferred_completions = events
        .lock()
        .expect("observer events mutex is not poisoned")
        .iter()
        .filter(|event| {
            matches!(event, ObserverEvent::RequestCompleted(name, _) if *name == type_name::<DeferredRequest>())
        })
        .count();
    assert_eq!(deferred_completions, 1);
}

#[tokio::test]
async fn runtime_observer_reports_mailbox_rejection() {
    let (runtime, events, signal) = observed_runtime();
    let handle = runtime
        .spawn_actor(
            ObservedActor,
            ActorSpawnOptions {
                mailbox: MailboxConfig::bounded(1),
                ..ActorSpawnOptions::default()
            },
        )
        .await
        .unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    handle
        .tell(BlockingTell {
            entered: entered.clone(),
            release: release.clone(),
        })
        .await
        .unwrap();
    entered.acquire().await.unwrap().forget();

    handle.tell(QueuedTell).await.unwrap();
    assert!(matches!(
        handle.try_tell(QueuedTell),
        Err(ActorTellError::MailboxFull(_))
    ));
    assert!(matches!(
        handle.ask(QueuedRequest, ASK_TIMEOUT).await,
        Err(ActorCallError::MailboxFull)
    ));
    wait_for_event(&events, &signal, |event| {
        *event == ObserverEvent::MailboxRejected(type_name::<QueuedTell>(), MailboxRejection::Full)
    })
    .await;
    wait_for_event(&events, &signal, |event| {
        *event
            == ObserverEvent::RequestCompleted(
                type_name::<QueuedRequest>(),
                RequestCompletion::MailboxFull,
            )
    })
    .await;
    release.add_permits(1);
}
