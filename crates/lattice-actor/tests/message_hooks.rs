use lattice_actor::context::HandlerContext;
use std::any::type_name;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::reply::ReplyTo;
use lattice_actor::runtime::spawn_actor;
use lattice_actor::traits::{
    Actor, Handler, MessageKind, MessageLane, MessageMetadata, MessageOutcome, MessageView,
    Responder, ResponderErrorAction,
};
use tokio::sync::Semaphore;

#[derive(Debug, lattice_actor::Message)]
struct PayloadTell {
    value: u64,
}

#[derive(Debug, lattice_actor::Request)]
#[request(response = String)]
struct PayloadRequest {
    value: String,
}

#[derive(Debug, lattice_actor::Message)]
struct FailingTell;

#[derive(Debug, lattice_actor::Request)]
#[request(response = &'static str)]
struct RecoveredRequest;

#[derive(Debug, PartialEq, Eq)]
enum HookEvent {
    Before {
        type_name: &'static str,
        kind: MessageKind,
        lane: MessageLane,
        payload: String,
        has_deadline: bool,
    },
    After {
        type_name: &'static str,
        kind: MessageKind,
        outcome: MessageOutcome,
    },
}

struct HookActor {
    events: Arc<Mutex<Vec<HookEvent>>>,
    after_signal: Arc<Semaphore>,
}

impl Actor for HookActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    fn before_message(&mut self, _ctx: &mut ActorContext<Self>, message: MessageView<'_>) {
        let payload = if let Some(message) = message.downcast_ref::<PayloadTell>() {
            format!("tell:{}", message.value)
        } else if let Some(message) = message.downcast_ref::<PayloadRequest>() {
            format!("request:{}", message.value)
        } else if message.is::<FailingTell>() {
            "failing-tell".to_owned()
        } else if message.is::<RecoveredRequest>() {
            "recovered-request".to_owned()
        } else {
            "unknown".to_owned()
        };
        let metadata = message.metadata();
        self.events
            .lock()
            .expect("hook events mutex is not poisoned")
            .push(HookEvent::Before {
                type_name: metadata.type_name(),
                kind: metadata.kind(),
                lane: metadata.lane(),
                payload,
                has_deadline: metadata.deadline().is_some(),
            });
    }

    fn after_message(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        metadata: &MessageMetadata,
        outcome: MessageOutcome,
    ) {
        self.events
            .lock()
            .expect("hook events mutex is not poisoned")
            .push(HookEvent::After {
                type_name: metadata.type_name(),
                kind: metadata.kind(),
                outcome,
            });
        self.after_signal.add_permits(1);
    }
}

impl Handler<PayloadTell> for HookActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: PayloadTell,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

impl Responder<PayloadRequest> for HookActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: PayloadRequest,
        reply_to: ReplyTo<String>,
    ) -> Result<(), ActorError> {
        reply_to.send(format!("reply:{}", request.value))?;
        Ok(())
    }
}

impl Handler<FailingTell> for HookActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: FailingTell,
    ) -> Result<(), ActorError> {
        Err(ActorError::new("expected tell failure"))
    }
}

impl Responder<RecoveredRequest> for HookActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _request: RecoveredRequest,
        _reply_to: ReplyTo<&'static str>,
    ) -> Result<(), ActorError> {
        Err(ActorError::new("expected request failure"))
    }

    async fn respond_error(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _error: ActorError,
    ) -> ResponderErrorAction<&'static str, ActorError> {
        ResponderErrorAction::Respond("recovered")
    }
}

#[tokio::test]
async fn actor_hooks_can_downcast_tell_and_request_payloads() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let after_signal = Arc::new(Semaphore::new(0));
    let handle = spawn_actor(
        HookActor {
            events: events.clone(),
            after_signal: after_signal.clone(),
        },
        MailboxConfig::bounded(16),
    );

    handle.tell(PayloadTell { value: 41 }).await.unwrap();
    let response = handle
        .ask(
            PayloadRequest {
                value: "hello".to_owned(),
            },
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(response, "reply:hello");

    handle.tell(FailingTell).await.unwrap();
    assert_eq!(
        handle
            .ask(RecoveredRequest, Duration::from_secs(1))
            .await
            .unwrap(),
        "recovered"
    );

    let permits = tokio::time::timeout(Duration::from_secs(1), after_signal.acquire_many(4))
        .await
        .expect("all after-message hooks should run")
        .expect("after-message signal should remain open");
    permits.forget();

    assert_eq!(
        *events.lock().expect("hook events mutex is not poisoned"),
        vec![
            HookEvent::Before {
                type_name: type_name::<PayloadTell>(),
                kind: MessageKind::Tell,
                lane: MessageLane::Normal,
                payload: "tell:41".to_owned(),
                has_deadline: false,
            },
            HookEvent::After {
                type_name: type_name::<PayloadTell>(),
                kind: MessageKind::Tell,
                outcome: MessageOutcome::Handled,
            },
            HookEvent::Before {
                type_name: type_name::<PayloadRequest>(),
                kind: MessageKind::Request,
                lane: MessageLane::Normal,
                payload: "request:hello".to_owned(),
                has_deadline: true,
            },
            HookEvent::After {
                type_name: type_name::<PayloadRequest>(),
                kind: MessageKind::Request,
                outcome: MessageOutcome::Handled,
            },
            HookEvent::Before {
                type_name: type_name::<FailingTell>(),
                kind: MessageKind::Tell,
                lane: MessageLane::Normal,
                payload: "failing-tell".to_owned(),
                has_deadline: false,
            },
            HookEvent::After {
                type_name: type_name::<FailingTell>(),
                kind: MessageKind::Tell,
                outcome: MessageOutcome::HandlerFailed,
            },
            HookEvent::Before {
                type_name: type_name::<RecoveredRequest>(),
                kind: MessageKind::Request,
                lane: MessageLane::Normal,
                payload: "recovered-request".to_owned(),
                has_deadline: true,
            },
            HookEvent::After {
                type_name: type_name::<RecoveredRequest>(),
                kind: MessageKind::Request,
                outcome: MessageOutcome::HandlerErrorRecovered,
            },
        ]
    );
}
