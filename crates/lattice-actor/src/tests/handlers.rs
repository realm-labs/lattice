//! Typed handler admission and business-error propagation.

use std::sync::Arc;

use tokio::sync::Mutex;

use super::{
    ASK_TIMEOUT, BusinessErrorActor, LoadBusinessState, Ping, Record, RecoverBusinessState,
    TestActor,
};
use crate::{
    error::ActorCallError,
    mailbox::MailboxConfig,
    runtime::spawn_actor,
    traits::{Handler, Message, Request, Responder},
};

fn assert_handler_bound<A, M>()
where
    A: Handler<M>,
    <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
    M: Message,
{
}

#[test]
fn handler_compile_time_bounds_are_typed() {
    assert_handler_bound::<TestActor, Record>();
    fn assert_responder_bound<A, R>()
    where
        A: Responder<R>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<R>,
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
