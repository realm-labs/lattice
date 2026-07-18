use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use lattice_actor::{
    context::ActorContext,
    error::{ActorCallError, ActorError},
    handle::ActorHandle,
    mailbox::MailboxConfig,
    reply::ReplyTo,
    runtime::spawn_actor,
    traits::{Actor, Handler, Responder, StopReason},
};
use tokio::sync::Semaphore;

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

struct DeferredActor {
    value: u64,
    continuations: Arc<AtomicUsize>,
}

#[async_trait]
impl Actor for DeferredActor {
    type Error = ActorError;
}

#[derive(lattice_actor::Request)]
#[request(response = u64)]
struct Query {
    gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
    database_value: u64,
}

#[derive(lattice_actor::Message)]
struct QueryReady {
    database_value: u64,
    reply_to: ReplyTo<u64>,
}

#[derive(lattice_actor::Message)]
struct SetValue(u64);

#[async_trait]
impl Responder<Query> for DeferredActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: Query,
        reply_to: ReplyTo<u64>,
    ) -> Result<(), ActorError> {
        request.entered.add_permits(1);
        let gate = request.gate;
        let database_value = request.database_value;
        ctx.defer_reply(
            reply_to,
            async move {
                if let Ok(permit) = gate.acquire_owned().await {
                    permit.forget();
                }
                database_value
            },
            |database_value, reply_to| QueryReady {
                database_value,
                reply_to,
            },
        )?;
        Ok(())
    }
}

#[async_trait]
impl Handler<QueryReady> for DeferredActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        message: QueryReady,
    ) -> Result<(), ActorError> {
        self.continuations.fetch_add(1, Ordering::SeqCst);
        message.reply_to.send(message.database_value + self.value)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<SetValue> for DeferredActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        message: SetValue,
    ) -> Result<(), ActorError> {
        self.value = message.0;
        Ok(())
    }
}

#[derive(lattice_actor::Request)]
#[request(response = u64)]
struct FailAfterPipe {
    gate: Arc<Semaphore>,
    entered: Arc<Semaphore>,
}

#[async_trait]
impl Responder<FailAfterPipe> for DeferredActor {
    async fn respond(
        &mut self,
        ctx: &mut ActorContext<Self>,
        request: FailAfterPipe,
        reply_to: ReplyTo<u64>,
    ) -> Result<(), ActorError> {
        request.entered.add_permits(1);
        let gate = request.gate;
        ctx.defer_reply(
            reply_to,
            async move {
                if let Ok(permit) = gate.acquire_owned().await {
                    permit.forget();
                }
                100
            },
            |database_value, reply_to| QueryReady {
                database_value,
                reply_to,
            },
        )?;
        Err(ActorError::new("responder failed after starting work"))
    }
}

#[derive(lattice_actor::Request)]
#[request(response = ())]
struct ForgetReply;

#[derive(lattice_actor::Request)]
#[request(response = u64)]
struct ReplyThenFail;

#[async_trait]
impl Responder<ReplyThenFail> for DeferredActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: ReplyThenFail,
        reply_to: ReplyTo<u64>,
    ) -> Result<(), ActorError> {
        reply_to.send(42)?;
        Err(ActorError::new("failure after provisional reply"))
    }
}

#[async_trait]
impl Responder<ForgetReply> for DeferredActor {
    async fn respond(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _request: ForgetReply,
        _reply_to: ReplyTo<()>,
    ) -> Result<(), ActorError> {
        Ok(())
    }
}

fn query(gate: Arc<Semaphore>, entered: Arc<Semaphore>, database_value: u64) -> Query {
    Query {
        gate,
        entered,
        database_value,
    }
}

fn actor(continuations: Arc<AtomicUsize>, mailbox: MailboxConfig) -> ActorHandle<DeferredActor> {
    spawn_actor(
        DeferredActor {
            value: 1,
            continuations,
        },
        mailbox,
    )
}

#[tokio::test]
async fn continuation_combines_async_result_with_current_actor_state() {
    let continuations = Arc::new(AtomicUsize::new(0));
    let handle = actor(continuations.clone(), MailboxConfig::default());
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let ask = tokio::spawn({
        let handle = handle.clone();
        let gate = gate.clone();
        let entered = entered.clone();
        async move { handle.ask(query(gate, entered, 10), ASK_TIMEOUT).await }
    });

    entered.acquire().await.unwrap().forget();
    handle.tell(SetValue(7)).await.unwrap();
    gate.add_permits(1);

    assert_eq!(ask.await.unwrap().unwrap(), 17);
    assert_eq!(continuations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn deferred_capacity_rejects_saturation_and_is_reused_after_completion() {
    let continuations = Arc::new(AtomicUsize::new(0));
    let handle = actor(
        continuations.clone(),
        MailboxConfig::bounded(8).with_deferred_capacity(1),
    );
    let first_gate = Arc::new(Semaphore::new(0));
    let first_entered = Arc::new(Semaphore::new(0));
    let first = tokio::spawn({
        let handle = handle.clone();
        let gate = first_gate.clone();
        let entered = first_entered.clone();
        async move { handle.ask(query(gate, entered, 1), ASK_TIMEOUT).await }
    });
    first_entered.acquire().await.unwrap().forget();

    let rejected = handle
        .ask(
            query(Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)), 2),
            ASK_TIMEOUT,
        )
        .await;
    assert!(matches!(rejected, Err(ActorCallError::MailboxFull)));

    first_gate.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap(), 2);

    let next = handle
        .ask(
            query(Arc::new(Semaphore::new(1)), Arc::new(Semaphore::new(0)), 3),
            ASK_TIMEOUT,
        )
        .await
        .unwrap();
    assert_eq!(next, 4);
    assert_eq!(continuations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn responder_error_invalidates_a_reply_token_moved_to_background_work() {
    let continuations = Arc::new(AtomicUsize::new(0));
    let handle = actor(continuations.clone(), MailboxConfig::default());
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));

    let result = handle
        .ask(
            FailAfterPipe {
                gate: gate.clone(),
                entered: entered.clone(),
            },
            ASK_TIMEOUT,
        )
        .await;
    assert!(matches!(result, Err(ActorCallError::Handler(_))));

    gate.add_permits(1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(continuations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn timeout_cancels_deferred_work_without_posting_a_continuation() {
    let continuations = Arc::new(AtomicUsize::new(0));
    let handle = actor(continuations.clone(), MailboxConfig::default());
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let result = handle
        .ask(
            query(gate.clone(), entered.clone(), 10),
            Duration::from_millis(30),
        )
        .await;
    assert!(matches!(result, Err(ActorCallError::DeadlineExceeded)));

    gate.add_permits(1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(continuations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn zero_timeout_expires_without_enqueuing_the_request() {
    let handle = actor(Arc::new(AtomicUsize::new(0)), MailboxConfig::default());

    assert!(matches!(
        handle.ask(ForgetReply, Duration::ZERO).await,
        Err(ActorCallError::DeadlineExceeded)
    ));
}

#[tokio::test]
async fn unrepresentable_timeout_is_rejected_without_panicking() {
    let handle = actor(Arc::new(AtomicUsize::new(0)), MailboxConfig::default());

    assert!(matches!(
        handle.ask(ForgetReply, Duration::MAX).await,
        Err(ActorCallError::InvalidTimeout)
    ));
}

#[tokio::test]
async fn stopping_actor_fails_an_active_deferred_request() {
    let continuations = Arc::new(AtomicUsize::new(0));
    let handle = actor(continuations.clone(), MailboxConfig::default());
    let gate = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let ask = tokio::spawn({
        let handle = handle.clone();
        let gate = gate.clone();
        let entered = entered.clone();
        async move { handle.ask(query(gate, entered, 10), ASK_TIMEOUT).await }
    });

    entered.acquire().await.unwrap().forget();
    handle.stop(StopReason::Requested).await.unwrap();

    assert!(matches!(
        ask.await.unwrap(),
        Err(ActorCallError::MailboxClosed)
    ));
    gate.add_permits(1);
    assert_eq!(continuations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_reply_token_completes_request_explicitly() {
    let handle = actor(Arc::new(AtomicUsize::new(0)), MailboxConfig::default());

    assert!(matches!(
        handle.ask(ForgetReply, ASK_TIMEOUT).await,
        Err(ActorCallError::ResponseDropped)
    ));
}

#[tokio::test]
async fn responder_failure_wins_over_a_reply_sent_before_returning() {
    let handle = actor(Arc::new(AtomicUsize::new(0)), MailboxConfig::default());

    let result = handle.ask(ReplyThenFail, ASK_TIMEOUT).await;
    let Err(ActorCallError::Handler(error)) = result else {
        panic!("expected handler failure, got {result:?}");
    };
    assert_eq!(error.message(), "failure after provisional reply");
}
