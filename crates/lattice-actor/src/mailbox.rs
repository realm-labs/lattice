use std::any::type_name;
use std::time::Instant;

use lattice_core::actor_ref::ActorRef;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::context::{ActorContext, HandlerContext};
use crate::error::{ActorAdminError, ActorCallError};
use crate::handle::ForceStopAuthorization;
use crate::observation::{RequestCompletion, RequestObservation};
use crate::reply::ReplyTo;
use crate::traits::{
    Actor, Handler, Message, MessageKind, MessageLane, MessageMetadata, MessageOutcome,
    MessageRejection, MessageView, Request, Responder, ResponderErrorAction, StopReason,
};

pub(crate) mod channel;
pub(crate) mod continuation;
mod envelope;
mod future;
mod pool;

use envelope::PooledEnvelope;
use future::PooledFuture;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxConfig {
    normal_capacity: usize,
    system_capacity: usize,
    deferred_capacity: usize,
    turn_budget: usize,
}

impl MailboxConfig {
    const DEFAULT_TURN_BUDGET: usize = 64;

    pub fn bounded(capacity: usize) -> Self {
        Self {
            normal_capacity: capacity,
            system_capacity: capacity,
            deferred_capacity: capacity,
            turn_budget: Self::DEFAULT_TURN_BUDGET,
        }
    }

    pub fn with_lanes(normal_capacity: usize, system_capacity: usize) -> Self {
        Self {
            normal_capacity,
            system_capacity,
            deferred_capacity: normal_capacity,
            turn_budget: Self::DEFAULT_TURN_BUDGET,
        }
    }

    pub fn with_deferred_capacity(mut self, deferred_capacity: usize) -> Self {
        self.deferred_capacity = deferred_capacity;
        self
    }

    /// Sets the maximum number of normal-lane messages processed in one Actor turn.
    ///
    /// System-lane priority is reconsidered at every turn boundary. A larger budget amortizes
    /// mailbox polling overhead under load, while a smaller budget bounds the number of normal
    /// messages that can precede a waiting system message. Handler execution time is not bounded.
    pub fn with_turn_budget(mut self, turn_budget: usize) -> Self {
        assert!(turn_budget > 0, "actor mailbox turn budget must be nonzero");
        self.turn_budget = turn_budget;
        self
    }

    pub(crate) fn normal_capacity(&self) -> usize {
        self.normal_capacity
    }

    pub(crate) fn system_capacity(&self) -> usize {
        self.system_capacity
    }

    pub(crate) fn deferred_capacity(&self) -> usize {
        self.deferred_capacity
    }

    pub(crate) fn turn_budget(&self) -> usize {
        self.turn_budget
    }
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self::bounded(1024)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MailboxLane {
    Normal,
    System,
}

impl MailboxLane {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::System => "system",
        }
    }
}

impl From<MailboxLane> for MessageLane {
    fn from(value: MailboxLane) -> Self {
        match value {
            MailboxLane::Normal => Self::Normal,
            MailboxLane::System => Self::System,
        }
    }
}

pub(crate) enum ActorCommand<A: Actor> {
    Envelope(PooledEnvelope<A>),
    Stop(StopReason),
    RetryStop(oneshot::Sender<Result<(), ActorAdminError>>),
    Quarantine(oneshot::Sender<Result<(), ActorAdminError>>),
    ForceStop {
        authorization: ForceStopAuthorization,
        result: oneshot::Sender<Result<(), ActorAdminError>>,
    },
}

impl<A: Actor> ActorCommand<A> {
    pub(crate) fn envelope<T>(envelope: T) -> Self
    where
        T: ActorEnvelope<A> + 'static,
    {
        Self::Envelope(PooledEnvelope::new(envelope))
    }

    pub(crate) fn metadata(&self, lane: MailboxLane) -> Option<MessageMetadata> {
        match self {
            Self::Envelope(envelope) => Some(envelope.metadata(lane)),
            Self::Stop(_) | Self::RetryStop(_) | Self::Quarantine(_) | Self::ForceStop { .. } => {
                None
            }
        }
    }
}

pub(crate) type EnvelopeFuture<'a> = PooledFuture<'a>;

pub(crate) trait ActorEnvelope<A: Actor>: Send {
    fn metadata(&self, lane: MailboxLane) -> MessageMetadata;

    fn reject_panicked(&mut self) -> Option<RequestCompletion> {
        None
    }

    fn handle<'a>(
        &'a mut self,
        actor: &'a mut A,
        behavior: &'a mut A::Behavior,
        ctx: &'a mut ActorContext<A>,
        metadata: &'a MessageMetadata,
    ) -> EnvelopeFuture<'a>;
}

pub(crate) struct RequestEnvelope<R: Request> {
    request: Option<R>,
    reply_tx: Option<oneshot::Sender<Result<R::Response, ActorCallError>>>,
    deadline: Option<Instant>,
}

impl<R: Request> RequestEnvelope<R> {
    pub(crate) fn new(
        request: R,
        reply_tx: oneshot::Sender<Result<R::Response, ActorCallError>>,
        deadline: Instant,
    ) -> Self {
        Self {
            request: Some(request),
            reply_tx: Some(reply_tx),
            deadline: Some(deadline),
        }
    }
}

pub(crate) struct TellEnvelope<M: Message> {
    msg: Option<M>,
    sender: Option<ActorRef>,
}

impl<M: Message> TellEnvelope<M> {
    pub(crate) fn new(msg: M, sender: Option<ActorRef>) -> Self {
        Self {
            msg: Some(msg),
            sender,
        }
    }
}

impl<A, M> ActorEnvelope<A> for TellEnvelope<M>
where
    A: Handler<M>,
    <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
    M: Message,
{
    fn metadata(&self, lane: MailboxLane) -> MessageMetadata {
        MessageMetadata::new(type_name::<M>(), MessageKind::Tell, lane.into(), None)
    }

    fn handle<'a>(
        &'a mut self,
        actor: &'a mut A,
        behavior: &'a mut A::Behavior,
        ctx: &'a mut ActorContext<A>,
        metadata: &'a MessageMetadata,
    ) -> EnvelopeFuture<'a> {
        PooledFuture::new(async move {
            ctx.clear_sender();
            ctx.set_current_deadline(None);
            if let Some(sender) = self.sender.take() {
                ctx.set_sender(sender);
            }
            let msg = self
                .msg
                .as_ref()
                .expect("tell envelope message is present before dispatch");
            actor.before_message(ctx, MessageView::new(metadata, msg));
            if !<A::Behavior as crate::state_machine::Accepts<M>>::ALWAYS
                && !crate::state_machine::Accepts::<M>::accepts(behavior)
            {
                let outcome = MessageOutcome::Rejected(MessageRejection::UnhandledInCurrentState);
                actor.after_message(ctx, metadata, outcome);
                ctx.clear_sender();
                ctx.set_current_deadline(None);
                return outcome;
            }
            let msg = self
                .msg
                .take()
                .expect("tell envelope message is present before dispatch");
            let outcome = match actor
                .handle(&mut HandlerContext::new(ctx, behavior), msg)
                .await
            {
                Ok(()) => MessageOutcome::Handled,
                Err(error) => {
                    warn!(message.type = type_name::<M>(), %error, "tell handler returned error");
                    actor.on_error::<M>(ctx, metadata, &error).await;
                    MessageOutcome::HandlerFailed
                }
            };
            actor.after_message(ctx, metadata, outcome);
            ctx.clear_sender();
            ctx.set_current_deadline(None);
            outcome
        })
    }
}

impl<A, R> ActorEnvelope<A> for RequestEnvelope<R>
where
    A: Responder<R>,
    <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<R>,
    R: Request,
{
    fn metadata(&self, lane: MailboxLane) -> MessageMetadata {
        MessageMetadata::new(
            type_name::<R>(),
            MessageKind::Request,
            lane.into(),
            self.deadline,
        )
    }

    fn reject_panicked(&mut self) -> Option<RequestCompletion> {
        let reply_tx = self.reply_tx.take()?;
        Some(
            if reply_tx.send(Err(ActorCallError::ActorPanicked)).is_ok() {
                RequestCompletion::ActorPanicked
            } else {
                RequestCompletion::CallerDropped
            },
        )
    }

    fn handle<'a>(
        &'a mut self,
        actor: &'a mut A,
        behavior: &'a mut A::Behavior,
        ctx: &'a mut ActorContext<A>,
        metadata: &'a MessageMetadata,
    ) -> EnvelopeFuture<'a> {
        PooledFuture::new(async move {
            ctx.clear_sender();
            ctx.set_current_deadline(metadata.deadline());
            let request = self
                .request
                .as_ref()
                .expect("request envelope message is present before dispatch");
            actor.before_message(ctx, MessageView::new(metadata, request));
            let handle = ctx.self_handle();
            let observation = RequestObservation::new(
                handle.observer().clone(),
                handle.observation_metadata().clone(),
                *metadata,
            );
            if self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                if let Some(reply_tx) = self.reply_tx.take() {
                    let _ = reply_tx.send(Err(ActorCallError::DeadlineExceeded));
                }
                observation.complete(RequestCompletion::DeadlineExceeded);
                let outcome = MessageOutcome::Rejected(MessageRejection::DeadlineExceeded);
                actor.after_message(ctx, metadata, outcome);
                ctx.set_current_deadline(None);
                return outcome;
            }
            if !<A::Behavior as crate::state_machine::Accepts<R>>::ALWAYS
                && !crate::state_machine::Accepts::<R>::accepts(behavior)
            {
                if let Some(reply_tx) = self.reply_tx.take() {
                    let _ = reply_tx.send(Err(ActorCallError::UnhandledInCurrentState));
                }
                observation.complete(RequestCompletion::UnhandledInCurrentState);
                let outcome = MessageOutcome::Rejected(MessageRejection::UnhandledInCurrentState);
                actor.after_message(ctx, metadata, outcome);
                ctx.set_current_deadline(None);
                return outcome;
            }
            let reply_tx = self
                .reply_tx
                .take()
                .expect("request envelope reply sender is present");
            let (reply_to, control) = ReplyTo::new(reply_tx, self.deadline, observation);
            if !ctx.register_pending_reply(control.clone()) {
                control.cancel(ActorCallError::MailboxFull);
                let outcome =
                    MessageOutcome::Rejected(MessageRejection::DeferredReplyCapacityExceeded);
                actor.after_message(ctx, metadata, outcome);
                ctx.set_current_deadline(None);
                return outcome;
            }

            let request = self
                .request
                .take()
                .expect("request envelope message is present");
            let outcome = match actor
                .respond(&mut HandlerContext::new(ctx, behavior), request, reply_to)
                .await
            {
                Ok(()) => {
                    control.handler_succeeded();
                    MessageOutcome::Handled
                }
                Err(error) => {
                    warn!(
                        message.type = type_name::<R>(),
                        %error,
                        "actor responder returned error"
                    );
                    actor.on_error::<R>(ctx, metadata, &error).await;
                    match actor
                        .respond_error(&mut HandlerContext::new(ctx, behavior), error)
                        .await
                    {
                        ResponderErrorAction::Respond(response) => {
                            debug!(
                                message.type = type_name::<R>(),
                                "actor responder error recovered"
                            );
                            control.respond_after_error(response);
                            MessageOutcome::HandlerErrorRecovered
                        }
                        ResponderErrorAction::Propagate(error) => {
                            warn!(
                                message.type = type_name::<R>(),
                                %error,
                                "actor responder error propagated"
                            );
                            control.handler_failed(error);
                            MessageOutcome::HandlerFailed
                        }
                    }
                }
            };
            actor.after_message(ctx, metadata, outcome);
            ctx.set_current_deadline(None);
            outcome
        })
    }
}

impl<R: Request> Drop for RequestEnvelope<R> {
    fn drop(&mut self) {
        if let Some(reply_tx) = self.reply_tx.take() {
            let _ = reply_tx.send(Err(ActorCallError::MailboxClosed));
        }
    }
}
