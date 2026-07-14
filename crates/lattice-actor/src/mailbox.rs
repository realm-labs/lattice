use std::any::type_name;
use std::time::Instant;

use async_trait::async_trait;
use lattice_core::actor_ref::ActorRef;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::context::ActorContext;
use crate::error::ActorCallError;
use crate::observation::{RequestCompletion, RequestObservation};
use crate::reply::ReplyTo;
use crate::traits::{
    Actor, Handler, Message, MessageKind, MessageLane, MessageMetadata, MessageOutcome,
    MessageRejection, MessageView, Request, Responder, ResponderErrorAction, StopReason,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxConfig {
    normal_capacity: usize,
    system_capacity: usize,
    deferred_capacity: usize,
}

impl MailboxConfig {
    pub fn bounded(capacity: usize) -> Self {
        Self {
            normal_capacity: capacity,
            system_capacity: capacity,
            deferred_capacity: capacity,
        }
    }

    pub fn with_lanes(normal_capacity: usize, system_capacity: usize) -> Self {
        Self {
            normal_capacity,
            system_capacity,
            deferred_capacity: normal_capacity,
        }
    }

    pub fn with_deferred_capacity(mut self, deferred_capacity: usize) -> Self {
        self.deferred_capacity = deferred_capacity;
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
    Envelope(Box<dyn ActorEnvelope<A>>),
    Stop(StopReason),
}

impl<A: Actor> ActorCommand<A> {
    pub(crate) fn metadata(&self, lane: MailboxLane) -> Option<MessageMetadata> {
        match self {
            Self::Envelope(envelope) => Some(envelope.metadata(lane)),
            Self::Stop(_) => None,
        }
    }
}

#[async_trait]
pub(crate) trait ActorEnvelope<A: Actor>: Send {
    fn metadata(&self, lane: MailboxLane) -> MessageMetadata;

    async fn handle(
        self: Box<Self>,
        actor: &mut A,
        ctx: &mut ActorContext<A>,
        metadata: &MessageMetadata,
    ) -> MessageOutcome;
}

pub(crate) struct RequestEnvelope<R: Request> {
    request: Option<R>,
    reply_tx: Option<oneshot::Sender<Result<R::Response, ActorCallError>>>,
    enqueued_at: Instant,
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
            enqueued_at: Instant::now(),
            deadline: Some(deadline),
        }
    }
}

pub(crate) struct TellEnvelope<M: Message> {
    msg: M,
    sender: Option<ActorRef>,
    enqueued_at: Instant,
}

impl<M: Message> TellEnvelope<M> {
    pub(crate) fn new(msg: M, sender: Option<ActorRef>) -> Self {
        Self {
            msg,
            sender,
            enqueued_at: Instant::now(),
        }
    }
}

#[async_trait]
impl<A, M> ActorEnvelope<A> for TellEnvelope<M>
where
    A: Handler<M>,
    M: Message,
{
    fn metadata(&self, lane: MailboxLane) -> MessageMetadata {
        MessageMetadata::new(
            type_name::<M>(),
            MessageKind::Tell,
            lane.into(),
            self.enqueued_at,
            None,
        )
    }

    async fn handle(
        self: Box<Self>,
        actor: &mut A,
        ctx: &mut ActorContext<A>,
        metadata: &MessageMetadata,
    ) -> MessageOutcome {
        ctx.clear_sender();
        ctx.set_current_deadline(None);
        if let Some(sender) = self.sender {
            ctx.set_sender(sender);
        }
        actor.before_message(ctx, MessageView::new(metadata, &self.msg));
        let outcome = match actor.handle(ctx, self.msg).await {
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
    }
}

#[async_trait]
impl<A, R> ActorEnvelope<A> for RequestEnvelope<R>
where
    A: Responder<R>,
    R: Request,
{
    fn metadata(&self, lane: MailboxLane) -> MessageMetadata {
        MessageMetadata::new(
            type_name::<R>(),
            MessageKind::Request,
            lane.into(),
            self.enqueued_at,
            self.deadline,
        )
    }

    async fn handle(
        mut self: Box<Self>,
        actor: &mut A,
        ctx: &mut ActorContext<A>,
        metadata: &MessageMetadata,
    ) -> MessageOutcome {
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
        let reply_tx = self
            .reply_tx
            .take()
            .expect("request envelope reply sender is present");
        let (reply_to, control) = ReplyTo::new(reply_tx, self.deadline, observation);
        if !ctx.register_pending_reply(control.clone()) {
            control.cancel(ActorCallError::MailboxFull);
            let outcome = MessageOutcome::Rejected(MessageRejection::DeferredReplyCapacityExceeded);
            actor.after_message(ctx, metadata, outcome);
            ctx.set_current_deadline(None);
            return outcome;
        }

        let request = self
            .request
            .take()
            .expect("request envelope message is present");
        let outcome = match actor.respond(ctx, request, reply_to).await {
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
                match actor.respond_error(ctx, error).await {
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
    }
}

impl<R: Request> Drop for RequestEnvelope<R> {
    fn drop(&mut self) {
        if let Some(reply_tx) = self.reply_tx.take() {
            let _ = reply_tx.send(Err(ActorCallError::MailboxClosed));
        }
    }
}
