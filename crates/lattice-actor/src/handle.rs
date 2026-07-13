use std::fmt;
use std::marker::PhantomData;
use std::time::Instant;

use lattice_core::actor_ref::ActorRef;
use tokio::sync::{
    broadcast,
    mpsc::{self, error::TrySendError},
    oneshot, watch,
};

use crate::error::{ActorCallError, ActorTellError};
use crate::mailbox::{ActorCommand, MailboxLane, RequestEnvelope, TellEnvelope};
use crate::traits::{Actor, ActorLifecycleState, Handler, Message, Request, Responder, StopReason};
use crate::watch::{ActorTerminated, LocalActorRef};

pub struct ActorHandle<A: Actor> {
    local_ref: LocalActorRef,
    terminated_tx: broadcast::Sender<ActorTerminated>,
    lifecycle_tx: watch::Sender<ActorLifecycleState>,
    normal_tx: mpsc::Sender<ActorCommand<A>>,
    system_tx: mpsc::Sender<ActorCommand<A>>,
    actor_ref: Option<ActorRef<A>>,
    _marker: PhantomData<fn() -> A>,
}

impl<A: Actor> fmt::Debug for ActorHandle<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorHandle")
            .field("local_ref", &self.local_ref)
            .field("lifecycle_state", &self.lifecycle_state())
            .finish_non_exhaustive()
    }
}

impl<A: Actor> Clone for ActorHandle<A> {
    fn clone(&self) -> Self {
        Self {
            local_ref: self.local_ref,
            terminated_tx: self.terminated_tx.clone(),
            lifecycle_tx: self.lifecycle_tx.clone(),
            normal_tx: self.normal_tx.clone(),
            system_tx: self.system_tx.clone(),
            actor_ref: self.actor_ref.as_ref().map(ActorRef::cast),
            _marker: PhantomData,
        }
    }
}

impl<A: Actor> ActorHandle<A> {
    pub(crate) fn new(
        local_ref: LocalActorRef,
        terminated_tx: broadcast::Sender<ActorTerminated>,
        lifecycle_tx: watch::Sender<ActorLifecycleState>,
        normal_tx: mpsc::Sender<ActorCommand<A>>,
        system_tx: mpsc::Sender<ActorCommand<A>>,
        actor_ref: Option<ActorRef<A>>,
    ) -> Self {
        Self {
            local_ref,
            terminated_tx,
            lifecycle_tx,
            normal_tx,
            system_tx,
            actor_ref,
            _marker: PhantomData,
        }
    }

    pub fn local_ref(&self) -> LocalActorRef {
        self.local_ref
    }

    pub fn actor_ref(&self) -> Option<&ActorRef<A>> {
        self.actor_ref.as_ref()
    }

    pub fn lifecycle_state(&self) -> ActorLifecycleState {
        *self.lifecycle_tx.borrow()
    }

    pub async fn ask<R>(&self, request: R) -> Result<R::Response, ActorCallError>
    where
        A: Responder<R>,
        R: Request,
    {
        self.ask_on_lane(request, MailboxLane::Normal).await
    }

    pub async fn ask_before<R>(
        &self,
        request: R,
        deadline: Instant,
    ) -> Result<R::Response, ActorCallError>
    where
        A: Responder<R>,
        R: Request,
    {
        if Instant::now() >= deadline {
            return Err(ActorCallError::DeadlineExceeded);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = ActorCommand::Envelope(Box::new(RequestEnvelope::with_deadline(
            request, reply_tx, deadline,
        )));
        self.send_command(command, MailboxLane::Normal)?;
        match tokio::time::timeout_at(deadline.into(), reply_rx).await {
            Ok(result) => result.map_err(|_| ActorCallError::ResponseDropped)?,
            Err(_) => Err(ActorCallError::DeadlineExceeded),
        }
    }

    pub(crate) async fn ask_before_owned<R>(
        self,
        request: R,
        deadline: Instant,
    ) -> Result<R::Response, ActorCallError>
    where
        A: Responder<R>,
        R: Request,
    {
        if Instant::now() >= deadline {
            return Err(ActorCallError::DeadlineExceeded);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = ActorCommand::Envelope(Box::new(RequestEnvelope::with_deadline(
            request, reply_tx, deadline,
        )));
        self.send_command(command, MailboxLane::Normal)?;
        match tokio::time::timeout_at(deadline.into(), reply_rx).await {
            Ok(result) => result.map_err(|_| ActorCallError::ResponseDropped)?,
            Err(_) => Err(ActorCallError::DeadlineExceeded),
        }
    }

    pub async fn tell<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::Normal)
    }

    pub fn try_tell<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::Normal)
    }

    pub async fn stop(&self, reason: StopReason) -> Result<(), ActorTellError> {
        self.send_system_command(ActorCommand::Stop(reason))
    }

    pub(crate) fn try_stop_internal(&self, reason: StopReason) -> Result<(), ActorTellError> {
        self.send_system_command(ActorCommand::Stop(reason))
    }

    pub(crate) fn try_tell_internal<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::Normal)
    }

    pub(crate) async fn send_tell_internal<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message,
    {
        let command = ActorCommand::Envelope(Box::new(TellEnvelope::new(msg, None)));
        self.normal_tx
            .send(command)
            .await
            .map_err(|_| ActorTellError::MailboxClosed)
    }

    pub(crate) fn subscribe_terminated(&self) -> broadcast::Receiver<ActorTerminated> {
        self.terminated_tx.subscribe()
    }

    pub fn subscribe_lifecycle(&self) -> watch::Receiver<ActorLifecycleState> {
        self.lifecycle_tx.subscribe()
    }

    pub(crate) fn set_lifecycle_state(&self, state: ActorLifecycleState) {
        self.lifecycle_tx.send_replace(state);
    }

    pub(crate) fn publish_terminated(&self, notification: ActorTerminated) {
        let _ = self.terminated_tx.send(notification);
    }

    #[cfg(test)]
    pub(crate) fn try_tell_for_test<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::Normal)
    }

    #[cfg(test)]
    pub(crate) fn try_tell_system_for_test<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::System)
    }

    async fn ask_on_lane<R>(
        &self,
        request: R,
        lane: MailboxLane,
    ) -> Result<R::Response, ActorCallError>
    where
        A: Responder<R>,
        R: Request,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = ActorCommand::Envelope(Box::new(RequestEnvelope::new(request, reply_tx)));
        self.send_command(command, lane)?;
        reply_rx
            .await
            .map_err(|_| ActorCallError::ResponseDropped)?
    }

    pub(crate) fn try_tell_from<M>(
        &self,
        msg: M,
        sender: Option<ActorRef<()>>,
    ) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, sender, MailboxLane::Normal)
    }

    fn try_tell_on_lane<M>(
        &self,
        msg: M,
        sender: Option<ActorRef<()>>,
        lane: MailboxLane,
    ) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message,
    {
        let command = ActorCommand::Envelope(Box::new(TellEnvelope::new(msg, sender)));
        self.send_command(command, lane)
            .map_err(ActorTellError::from)
    }

    fn send_system_command(&self, command: ActorCommand<A>) -> Result<(), ActorTellError> {
        self.system_tx
            .try_send(command)
            .map_err(|error| match error {
                TrySendError::Full(_) => ActorTellError::MailboxFull,
                TrySendError::Closed(_) => ActorTellError::MailboxClosed,
            })
    }

    fn send_command(
        &self,
        command: ActorCommand<A>,
        lane: MailboxLane,
    ) -> Result<(), ActorCallError> {
        let result = match lane {
            MailboxLane::Normal => self.normal_tx.try_send(command),
            MailboxLane::System => self.system_tx.try_send(command),
        };

        result.map_err(|error| match error {
            TrySendError::Full(_) => ActorCallError::MailboxFull,
            TrySendError::Closed(_) => ActorCallError::MailboxClosed,
        })
    }
}

impl From<ActorCallError> for ActorTellError {
    fn from(value: ActorCallError) -> Self {
        match value {
            ActorCallError::MailboxFull => Self::MailboxFull,
            ActorCallError::MailboxClosed => Self::MailboxClosed,
            ActorCallError::ResponseDropped
            | ActorCallError::DeadlineExceeded
            | ActorCallError::Handler(_) => Self::MailboxClosed,
        }
    }
}
