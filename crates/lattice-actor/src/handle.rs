use std::marker::PhantomData;

use tokio::sync::{
    broadcast,
    mpsc::{self, error::TrySendError},
    oneshot, watch,
};

use crate::mailbox::{ActorCommand, EnvelopeMessage, MailboxLane};
use crate::{
    Actor, ActorCallError, ActorLifecycleState, ActorTellError, ActorTerminated, Handler,
    LocalActorRef, Message, StopReason,
};

pub struct ActorHandle<A: Actor> {
    local_ref: LocalActorRef,
    terminated_tx: broadcast::Sender<ActorTerminated>,
    lifecycle_tx: watch::Sender<ActorLifecycleState>,
    normal_tx: mpsc::Sender<ActorCommand<A>>,
    system_tx: mpsc::Sender<ActorCommand<A>>,
    _marker: PhantomData<A>,
}

impl<A: Actor> Clone for ActorHandle<A> {
    fn clone(&self) -> Self {
        Self {
            local_ref: self.local_ref,
            terminated_tx: self.terminated_tx.clone(),
            lifecycle_tx: self.lifecycle_tx.clone(),
            normal_tx: self.normal_tx.clone(),
            system_tx: self.system_tx.clone(),
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
    ) -> Self {
        Self {
            local_ref,
            terminated_tx,
            lifecycle_tx,
            normal_tx,
            system_tx,
            _marker: PhantomData,
        }
    }

    pub fn local_ref(&self) -> LocalActorRef {
        self.local_ref
    }

    pub fn lifecycle_state(&self) -> ActorLifecycleState {
        *self.lifecycle_tx.borrow()
    }

    pub async fn call<M>(&self, msg: M) -> Result<M::Reply, ActorCallError>
    where
        A: Handler<M>,
        M: Message,
    {
        self.call_on_lane(msg, MailboxLane::Normal).await
    }

    pub async fn tell<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message<Reply = ()>,
    {
        self.call(msg).await.map_err(ActorTellError::from)
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
        M: Message<Reply = ()>,
    {
        self.try_tell_on_lane(msg, MailboxLane::Normal)
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
        M: Message<Reply = ()>,
    {
        self.try_tell_on_lane(msg, MailboxLane::Normal)
    }

    #[cfg(test)]
    pub(crate) fn try_tell_system_for_test<M>(&self, msg: M) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message<Reply = ()>,
    {
        self.try_tell_on_lane(msg, MailboxLane::System)
    }

    async fn call_on_lane<M>(&self, msg: M, lane: MailboxLane) -> Result<M::Reply, ActorCallError>
    where
        A: Handler<M>,
        M: Message,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = ActorCommand::Envelope(Box::new(EnvelopeMessage::new(msg, reply_tx)));
        self.send_command(command, lane)?;
        reply_rx
            .await
            .map_err(|_| ActorCallError::ResponseDropped)?
    }

    fn try_tell_on_lane<M>(&self, msg: M, lane: MailboxLane) -> Result<(), ActorTellError>
    where
        A: Handler<M>,
        M: Message<Reply = ()>,
    {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let command = ActorCommand::Envelope(Box::new(EnvelopeMessage::new(msg, reply_tx)));
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
            ActorCallError::ResponseDropped => Self::ResponseDropped,
            ActorCallError::Handler(error) => Self::Handler(error),
        }
    }
}
