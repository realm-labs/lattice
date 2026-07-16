use std::any::type_name;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use lattice_core::actor_ref::{ActorRef, ProtocolTag, ReferenceError};
use tokio::sync::{
    broadcast,
    mpsc::{self, error::TrySendError},
    oneshot, watch,
};

use crate::error::{ActorAdminError, ActorCallError, ActorTellError};
use crate::mailbox::{ActorCommand, MailboxLane, RequestEnvelope, TellEnvelope};
use crate::observation::{ActorMetadata, ActorObserverHandle, MailboxRejection, RequestCompletion};
use crate::traits::{Actor, ActorLifecycleState, Handler, Message, Request, Responder, StopReason};
use crate::watch::{ActorTerminated, LocalActorRef};

pub(crate) type TerminalHook = Box<dyn FnOnce(LocalActorRef) + Send + 'static>;

pub struct ActorHandle<A: Actor> {
    local_ref: LocalActorRef,
    terminated_tx: broadcast::Sender<ActorTerminated>,
    lifecycle_tx: watch::Sender<ActorLifecycleState>,
    stop_failure: Arc<Mutex<Option<StopFailureRecord>>>,
    forced_data_loss_tx: broadcast::Sender<ForcedDataLossEvent>,
    terminal_hook: Arc<Mutex<Option<TerminalHook>>>,
    normal_tx: mpsc::Sender<ActorCommand<A>>,
    system_tx: mpsc::Sender<ActorCommand<A>>,
    metadata: Arc<ActorMetadata>,
    observer: ActorObserverHandle,
    _marker: PhantomData<fn() -> A>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopFailureRecord {
    pub reason: StopReason,
    pub previous_phase: ActorLifecycleState,
    pub error: String,
    pub first_failure_time: SystemTime,
    pub latest_attempt_time: SystemTime,
    pub attempt_count: u32,
    pub authoritative: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceStopAuthorization {
    pub reason: String,
    pub ticket: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForcedDataLossEvent {
    pub target: LocalActorRef,
    pub stop_reason: StopReason,
    pub reason: String,
    pub ticket: String,
    pub failed_attempts: u32,
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
            stop_failure: self.stop_failure.clone(),
            forced_data_loss_tx: self.forced_data_loss_tx.clone(),
            terminal_hook: self.terminal_hook.clone(),
            normal_tx: self.normal_tx.clone(),
            system_tx: self.system_tx.clone(),
            metadata: self.metadata.clone(),
            observer: self.observer.clone(),
            _marker: PhantomData,
        }
    }
}

impl<A: Actor> ActorHandle<A> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_ref: LocalActorRef,
        terminated_tx: broadcast::Sender<ActorTerminated>,
        lifecycle_tx: watch::Sender<ActorLifecycleState>,
        stop_failure: Arc<Mutex<Option<StopFailureRecord>>>,
        forced_data_loss_tx: broadcast::Sender<ForcedDataLossEvent>,
        terminal_hook: Arc<Mutex<Option<TerminalHook>>>,
        normal_tx: mpsc::Sender<ActorCommand<A>>,
        system_tx: mpsc::Sender<ActorCommand<A>>,
        actor_ref: Option<ActorRef>,
        observer: ActorObserverHandle,
    ) -> Self {
        Self {
            local_ref,
            terminated_tx,
            lifecycle_tx,
            stop_failure,
            forced_data_loss_tx,
            terminal_hook,
            normal_tx,
            system_tx,
            metadata: Arc::new(ActorMetadata::new(type_name::<A>(), local_ref, actor_ref)),
            observer,
            _marker: PhantomData,
        }
    }

    pub fn local_ref(&self) -> LocalActorRef {
        self.local_ref
    }

    pub fn actor_ref(&self) -> Option<&ActorRef> {
        self.metadata.actor_ref()
    }

    pub(crate) fn observer(&self) -> &ActorObserverHandle {
        &self.observer
    }

    pub(crate) fn observation_metadata(&self) -> &ActorMetadata {
        &self.metadata
    }

    /// Returns this activation's exact reference typed by a protocol marker.
    /// The embedded protocol ID is checked before the typed reference is
    /// returned.
    pub fn typed_actor_ref<P: ProtocolTag>(&self) -> Result<Option<ActorRef<P>>, ReferenceError> {
        self.actor_ref().map(ActorRef::try_typed::<P>).transpose()
    }

    pub fn lifecycle_state(&self) -> ActorLifecycleState {
        *self.lifecycle_tx.borrow()
    }

    /// Sends a request and waits up to `timeout` for the complete response.
    ///
    /// The timeout covers mailbox admission and waiting, handler execution,
    /// and deferred reply delivery.
    pub async fn ask<R>(&self, request: R, timeout: Duration) -> Result<R::Response, ActorCallError>
    where
        A: Responder<R>,
        R: Request,
    {
        if timeout.is_zero() {
            return Err(ActorCallError::DeadlineExceeded);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(ActorCallError::InvalidTimeout)?;
        self.ask_until(request, deadline).await
    }

    pub(crate) async fn ask_until<R>(
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
        let command =
            ActorCommand::Envelope(Box::new(RequestEnvelope::new(request, reply_tx, deadline)));
        self.send_command(command, MailboxLane::Normal)?;
        match tokio::time::timeout_at(deadline.into(), reply_rx).await {
            Ok(result) => result.map_err(|_| ActorCallError::ResponseDropped)?,
            Err(_) => Err(ActorCallError::DeadlineExceeded),
        }
    }

    pub(crate) async fn ask_until_owned<R>(
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
        let command =
            ActorCommand::Envelope(Box::new(RequestEnvelope::new(request, reply_tx, deadline)));
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

    pub fn inspect_stop_failure(&self) -> Option<StopFailureRecord> {
        self.stop_failure
            .lock()
            .expect("actor stop failure mutex poisoned")
            .clone()
    }

    pub async fn retry_stop(&self) -> Result<(), ActorAdminError> {
        let state = self.lifecycle_state();
        if !matches!(
            state,
            ActorLifecycleState::StopFailed | ActorLifecycleState::Quarantined
        ) {
            return Err(ActorAdminError::InvalidState {
                operation: "retry_stop",
                state,
            });
        }
        let (result_tx, result_rx) = oneshot::channel();
        self.send_admin_command(ActorCommand::RetryStop(result_tx))?;
        result_rx
            .await
            .map_err(|_| ActorAdminError::ResponseDropped)?
    }

    pub async fn quarantine_after_authority_loss(&self) -> Result<(), ActorAdminError> {
        let state = self.lifecycle_state();
        if state != ActorLifecycleState::StopFailed {
            return Err(ActorAdminError::InvalidState {
                operation: "quarantine_after_authority_loss",
                state,
            });
        }
        let (result_tx, result_rx) = oneshot::channel();
        self.send_admin_command(ActorCommand::Quarantine(result_tx))?;
        result_rx
            .await
            .map_err(|_| ActorAdminError::ResponseDropped)?
    }

    pub async fn force_stop(
        &self,
        reason: impl Into<String>,
        ticket: impl Into<String>,
    ) -> Result<(), ActorAdminError> {
        let state = self.lifecycle_state();
        if !matches!(
            state,
            ActorLifecycleState::StopFailed | ActorLifecycleState::Quarantined
        ) {
            return Err(ActorAdminError::InvalidState {
                operation: "force_stop",
                state,
            });
        }
        let (result_tx, result_rx) = oneshot::channel();
        self.send_admin_command(ActorCommand::ForceStop {
            authorization: ForceStopAuthorization {
                reason: reason.into(),
                ticket: ticket.into(),
            },
            result: result_tx,
        })?;
        result_rx
            .await
            .map_err(|_| ActorAdminError::ResponseDropped)?
    }

    pub(crate) fn try_stop_internal(&self, reason: StopReason) -> Result<(), ActorTellError> {
        self.send_system_command(ActorCommand::Stop(reason))
    }

    pub(crate) fn mark_external_authority_lost(&self) -> bool {
        let was_retained = self.lifecycle_state() == ActorLifecycleState::StopFailed;
        self.mark_stop_failure_quarantined();
        self.set_lifecycle_state(ActorLifecycleState::Quarantined);
        was_retained
    }

    pub(crate) fn begin_fenced_stop(
        &self,
        was_retained: bool,
        reason: StopReason,
    ) -> Result<(), ActorTellError> {
        if was_retained {
            let (result, _response) = oneshot::channel();
            self.send_system_command(ActorCommand::Quarantine(result))
        } else {
            self.send_system_command(ActorCommand::Stop(reason))
        }
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
        let metadata = command.metadata(MailboxLane::Normal);
        match self.normal_tx.send(command).await {
            Ok(()) => {
                if let Some(metadata) = metadata {
                    self.observer.message_enqueued(
                        self.observation_metadata(),
                        &metadata,
                        self.normal_tx.max_capacity() - self.normal_tx.capacity(),
                    );
                }
                Ok(())
            }
            Err(_) => {
                if let Some(metadata) = metadata {
                    self.observer.mailbox_rejected(
                        self.observation_metadata(),
                        &metadata,
                        MailboxRejection::Closed,
                    );
                }
                Err(ActorTellError::MailboxClosed)
            }
        }
    }

    pub fn subscribe_terminated(&self) -> broadcast::Receiver<ActorTerminated> {
        self.terminated_tx.subscribe()
    }

    pub fn subscribe_forced_data_loss(&self) -> broadcast::Receiver<ForcedDataLossEvent> {
        self.forced_data_loss_tx.subscribe()
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

    pub(crate) fn publish_forced_data_loss(&self, event: ForcedDataLossEvent) {
        let _ = self.forced_data_loss_tx.send(event);
    }

    pub(crate) fn record_stop_failure(&self, record: StopFailureRecord) {
        *self
            .stop_failure
            .lock()
            .expect("actor stop failure mutex poisoned") = Some(record);
    }

    pub(crate) fn clear_stop_failure(&self) {
        self.stop_failure
            .lock()
            .expect("actor stop failure mutex poisoned")
            .take();
    }

    pub(crate) fn mark_stop_failure_quarantined(&self) {
        if let Some(failure) = self
            .stop_failure
            .lock()
            .expect("actor stop failure mutex poisoned")
            .as_mut()
        {
            failure.authoritative = false;
        }
    }

    pub(crate) fn run_terminal_hook(&self) {
        if let Some(hook) = self
            .terminal_hook
            .lock()
            .expect("actor terminal hook mutex poisoned")
            .take()
        {
            hook(self.local_ref());
        }
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

    pub(crate) fn try_tell_from<M>(
        &self,
        msg: M,
        sender: Option<ActorRef>,
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
        sender: Option<ActorRef>,
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

    fn send_admin_command(&self, command: ActorCommand<A>) -> Result<(), ActorAdminError> {
        self.system_tx
            .try_send(command)
            .map_err(|error| match error {
                TrySendError::Full(_) => ActorAdminError::MailboxFull,
                TrySendError::Closed(_) => ActorAdminError::MailboxClosed,
            })
    }

    fn send_command(
        &self,
        command: ActorCommand<A>,
        lane: MailboxLane,
    ) -> Result<(), ActorCallError> {
        if lane == MailboxLane::Normal {
            let state = self.lifecycle_state();
            if matches!(
                state,
                ActorLifecycleState::Passivating
                    | ActorLifecycleState::Stopping
                    | ActorLifecycleState::StopFailed
                    | ActorLifecycleState::Quarantined
                    | ActorLifecycleState::Stopped
            ) {
                return Err(ActorCallError::LifecycleUnavailable { state });
            }
        }
        let metadata = command.metadata(lane);
        let sender = match lane {
            MailboxLane::Normal => &self.normal_tx,
            MailboxLane::System => &self.system_tx,
        };
        match sender.try_send(command) {
            Ok(()) => {
                if let Some(metadata) = metadata {
                    self.observer.message_enqueued(
                        self.observation_metadata(),
                        &metadata,
                        sender.max_capacity() - sender.capacity(),
                    );
                }
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                if let Some(metadata) = metadata {
                    self.observer.mailbox_rejected(
                        self.observation_metadata(),
                        &metadata,
                        MailboxRejection::Full,
                    );
                    if metadata.kind() == crate::traits::MessageKind::Request {
                        self.observer.request_completed(
                            self.observation_metadata(),
                            &metadata,
                            RequestCompletion::MailboxFull,
                        );
                    }
                }
                Err(ActorCallError::MailboxFull)
            }
            Err(TrySendError::Closed(_)) => {
                if let Some(metadata) = metadata {
                    self.observer.mailbox_rejected(
                        self.observation_metadata(),
                        &metadata,
                        MailboxRejection::Closed,
                    );
                    if metadata.kind() == crate::traits::MessageKind::Request {
                        self.observer.request_completed(
                            self.observation_metadata(),
                            &metadata,
                            RequestCompletion::MailboxClosed,
                        );
                    }
                }
                Err(ActorCallError::MailboxClosed)
            }
        }
    }
}

impl From<ActorCallError> for ActorTellError {
    fn from(value: ActorCallError) -> Self {
        match value {
            ActorCallError::InvalidTimeout => Self::MailboxClosed,
            ActorCallError::MailboxFull => Self::MailboxFull,
            ActorCallError::MailboxClosed => Self::MailboxClosed,
            ActorCallError::LifecycleUnavailable { state } => Self::LifecycleUnavailable { state },
            ActorCallError::ResponseDropped
            | ActorCallError::DeadlineExceeded
            | ActorCallError::Handler(_) => Self::MailboxClosed,
        }
    }
}
