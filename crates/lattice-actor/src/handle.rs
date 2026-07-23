use std::{
    any::type_name,
    fmt,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use broadcast::error::{RecvError, TryRecvError};
use lattice_core::actor_ref::{ActorRef, ProtocolTag, ReferenceError};
use tokio::sync::{broadcast, oneshot, watch};

use crate::{
    error::{ActorAdminError, ActorCallError, ActorTellError},
    mailbox::{
        ActorCommand, ActorEnvelope, MailboxLane, RequestEnvelope, TellEnvelope,
        channel::{Sender, TrySendError},
    },
    observation::{ActorMetadata, ActorObserverHandle, MailboxRejection, RequestCompletion},
    traits::{
        Actor, ActorLifecycleState, Handler, Message, MessageKind, MessageMetadata, Request,
        Responder, StopReason,
    },
    watch::{ActorTerminated, LocalActorRef},
};

pub(crate) type TerminalHook = Box<dyn FnOnce(LocalActorRef) + Send + 'static>;

pub(crate) struct ActorHandleInit<A: Actor> {
    pub(crate) local_ref: LocalActorRef,
    pub(crate) terminated_tx: broadcast::Sender<ActorTerminated>,
    pub(crate) lifecycle_tx: watch::Sender<ActorLifecycleState>,
    pub(crate) stop_failure: Arc<Mutex<Option<StopFailureRecord>>>,
    pub(crate) forced_data_loss_tx: broadcast::Sender<ForcedDataLossEvent>,
    pub(crate) terminal_hook: Arc<Mutex<Option<TerminalHook>>>,
    pub(crate) normal_tx: Sender<ActorCommand<A>>,
    pub(crate) system_tx: Sender<ActorCommand<A>>,
    pub(crate) actor_ref: Option<ActorRef>,
    pub(crate) observer: ActorObserverHandle,
}

pub struct ActorHandle<A: Actor> {
    local_ref: LocalActorRef,
    terminated_tx: broadcast::Sender<ActorTerminated>,
    termination: Arc<Mutex<Option<ActorTerminated>>>,
    terminal_cleanup_started: Arc<AtomicBool>,
    lifecycle_tx: watch::Sender<ActorLifecycleState>,
    stop_failure: Arc<Mutex<Option<StopFailureRecord>>>,
    forced_data_loss_tx: broadcast::Sender<ForcedDataLossEvent>,
    terminal_hook: Arc<Mutex<Option<TerminalHook>>>,
    normal_tx: Sender<ActorCommand<A>>,
    system_tx: Sender<ActorCommand<A>>,
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

/// A retained, single-delivery subscription to an Actor activation's terminal event.
///
/// Unlike a bare broadcast receiver, subscriptions created after termination still
/// observe the terminal event. Each subscription yields the event at most once.
pub struct ActorTerminationSubscription {
    retained: Option<ActorTerminated>,
    receiver: broadcast::Receiver<ActorTerminated>,
    delivered: bool,
}

impl ActorTerminationSubscription {
    pub fn try_recv(&mut self) -> Result<ActorTerminated, TryRecvError> {
        if self.delivered {
            return Err(TryRecvError::Closed);
        }
        if let Some(termination) = self.retained.take() {
            self.delivered = true;
            return Ok(termination);
        }
        let termination = self.receiver.try_recv()?;
        self.delivered = true;
        Ok(termination)
    }

    pub async fn recv(&mut self) -> Result<ActorTerminated, RecvError> {
        if self.delivered {
            return Err(RecvError::Closed);
        }
        if let Some(termination) = self.retained.take() {
            self.delivered = true;
            return Ok(termination);
        }
        let termination = self.receiver.recv().await?;
        self.delivered = true;
        Ok(termination)
    }
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
            termination: self.termination.clone(),
            terminal_cleanup_started: self.terminal_cleanup_started.clone(),
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
    pub(crate) fn new(init: ActorHandleInit<A>) -> Self {
        Self {
            local_ref: init.local_ref,
            terminated_tx: init.terminated_tx,
            termination: Arc::new(Mutex::new(None)),
            terminal_cleanup_started: Arc::new(AtomicBool::new(false)),
            lifecycle_tx: init.lifecycle_tx,
            stop_failure: init.stop_failure,
            forced_data_loss_tx: init.forced_data_loss_tx,
            terminal_hook: init.terminal_hook,
            normal_tx: init.normal_tx,
            system_tx: init.system_tx,
            metadata: Arc::new(ActorMetadata::new(
                type_name::<A>(),
                init.local_ref,
                init.actor_ref,
            )),
            observer: init.observer,
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
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<R>,
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
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<R>,
        R: Request,
    {
        if Instant::now() >= deadline {
            return Err(ActorCallError::DeadlineExceeded);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = ActorCommand::envelope(RequestEnvelope::new(request, reply_tx, deadline));
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
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<R>,
        R: Request,
    {
        if Instant::now() >= deadline {
            return Err(ActorCallError::DeadlineExceeded);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = ActorCommand::envelope(RequestEnvelope::new(request, reply_tx, deadline));
        self.send_command(command, MailboxLane::Normal)?;
        match tokio::time::timeout_at(deadline.into(), reply_rx).await {
            Ok(result) => result.map_err(|_| ActorCallError::ResponseDropped)?,
            Err(_) => Err(ActorCallError::DeadlineExceeded),
        }
    }

    /// Waits for normal-mailbox capacity and admits one one-way message.
    ///
    /// If the Actor closes or stops admitting business traffic while waiting,
    /// the error returns ownership of `msg`.
    pub async fn tell<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        self.send_tell_on_lane(msg, None, MailboxLane::Normal).await
    }

    /// Attempts to admit one one-way message without waiting for capacity.
    ///
    /// Full, closed, and lifecycle-rejected results return ownership of `msg`.
    pub fn try_tell<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::Normal)
    }

    pub async fn stop(&self, reason: StopReason) -> Result<(), ActorTellError<StopReason>> {
        self.try_send_stop(reason)
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

    pub(crate) fn try_stop_internal(
        &self,
        reason: StopReason,
    ) -> Result<(), ActorTellError<StopReason>> {
        self.try_send_stop(reason)
    }

    pub(crate) fn mark_external_authority_lost(&self) -> ActorLifecycleState {
        let previous = self.lifecycle_state();
        self.mark_stop_failure_quarantined();
        self.set_lifecycle_state(ActorLifecycleState::Quarantined);
        previous
    }

    pub(crate) fn begin_fenced_stop(
        &self,
        previous: ActorLifecycleState,
        reason: StopReason,
    ) -> Result<(), ActorAdminError> {
        if matches!(
            previous,
            ActorLifecycleState::Passivating
                | ActorLifecycleState::Stopping
                | ActorLifecycleState::StopFailed
        ) {
            let (result, _response) = oneshot::channel();
            self.send_admin_command(ActorCommand::Quarantine(result))
        } else {
            self.try_send_stop(reason).map_err(|error| match error {
                ActorTellError::MailboxFull(_) => ActorAdminError::MailboxFull,
                ActorTellError::MailboxClosed(_) | ActorTellError::LifecycleUnavailable { .. } => {
                    ActorAdminError::MailboxClosed
                }
            })
        }
    }

    pub(crate) fn try_tell_internal<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::Normal)
    }

    pub(crate) async fn send_tell_internal<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        self.send_tell_on_lane(msg, None, MailboxLane::Normal).await
    }

    pub(crate) async fn send_envelope_internal<E>(&self, envelope: E) -> Result<(), ActorCallError>
    where
        E: ActorEnvelope<A> + 'static,
    {
        self.send_command_wait(ActorCommand::envelope(envelope), MailboxLane::Normal)
            .await
    }

    pub fn subscribe_terminated(&self) -> ActorTerminationSubscription {
        let receiver = self.terminated_tx.subscribe();
        let retained = self
            .termination
            .lock()
            .expect("actor termination mutex poisoned")
            .clone();
        ActorTerminationSubscription {
            retained,
            receiver,
            delivered: false,
        }
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

    pub(crate) fn mark_terminal_cleanup_started(&self) {
        self.terminal_cleanup_started.store(true, Ordering::Release);
    }

    pub(crate) fn terminal_cleanup_started(&self) -> bool {
        self.terminal_cleanup_started.load(Ordering::Acquire)
    }

    pub(crate) fn publish_terminated(&self, notification: ActorTerminated) {
        let should_publish = {
            let mut termination = self
                .termination
                .lock()
                .expect("actor termination mutex poisoned");
            if termination.is_some() {
                false
            } else {
                *termination = Some(notification.clone());
                true
            }
        };
        if should_publish {
            let _ = self.terminated_tx.send(notification);
        }
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

    pub(crate) fn clear_stop_failure(&self) -> bool {
        self.stop_failure
            .lock()
            .expect("actor stop failure mutex poisoned")
            .take()
            .is_some()
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
    pub(crate) fn try_tell_for_test<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::Normal)
    }

    #[cfg(test)]
    pub(crate) fn try_tell_system_for_test<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, None, MailboxLane::System)
    }

    pub(crate) fn try_tell_from<M>(
        &self,
        msg: M,
        sender: Option<ActorRef>,
    ) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, sender, MailboxLane::Normal)
    }

    pub(crate) async fn tell_from<M>(
        &self,
        msg: M,
        sender: Option<ActorRef>,
    ) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        self.send_tell_on_lane(msg, sender, MailboxLane::Normal)
            .await
    }

    fn try_tell_on_lane<M>(
        &self,
        msg: M,
        sender: Option<ActorRef>,
        lane: MailboxLane,
    ) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        if let Some(state) = self.unavailable_lifecycle(lane) {
            return Err(ActorTellError::LifecycleUnavailable {
                state,
                message: msg,
            });
        }
        let channel = self.channel(lane);
        let permit = match channel.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Full(())) => {
                self.observe_tell_rejection::<M>(lane, MailboxRejection::Full);
                return Err(ActorTellError::MailboxFull(msg));
            }
            Err(TrySendError::Closed(())) => {
                self.observe_tell_rejection::<M>(lane, MailboxRejection::Closed);
                return Err(ActorTellError::MailboxClosed(msg));
            }
        };
        if let Some(state) = self.unavailable_lifecycle(lane) {
            return Err(ActorTellError::LifecycleUnavailable {
                state,
                message: msg,
            });
        }
        let command = ActorCommand::envelope(TellEnvelope::new(msg, sender));
        let metadata = self.observed_metadata(&command, lane);
        permit.send(command);
        self.observe_command_enqueued(metadata, channel);
        Ok(())
    }

    async fn send_tell_on_lane<M>(
        &self,
        msg: M,
        sender: Option<ActorRef>,
        lane: MailboxLane,
    ) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as crate::traits::Actor>::Behavior: crate::state_machine::Accepts<M>,
        M: Message,
    {
        if let Some(state) = self.unavailable_lifecycle(lane) {
            return Err(ActorTellError::LifecycleUnavailable {
                state,
                message: msg,
            });
        }
        let channel = self.channel(lane);
        let permit = match channel.reserve().await {
            Ok(permit) => permit,
            Err(_) => {
                self.observe_tell_rejection::<M>(lane, MailboxRejection::Closed);
                return Err(ActorTellError::MailboxClosed(msg));
            }
        };
        if let Some(state) = self.unavailable_lifecycle(lane) {
            return Err(ActorTellError::LifecycleUnavailable {
                state,
                message: msg,
            });
        }
        let command = ActorCommand::envelope(TellEnvelope::new(msg, sender));
        let metadata = self.observed_metadata(&command, lane);
        permit.send(command);
        self.observe_command_enqueued(metadata, channel);
        Ok(())
    }

    fn try_send_stop(&self, reason: StopReason) -> Result<(), ActorTellError<StopReason>> {
        self.system_tx
            .try_send(ActorCommand::Stop(reason))
            .map_err(|error| match error {
                TrySendError::Full(_) => ActorTellError::MailboxFull(reason),
                TrySendError::Closed(_) => ActorTellError::MailboxClosed(reason),
            })
    }

    fn channel(&self, lane: MailboxLane) -> &Sender<ActorCommand<A>> {
        match lane {
            MailboxLane::Normal => &self.normal_tx,
            MailboxLane::System => &self.system_tx,
        }
    }

    fn unavailable_lifecycle(&self, lane: MailboxLane) -> Option<ActorLifecycleState> {
        if lane != MailboxLane::Normal {
            return None;
        }
        let state = self.lifecycle_state();
        matches!(
            state,
            ActorLifecycleState::Passivating
                | ActorLifecycleState::Stopping
                | ActorLifecycleState::StopFailed
                | ActorLifecycleState::Quarantined
                | ActorLifecycleState::Stopped
        )
        .then_some(state)
    }

    fn observed_metadata(
        &self,
        command: &ActorCommand<A>,
        lane: MailboxLane,
    ) -> Option<MessageMetadata> {
        self.observer
            .is_enabled()
            .then(|| command.metadata(lane))
            .flatten()
    }

    fn observe_command_enqueued(
        &self,
        metadata: Option<MessageMetadata>,
        channel: &Sender<ActorCommand<A>>,
    ) {
        if let Some(metadata) = metadata {
            self.observer.message_enqueued(
                self.observation_metadata(),
                &metadata,
                channel.max_capacity() - channel.capacity(),
            );
        }
    }

    fn observe_tell_rejection<M: Message>(&self, lane: MailboxLane, reason: MailboxRejection) {
        if !self.observer.is_enabled() {
            return;
        }
        let metadata = MessageMetadata::new(type_name::<M>(), MessageKind::Tell, lane.into(), None);
        self.observer
            .mailbox_rejected(self.observation_metadata(), &metadata, reason);
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
        let metadata = self
            .observer
            .is_enabled()
            .then(|| command.metadata(lane))
            .flatten();
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
                    if metadata.kind() == MessageKind::Request {
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
                    if metadata.kind() == MessageKind::Request {
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

    async fn send_command_wait(
        &self,
        command: ActorCommand<A>,
        lane: MailboxLane,
    ) -> Result<(), ActorCallError> {
        if let Some(state) = self.unavailable_lifecycle(lane) {
            return Err(ActorCallError::LifecycleUnavailable { state });
        }
        let metadata = self.observed_metadata(&command, lane);
        let channel = self.channel(lane);
        let permit = channel.reserve().await.map_err(|_| {
            if let Some(metadata) = metadata {
                self.observer.mailbox_rejected(
                    self.observation_metadata(),
                    &metadata,
                    MailboxRejection::Closed,
                );
            }
            ActorCallError::MailboxClosed
        })?;
        if let Some(state) = self.unavailable_lifecycle(lane) {
            return Err(ActorCallError::LifecycleUnavailable { state });
        }
        permit.send(command);
        self.observe_command_enqueued(metadata, channel);
        Ok(())
    }
}
