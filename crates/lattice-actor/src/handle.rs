use std::{
    any::type_name,
    fmt,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use broadcast::error::{RecvError, TryRecvError};
use tokio::sync::{Notify, broadcast, oneshot, watch};

use crate::{
    error::{ActorAdminError, ActorCallError, ActorTellError},
    mailbox::{
        ActorCommand, ActorEnvelope, MailboxLane, RequestEnvelope, TellEnvelope,
        channel::{Sender, TrySendError},
    },
    observation::{ActorMetadata, ActorObserverHandle, MailboxRejection, RequestCompletion},
    state_machine::Accepts,
    traits::{
        Actor, ActorLifecycleState, Handler, Message, MessageKind, MessageMetadata, Request,
        Responder, StopReason,
    },
    watch::{ActorTermination, LocalActorRef, LocalWatchSubscription},
};

pub(crate) type TerminalHook = Box<dyn FnOnce(LocalActorRef) + Send + 'static>;

const COOPERATIVE_CAPACITY_WAITER_LIMIT: usize = 2;

pub(crate) struct ActorHandleInit<A: Actor> {
    pub(crate) local_ref: LocalActorRef,
    pub(crate) terminated_tx: broadcast::Sender<ActorTermination>,
    pub(crate) lifecycle_tx: watch::Sender<ActorLifecycleState>,
    pub(crate) stop_failure: Arc<Mutex<Option<StopFailureRecord>>>,
    pub(crate) forced_data_loss_tx: broadcast::Sender<ForcedDataLossEvent>,
    pub(crate) terminal_hook: Arc<Mutex<Option<TerminalHook>>>,
    pub(crate) normal_tx: Sender<ActorCommand<A>>,
    pub(crate) system_tx: Sender<ActorCommand<A>>,
    pub(crate) observer: ActorObserverHandle,
}

pub struct ActorHandle<A: Actor> {
    local_ref: LocalActorRef,
    terminated_tx: broadcast::Sender<ActorTermination>,
    termination: Arc<Mutex<Option<ActorTermination>>>,
    terminal_cleanup_started: Arc<AtomicBool>,
    lifecycle_tx: watch::Sender<ActorLifecycleState>,
    lifecycle_state: Arc<AtomicU8>,
    business_fenced: Arc<AtomicBool>,
    fence_notify: Arc<Notify>,
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
    retained: Option<ActorTermination>,
    receiver: broadcast::Receiver<ActorTermination>,
    delivered: bool,
}

impl ActorTerminationSubscription {
    pub fn try_recv(&mut self) -> Result<ActorTermination, TryRecvError> {
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

    pub async fn recv(&mut self) -> Result<ActorTermination, RecvError> {
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
            lifecycle_state: self.lifecycle_state.clone(),
            business_fenced: self.business_fenced.clone(),
            fence_notify: self.fence_notify.clone(),
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
            lifecycle_state: Arc::new(AtomicU8::new(ActorLifecycleState::Starting as u8)),
            business_fenced: Arc::new(AtomicBool::new(false)),
            fence_notify: Arc::new(Notify::new()),
            stop_failure: init.stop_failure,
            forced_data_loss_tx: init.forced_data_loss_tx,
            terminal_hook: init.terminal_hook,
            normal_tx: init.normal_tx,
            system_tx: init.system_tx,
            metadata: Arc::new(ActorMetadata::new(type_name::<A>(), init.local_ref)),
            observer: init.observer,
            _marker: PhantomData,
        }
    }

    pub fn local_ref(&self) -> LocalActorRef {
        self.local_ref
    }

    /// Observes this local activation through the same DeathWatch shape used by
    /// bound distributed references.
    pub fn watch(
        &self,
    ) -> impl Future<Output = Result<LocalWatchSubscription, std::convert::Infallible>> + Send {
        std::future::ready(Ok(LocalWatchSubscription::new(self.subscribe_terminated())))
    }

    pub(crate) fn observer(&self) -> &ActorObserverHandle {
        &self.observer
    }

    pub(crate) fn observation_metadata(&self) -> &ActorMetadata {
        &self.metadata
    }

    pub fn lifecycle_state(&self) -> ActorLifecycleState {
        ActorLifecycleState::try_from(self.lifecycle_state.load(Ordering::Acquire))
            .expect("actor lifecycle atomic contains an invalid state")
    }

    /// Sends a request and waits up to `timeout` for the complete response.
    ///
    /// The timeout covers mailbox admission and waiting, handler execution,
    /// and deferred reply delivery.
    pub async fn ask<R>(&self, request: R, timeout: Duration) -> Result<R::Response, ActorCallError>
    where
        A: Responder<R>,
        <A as Actor>::Behavior: Accepts<R>,
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
        <A as Actor>::Behavior: Accepts<R>,
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

    #[doc(hidden)]
    pub async fn ask_until_owned<R>(
        self,
        request: R,
        deadline: Instant,
    ) -> Result<R::Response, ActorCallError>
    where
        A: Responder<R>,
        <A as Actor>::Behavior: Accepts<R>,
        R: Request,
    {
        self.ask_until(request, deadline).await
    }

    /// Waits for normal-mailbox capacity and admits one one-way message.
    ///
    /// If the Actor closes or stops admitting business traffic while waiting,
    /// the error returns ownership of `msg`.
    pub async fn tell<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as Actor>::Behavior: Accepts<M>,
        M: Message,
    {
        self.send_tell_on_lane(msg, MailboxLane::Normal).await
    }

    /// Attempts to admit one one-way message without waiting for capacity.
    ///
    /// Full, closed, and lifecycle-rejected results return ownership of `msg`.
    pub fn try_tell<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as Actor>::Behavior: Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, MailboxLane::Normal)
    }

    pub fn stop(&self, reason: StopReason) -> Result<(), ActorTellError<StopReason>> {
        self.try_send_stop(reason)
    }

    /// Irrevocably closes business admission when this activation loses external authority.
    ///
    /// The runtime finishes any turn already admitted, rejects queued turns, and enters normal
    /// persistence-aware stopping. This signal does not require system mailbox capacity and never
    /// discards retained state after a stopping failure.
    #[doc(hidden)]
    pub fn fence_business_admission(&self) {
        if !self.business_fenced.swap(true, Ordering::AcqRel) {
            self.fence_notify.notify_one();
        }
    }

    #[doc(hidden)]
    pub fn business_admission_fenced(&self) -> bool {
        self.business_fenced.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_business_fenced(&self) {
        while !self.business_admission_fenced() {
            self.fence_notify.notified().await;
        }
    }

    pub fn inspect_stop_failure(&self) -> Option<StopFailureRecord> {
        self.stop_failure
            .lock()
            .expect("actor stop failure mutex poisoned")
            .clone()
    }

    pub async fn retry_stop(&self) -> Result<(), ActorAdminError> {
        let state = self.lifecycle_state();
        if state != ActorLifecycleState::StopFailed {
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

    pub async fn force_stop(
        &self,
        reason: impl Into<String>,
        ticket: impl Into<String>,
    ) -> Result<(), ActorAdminError> {
        let state = self.lifecycle_state();
        if state != ActorLifecycleState::StopFailed {
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

    #[doc(hidden)]
    pub fn try_stop_internal(&self, reason: StopReason) -> Result<(), ActorTellError<StopReason>> {
        self.try_send_stop(reason)
    }

    /// Waits for system-lane capacity before admitting a stop request.
    ///
    /// Used when losing the request would strand an Actor that no longer has an
    /// owner able to retry.
    pub(crate) async fn send_stop_internal(
        &self,
        reason: StopReason,
    ) -> Result<(), ActorTellError<StopReason>> {
        match self.system_tx.reserve().await {
            Ok(permit) => {
                permit.send(ActorCommand::Stop(reason));
                Ok(())
            }
            Err(_) => Err(ActorTellError::MailboxClosed(reason)),
        }
    }

    pub(crate) fn try_tell_internal<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as Actor>::Behavior: Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, MailboxLane::Normal)
    }

    pub(crate) async fn send_tell_internal<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as Actor>::Behavior: Accepts<M>,
        M: Message,
    {
        self.send_tell_on_lane(msg, MailboxLane::Normal).await
    }

    pub(crate) async fn send_system_tell_internal<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as Actor>::Behavior: Accepts<M>,
        M: Message,
    {
        self.send_tell_on_lane(msg, MailboxLane::System).await
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
        self.lifecycle_state.store(state as u8, Ordering::Release);
        self.lifecycle_tx.send_replace(state);
    }

    pub(crate) fn mark_terminal_cleanup_started(&self) {
        self.terminal_cleanup_started.store(true, Ordering::Release);
    }

    #[doc(hidden)]
    pub fn terminal_cleanup_started(&self) -> bool {
        self.terminal_cleanup_started.load(Ordering::Acquire)
    }

    pub(crate) fn publish_terminated(&self, notification: ActorTermination) {
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
        <A as Actor>::Behavior: Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, MailboxLane::Normal)
    }

    #[cfg(test)]
    pub(crate) fn try_tell_system_for_test<M>(&self, msg: M) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as Actor>::Behavior: Accepts<M>,
        M: Message,
    {
        self.try_tell_on_lane(msg, MailboxLane::System)
    }

    fn try_tell_on_lane<M>(&self, msg: M, lane: MailboxLane) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as Actor>::Behavior: Accepts<M>,
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
        let command = ActorCommand::envelope(TellEnvelope::new(msg));
        let metadata = self.observed_metadata(&command, lane);
        permit.send(command);
        self.observe_command_enqueued(metadata, channel);
        Ok(())
    }

    async fn send_tell_on_lane<M>(&self, msg: M, lane: MailboxLane) -> Result<(), ActorTellError<M>>
    where
        A: Handler<M>,
        <A as Actor>::Behavior: Accepts<M>,
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
                // The first sender parks immediately, preserving the single-producer path. A
                // small contending cohort yields once so the Actor can drain a batch before those
                // senders register; larger cohorts park immediately to avoid a scheduler storm.
                if matches!(
                    channel.capacity_waiters(),
                    1..=COOPERATIVE_CAPACITY_WAITER_LIMIT
                ) {
                    tokio::task::yield_now().await;
                }
                match channel.reserve_after_full().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        self.observe_tell_rejection::<M>(lane, MailboxRejection::Closed);
                        return Err(ActorTellError::MailboxClosed(msg));
                    }
                }
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
        let command = ActorCommand::envelope(TellEnvelope::new(msg));
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
        if self.business_admission_fenced() {
            return Some(match state {
                ActorLifecycleState::Starting | ActorLifecycleState::Running => {
                    ActorLifecycleState::Stopping
                }
                state => state,
            });
        }
        matches!(
            state,
            ActorLifecycleState::Passivating
                | ActorLifecycleState::Stopping
                | ActorLifecycleState::StopFailed
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
