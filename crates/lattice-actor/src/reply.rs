use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use tokio::sync::oneshot;

use crate::error::{ActorCallError, ActorError, ReplyError};
use crate::observation::{RequestCompletion, RequestObservation};

/// A single-use, typed capability for completing an actor request.
///
/// The token can be moved into a later one-way message. Dropping it without
/// replying completes the request with [`ActorCallError::ResponseDropped`].
pub struct ReplyTo<T: Send + 'static> {
    slot: Arc<ReplySlot<T>>,
}

impl<T: Send + 'static> fmt::Debug for ReplyTo<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        let closed = state.sender.is_none()
            || state
                .sender
                .as_ref()
                .is_some_and(oneshot::Sender::is_closed)
            || state
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline);
        formatter
            .debug_struct("ReplyTo")
            .field("deadline", &state.deadline)
            .field("closed", &closed)
            .finish()
    }
}

impl<T: Send + 'static> ReplyTo<T> {
    pub(crate) fn new(
        sender: oneshot::Sender<Result<T, ActorCallError>>,
        deadline: Option<Instant>,
        observation: RequestObservation,
    ) -> (Self, ReplyControl<T>) {
        let slot = Arc::new(ReplySlot {
            state: Mutex::new(ReplyState {
                sender: Some(sender),
                deadline,
                handler: HandlerState::Running,
                token: TokenState::Alive,
                buffered: None,
            }),
            observation,
        });
        (Self { slot: slot.clone() }, ReplyControl { slot })
    }

    /// Completes the request successfully. The token cannot be reused.
    pub fn send(self, response: T) -> Result<(), ReplyError> {
        self.finish(Ok(response))
    }

    /// Completes the request with a handler error. This is useful in a
    /// continuation message after an asynchronous operation fails.
    pub fn fail<E>(self, error: E) -> Result<(), ReplyError>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.finish(Err(ActorCallError::Handler(ActorError::from_error(error))))
    }

    /// Completes the request with a specific actor-call error.
    pub fn fail_with(self, error: ActorCallError) -> Result<(), ReplyError> {
        self.finish(Err(error))
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.lock().deadline
    }

    pub fn is_closed(&self) -> bool {
        let control = self.control();
        control.is_complete()
    }

    pub(crate) fn control(&self) -> ReplyControl<T> {
        ReplyControl {
            slot: self.slot.clone(),
        }
    }

    fn finish(&self, result: Result<T, ActorCallError>) -> Result<(), ReplyError> {
        let delivery = {
            let mut state = self.lock();
            if state.sender.is_none() || state.token != TokenState::Alive {
                return Err(ReplyError::AlreadyCompleted);
            }
            if state
                .sender
                .as_ref()
                .is_some_and(oneshot::Sender::is_closed)
            {
                state.sender.take();
                state.buffered = None;
                state.handler = HandlerState::Failed;
                state.token = TokenState::Invalidated;
                drop(state);
                self.slot
                    .observation
                    .complete(RequestCompletion::CallerDropped);
                return Err(ReplyError::ResponseDropped);
            }
            if state
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                state.token = TokenState::Responded;
                state.handler = HandlerState::Failed;
                state.buffered = None;
                state
                    .sender
                    .take()
                    .map(|sender| (sender, Err(ActorCallError::DeadlineExceeded)))
            } else {
                state.token = TokenState::Responded;
                match state.handler {
                    HandlerState::Running => {
                        state.buffered = Some(result);
                        return Ok(());
                    }
                    HandlerState::Succeeded => state.sender.take().map(|sender| (sender, result)),
                    HandlerState::Failed => return Err(ReplyError::AlreadyCompleted),
                }
            }
        };

        let Some((sender, result)) = delivery else {
            return Err(ReplyError::AlreadyCompleted);
        };
        let completion = completion_for_result(&result);
        if sender.send(result).is_err() {
            self.slot
                .observation
                .complete(RequestCompletion::CallerDropped);
            return Err(ReplyError::ResponseDropped);
        }
        self.slot.observation.complete(completion);
        if completion == RequestCompletion::DeadlineExceeded {
            Err(ReplyError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }

    fn lock(&self) -> MutexGuard<'_, ReplyState<T>> {
        self.slot.state.lock().expect("reply slot poisoned")
    }
}

impl<T: Send + 'static> Drop for ReplyTo<T> {
    fn drop(&mut self) {
        let delivery = {
            let mut state = self.lock();
            if state.token != TokenState::Alive {
                return;
            }
            state.token = TokenState::Dropped;
            if state.handler == HandlerState::Succeeded {
                state
                    .sender
                    .take()
                    .map(|sender| (sender, Err(ActorCallError::ResponseDropped)))
            } else {
                None
            }
        };
        if let Some((sender, result)) = delivery {
            if sender.send(result).is_err() {
                self.slot
                    .observation
                    .complete(RequestCompletion::CallerDropped);
            } else {
                self.slot
                    .observation
                    .complete(RequestCompletion::ResponseDropped);
            }
        }
    }
}

pub(crate) trait PendingReply: Send + Sync {
    fn cancel(&self, error: &ActorCallError);
    fn is_complete(&self) -> bool;
}

pub(crate) struct ReplyControl<T: Send + 'static> {
    slot: Arc<ReplySlot<T>>,
}

impl<T: Send + 'static> Clone for ReplyControl<T> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
        }
    }
}

impl<T: Send + 'static> ReplyControl<T> {
    pub(crate) fn handler_succeeded(&self) {
        let delivery = {
            let mut state = self.lock();
            if state.handler != HandlerState::Running || state.sender.is_none() {
                return;
            }
            state.handler = HandlerState::Succeeded;
            if let Some(result) = state.buffered.take() {
                state.sender.take().map(|sender| (sender, result))
            } else if state.token == TokenState::Dropped {
                state
                    .sender
                    .take()
                    .map(|sender| (sender, Err(ActorCallError::ResponseDropped)))
            } else {
                None
            }
        };
        if let Some((sender, result)) = delivery {
            let completion = completion_for_result(&result);
            if sender.send(result).is_err() {
                self.slot
                    .observation
                    .complete(RequestCompletion::CallerDropped);
            } else {
                self.slot.observation.complete(completion);
            }
        }
    }

    pub(crate) fn handler_failed<E>(&self, error: E)
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.cancel(ActorCallError::Handler(ActorError::from_error(error)));
    }

    pub(crate) fn respond_after_error(&self, response: T) {
        let delivery = {
            let mut state = self.lock();
            if state.sender.is_none() {
                return;
            }
            state.handler = HandlerState::Failed;
            state.token = TokenState::Invalidated;
            state.buffered = None;
            state.sender.take().map(|sender| (sender, Ok(response)))
        };
        if let Some((sender, result)) = delivery {
            if sender.send(result).is_err() {
                self.slot
                    .observation
                    .complete(RequestCompletion::CallerDropped);
            } else {
                self.slot
                    .observation
                    .complete(RequestCompletion::RecoveredReplyDelivered);
            }
        }
    }

    pub(crate) fn cancel(&self, error: ActorCallError) {
        let completion = completion_for_error(&error);
        let delivery = {
            let mut state = self.lock();
            state.handler = HandlerState::Failed;
            state.token = TokenState::Invalidated;
            state.buffered = None;
            state.sender.take().map(|sender| (sender, Err(error)))
        };
        if let Some((sender, result)) = delivery {
            if sender.send(result).is_err() {
                self.slot
                    .observation
                    .complete(RequestCompletion::CallerDropped);
            } else {
                self.slot.observation.complete(completion);
            }
        }
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.lock().deadline
    }

    pub(crate) fn is_complete(&self) -> bool {
        let expiration = {
            let mut state = self.lock();
            if state.sender.is_none() {
                return true;
            }
            if state
                .sender
                .as_ref()
                .is_some_and(oneshot::Sender::is_closed)
            {
                state.sender.take();
                state.buffered = None;
                state.handler = HandlerState::Failed;
                state.token = TokenState::Invalidated;
                drop(state);
                self.slot
                    .observation
                    .complete(RequestCompletion::CallerDropped);
                return true;
            }
            if state
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                state.handler = HandlerState::Failed;
                state.token = TokenState::Invalidated;
                state.buffered = None;
                state
                    .sender
                    .take()
                    .map(|sender| (sender, Err(ActorCallError::DeadlineExceeded)))
            } else {
                None
            }
        };
        if let Some((sender, result)) = expiration {
            let _ = sender.send(result);
            self.slot
                .observation
                .complete(RequestCompletion::DeadlineExceeded);
            true
        } else {
            false
        }
    }

    fn lock(&self) -> MutexGuard<'_, ReplyState<T>> {
        self.slot.state.lock().expect("reply slot poisoned")
    }
}

impl<T: Send + 'static> PendingReply for ReplyControl<T> {
    fn cancel(&self, error: &ActorCallError) {
        self.cancel(error.clone());
    }

    fn is_complete(&self) -> bool {
        self.is_complete()
    }
}

struct ReplySlot<T: Send + 'static> {
    state: Mutex<ReplyState<T>>,
    observation: RequestObservation,
}

struct ReplyState<T: Send + 'static> {
    sender: Option<oneshot::Sender<Result<T, ActorCallError>>>,
    deadline: Option<Instant>,
    handler: HandlerState,
    token: TokenState,
    buffered: Option<Result<T, ActorCallError>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HandlerState {
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenState {
    Alive,
    Responded,
    Dropped,
    Invalidated,
}

fn completion_for_result<T>(result: &Result<T, ActorCallError>) -> RequestCompletion {
    match result {
        Ok(_) => RequestCompletion::ReplyDelivered,
        Err(error) => completion_for_error(error),
    }
}

fn completion_for_error(error: &ActorCallError) -> RequestCompletion {
    match error {
        ActorCallError::InvalidTimeout => RequestCompletion::InvalidTimeout,
        ActorCallError::MailboxFull => RequestCompletion::MailboxFull,
        ActorCallError::MailboxClosed => RequestCompletion::MailboxClosed,
        ActorCallError::LifecycleUnavailable { .. } => RequestCompletion::LifecycleUnavailable,
        ActorCallError::ResponseDropped => RequestCompletion::ResponseDropped,
        ActorCallError::DeadlineExceeded => RequestCompletion::DeadlineExceeded,
        ActorCallError::Handler(_) => RequestCompletion::HandlerFailed,
    }
}
