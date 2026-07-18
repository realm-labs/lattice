//! Typed, single-use replies for actor requests.
//!
//! A reply has two related lifetimes: the responder callback and the
//! [`ReplyTo`] token. The callback may move the token into later work, so the
//! request is not necessarily complete when the callback returns. Conversely,
//! a callback may send a reply and then fail, in which case the callback error
//! must win. Replies are therefore handled as a small transaction.
//!
//! # Lifecycle
//!
//! While the [`crate::traits::Responder`] callback is running, the reply is in
//! `Responding`. Sending through the token stages the result until the callback
//! succeeds. Dropping the token is also staged so that a callback error can
//! still take precedence over [`ActorCallError::ResponseDropped`].
//!
//! ```text
//! Responding(Alive)
//!  |-- ReplyTo::send/fail -----> Responding(Staged)
//!  |-- ReplyTo drop ----------> Responding(Dropped)
//!  |-- responder returns Ok --> Deferred
//!  `-- error/cancel -----------> Completed
//!
//! Responding(Staged)
//!  |-- responder returns Ok --> Completed (deliver staged result)
//!  `-- error/cancel -----------> Completed (discard staged result)
//!
//! Responding(Dropped)
//!  |-- responder returns Ok --> Completed (ResponseDropped)
//!  `-- error/cancel -----------> Completed (callback error wins)
//!
//! Deferred
//!  |-- ReplyTo::send/fail -----> Completed (deliver result)
//!  |-- ReplyTo drop ----------> Completed (ResponseDropped)
//!  `-- deadline/cancel --------> Completed
//! ```
//!
//! Error recovery is terminal as well: if `Responder::respond_error` produces
//! a response, that response replaces any staged result and completes the
//! request. Caller disconnection, actor termination, and deadline expiry move
//! every non-terminal state directly to `Completed`.
//!
//! # Concurrency and effects
//!
//! A [`ReplyTo`] may be moved to another task, while the actor runtime retains
//! a control handle. State transitions are serialized by one mutex. The
//! transition removes the sender from the state before releasing the lock, so
//! exactly one transition can become terminal. Sending on the oneshot channel
//! and publishing request observations happen after the lock is released.
//!
//! The runtime calls `reap` opportunistically to settle an expired request or
//! a request whose caller has disconnected. It is intentionally named as an
//! operation rather than a query because it may perform a terminal transition.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::oneshot;

use crate::error::{ActorCallError, ActorError, ReplyError};
use crate::observation::{RequestCompletion, RequestObservation};

/// A single-use, typed capability for completing an actor request.
///
/// The token can be moved into a later one-way message. Dropping it without
/// replying completes the request with [`ActorCallError::ResponseDropped`].
///
/// A reply sent before the responder callback returns is provisional. It is
/// delivered only if the callback succeeds; a propagated callback error or a
/// recovered response takes precedence. Once the callback has succeeded, a
/// token moved into later work becomes a deferred reply and completes the
/// request immediately when used.
pub struct ReplyTo<T: Send + 'static> {
    slot: Arc<ReplySlot<T>>,
}

impl<T: Send + 'static> fmt::Debug for ReplyTo<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.slot.lock();
        let closed = state.is_terminal()
            || state.sender_is_closed()
            || self
                .slot
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline);
        formatter
            .debug_struct("ReplyTo")
            .field("deadline", &self.slot.deadline)
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
            state: Mutex::new(ReplyState::Responding {
                sender,
                token: RespondingToken::Alive,
            }),
            deadline,
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
        self.slot.deadline
    }

    pub fn is_closed(&self) -> bool {
        let control = self.control();
        control.reap()
    }

    pub(crate) fn control(&self) -> ReplyControl<T> {
        ReplyControl {
            slot: self.slot.clone(),
        }
    }

    fn finish(&self, result: Result<T, ActorCallError>) -> Result<(), ReplyError> {
        let expired = self
            .slot
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        match self
            .slot
            .transition(|state| state.token_replied(result, expired))
        {
            TokenReplyAction::Staged => Ok(()),
            TokenReplyAction::Deliver(delivery) => {
                let completion = delivery.completion;
                if !self.slot.deliver(delivery) {
                    Err(ReplyError::ResponseDropped)
                } else if completion == RequestCompletion::DeadlineExceeded {
                    Err(ReplyError::DeadlineExceeded)
                } else {
                    Ok(())
                }
            }
            TokenReplyAction::CallerDropped => {
                self.slot
                    .observation
                    .complete(RequestCompletion::CallerDropped);
                Err(ReplyError::ResponseDropped)
            }
            TokenReplyAction::AlreadyCompleted => Err(ReplyError::AlreadyCompleted),
        }
    }
}

impl<T: Send + 'static> Drop for ReplyTo<T> {
    fn drop(&mut self) {
        let delivery = self.slot.transition(ReplyState::token_dropped);
        if let Some(delivery) = delivery {
            self.slot.deliver(delivery);
        }
    }
}

pub(crate) trait PendingReply: Send + Sync {
    fn cancel(&self, error: &ActorCallError);
    fn reap(&self) -> bool;
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
        let delivery = self.slot.transition(ReplyState::handler_succeeded);
        if let Some(delivery) = delivery {
            self.slot.deliver(delivery);
        }
    }

    pub(crate) fn handler_failed<E>(&self, error: E)
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.cancel(ActorCallError::Handler(ActorError::from_error(error)));
    }

    pub(crate) fn respond_after_error(&self, response: T) {
        let delivery = self.slot.transition(|state| {
            state.complete(Ok(response), RequestCompletion::RecoveredReplyDelivered)
        });
        if let Some(delivery) = delivery {
            self.slot.deliver(delivery);
        }
    }

    pub(crate) fn cancel(&self, error: ActorCallError) {
        let completion = completion_for_error(&error);
        let delivery = self
            .slot
            .transition(|state| state.complete(Err(error), completion));
        if let Some(delivery) = delivery {
            self.slot.deliver(delivery);
        }
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.slot.deadline
    }

    pub(crate) fn reap(&self) -> bool {
        let expired = self
            .slot
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        match self.slot.transition(|state| state.reap(expired)) {
            ReapAction::Pending => false,
            ReapAction::Completed => true,
            ReapAction::CallerDropped => {
                self.slot
                    .observation
                    .complete(RequestCompletion::CallerDropped);
                true
            }
            ReapAction::Deliver(delivery) => {
                self.slot.deliver(delivery);
                true
            }
        }
    }
}

impl<T: Send + 'static> PendingReply for ReplyControl<T> {
    fn cancel(&self, error: &ActorCallError) {
        self.cancel(error.clone());
    }

    fn reap(&self) -> bool {
        self.reap()
    }
}

/// Shared synchronization point between the public token and runtime control.
///
/// `deadline` is immutable request metadata rather than part of the lifecycle
/// state. This prevents deadline bookkeeping from creating additional state
/// combinations.
struct ReplySlot<T: Send + 'static> {
    state: Mutex<ReplyState<T>>,
    deadline: Option<Instant>,
    observation: RequestObservation,
}

/// The complete reply lifecycle.
///
/// The variants encode the ownership invariants directly:
///
/// - `Responding` owns the sender and records what happened to the token during
///   the responder callback.
/// - `Deferred` owns the sender and implies that the callback succeeded while
///   a live token was moved elsewhere.
/// - `Completed` owns neither a sender nor a staged result.
///
/// In particular, there is no representable combination such as a failed
/// handler with a live token or a completed request with a buffered response.
enum ReplyState<T: Send + 'static> {
    Responding {
        sender: oneshot::Sender<Result<T, ActorCallError>>,
        token: RespondingToken<T>,
    },
    Deferred {
        sender: oneshot::Sender<Result<T, ActorCallError>>,
    },
    Completed,
}

/// Token activity observed while the responder callback is still running.
///
/// Both a reply and a token drop remain provisional until the runtime records
/// whether the callback succeeded.
enum RespondingToken<T: Send + 'static> {
    Alive,
    Staged(Result<T, ActorCallError>),
    Dropped,
}

/// A terminal side effect produced by a state transition.
///
/// Deliveries are executed after releasing the state mutex.
struct Delivery<T: Send + 'static> {
    sender: oneshot::Sender<Result<T, ActorCallError>>,
    result: Result<T, ActorCallError>,
    completion: RequestCompletion,
}

enum TokenReplyAction<T: Send + 'static> {
    Staged,
    Deliver(Delivery<T>),
    CallerDropped,
    AlreadyCompleted,
}

enum ReapAction<T: Send + 'static> {
    Pending,
    Completed,
    CallerDropped,
    Deliver(Delivery<T>),
}

impl<T: Send + 'static> ReplySlot<T> {
    fn lock(&self) -> std::sync::MutexGuard<'_, ReplyState<T>> {
        self.state.lock().expect("reply slot poisoned")
    }

    fn transition<R>(&self, transition: impl FnOnce(ReplyState<T>) -> (ReplyState<T>, R)) -> R {
        let mut state = self.lock();
        let current = std::mem::replace(&mut *state, ReplyState::Completed);
        let (next, result) = transition(current);
        *state = next;
        result
    }

    fn deliver(&self, delivery: Delivery<T>) -> bool {
        if delivery.sender.send(delivery.result).is_err() {
            self.observation.complete(RequestCompletion::CallerDropped);
            false
        } else {
            self.observation.complete(delivery.completion);
            true
        }
    }
}

impl<T: Send + 'static> ReplyState<T> {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed)
    }

    fn sender_is_closed(&self) -> bool {
        match self {
            Self::Responding { sender, .. } | Self::Deferred { sender } => sender.is_closed(),
            Self::Completed => true,
        }
    }

    fn token_replied(
        self,
        result: Result<T, ActorCallError>,
        expired: bool,
    ) -> (Self, TokenReplyAction<T>) {
        match self {
            Self::Responding {
                sender,
                token: RespondingToken::Alive,
            } => {
                if sender.is_closed() {
                    (Self::Completed, TokenReplyAction::CallerDropped)
                } else if expired {
                    let delivery = deadline_delivery(sender);
                    (Self::Completed, TokenReplyAction::Deliver(delivery))
                } else {
                    (
                        Self::Responding {
                            sender,
                            token: RespondingToken::Staged(result),
                        },
                        TokenReplyAction::Staged,
                    )
                }
            }
            Self::Responding { sender, token } => (
                Self::Responding { sender, token },
                TokenReplyAction::AlreadyCompleted,
            ),
            Self::Deferred { sender } => {
                if sender.is_closed() {
                    (Self::Completed, TokenReplyAction::CallerDropped)
                } else {
                    let delivery = if expired {
                        deadline_delivery(sender)
                    } else {
                        delivery(sender, result)
                    };
                    (Self::Completed, TokenReplyAction::Deliver(delivery))
                }
            }
            Self::Completed => (Self::Completed, TokenReplyAction::AlreadyCompleted),
        }
    }

    fn token_dropped(self) -> (Self, Option<Delivery<T>>) {
        match self {
            Self::Responding {
                sender,
                token: RespondingToken::Alive,
            } => (
                Self::Responding {
                    sender,
                    token: RespondingToken::Dropped,
                },
                None,
            ),
            Self::Responding { sender, token } => (Self::Responding { sender, token }, None),
            Self::Deferred { sender } => (
                Self::Completed,
                Some(delivery(sender, Err(ActorCallError::ResponseDropped))),
            ),
            Self::Completed => (Self::Completed, None),
        }
    }

    fn handler_succeeded(self) -> (Self, Option<Delivery<T>>) {
        match self {
            Self::Responding {
                sender,
                token: RespondingToken::Alive,
            } => (Self::Deferred { sender }, None),
            Self::Responding {
                sender,
                token: RespondingToken::Staged(result),
            } => (Self::Completed, Some(delivery(sender, result))),
            Self::Responding {
                sender,
                token: RespondingToken::Dropped,
            } => (
                Self::Completed,
                Some(delivery(sender, Err(ActorCallError::ResponseDropped))),
            ),
            Self::Deferred { sender } => (Self::Deferred { sender }, None),
            Self::Completed => (Self::Completed, None),
        }
    }

    fn complete(
        self,
        result: Result<T, ActorCallError>,
        completion: RequestCompletion,
    ) -> (Self, Option<Delivery<T>>) {
        match self {
            Self::Responding { sender, .. } | Self::Deferred { sender } => (
                Self::Completed,
                Some(Delivery {
                    sender,
                    result,
                    completion,
                }),
            ),
            Self::Completed => (Self::Completed, None),
        }
    }

    fn reap(self, expired: bool) -> (Self, ReapAction<T>) {
        match self {
            Self::Responding { sender, token } => {
                Self::reap_pending(sender, expired, |sender| Self::Responding { sender, token })
            }
            Self::Deferred { sender } => {
                Self::reap_pending(sender, expired, |sender| Self::Deferred { sender })
            }
            Self::Completed => (Self::Completed, ReapAction::Completed),
        }
    }

    fn reap_pending(
        sender: oneshot::Sender<Result<T, ActorCallError>>,
        expired: bool,
        pending: impl FnOnce(oneshot::Sender<Result<T, ActorCallError>>) -> Self,
    ) -> (Self, ReapAction<T>) {
        if sender.is_closed() {
            (Self::Completed, ReapAction::CallerDropped)
        } else if expired {
            (
                Self::Completed,
                ReapAction::Deliver(deadline_delivery(sender)),
            )
        } else {
            (pending(sender), ReapAction::Pending)
        }
    }
}

fn delivery<T: Send + 'static>(
    sender: oneshot::Sender<Result<T, ActorCallError>>,
    result: Result<T, ActorCallError>,
) -> Delivery<T> {
    let completion = completion_for_result(&result);
    Delivery {
        sender,
        result,
        completion,
    }
}

fn deadline_delivery<T: Send + 'static>(
    sender: oneshot::Sender<Result<T, ActorCallError>>,
) -> Delivery<T> {
    delivery(sender, Err(ActorCallError::DeadlineExceeded))
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
        ActorCallError::ActorPanicked => RequestCompletion::ActorPanicked,
        ActorCallError::LifecycleUnavailable { .. } => RequestCompletion::LifecycleUnavailable,
        ActorCallError::ResponseDropped => RequestCompletion::ResponseDropped,
        ActorCallError::DeadlineExceeded => RequestCompletion::DeadlineExceeded,
        ActorCallError::Handler(_) => RequestCompletion::HandlerFailed,
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;

    type TestReceiver = oneshot::Receiver<Result<u64, ActorCallError>>;

    fn responding(token: RespondingToken<u64>) -> (ReplyState<u64>, TestReceiver) {
        let (sender, receiver) = oneshot::channel();
        (ReplyState::Responding { sender, token }, receiver)
    }

    #[test]
    fn handler_success_defers_a_live_token_and_commits_a_staged_reply() {
        let (state, _receiver) = responding(RespondingToken::Alive);
        let (state, delivery) = state.handler_succeeded();
        assert!(matches!(state, ReplyState::Deferred { .. }));
        assert!(delivery.is_none());

        let (state, _receiver) = responding(RespondingToken::Staged(Ok(42)));
        let (state, delivery) = state.handler_succeeded();
        assert!(matches!(state, ReplyState::Completed));
        let delivery = delivery.expect("staged reply is committed");
        assert!(matches!(delivery.result, Ok(42)));
        assert_eq!(delivery.completion, RequestCompletion::ReplyDelivered);
    }

    #[test]
    fn handler_failure_discards_a_staged_reply() {
        let (state, _receiver) = responding(RespondingToken::Staged(Ok(42)));
        let error = ActorCallError::Handler(ActorError::new("handler failed"));
        let (state, delivery) = state.complete(Err(error), RequestCompletion::HandlerFailed);

        assert!(matches!(state, ReplyState::Completed));
        let delivery = delivery.expect("handler failure completes the request");
        assert!(matches!(delivery.result, Err(ActorCallError::Handler(_))));
        assert_eq!(delivery.completion, RequestCompletion::HandlerFailed);
    }

    #[test]
    fn dropped_token_is_settled_only_after_the_handler_finishes() {
        let (state, _receiver) = responding(RespondingToken::Alive);
        let (state, delivery) = state.token_dropped();
        assert!(matches!(
            state,
            ReplyState::Responding {
                token: RespondingToken::Dropped,
                ..
            }
        ));
        assert!(delivery.is_none());

        let (state, delivery) = state.handler_succeeded();
        assert!(matches!(state, ReplyState::Completed));
        let delivery = delivery.expect("successful handler settles the dropped token");
        assert!(matches!(
            delivery.result,
            Err(ActorCallError::ResponseDropped)
        ));
        assert_eq!(delivery.completion, RequestCompletion::ResponseDropped);
    }

    #[test]
    fn reap_distinguishes_caller_drop_from_deadline_expiry() {
        let (state, receiver) = responding(RespondingToken::Alive);
        drop(receiver);
        let (state, action) = state.reap(false);
        assert!(matches!(state, ReplyState::Completed));
        assert!(matches!(action, ReapAction::CallerDropped));

        let (sender, _receiver) = oneshot::channel::<Result<u64, ActorCallError>>();
        let state = ReplyState::Deferred { sender };
        let (state, action) = state.reap(true);
        assert!(matches!(state, ReplyState::Completed));
        let ReapAction::Deliver(delivery) = action else {
            panic!("deadline expiry must produce a delivery");
        };
        assert!(matches!(
            delivery.result,
            Err(ActorCallError::DeadlineExceeded)
        ));
        assert_eq!(delivery.completion, RequestCompletion::DeadlineExceeded);
    }
}
