use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use lattice_core::actor_ref::ActorRef;

use crate::traits::{MessageKind, MessageMetadata, MessageOutcome, StopReason};
use crate::watch::LocalActorRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorMetadata {
    actor_type: &'static str,
    local_ref: LocalActorRef,
    actor_ref: Option<ActorRef>,
}

impl ActorMetadata {
    pub(crate) fn new(
        actor_type: &'static str,
        local_ref: LocalActorRef,
        actor_ref: Option<ActorRef>,
    ) -> Self {
        Self {
            actor_type,
            local_ref,
            actor_ref,
        }
    }

    pub fn actor_type(&self) -> &'static str {
        self.actor_type
    }

    pub fn local_ref(&self) -> LocalActorRef {
        self.local_ref
    }

    pub fn actor_ref(&self) -> Option<&ActorRef> {
        self.actor_ref.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MailboxRejection {
    Full,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestCompletion {
    ReplyDelivered,
    RecoveredReplyDelivered,
    HandlerFailed,
    ResponseDropped,
    DeadlineExceeded,
    MailboxFull,
    MailboxClosed,
    CallerDropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorLifecycleEvent {
    Started,
    StartFailed,
    Stopped(StopReason),
    StopFailed(StopReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolFailure {
    UnknownMessage,
    ModeMismatch,
    PayloadTooLarge,
    DecodeFailed,
    EncodeFailed,
    MissingDeadline,
    MailboxRejected,
    ActorFailed,
    ReplyTypeMismatch,
}

pub trait ActorObserver: Send + Sync + 'static {
    fn message_enqueued(
        &self,
        _actor: &ActorMetadata,
        _message: &MessageMetadata,
        _queue_depth: usize,
    ) {
    }

    fn mailbox_rejected(
        &self,
        _actor: &ActorMetadata,
        _message: &MessageMetadata,
        _reason: MailboxRejection,
    ) {
    }

    fn message_started(
        &self,
        _actor: &ActorMetadata,
        _message: &MessageMetadata,
        _queue_time: Duration,
    ) {
    }

    fn message_finished(
        &self,
        _actor: &ActorMetadata,
        _message: &MessageMetadata,
        _outcome: MessageOutcome,
        _processing_time: Duration,
    ) {
    }

    fn request_completed(
        &self,
        _actor: &ActorMetadata,
        _message: &MessageMetadata,
        _completion: RequestCompletion,
        _total_time: Duration,
    ) {
    }

    fn lifecycle(&self, _actor: &ActorMetadata, _event: ActorLifecycleEvent) {}

    fn protocol_failed(
        &self,
        _actor: &ActorMetadata,
        _message_id: u64,
        _kind: MessageKind,
        _payload_size: usize,
        _failure: ProtocolFailure,
    ) {
    }
}

#[derive(Clone)]
pub struct ActorObserverHandle {
    inner: Arc<dyn ActorObserver>,
}

impl ActorObserverHandle {
    pub fn new<O>(observer: O) -> Self
    where
        O: ActorObserver,
    {
        Self {
            inner: Arc::new(observer),
        }
    }

    pub fn from_arc(observer: Arc<dyn ActorObserver>) -> Self {
        Self { inner: observer }
    }

    pub(crate) fn message_enqueued(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        queue_depth: usize,
    ) {
        self.inner.message_enqueued(actor, message, queue_depth);
    }

    pub(crate) fn mailbox_rejected(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        reason: MailboxRejection,
    ) {
        self.inner.mailbox_rejected(actor, message, reason);
    }

    pub(crate) fn message_started(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        queue_time: Duration,
    ) {
        self.inner.message_started(actor, message, queue_time);
    }

    pub(crate) fn message_finished(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        outcome: MessageOutcome,
        processing_time: Duration,
    ) {
        self.inner
            .message_finished(actor, message, outcome, processing_time);
    }

    pub(crate) fn request_completed(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        completion: RequestCompletion,
    ) {
        self.inner.request_completed(
            actor,
            message,
            completion,
            Instant::now().saturating_duration_since(message.enqueued_at()),
        );
    }

    pub(crate) fn lifecycle(&self, actor: &ActorMetadata, event: ActorLifecycleEvent) {
        self.inner.lifecycle(actor, event);
    }

    pub(crate) fn protocol_failed(
        &self,
        actor: &ActorMetadata,
        message_id: u64,
        kind: MessageKind,
        payload_size: usize,
        failure: ProtocolFailure,
    ) {
        self.inner
            .protocol_failed(actor, message_id, kind, payload_size, failure);
    }
}

impl Default for ActorObserverHandle {
    fn default() -> Self {
        Self::new(NoopActorObserver)
    }
}

impl fmt::Debug for ActorObserverHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorObserverHandle")
            .finish_non_exhaustive()
    }
}

struct NoopActorObserver;

impl ActorObserver for NoopActorObserver {}

pub(crate) struct RequestObservation {
    observer: ActorObserverHandle,
    actor: ActorMetadata,
    message: MessageMetadata,
    completed: AtomicBool,
}

impl RequestObservation {
    pub(crate) fn new(
        observer: ActorObserverHandle,
        actor: ActorMetadata,
        message: MessageMetadata,
    ) -> Self {
        Self {
            observer,
            actor,
            message,
            completed: AtomicBool::new(false),
        }
    }

    pub(crate) fn complete(&self, completion: RequestCompletion) {
        if !self.completed.swap(true, Ordering::AcqRel) {
            self.observer
                .request_completed(&self.actor, &self.message, completion);
        }
    }
}
