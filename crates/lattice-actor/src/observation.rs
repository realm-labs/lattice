use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::traits::{MessageMetadata, MessageOutcome, StopReason};
use crate::watch::LocalActorRef;

static ACTIVE_STOP_FAILURES: AtomicU64 = AtomicU64::new(0);
static STOP_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static FORCED_DATA_LOSS_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorLifecycleMetricsSnapshot {
    pub active_stop_failures: u64,
    pub stop_failures_total: u64,
    pub forced_data_loss_total: u64,
}

pub fn actor_lifecycle_metrics() -> ActorLifecycleMetricsSnapshot {
    ActorLifecycleMetricsSnapshot {
        active_stop_failures: ACTIVE_STOP_FAILURES.load(Ordering::Relaxed),
        stop_failures_total: STOP_FAILURES_TOTAL.load(Ordering::Relaxed),
        forced_data_loss_total: FORCED_DATA_LOSS_TOTAL.load(Ordering::Relaxed),
    }
}

pub(crate) fn record_new_stop_failure() {
    ACTIVE_STOP_FAILURES.fetch_add(1, Ordering::Relaxed);
    STOP_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_resolved_stop_failure(forced: bool) {
    ACTIVE_STOP_FAILURES.fetch_sub(1, Ordering::Relaxed);
    if forced {
        FORCED_DATA_LOSS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn record_abandoned_stop_failure() {
    ACTIVE_STOP_FAILURES.fetch_sub(1, Ordering::Relaxed);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorMetadata {
    actor_type: &'static str,
    local_ref: LocalActorRef,
}

impl ActorMetadata {
    pub(crate) fn new(actor_type: &'static str, local_ref: LocalActorRef) -> Self {
        Self {
            actor_type,
            local_ref,
        }
    }

    pub fn actor_type(&self) -> &'static str {
        self.actor_type
    }

    pub fn local_ref(&self) -> LocalActorRef {
        self.local_ref
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
    InvalidTimeout,
    HandlerFailed,
    UnhandledInCurrentState,
    ResponseDropped,
    DeadlineExceeded,
    MailboxFull,
    MailboxClosed,
    ActorPanicked,
    LifecycleUnavailable,
    CallerDropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorLifecycleEvent {
    Started,
    StartFailed,
    Panicked,
    Stopped(StopReason),
    StopFailed(StopReason),
    StopRetried(StopReason),
    ForcedDataLoss(StopReason),
}

/// Observes Actor activity without controlling its lifecycle.
///
/// A callback panic is isolated from message delivery and Actor cleanup. The
/// observer is disabled for all clones of its handle after the first panic;
/// callbacks already in progress may still complete.
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

    fn message_started(&self, _actor: &ActorMetadata, _message: &MessageMetadata) {}

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
    ) {
    }

    fn lifecycle(&self, _actor: &ActorMetadata, _event: ActorLifecycleEvent) {}
}

#[derive(Clone)]
pub struct ActorObserverHandle {
    inner: Arc<dyn ActorObserver>,
    enabled: bool,
    panicked: Arc<AtomicBool>,
}

impl ActorObserverHandle {
    pub fn new<O>(observer: O) -> Self
    where
        O: ActorObserver,
    {
        Self {
            inner: Arc::new(observer),
            enabled: true,
            panicked: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn from_arc(observer: Arc<dyn ActorObserver>) -> Self {
        Self {
            inner: observer,
            enabled: true,
            panicked: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled && !self.panicked.load(Ordering::Acquire)
    }

    pub(crate) fn message_enqueued(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        queue_depth: usize,
    ) {
        self.observe(actor, "message_enqueued", |observer| {
            observer.message_enqueued(actor, message, queue_depth);
        });
    }

    pub(crate) fn mailbox_rejected(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        reason: MailboxRejection,
    ) {
        self.observe(actor, "mailbox_rejected", |observer| {
            observer.mailbox_rejected(actor, message, reason);
        });
    }

    pub(crate) fn message_started(&self, actor: &ActorMetadata, message: &MessageMetadata) {
        self.observe(actor, "message_started", |observer| {
            observer.message_started(actor, message);
        });
    }

    pub(crate) fn message_finished(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        outcome: MessageOutcome,
        processing_time: Duration,
    ) {
        self.observe(actor, "message_finished", |observer| {
            observer.message_finished(actor, message, outcome, processing_time);
        });
    }

    pub(crate) fn request_completed(
        &self,
        actor: &ActorMetadata,
        message: &MessageMetadata,
        completion: RequestCompletion,
    ) {
        self.observe(actor, "request_completed", |observer| {
            observer.request_completed(actor, message, completion);
        });
    }

    pub(crate) fn lifecycle(&self, actor: &ActorMetadata, event: ActorLifecycleEvent) {
        self.observe(actor, "lifecycle", |observer| {
            observer.lifecycle(actor, event);
        });
    }

    fn observe(
        &self,
        actor: &ActorMetadata,
        callback: &'static str,
        observe: impl FnOnce(&dyn ActorObserver),
    ) {
        if !self.is_enabled() {
            return;
        }
        if catch_unwind(AssertUnwindSafe(|| observe(self.inner.as_ref()))).is_err()
            && !self.panicked.swap(true, Ordering::AcqRel)
        {
            tracing::error!(
                actor_type = actor.actor_type(),
                actor = ?actor.local_ref(),
                callback,
                "Actor observer panicked and has been disabled"
            );
        }
    }
}

impl Default for ActorObserverHandle {
    fn default() -> Self {
        Self {
            inner: Arc::new(NoopActorObserver),
            enabled: false,
            panicked: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl fmt::Debug for ActorObserverHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorObserverHandle")
            .field("enabled", &self.is_enabled())
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
