use std::{
    collections::HashMap,
    fmt,
    future::Future,
    sync::{
        Arc, Mutex as SyncMutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tracing::{Instrument, warn};

use crate::{
    error::EventBusError,
    types::{EventEnvelope, EventSubscription},
};

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    async fn handle(&self, event: EventEnvelope) -> Result<(), EventBusError>;
}

#[async_trait]
impl<F, Fut> EventHandler for F
where
    F: Fn(EventEnvelope) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), EventBusError>> + Send,
{
    async fn handle(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        self(event).await
    }
}

#[async_trait]
pub trait EventBus: Clone + Send + Sync + 'static {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError>;
    async fn subscribe<H>(
        &self,
        subscription: EventSubscription,
        handler: H,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        H: EventHandler;
    /// Cancels every live subscription and waits up to `deadline` for in-flight handlers to
    /// finish. Returns `false` when a subscription task had to be aborted at the deadline.
    async fn shutdown(&self, deadline: Duration) -> bool;
}

#[derive(Debug)]
pub(crate) struct SubscriptionState {
    cancel: watch::Sender<bool>,
    task: SyncMutex<Option<JoinHandle<()>>>,
}

impl SubscriptionState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            cancel: watch::channel(false).0,
            task: SyncMutex::new(None),
        })
    }

    pub(crate) fn attach(&self, task: JoinHandle<()>) {
        if let Ok(mut slot) = self.task.lock() {
            *slot = Some(task);
        }
    }

    pub(crate) fn cancellation(&self) -> Cancellation {
        Cancellation {
            receiver: self.cancel.subscribe(),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        *self.cancel.borrow()
    }

    pub(crate) fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    pub(crate) async fn shutdown(&self, deadline: Duration) -> bool {
        self.cancel();
        let Some(task) = self.take_task() else {
            return true;
        };
        let abort = task.abort_handle();
        match tokio::time::timeout(deadline, task).await {
            Ok(_) => true,
            Err(_) => {
                abort.abort();
                false
            }
        }
    }

    fn take_task(&self) -> Option<JoinHandle<()>> {
        self.task.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl Drop for SubscriptionState {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().ok().and_then(Option::take) {
            task.abort();
        }
    }
}

pub(crate) struct Cancellation {
    receiver: watch::Receiver<bool>,
}

impl Cancellation {
    pub(crate) async fn cancelled(&mut self) {
        loop {
            if *self.receiver.borrow_and_update() {
                return;
            }
            if self.receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Control handle of a live subscription. Every backend keeps the subscription running until it is
/// cancelled, so dropping the handle leaves delivery untouched; the background task is only torn
/// down once the bus itself releases the subscription.
#[derive(Debug, Clone)]
pub struct EventSubscriptionHandle {
    id: u64,
    state: Arc<SubscriptionState>,
}

impl EventSubscriptionHandle {
    pub(crate) fn new(id: u64, state: Arc<SubscriptionState>) -> Self {
        Self { id, state }
    }

    pub fn cancel(&self) {
        self.state.cancel();
    }

    /// Cancels the subscription and waits up to `deadline` for its background task to observe the
    /// cancellation. Returns `false` when the task had to be aborted at the deadline.
    pub async fn shutdown(&self, deadline: Duration) -> bool {
        self.state.shutdown(deadline).await
    }

    /// Releases the background task from subscription lifetime tracking so it outlives the bus
    /// that spawned it.
    pub fn detach(self) {
        self.state.take_task();
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.is_cancelled()
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug, Clone)]
pub struct LocalEventBus {
    inner: Arc<LocalEventBusInner>,
}

impl LocalEventBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(LocalEventBusInner {
                next_id: AtomicU64::new(1),
                subscribers: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub async fn subscription_count(&self) -> usize {
        self.inner.subscribers.lock().await.len()
    }
}

impl Default for LocalEventBus {
    fn default() -> Self {
        Self::new()
    }
}

struct LocalEventBusInner {
    next_id: AtomicU64,
    subscribers: Mutex<HashMap<u64, LocalSubscriber>>,
}

impl fmt::Debug for LocalEventBusInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalEventBusInner")
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

struct LocalSubscriber {
    subscription: EventSubscription,
    handler: Arc<dyn EventHandler>,
    state: Arc<SubscriptionState>,
}

#[async_trait]
impl EventBus for LocalEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        let span = tracing::info_span!(
            "eventbus.publish",
            otel.kind = "producer",
            event.subject = event.subject.as_str(),
            event.type = event.event_type.as_str(),
            source.service = event.source_service.as_str(),
            source.instance = event.source_instance.as_str()
        );
        async {
            let handlers = {
                let subscribers = self.inner.subscribers.lock().await;
                subscribers
                    .values()
                    .filter(|subscriber| {
                        !subscriber.state.is_cancelled()
                            && subscriber.subscription.filter.matches(&event.subject)
                    })
                    .map(|subscriber| subscriber.handler.clone())
                    .collect::<Vec<_>>()
            };

            let mut failures = 0usize;
            for handler in handlers {
                let consumer_span = tracing::info_span!(
                    "eventbus.consume",
                    otel.kind = "consumer",
                    event.subject = event.subject.as_str(),
                    event.type = event.event_type.as_str()
                );
                if let Err(error) = handler
                    .handle(event.clone())
                    .instrument(consumer_span)
                    .await
                {
                    failures += 1;
                    warn!(
                        %error,
                        subject = event.subject.as_str(),
                        "local event handler failed"
                    );
                }
            }
            if failures > 0 {
                warn!(
                    failures,
                    subject = event.subject.as_str(),
                    "local event fan-out completed with failing handlers"
                );
            }
            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn subscribe<H>(
        &self,
        subscription: EventSubscription,
        handler: H,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        H: EventHandler,
    {
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let state = SubscriptionState::new();
        self.inner.subscribers.lock().await.insert(
            id,
            LocalSubscriber {
                subscription,
                handler: Arc::new(handler),
                state: state.clone(),
            },
        );

        let inner = Arc::downgrade(&self.inner);
        let mut cancellation = state.cancellation();
        tokio::spawn(async move {
            cancellation.cancelled().await;
            if let Some(inner) = Weak::upgrade(&inner) {
                inner.subscribers.lock().await.remove(&id);
            }
        });

        Ok(EventSubscriptionHandle::new(id, state))
    }

    async fn shutdown(&self, _deadline: Duration) -> bool {
        let subscribers = std::mem::take(&mut *self.inner.subscribers.lock().await);
        for subscriber in subscribers.values() {
            subscriber.state.cancel();
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use lattice_core::{instance::InstanceId, service_kind, trace::TraceContext};

    use super::*;
    use crate::types::{EventId, Subject, SubjectFilter};

    #[tokio::test]
    async fn failing_handler_does_not_block_the_remaining_subscribers() {
        let bus = LocalEventBus::new();
        let delivered = Arc::new(AtomicU64::new(0));
        let mut failing = Vec::new();

        for _ in 0..4 {
            let delivered = delivered.clone();
            failing.push(
                bus.subscribe(
                    EventSubscription::local(SubjectFilter::new("game.>")),
                    move |_: EventEnvelope| {
                        let delivered = delivered.clone();
                        async move {
                            delivered.fetch_add(1, Ordering::SeqCst);
                            Err(EventBusError::Handler("boom".to_string()))
                        }
                    },
                )
                .await
                .unwrap(),
            );
        }
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sink = observed.clone();
        let survivor = bus
            .subscribe(
                EventSubscription::local(SubjectFilter::new("game.>")),
                move |event: EventEnvelope| {
                    let sink = sink.clone();
                    async move {
                        sink.lock().await.push(event.event_type);
                        Ok(())
                    }
                },
            )
            .await
            .unwrap();

        bus.publish(test_event()).await.unwrap();

        assert_eq!(delivered.load(Ordering::SeqCst), 4);
        assert_eq!(&*observed.lock().await, &["WorldEntered"]);
        assert_eq!(failing.len(), 4);
        survivor.cancel();
    }

    #[tokio::test]
    async fn cancelling_a_subscription_removes_it_from_the_bus() {
        let bus = LocalEventBus::new();
        let handle = bus
            .subscribe(
                EventSubscription::local(SubjectFilter::new("game.>")),
                move |_: EventEnvelope| async move { Ok(()) },
            )
            .await
            .unwrap();
        assert_eq!(bus.subscription_count().await, 1);

        handle.cancel();
        tokio::task::yield_now().await;

        assert!(handle.is_cancelled());
        assert_eq!(bus.subscription_count().await, 0);
    }

    #[tokio::test]
    async fn shutdown_stops_a_task_parked_on_its_message_stream() {
        let state = SubscriptionState::new();
        let observed_cancellation = Arc::new(AtomicBool::new(false));
        let flag = observed_cancellation.clone();
        let mut cancellation = state.cancellation();
        state.attach(tokio::spawn(async move {
            tokio::select! {
                () = cancellation.cancelled() => flag.store(true, Ordering::SeqCst),
                () = std::future::pending::<()>() => unreachable!(),
            }
        }));
        let handle = EventSubscriptionHandle::new(1, state);

        assert!(handle.shutdown(Duration::from_secs(5)).await);
        assert!(observed_cancellation.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_aborts_a_task_that_ignores_cancellation() {
        let state = SubscriptionState::new();
        state.attach(tokio::spawn(std::future::pending::<()>()));
        let handle = EventSubscriptionHandle::new(1, state);

        assert!(!handle.shutdown(Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn dropping_a_handle_keeps_the_subscription_running() {
        let bus = LocalEventBus::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sink = observed.clone();
        drop(
            bus.subscribe(
                EventSubscription::local(SubjectFilter::new("game.>")),
                move |event: EventEnvelope| {
                    let sink = sink.clone();
                    async move {
                        sink.lock().await.push(event.event_type);
                        Ok(())
                    }
                },
            )
            .await
            .unwrap(),
        );

        bus.publish(test_event()).await.unwrap();

        assert_eq!(&*observed.lock().await, &["WorldEntered"]);
    }

    #[tokio::test]
    async fn dropping_the_last_subscription_owner_aborts_its_task() {
        let state = SubscriptionState::new();
        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel::<()>();
        state.attach(tokio::spawn(async move {
            let _alive_tx = alive_tx;
            std::future::pending::<()>().await;
        }));
        let handle = EventSubscriptionHandle::new(1, state);

        drop(handle);

        assert!(alive_rx.await.is_err());
    }

    fn test_event() -> EventEnvelope {
        EventEnvelope {
            event_id: EventId::new("event-1"),
            subject: Subject::new("game.world.entered"),
            event_type: "WorldEntered".to_string(),
            source_service: service_kind!("World"),
            source_instance: InstanceId::new("world-a"),
            recipient: None,
            correlation_id: None,
            trace: TraceContext::default(),
            occurred_unix_ms: 1,
            payload: Vec::new(),
        }
    }
}
