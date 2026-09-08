use std::{
    collections::HashMap,
    fmt,
    future::Future,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{sync::Mutex, time::Instant};
use tracing::{Instrument, warn};

use crate::{
    error::EventBusError,
    lifecycle::{Activity, SubscriptionState, drain},
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
    /// Permanently rejects new publishes and subscriptions, cancels live subscriptions, and waits
    /// up to `deadline` for admitted operations and handlers to finish. A `false` result leaves
    /// unfinished work tracked for a later call; timing out or cancelling this future does not
    /// abort handlers. Already-admitted publishes may skip handlers that have not started yet.
    async fn shutdown(&self, deadline: Duration) -> bool;
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

    /// Cancels the subscription and waits up to `deadline` for its handlers and background task.
    /// Returns `false` while work remains; a later call can continue waiting for the same work.
    pub async fn shutdown(&self, deadline: Duration) -> bool {
        self.state.shutdown(deadline).await
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
                activity: Arc::default(),
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
    activity: Arc<Activity>,
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
            let _publishing = self.inner.activity.enter()?;
            let handlers = {
                let subscribers = self.inner.subscribers.lock().await;
                subscribers
                    .values()
                    .filter(|subscriber| {
                        !subscriber.state.is_cancelled()
                            && subscriber.subscription.filter.matches(&event.subject)
                    })
                    .map(|subscriber| (subscriber.handler.clone(), subscriber.state.clone()))
                    .collect::<Vec<_>>()
            };

            let mut failures = 0usize;
            for (handler, state) in handlers {
                let Ok(_handling) = state.activity.enter() else {
                    continue;
                };
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
        let setup = state.setup();
        let mut subscribers = self.inner.subscribers.lock().await;
        let _subscribing = self.inner.activity.enter()?;
        subscribers.insert(
            id,
            LocalSubscriber {
                subscription,
                handler: Arc::new(handler),
                state: state.clone(),
            },
        );
        drop(subscribers);

        let inner = Arc::downgrade(&self.inner);
        let mut cancellation = state.cancellation();
        let activity = state.activity.clone();
        state.attach(tokio::spawn(async move {
            cancellation.cancelled().await;
            activity.wait_idle().await;
            if let Some(inner) = Weak::upgrade(&inner) {
                inner.subscribers.lock().await.remove(&id);
            }
        }));

        setup.complete()?;
        Ok(EventSubscriptionHandle::new(id, state))
    }

    async fn shutdown(&self, deadline: Duration) -> bool {
        let deadline = Instant::now() + deadline;
        self.inner.activity.close();
        let states = self
            .inner
            .subscribers
            .lock()
            .await
            .values()
            .map(|subscriber| subscriber.state.clone())
            .collect();
        drain(&self.inner.activity, states, deadline).await
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
    async fn shutdown_retains_a_task_that_ignores_cancellation_until_it_finishes() {
        let state = SubscriptionState::new();
        let (release, waiting) = tokio::sync::oneshot::channel();
        state.attach(tokio::spawn(async move {
            waiting.await.unwrap();
        }));
        let handle = EventSubscriptionHandle::new(1, state);

        assert!(!handle.shutdown(Duration::ZERO).await);
        assert!(!handle.shutdown(Duration::ZERO).await);
        release.send(()).expect("shutdown must not abort the task");
        assert!(handle.shutdown(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn cancelling_a_shutdown_waiter_does_not_detach_the_task() {
        let state = SubscriptionState::new();
        let (release, waiting) = tokio::sync::oneshot::channel();
        state.attach(tokio::spawn(async move {
            waiting.await.unwrap();
        }));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                state.shutdown(Duration::from_secs(1)),
            )
            .await
            .is_err()
        );
        assert!(!state.shutdown(Duration::ZERO).await);
        release.send(()).expect("cancelled waiter must retain task");
        assert!(state.shutdown(Duration::from_secs(1)).await);
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
