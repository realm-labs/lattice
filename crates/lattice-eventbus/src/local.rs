use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::error::EventBusError;
use crate::types::{EventEnvelope, EventSubscription};

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    async fn handle(&self, event: EventEnvelope) -> Result<(), EventBusError>;
}

#[async_trait]
impl<F, Fut> EventHandler for F
where
    F: Fn(EventEnvelope) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), EventBusError>> + Send,
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
}

#[derive(Debug, Clone)]
pub struct EventSubscriptionHandle {
    id: u64,
    cancelled: Arc<AtomicBool>,
}

impl EventSubscriptionHandle {
    pub(crate) fn new(id: u64, cancelled: Arc<AtomicBool>) -> Self {
        Self { id, cancelled }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
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
    cancelled: Arc<AtomicBool>,
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
                        !subscriber.cancelled.load(Ordering::SeqCst)
                            && subscriber.subscription.filter.matches(&event.subject)
                    })
                    .map(|subscriber| subscriber.handler.clone())
                    .collect::<Vec<_>>()
            };

            for handler in handlers {
                let consumer_span = tracing::info_span!(
                    "eventbus.consume",
                    otel.kind = "consumer",
                    event.subject = event.subject.as_str(),
                    event.type = event.event_type.as_str()
                );
                handler
                    .handle(event.clone())
                    .instrument(consumer_span)
                    .await?;
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
        let cancelled = Arc::new(AtomicBool::new(false));
        self.inner.subscribers.lock().await.insert(
            id,
            LocalSubscriber {
                subscription,
                handler: Arc::new(handler),
                cancelled: cancelled.clone(),
            },
        );
        Ok(EventSubscriptionHandle::new(id, cancelled))
    }
}
