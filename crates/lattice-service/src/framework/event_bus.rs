use std::sync::Arc;

use async_trait::async_trait;
use lattice_eventbus::error::EventBusError;
use lattice_eventbus::local::{EventBus, EventHandler, EventSubscriptionHandle};
use lattice_eventbus::types::{EventEnvelope, EventSubscription};
use tokio::sync::Mutex;

#[async_trait]
pub trait DynEventBus: Send + Sync + 'static {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError>;
    async fn subscribe_boxed(
        &self,
        subscription: EventSubscription,
        handler: Arc<dyn EventHandler>,
    ) -> Result<EventSubscriptionHandle, EventBusError>;
}

#[async_trait]
impl<T> DynEventBus for T
where
    T: EventBus,
{
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        EventBus::publish(self, event).await
    }

    async fn subscribe_boxed(
        &self,
        subscription: EventSubscription,
        handler: Arc<dyn EventHandler>,
    ) -> Result<EventSubscriptionHandle, EventBusError> {
        EventBus::subscribe(self, subscription, move |event| {
            let handler = handler.clone();
            async move { handler.handle(event).await }
        })
        .await
    }
}

#[derive(Clone)]
pub struct ServiceEventBus {
    inner: Arc<dyn DynEventBus>,
    subscriptions: EventSubscriptionRegistry,
}

impl ServiceEventBus {
    fn new(inner: Arc<dyn DynEventBus>, subscriptions: EventSubscriptionRegistry) -> Self {
        Self {
            inner,
            subscriptions,
        }
    }
}

impl std::fmt::Debug for ServiceEventBus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceEventBus")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl EventBus for ServiceEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        self.inner.publish(event).await
    }

    async fn subscribe<H>(
        &self,
        subscription: EventSubscription,
        handler: H,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        H: EventHandler,
    {
        let handle = self
            .inner
            .subscribe_boxed(subscription, Arc::new(handler))
            .await?;
        self.subscriptions.own(handle.clone()).await;
        Ok(handle)
    }
}

#[derive(Debug, Clone, Default)]
struct EventSubscriptionRegistry {
    handles: Arc<Mutex<Vec<EventSubscriptionHandle>>>,
}

impl EventSubscriptionRegistry {
    async fn own(&self, handle: EventSubscriptionHandle) {
        self.handles.lock().await.push(handle);
    }

    pub(crate) async fn cancel_all(&self) -> usize {
        let mut handles = self.handles.lock().await;
        let count = handles.len();
        for handle in handles.drain(..) {
            handle.cancel();
        }
        count
    }
}

#[derive(Clone)]
pub struct ClusterEventBusComponent {
    inner: Arc<dyn DynEventBus>,
    subscriptions: EventSubscriptionRegistry,
}

impl ClusterEventBusComponent {
    pub fn new<T>(event_bus: T) -> Self
    where
        T: EventBus,
    {
        Self {
            inner: Arc::new(event_bus),
            subscriptions: EventSubscriptionRegistry::default(),
        }
    }

    pub fn bus(&self) -> ServiceEventBus {
        ServiceEventBus::new(self.inner.clone(), self.subscriptions.clone())
    }

    pub(crate) async fn cancel_owned_subscriptions(&self) -> usize {
        self.subscriptions.cancel_all().await
    }
}

impl std::fmt::Debug for ClusterEventBusComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterEventBusComponent")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct LocalEventBusComponent {
    inner: Arc<dyn DynEventBus>,
    subscriptions: EventSubscriptionRegistry,
}

impl LocalEventBusComponent {
    pub fn new<T>(event_bus: T) -> Self
    where
        T: EventBus,
    {
        Self {
            inner: Arc::new(event_bus),
            subscriptions: EventSubscriptionRegistry::default(),
        }
    }

    pub fn bus(&self) -> ServiceEventBus {
        ServiceEventBus::new(self.inner.clone(), self.subscriptions.clone())
    }

    pub(crate) async fn cancel_owned_subscriptions(&self) -> usize {
        self.subscriptions.cancel_all().await
    }
}

impl std::fmt::Debug for LocalEventBusComponent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalEventBusComponent")
            .finish_non_exhaustive()
    }
}
