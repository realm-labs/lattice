use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    EventBus, EventBusError, EventEnvelope, EventHandler, EventId, EventSubscription,
    EventSubscriptionHandle,
};

#[derive(Clone)]
pub struct NatsEventBus {
    client: InMemoryNatsClient,
    config: Option<NatsEventBusConfig>,
}

impl NatsEventBus {
    pub fn new(client: InMemoryNatsClient) -> Self {
        Self {
            client,
            config: None,
        }
    }

    pub fn from_config(config: NatsEventBusConfig) -> Self {
        Self {
            client: InMemoryNatsClient::new(),
            config: Some(config),
        }
    }

    pub fn config(&self) -> Option<&NatsEventBusConfig> {
        self.config.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NatsEventBusConfig {
    pub endpoint: String,
    pub stream: String,
    pub durable_prefix: String,
}

#[derive(Clone)]
pub struct InMemoryNatsClient {
    inner: Arc<NatsInner>,
}

impl InMemoryNatsClient {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(NatsInner {
                next_id: AtomicU64::new(1),
                stream: Mutex::new(Vec::new()),
                subscribers: Mutex::new(HashMap::new()),
                durable_acks: Mutex::new(HashMap::new()),
            }),
        }
    }
}

impl Default for InMemoryNatsClient {
    fn default() -> Self {
        Self::new()
    }
}

struct NatsInner {
    next_id: AtomicU64,
    stream: Mutex<Vec<EventEnvelope>>,
    subscribers: Mutex<HashMap<u64, NatsSubscriber>>,
    durable_acks: Mutex<HashMap<String, HashSet<EventId>>>,
}

#[derive(Clone)]
struct NatsSubscriber {
    subscription: EventSubscription,
    handler: Arc<dyn EventHandler>,
    cancelled: Arc<AtomicBool>,
}

#[async_trait]
impl EventBus for NatsEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        self.client.inner.stream.lock().await.push(event.clone());
        let subscribers = self
            .client
            .inner
            .subscribers
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();

        for subscriber in subscribers {
            deliver_if_needed(&self.client, &subscriber, event.clone()).await?;
        }
        Ok(())
    }

    async fn subscribe<H>(
        &self,
        subscription: EventSubscription,
        handler: H,
    ) -> Result<EventSubscriptionHandle, EventBusError>
    where
        H: EventHandler,
    {
        let id = self.client.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let cancelled = Arc::new(AtomicBool::new(false));
        let subscriber = NatsSubscriber {
            subscription,
            handler: Arc::new(handler),
            cancelled: cancelled.clone(),
        };
        self.client
            .inner
            .subscribers
            .lock()
            .await
            .insert(id, subscriber.clone());

        let replay = self.client.inner.stream.lock().await.clone();
        for event in replay {
            deliver_if_needed(&self.client, &subscriber, event).await?;
        }

        Ok(EventSubscriptionHandle { id, cancelled })
    }
}

async fn deliver_if_needed(
    client: &InMemoryNatsClient,
    subscriber: &NatsSubscriber,
    event: EventEnvelope,
) -> Result<(), EventBusError> {
    if subscriber.cancelled.load(Ordering::SeqCst)
        || !subscriber.subscription.filter.matches(&event.subject)
    {
        return Ok(());
    }

    if let Some(durable_name) = &subscriber.subscription.durable_name {
        {
            let durable_acks = client.inner.durable_acks.lock().await;
            if durable_acks
                .get(durable_name)
                .is_some_and(|seen| seen.contains(&event.event_id))
            {
                return Ok(());
            }
        }

        subscriber.handler.handle(event.clone()).await?;
        client
            .inner
            .durable_acks
            .lock()
            .await
            .entry(durable_name.clone())
            .or_default()
            .insert(event.event_id);
        return Ok(());
    }

    subscriber.handler.handle(event).await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lattice_core::{InstanceId, TraceContext, service_kind};
    use tokio::sync::Mutex;

    use super::*;
    use crate::{EventEnvelope, Subject, SubjectFilter};

    #[tokio::test]
    async fn durable_nats_subscriber_replays_unseen_stream_events() {
        let bus = NatsEventBus::new(InMemoryNatsClient::new());
        bus.publish(test_event("event-1")).await.unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        bus.subscribe(
            EventSubscription::durable(SubjectFilter::new("game.world.*"), "world-consumer"),
            move |event: EventEnvelope| {
                let seen = seen_clone.clone();
                async move {
                    seen.lock().await.push(event.event_id.as_str().to_string());
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(*seen.lock().await, vec!["event-1"]);
    }

    #[tokio::test]
    async fn durable_nats_subscriber_is_idempotent_by_event_id() {
        let bus = NatsEventBus::new(InMemoryNatsClient::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        bus.subscribe(
            EventSubscription::durable(SubjectFilter::new("game.world.*"), "world-consumer"),
            move |event: EventEnvelope| {
                let seen = seen_clone.clone();
                async move {
                    seen.lock().await.push(event.event_id.as_str().to_string());
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        bus.publish(test_event("event-1")).await.unwrap();
        bus.publish(test_event("event-1")).await.unwrap();

        assert_eq!(*seen.lock().await, vec!["event-1"]);
    }

    #[test]
    fn nats_event_bus_builds_from_config() {
        let config = NatsEventBusConfig {
            endpoint: "nats://nats:4222".to_string(),
            stream: "lattice-events".to_string(),
            durable_prefix: "world".to_string(),
        };
        let bus = NatsEventBus::from_config(config.clone());

        assert_eq!(bus.config(), Some(&config));
    }

    fn test_event(event_id: &str) -> EventEnvelope {
        EventEnvelope {
            event_id: crate::EventId::new(event_id),
            subject: Subject::new("game.world.player_entered"),
            event_type: "PlayerEntered".to_string(),
            source_service: service_kind!("World"),
            source_instance: InstanceId::new("world-a"),
            actor_kind: None,
            actor_id: None,
            request_id: None,
            trace: TraceContext::default(),
            occurred_unix_ms: 1,
            payload: Vec::new(),
        }
    }
}
