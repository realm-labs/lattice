use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_nats::jetstream;
use async_trait::async_trait;
use futures_util::StreamExt;
use lattice_core::service_context::ConfiguredComponent;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{Instrument, warn};

use crate::error::EventBusError;
use crate::local::{EventBus, EventHandler, EventSubscriptionHandle};
use crate::types::{EventEnvelope, EventId, EventSubscription};

#[derive(Debug, Clone)]
pub struct NatsEventBus {
    client: async_nats::Client,
    config: NatsEventBusConfig,
}

impl NatsEventBus {
    pub async fn connect(config: NatsEventBusConfig) -> Result<Self, EventBusError> {
        let client = async_nats::connect(config.endpoint.clone())
            .await
            .map_err(|error| EventBusError::Backend {
                reason: error.to_string(),
            })?;
        Ok(Self { client, config })
    }

    pub fn from_config() -> ConfiguredComponent<Self> {
        ConfiguredComponent::from_section("event_bus", |config| async move {
            Self::connect(config).await
        })
    }

    pub fn from_client(client: async_nats::Client, config: NatsEventBusConfig) -> Self {
        Self { client, config }
    }

    pub fn config(&self) -> &NatsEventBusConfig {
        &self.config
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NatsEventBusConfig {
    pub endpoint: String,
    pub stream: String,
    #[serde(default)]
    pub durable_prefix: String,
}

#[async_trait]
impl EventBus for NatsEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        let subject = event.subject.as_str().to_string();
        let payload =
            serde_json::to_vec(&event).map_err(|error| EventBusError::EncodeEnvelope {
                reason: error.to_string(),
            })?;
        let jetstream = jetstream::new(self.client.clone());
        ensure_stream(&jetstream, &self.config).await?;
        jetstream
            .publish(subject, payload.into())
            .await
            .map_err(|error| EventBusError::Backend {
                reason: error.to_string(),
            })?
            .await
            .map_err(|error| EventBusError::Backend {
                reason: error.to_string(),
            })?;
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
        let id = NATS_SUBSCRIPTION_ID.fetch_add(1, Ordering::SeqCst);
        let cancelled = Arc::new(AtomicBool::new(false));
        let handler = Arc::new(handler);
        if let Some(durable_name) = &subscription.durable_name {
            let jetstream = jetstream::new(self.client.clone());
            let stream = ensure_stream(&jetstream, &self.config).await?;
            let consumer_name = durable_consumer_name(&self.config, durable_name);
            let consumer = stream
                .get_or_create_consumer(
                    &consumer_name,
                    jetstream::consumer::pull::Config {
                        durable_name: Some(consumer_name.clone()),
                        filter_subject: subscription.filter.as_str().to_string(),
                        ack_policy: jetstream::consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| EventBusError::Backend {
                    reason: error.to_string(),
                })?;
            let mut messages =
                consumer
                    .messages()
                    .await
                    .map_err(|error| EventBusError::Backend {
                        reason: error.to_string(),
                    })?;

            let cancelled_task = cancelled.clone();
            let subject_filter = subscription.filter.clone();
            tokio::spawn(
                async move {
                    while !cancelled_task.load(Ordering::SeqCst) {
                        let Some(message) = messages.next().await else {
                            break;
                        };
                        let Ok(message) = message else {
                            continue;
                        };
                        if cancelled_task.load(Ordering::SeqCst) {
                            break;
                        }
                        let event: EventEnvelope =
                            match serde_json::from_slice(message.payload.as_ref()) {
                                Ok(event) => event,
                                Err(error) => {
                                    warn!(
                                        error = %error,
                                        subject = %message.subject,
                                        "failed to decode NATS event envelope"
                                    );
                                    let _ = message.ack().await;
                                    continue;
                                }
                            };
                        if !subject_filter.matches(&event.subject) {
                            let _ = message.ack().await;
                            continue;
                        }
                        match handler.handle(event).await {
                            Ok(()) => {
                                let _ = message.ack().await;
                            }
                            Err(error) => {
                                warn!(%error, "NATS event handler failed");
                            }
                        }
                    }
                }
                .instrument(tracing::info_span!("eventbus.nats.durable_subscription")),
            );
        } else {
            let mut subscriber = self
                .client
                .subscribe(subscription.filter.as_str().to_string())
                .await
                .map_err(|error| EventBusError::Backend {
                    reason: error.to_string(),
                })?;

            let cancelled_task = cancelled.clone();
            let subject_filter = subscription.filter.clone();
            tokio::spawn(
                async move {
                    while !cancelled_task.load(Ordering::SeqCst) {
                        let Some(message) = subscriber.next().await else {
                            break;
                        };
                        if cancelled_task.load(Ordering::SeqCst) {
                            break;
                        }
                        let event: EventEnvelope =
                            match serde_json::from_slice(message.payload.as_ref()) {
                                Ok(event) => event,
                                Err(error) => {
                                    warn!(
                                        error = %error,
                                        subject = %message.subject,
                                        "failed to decode NATS event envelope"
                                    );
                                    continue;
                                }
                            };
                        if !subject_filter.matches(&event.subject) {
                            continue;
                        }
                        if let Err(error) = handler.handle(event).await {
                            warn!(%error, "NATS event handler failed");
                        }
                    }
                }
                .instrument(tracing::info_span!("eventbus.nats.subscription")),
            );
        }

        Ok(EventSubscriptionHandle::new(id, cancelled))
    }
}

async fn ensure_stream(
    jetstream: &jetstream::Context,
    config: &NatsEventBusConfig,
) -> Result<jetstream::stream::Stream, EventBusError> {
    jetstream
        .get_or_create_stream(jetstream::stream::Config {
            name: config.stream.clone(),
            subjects: vec![">".to_string()],
            ..Default::default()
        })
        .await
        .map_err(|error| EventBusError::Backend {
            reason: error.to_string(),
        })
}

fn durable_consumer_name(config: &NatsEventBusConfig, durable_name: &str) -> String {
    if config.durable_prefix.is_empty() {
        durable_name.to_string()
    } else {
        format!("{}-{durable_name}", config.durable_prefix)
    }
}

#[cfg(test)]
fn durable_queue_group(config: &NatsEventBusConfig, durable_name: &str) -> String {
    if config.durable_prefix.is_empty() {
        durable_name.to_string()
    } else {
        format!("{}-{durable_name}", config.durable_prefix)
    }
}

static NATS_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct InMemoryNatsEventBus {
    client: InMemoryNatsClient,
    config: Option<NatsEventBusConfig>,
}

impl InMemoryNatsEventBus {
    pub fn new(client: InMemoryNatsClient) -> Self {
        Self {
            client,
            config: None,
        }
    }

    pub fn from_options(config: NatsEventBusConfig) -> Self {
        Self {
            client: InMemoryNatsClient::new(),
            config: Some(config),
        }
    }

    pub fn config(&self) -> Option<&NatsEventBusConfig> {
        self.config.as_ref()
    }
}

impl Default for InMemoryNatsEventBus {
    fn default() -> Self {
        Self::new(InMemoryNatsClient::new())
    }
}

#[derive(Debug, Clone)]
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

impl fmt::Debug for NatsInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsInner")
            .field("next_id", &self.next_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct NatsSubscriber {
    subscription: EventSubscription,
    handler: Arc<dyn EventHandler>,
    cancelled: Arc<AtomicBool>,
}

#[async_trait]
impl EventBus for InMemoryNatsEventBus {
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

        Ok(EventSubscriptionHandle::new(id, cancelled))
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

    use lattice_core::instance::InstanceId;
    use lattice_core::service_kind;
    use lattice_core::trace::TraceContext;
    use tokio::sync::Mutex;

    use super::*;
    use crate::types::{EventEnvelope, EventId, Subject, SubjectFilter};

    #[tokio::test]
    async fn in_memory_nats_subscriber_replays_unseen_stream_events() {
        let bus = InMemoryNatsEventBus::new(InMemoryNatsClient::new());
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
    async fn in_memory_nats_subscriber_is_idempotent_by_event_id() {
        let bus = InMemoryNatsEventBus::new(InMemoryNatsClient::new());
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

    #[tokio::test]
    async fn in_memory_nats_event_bus_builds_from_options() {
        let config = NatsEventBusConfig {
            endpoint: "nats://nats:4222".to_string(),
            stream: "lattice-events".to_string(),
            durable_prefix: "world".to_string(),
        };
        let bus = InMemoryNatsEventBus::from_options(config.clone());

        assert_eq!(bus.config(), Some(&config));
    }

    #[test]
    fn durable_queue_group_uses_configured_prefix() {
        let config = NatsEventBusConfig {
            endpoint: "nats://nats:4222".to_string(),
            stream: "lattice-events".to_string(),
            durable_prefix: "world".to_string(),
        };

        assert_eq!(
            durable_queue_group(&config, "cache"),
            "world-cache".to_string()
        );
    }

    fn test_event(event_id: &str) -> EventEnvelope {
        EventEnvelope {
            event_id: EventId::new(event_id),
            subject: Subject::new("game.world.player_entered"),
            event_type: "PlayerEntered".to_string(),
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
