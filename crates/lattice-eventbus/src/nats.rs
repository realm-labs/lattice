use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_nats::{HeaderMap, jetstream};
use async_trait::async_trait;
use futures_util::StreamExt;
use jetstream::{
    AckKind,
    consumer::{AckPolicy, pull::Config as PullConfig},
    message::PublishMessage,
    stream::{Config as StreamConfig, RetentionPolicy, Stream},
};
use lattice_core::service_context::ConfiguredComponent;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OnceCell};
use tracing::{Instrument, warn};

use crate::{
    error::EventBusError,
    local::{EventBus, EventHandler, EventSubscriptionHandle, SubscriptionState},
    types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter},
};

#[derive(Debug, Clone)]
pub struct NatsEventBus {
    client: async_nats::Client,
    jetstream: jetstream::Context,
    stream: Arc<OnceCell<Stream>>,
    subscriptions: Arc<Mutex<HashMap<u64, Arc<SubscriptionState>>>>,
    metrics: Arc<NatsEventBusMetrics>,
    config: NatsEventBusConfig,
}

impl NatsEventBus {
    pub async fn connect(config: NatsEventBusConfig) -> Result<Self, EventBusError> {
        config.validate()?;
        let client = async_nats::connect(config.endpoint.clone())
            .await
            .map_err(|error| EventBusError::Backend {
                reason: error.to_string(),
            })?;
        let bus = Self::from_client(client, config);
        bus.stream().await?;
        Ok(bus)
    }

    pub fn from_config() -> ConfiguredComponent<Self> {
        ConfiguredComponent::from_section("event_bus", |config| async move {
            Self::connect(config).await
        })
    }

    pub fn from_client(client: async_nats::Client, config: NatsEventBusConfig) -> Self {
        Self {
            jetstream: jetstream::new(client.clone()),
            client,
            stream: Arc::new(OnceCell::new()),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(NatsEventBusMetrics::default()),
            config,
        }
    }

    pub fn config(&self) -> &NatsEventBusConfig {
        &self.config
    }

    pub fn stats(&self) -> NatsEventBusStats {
        self.metrics.snapshot()
    }

    async fn stream(&self) -> Result<&Stream, EventBusError> {
        self.stream
            .get_or_try_init(|| async {
                self.config.validate()?;
                self.jetstream
                    .get_or_create_stream(self.config.stream_config())
                    .await
                    .map_err(|error| EventBusError::Backend {
                        reason: error.to_string(),
                    })
            })
            .await
    }

    async fn track(&self, id: u64, state: &Arc<SubscriptionState>) {
        self.subscriptions.lock().await.insert(id, state.clone());

        let subscriptions = Arc::downgrade(&self.subscriptions);
        let mut cancellation = state.cancellation();
        tokio::spawn(async move {
            cancellation.cancelled().await;
            if let Some(subscriptions) = Weak::upgrade(&subscriptions) {
                subscriptions.lock().await.remove(&id);
            }
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NatsEventBusConfig {
    /// NATS endpoint the bus connects to.
    pub endpoint: String,
    /// JetStream stream backing durable subscriptions.
    pub stream: String,
    /// Prefix applied to durable consumer names so several deployments can share one stream.
    #[serde(default)]
    pub durable_prefix: String,
    /// Subjects captured by the backing stream. JetStream refuses to create a second stream whose
    /// subjects overlap, so every cluster sharing a NATS server needs its own prefix, for example
    /// `lattice.<cluster>.>`.
    #[serde(default = "default_subjects")]
    pub subjects: Vec<String>,
    /// Retention policy of the backing stream.
    #[serde(default)]
    pub retention: RetentionPolicy,
    /// Byte ceiling of the backing stream before old messages are discarded. Non-positive means
    /// unbounded, which is only accepted together with a `max_age_secs` bound.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: i64,
    /// Age ceiling of stored messages in seconds. Zero means unbounded, which is only accepted
    /// together with a `max_bytes` bound.
    #[serde(default = "default_max_age_secs")]
    pub max_age_secs: u64,
    /// Window in which JetStream collapses republished events carrying the same event id.
    #[serde(default = "default_duplicate_window_secs")]
    pub duplicate_window_secs: u64,
    /// How long a durable consumer may hold a message before JetStream redelivers it. Handlers
    /// that run longer keep the message alive by sending in-progress acknowledgements.
    #[serde(default = "default_ack_wait_secs")]
    pub ack_wait_secs: u64,
    /// Delivery attempts before an event is dead lettered instead of redelivered.
    #[serde(default = "default_max_deliver")]
    pub max_deliver: i64,
    /// Delay in seconds applied before each redelivery attempt. The last entry is reused once the
    /// attempt count exceeds the list.
    #[serde(default = "default_redelivery_backoff_secs")]
    pub redelivery_backoff_secs: Vec<u64>,
    /// Subject receiving events the consumer gave up on. It must not be captured by `subjects`,
    /// otherwise dead letters are redelivered through the same consumer.
    #[serde(default)]
    pub dead_letter_subject: Option<String>,
}

impl NatsEventBusConfig {
    pub fn new(endpoint: impl Into<String>, stream: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            stream: stream.into(),
            durable_prefix: String::new(),
            subjects: default_subjects(),
            retention: RetentionPolicy::default(),
            max_bytes: default_max_bytes(),
            max_age_secs: default_max_age_secs(),
            duplicate_window_secs: default_duplicate_window_secs(),
            ack_wait_secs: default_ack_wait_secs(),
            max_deliver: default_max_deliver(),
            redelivery_backoff_secs: default_redelivery_backoff_secs(),
            dead_letter_subject: None,
        }
    }

    pub fn with_durable_prefix(mut self, durable_prefix: impl Into<String>) -> Self {
        self.durable_prefix = durable_prefix.into();
        self
    }

    pub fn with_subjects(mut self, subjects: Vec<String>) -> Self {
        self.subjects = subjects;
        self
    }

    pub fn with_dead_letter_subject(mut self, subject: impl Into<String>) -> Self {
        self.dead_letter_subject = Some(subject.into());
        self
    }

    pub fn validate(&self) -> Result<(), EventBusError> {
        if self.stream.trim().is_empty() {
            return Err(EventBusError::Config {
                reason: "stream name must not be empty".to_string(),
            });
        }
        if self.subjects.is_empty() {
            return Err(EventBusError::Config {
                reason: "stream must capture at least one subject".to_string(),
            });
        }
        for subject in &self.subjects {
            if subject.trim() == ">" {
                return Err(EventBusError::Config {
                    reason: format!(
                        "stream {} would capture every subject on the server; scope it with a cluster prefix such as `lattice.<cluster>.>`",
                        self.stream
                    ),
                });
            }
        }
        if self.max_bytes <= 0 && self.max_age_secs == 0 {
            return Err(EventBusError::Config {
                reason: format!(
                    "stream {} needs a max_bytes or max_age_secs bound to keep storage finite",
                    self.stream
                ),
            });
        }
        if self.ack_wait_secs == 0 {
            return Err(EventBusError::Config {
                reason: "ack_wait_secs must be nonzero".to_string(),
            });
        }
        if let Some(dead_letter_subject) = &self.dead_letter_subject {
            let dead_letter = Subject::new(dead_letter_subject.clone());
            if self
                .subjects
                .iter()
                .any(|subject| SubjectFilter::new(subject.clone()).matches(&dead_letter))
            {
                return Err(EventBusError::Config {
                    reason: format!(
                        "dead letter subject {dead_letter_subject} is captured by the stream and would be redelivered"
                    ),
                });
            }
        }
        Ok(())
    }

    fn stream_config(&self) -> StreamConfig {
        StreamConfig {
            name: self.stream.clone(),
            subjects: self.subjects.clone(),
            retention: self.retention,
            max_bytes: self.max_bytes,
            max_age: Duration::from_secs(self.max_age_secs),
            duplicate_window: Duration::from_secs(self.duplicate_window_secs),
            ..Default::default()
        }
    }

    fn consumer_config(&self, consumer_name: String, filter: &SubjectFilter) -> PullConfig {
        PullConfig {
            durable_name: Some(consumer_name.clone()),
            name: Some(consumer_name),
            filter_subject: filter.as_str().to_string(),
            ack_policy: AckPolicy::Explicit,
            ack_wait: self.ack_wait(),
            max_deliver: self.max_deliver,
            backoff: self
                .redelivery_backoff_secs
                .iter()
                .map(|seconds| Duration::from_secs(*seconds))
                .collect(),
            ..Default::default()
        }
    }

    fn ack_wait(&self) -> Duration {
        Duration::from_secs(self.ack_wait_secs)
    }

    fn redelivery_delay(&self, delivered: i64) -> Duration {
        if self.redelivery_backoff_secs.is_empty() {
            return self.ack_wait();
        }
        let attempt = delivered.max(1) as usize - 1;
        let index = attempt.min(self.redelivery_backoff_secs.len() - 1);
        Duration::from_secs(self.redelivery_backoff_secs[index])
    }

    fn exhausted_deliveries(&self, delivered: i64) -> bool {
        self.max_deliver > 0 && delivered >= self.max_deliver
    }
}

fn default_subjects() -> Vec<String> {
    vec!["lattice.>".to_string()]
}

fn default_max_bytes() -> i64 {
    1024 * 1024 * 1024
}

fn default_max_age_secs() -> u64 {
    7 * 24 * 60 * 60
}

fn default_duplicate_window_secs() -> u64 {
    120
}

fn default_ack_wait_secs() -> u64 {
    30
}

fn default_max_deliver() -> i64 {
    5
}

fn default_redelivery_backoff_secs() -> Vec<u64> {
    vec![1, 5, 15, 60]
}

#[async_trait]
impl EventBus for NatsEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        let subject = event.subject.as_str().to_string();
        let message_id = event.event_id.as_str().to_string();
        let payload =
            serde_json::to_vec(&event).map_err(|error| EventBusError::EncodeEnvelope {
                reason: error.to_string(),
            })?;
        self.jetstream
            .send_publish(
                subject,
                PublishMessage::build()
                    .payload(payload.into())
                    .message_id(message_id),
            )
            .await
            .map_err(|error| EventBusError::Backend {
                reason: error.to_string(),
            })?
            .await
            .map_err(|error| EventBusError::Backend {
                reason: error.to_string(),
            })?;
        self.metrics.published.fetch_add(1, Ordering::Relaxed);
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
        let state = SubscriptionState::new();
        let handler = Arc::new(handler);
        let filter = subscription.filter.clone();
        let mut cancellation = state.cancellation();

        if let Some(durable_name) = &subscription.durable_name {
            let stream = self.stream().await?;
            let consumer_name = durable_consumer_name(&self.config, durable_name);
            let consumer = stream
                .get_or_create_consumer(
                    &consumer_name,
                    self.config.consumer_config(consumer_name.clone(), &filter),
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

            let config = self.config.clone();
            let jetstream = self.jetstream.clone();
            let metrics = self.metrics.clone();
            state.attach(tokio::spawn(
                async move {
                    loop {
                        let message = tokio::select! {
                            biased;
                            () = cancellation.cancelled() => break,
                            message = messages.next() => message,
                        };
                        let Some(message) = message else {
                            break;
                        };
                        match message {
                            Ok(message) => {
                                consume_durable_message(
                                    handler.as_ref(),
                                    &filter,
                                    &jetstream,
                                    &config,
                                    &metrics,
                                    message,
                                )
                                .await;
                            }
                            Err(error) => {
                                warn!(%error, consumer = %consumer_name, "NATS durable stream failed");
                            }
                        }
                    }
                }
                .instrument(tracing::info_span!("eventbus.nats.durable_subscription")),
            ));
        } else {
            let mut subscriber = self
                .client
                .subscribe(filter.as_str().to_string())
                .await
                .map_err(|error| EventBusError::Backend {
                    reason: error.to_string(),
                })?;

            let metrics = self.metrics.clone();
            state.attach(tokio::spawn(
                async move {
                    loop {
                        let message = tokio::select! {
                            biased;
                            () = cancellation.cancelled() => break,
                            message = subscriber.next() => message,
                        };
                        let Some(message) = message else {
                            break;
                        };
                        consume_core_message(handler.as_ref(), &filter, &metrics, message).await;
                    }
                }
                .instrument(tracing::info_span!("eventbus.nats.subscription")),
            ));
        }

        self.track(id, &state).await;
        Ok(EventSubscriptionHandle::new(id, state))
    }

    async fn shutdown(&self, deadline: Duration) -> bool {
        let states = std::mem::take(&mut *self.subscriptions.lock().await);
        let started = Instant::now();
        let mut drained = true;
        for state in states.into_values() {
            let remaining = deadline.saturating_sub(started.elapsed());
            drained &= state.shutdown(remaining).await;
        }
        drained
    }
}

async fn consume_durable_message(
    handler: &dyn EventHandler,
    filter: &SubjectFilter,
    jetstream: &jetstream::Context,
    config: &NatsEventBusConfig,
    metrics: &NatsEventBusMetrics,
    message: jetstream::Message,
) {
    let event = match serde_json::from_slice::<EventEnvelope>(message.payload.as_ref()) {
        Ok(event) => event,
        Err(error) => {
            metrics.decode_failures.fetch_add(1, Ordering::Relaxed);
            warn!(
                error = %error,
                subject = %message.subject,
                "failed to decode NATS event envelope"
            );
            dead_letter(jetstream, config, &message, "envelope decode failed").await;
            let _ = message.ack().await;
            return;
        }
    };
    if !filter.matches(&event.subject) {
        let _ = message.ack().await;
        return;
    }

    let event_id = event.event_id.clone();
    let Err(error) = handle_with_progress(handler, &message, event, config.ack_wait()).await else {
        metrics.delivered.fetch_add(1, Ordering::Relaxed);
        let _ = message.ack().await;
        return;
    };

    metrics.handler_failures.fetch_add(1, Ordering::Relaxed);
    let delivered = message.info().map(|info| info.delivered).unwrap_or(1);
    if config.exhausted_deliveries(delivered) {
        metrics.dead_lettered.fetch_add(1, Ordering::Relaxed);
        warn!(
            %error,
            event_id = event_id.as_str(),
            delivered,
            max_deliver = config.max_deliver,
            "NATS event handler exhausted delivery attempts"
        );
        dead_letter(
            jetstream,
            config,
            &message,
            "handler exhausted delivery attempts",
        )
        .await;
        let _ = message.ack_with(AckKind::Term).await;
        return;
    }

    metrics.redeliveries.fetch_add(1, Ordering::Relaxed);
    let delay = config.redelivery_delay(delivered);
    warn!(
        %error,
        event_id = event_id.as_str(),
        delivered,
        delay_millis = delay.as_millis() as u64,
        "NATS event handler failed, scheduling redelivery"
    );
    let _ = message.ack_with(AckKind::Nak(Some(delay))).await;
}

async fn consume_core_message(
    handler: &dyn EventHandler,
    filter: &SubjectFilter,
    metrics: &NatsEventBusMetrics,
    message: async_nats::Message,
) {
    let event = match serde_json::from_slice::<EventEnvelope>(message.payload.as_ref()) {
        Ok(event) => event,
        Err(error) => {
            metrics.decode_failures.fetch_add(1, Ordering::Relaxed);
            warn!(
                error = %error,
                subject = %message.subject,
                "failed to decode NATS event envelope"
            );
            return;
        }
    };
    if !filter.matches(&event.subject) {
        return;
    }
    match handler.handle(event).await {
        Ok(()) => {
            metrics.delivered.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            metrics.handler_failures.fetch_add(1, Ordering::Relaxed);
            warn!(%error, "NATS event handler failed");
        }
    }
}

async fn handle_with_progress(
    handler: &dyn EventHandler,
    message: &jetstream::Message,
    event: EventEnvelope,
    ack_wait: Duration,
) -> Result<(), EventBusError> {
    let mut progress = tokio::time::interval(progress_interval(ack_wait));
    progress.tick().await;
    let mut handled = handler.handle(event);
    loop {
        tokio::select! {
            outcome = &mut handled => return outcome,
            _ = progress.tick() => {
                let _ = message.ack_with(AckKind::Progress).await;
            }
        }
    }
}

fn progress_interval(ack_wait: Duration) -> Duration {
    (ack_wait / 2).max(Duration::from_secs(1))
}

async fn dead_letter(
    jetstream: &jetstream::Context,
    config: &NatsEventBusConfig,
    message: &jetstream::Message,
    reason: &str,
) {
    let Some(subject) = &config.dead_letter_subject else {
        return;
    };
    let mut headers = HeaderMap::new();
    headers.insert(DEAD_LETTER_REASON_HEADER, reason);
    headers.insert(DEAD_LETTER_SUBJECT_HEADER, message.subject.as_str());
    if let Err(error) = jetstream
        .publish_with_headers(subject.clone(), headers, message.payload.clone())
        .await
    {
        warn!(%error, subject, "failed to dead letter NATS event");
    }
}

const DEAD_LETTER_REASON_HEADER: &str = "Lattice-Dead-Letter-Reason";
const DEAD_LETTER_SUBJECT_HEADER: &str = "Lattice-Dead-Letter-Subject";

fn durable_consumer_name(config: &NatsEventBusConfig, durable_name: &str) -> String {
    if config.durable_prefix.is_empty() {
        durable_name.to_string()
    } else {
        format!("{}-{durable_name}", config.durable_prefix)
    }
}

static NATS_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Default)]
struct NatsEventBusMetrics {
    published: AtomicU64,
    delivered: AtomicU64,
    handler_failures: AtomicU64,
    redeliveries: AtomicU64,
    dead_lettered: AtomicU64,
    decode_failures: AtomicU64,
}

impl NatsEventBusMetrics {
    fn snapshot(&self) -> NatsEventBusStats {
        NatsEventBusStats {
            published: self.published.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            handler_failures: self.handler_failures.load(Ordering::Relaxed),
            redeliveries: self.redeliveries.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
            decode_failures: self.decode_failures.load(Ordering::Relaxed),
        }
    }
}

/// Delivery counters of a single `NatsEventBus` handle tree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NatsEventBusStats {
    pub published: u64,
    pub delivered: u64,
    pub handler_failures: u64,
    pub redeliveries: u64,
    pub dead_lettered: u64,
    pub decode_failures: u64,
}

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
                duplicate_window: Mutex::new(HashSet::new()),
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
    duplicate_window: Mutex<HashSet<EventId>>,
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
    state: Arc<SubscriptionState>,
}

#[async_trait]
impl EventBus for InMemoryNatsEventBus {
    async fn publish(&self, event: EventEnvelope) -> Result<(), EventBusError> {
        let stored = self
            .client
            .inner
            .duplicate_window
            .lock()
            .await
            .insert(event.event_id.clone());
        if stored {
            self.client.inner.stream.lock().await.push(event.clone());
        }

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
            if subscriber.subscription.durable_name.is_some() && !stored {
                continue;
            }
            deliver(&subscriber, event.clone()).await;
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
        let state = SubscriptionState::new();
        let subscriber = NatsSubscriber {
            subscription,
            handler: Arc::new(handler),
            state: state.clone(),
        };
        self.client
            .inner
            .subscribers
            .lock()
            .await
            .insert(id, subscriber.clone());

        let inner = Arc::downgrade(&self.client.inner);
        let mut cancellation = state.cancellation();
        tokio::spawn(async move {
            cancellation.cancelled().await;
            if let Some(inner) = Weak::upgrade(&inner) {
                inner.subscribers.lock().await.remove(&id);
            }
        });

        if subscriber.subscription.durable_name.is_some() {
            let replay = self.client.inner.stream.lock().await.clone();
            for event in replay {
                deliver(&subscriber, event).await;
            }
        }

        Ok(EventSubscriptionHandle::new(id, state))
    }

    async fn shutdown(&self, _deadline: Duration) -> bool {
        let subscribers = std::mem::take(&mut *self.client.inner.subscribers.lock().await);
        for subscriber in subscribers.values() {
            subscriber.state.cancel();
        }
        true
    }
}

async fn deliver(subscriber: &NatsSubscriber, event: EventEnvelope) {
    if subscriber.state.is_cancelled() || !subscriber.subscription.filter.matches(&event.subject) {
        return;
    }
    if let Err(error) = subscriber.handler.handle(event).await {
        warn!(%error, "in-memory NATS event handler failed");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lattice_core::{instance::InstanceId, service_kind, trace::TraceContext};
    use tokio::sync::Mutex;

    use super::*;
    use crate::types::{EventEnvelope, EventId, Subject, SubjectFilter};

    #[tokio::test]
    async fn in_memory_nats_subscriber_replays_unseen_stream_events() {
        let bus = InMemoryNatsEventBus::new(InMemoryNatsClient::new());
        bus.publish(test_event("event-1")).await.unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        let _subscription = bus
            .subscribe(
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
    async fn in_memory_nats_stream_collapses_republished_event_ids() {
        let bus = InMemoryNatsEventBus::new(InMemoryNatsClient::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let _subscription = bus
            .subscribe(
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
    async fn in_memory_nats_failing_handler_does_not_block_other_subscribers() {
        let bus = InMemoryNatsEventBus::new(InMemoryNatsClient::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let _failing = bus
            .subscribe(
                EventSubscription::local(SubjectFilter::new("game.world.>")),
                move |_: EventEnvelope| async move {
                    Err(EventBusError::Handler("boom".to_string()))
                },
            )
            .await
            .unwrap();
        let _healthy = bus
            .subscribe(
                EventSubscription::local(SubjectFilter::new("game.world.>")),
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

        assert_eq!(*seen.lock().await, vec!["event-1"]);
    }

    #[tokio::test]
    async fn in_memory_nats_event_bus_builds_from_options() {
        let config = NatsEventBusConfig::new("nats://nats:4222", "lattice-events")
            .with_durable_prefix("world");
        let bus = InMemoryNatsEventBus::from_options(config.clone());

        assert_eq!(bus.config(), Some(&config));
    }

    #[test]
    fn durable_consumer_name_uses_configured_prefix() {
        let config = NatsEventBusConfig::new("nats://nats:4222", "lattice-events")
            .with_durable_prefix("world");

        assert_eq!(
            durable_consumer_name(&config, "cache"),
            "world-cache".to_string()
        );
    }

    #[test]
    fn default_config_bounds_the_stream_and_scopes_its_subjects() {
        let config = NatsEventBusConfig::new("nats://nats:4222", "lattice-events");

        config.validate().unwrap();
        assert_eq!(config.subjects, vec!["lattice.>".to_string()]);
        let stream = config.stream_config();
        assert_eq!(stream.max_bytes, 1024 * 1024 * 1024);
        assert_eq!(stream.max_age, Duration::from_secs(7 * 24 * 60 * 60));
        assert_eq!(stream.duplicate_window, Duration::from_secs(120));
    }

    #[test]
    fn validate_rejects_a_stream_capturing_every_subject() {
        let config = NatsEventBusConfig::new("nats://nats:4222", "lattice-events")
            .with_subjects(vec![">".to_string()]);

        assert!(matches!(
            config.validate(),
            Err(EventBusError::Config { .. })
        ));
    }

    #[test]
    fn validate_rejects_an_unbounded_stream() {
        let mut config = NatsEventBusConfig::new("nats://nats:4222", "lattice-events");
        config.max_bytes = 0;
        config.max_age_secs = 0;

        assert!(matches!(
            config.validate(),
            Err(EventBusError::Config { .. })
        ));
    }

    #[test]
    fn validate_rejects_a_dead_letter_subject_inside_the_stream() {
        let config = NatsEventBusConfig::new("nats://nats:4222", "lattice-events")
            .with_dead_letter_subject("lattice.dead-letters");

        assert!(matches!(
            config.validate(),
            Err(EventBusError::Config { .. })
        ));
        assert!(
            NatsEventBusConfig::new("nats://nats:4222", "lattice-events")
                .with_dead_letter_subject("lattice-dead-letters")
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn redelivery_delay_walks_the_configured_backoff() {
        let config = NatsEventBusConfig::new("nats://nats:4222", "lattice-events");

        assert_eq!(config.redelivery_delay(1), Duration::from_secs(1));
        assert_eq!(config.redelivery_delay(2), Duration::from_secs(5));
        assert_eq!(config.redelivery_delay(4), Duration::from_secs(60));
        assert_eq!(config.redelivery_delay(9), Duration::from_secs(60));
        assert!(!config.exhausted_deliveries(4));
        assert!(config.exhausted_deliveries(5));
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
