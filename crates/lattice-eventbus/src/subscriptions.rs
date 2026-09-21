use std::{collections::HashMap, mem::take, time::Duration};

use tokio::time::Instant;

use crate::local::EventSubscriptionHandle;

/// Lifecycle-owned EventBus subscriptions for one Actor activation.
///
/// Logical subscriptions share their EventBus backend connection, but their handles follow the
/// Actor activation that installed them. Actors should install subscriptions from `started`, use
/// `replace` when a business identity such as alliance membership changes, and await `shutdown`
/// from `stopping`.
#[derive(Debug, Default)]
pub struct ActorSubscriptions {
    handles: HashMap<String, EventSubscriptionHandle>,
    retiring: Vec<EventSubscriptionHandle>,
}

impl ActorSubscriptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts both installed subscriptions and retired subscriptions still draining.
    pub fn len(&self) -> usize {
        self.handles.len() + self.retiring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty() && self.retiring.is_empty()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.handles.contains_key(key)
    }

    /// Installs `handle` before shutting down the previous subscription under `key`.
    ///
    /// Installing first avoids a delivery gap when an Actor changes a dynamic subscription. The
    /// Actor message should still validate the current business identity so an in-flight event from
    /// the replaced subscription is harmless.
    pub async fn replace(
        &mut self,
        key: impl Into<String>,
        handle: EventSubscriptionHandle,
        deadline: Duration,
    ) -> bool {
        if let Some(previous) = self.handles.insert(key.into(), handle) {
            self.retiring.push(previous);
        }
        self.drain_retiring(deadline).await
    }

    pub async fn cancel(&mut self, key: &str, deadline: Duration) -> bool {
        if let Some(handle) = self.handles.remove(key) {
            self.retiring.push(handle);
        }
        self.drain_retiring(deadline).await
    }

    /// Cancels every subscription and waits at most `deadline` in total for their work to finish.
    /// Timed-out subscriptions remain owned here so later calls can finish draining them.
    pub async fn shutdown(&mut self, deadline: Duration) -> bool {
        self.retiring.extend(take(&mut self.handles).into_values());
        self.drain_retiring(deadline).await
    }

    async fn drain_retiring(&mut self, deadline: Duration) -> bool {
        let started = Instant::now();
        for handle in &self.retiring {
            handle.cancel();
        }
        let mut index = 0;
        while index < self.retiring.len() {
            let remaining = deadline.saturating_sub(started.elapsed());
            if self.retiring[index].shutdown(remaining).await {
                self.retiring.swap_remove(index);
            } else {
                index += 1;
            }
        }
        self.retiring.is_empty()
    }
}

impl Drop for ActorSubscriptions {
    fn drop(&mut self) {
        for handle in self.handles.values().chain(self.retiring.iter()) {
            handle.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        local::{EventBus, LocalEventBus},
        types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter},
    };
    use lattice_model::{service::ServiceInstanceId, service_name, trace::TraceContext};

    fn event(subject: &str) -> EventEnvelope {
        EventEnvelope {
            event_id: EventId::new(format!("event-{subject}")),
            subject: Subject::new(subject),
            event_type: "test".to_owned(),
            source_service: service_name!("Test"),
            source_instance: ServiceInstanceId::new("test-a"),
            recipient: None,
            correlation_id: None,
            trace: TraceContext::default(),
            occurred_unix_ms: 1,
            payload: Vec::new(),
        }
    }

    async fn subscription(bus: &LocalEventBus, subject: &str) -> EventSubscriptionHandle {
        bus.subscribe(
            EventSubscription::local(SubjectFilter::new(subject)),
            |_event: EventEnvelope| async { Ok(()) },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn replace_installs_new_subscription_and_stops_previous_one() {
        let bus = LocalEventBus::new();
        let mut subscriptions = ActorSubscriptions::new();
        subscriptions
            .replace(
                "alliance",
                subscription(&bus, "alliance.1").await,
                Duration::from_secs(1),
            )
            .await;
        assert_eq!(bus.subscription_count().await, 1);

        assert!(
            subscriptions
                .replace(
                    "alliance",
                    subscription(&bus, "alliance.2").await,
                    Duration::from_secs(1),
                )
                .await
        );
        tokio::task::yield_now().await;
        assert_eq!(bus.subscription_count().await, 1);
        bus.publish(event("alliance.2")).await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_every_owned_subscription() {
        let bus = LocalEventBus::new();
        let mut subscriptions = ActorSubscriptions::new();
        for (key, subject) in [("world", "world.1"), ("player", "player.7")] {
            subscriptions
                .replace(
                    key,
                    subscription(&bus, subject).await,
                    Duration::from_secs(1),
                )
                .await;
        }
        assert_eq!(subscriptions.len(), 2);
        assert_eq!(bus.subscription_count().await, 2);

        assert!(subscriptions.shutdown(Duration::from_secs(1)).await);
        tokio::task::yield_now().await;
        assert!(subscriptions.is_empty());
        assert_eq!(bus.subscription_count().await, 0);
    }

    #[tokio::test]
    async fn drop_cancels_subscriptions_as_a_non_blocking_backstop() {
        let bus = LocalEventBus::new();
        let mut subscriptions = ActorSubscriptions::new();
        subscriptions
            .replace(
                "world",
                subscription(&bus, "world.1").await,
                Duration::from_secs(1),
            )
            .await;
        drop(subscriptions);
        tokio::task::yield_now().await;

        assert_eq!(bus.subscription_count().await, 0);
    }
}
