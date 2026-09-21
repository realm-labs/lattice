use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_eventbus::{
    local::{EventBus, EventSubscriptionHandle, LocalEventBus},
    nats::InMemoryNatsEventBus,
    subscriptions::ActorSubscriptions,
    types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter},
};
use lattice_model::{service::ServiceInstanceId, service_name, trace::TraceContext};
use tokio::sync::Semaphore;

fn event(id: &str) -> EventEnvelope {
    EventEnvelope {
        event_id: EventId::new(id),
        subject: Subject::new("shutdown.test"),
        event_type: "test".to_owned(),
        source_service: service_name!("Test"),
        source_instance: ServiceInstanceId::new("test"),
        recipient: None,
        correlation_id: None,
        trace: TraceContext::default(),
        occurred_unix_ms: 1,
        payload: Vec::new(),
    }
}

async fn drain_contract<B: EventBus>(bus: B) {
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let handle = bus
        .subscribe(
            EventSubscription::local(SubjectFilter::new("shutdown.>")),
            {
                let entered = entered.clone();
                let release = release.clone();
                let completed = completed.clone();
                move |_event: EventEnvelope| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let completed = completed.clone();
                    async move {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap();
    let publishing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.publish(event("one")).await }
    });
    entered.acquire().await.unwrap().forget();

    assert!(
        !bus.shutdown(Duration::ZERO).await,
        "a live inline handler must prevent successful shutdown"
    );
    assert!(
        !handle.shutdown(Duration::ZERO).await,
        "subscription shutdown must also account for its live handler"
    );
    assert!(
        !bus.shutdown(Duration::ZERO).await,
        "a second shutdown must still see unfinished work"
    );
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert!(bus.publish(event("late")).await.is_err());
    assert!(
        bus.subscribe(
            EventSubscription::local(SubjectFilter::new("shutdown.>")),
            |_: EventEnvelope| async { Ok(()) }
        )
        .await
        .is_err()
    );

    release.add_permits(1);
    publishing.await.unwrap().unwrap();
    assert!(bus.shutdown(Duration::from_secs(1)).await);
    assert!(handle.shutdown(Duration::from_secs(1)).await);
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn local_shutdown_tracks_inline_handlers_across_deadline_and_retry() {
    drain_contract(LocalEventBus::new()).await;
}

#[tokio::test]
async fn in_memory_nats_shutdown_tracks_inline_handlers_across_deadline_and_retry() {
    drain_contract(InMemoryNatsEventBus::default()).await;
}

async fn cancelled_shutdown_remains_tracked<B: EventBus>(bus: B) {
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let _handle = bus
        .subscribe(
            EventSubscription::local(SubjectFilter::new("shutdown.>")),
            {
                let entered = entered.clone();
                let release = release.clone();
                move |_: EventEnvelope| {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap();
    let publishing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.publish(event("one")).await }
    });
    entered.acquire().await.unwrap().forget();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            bus.shutdown(Duration::from_secs(1))
        )
        .await
        .is_err()
    );
    assert!(!bus.shutdown(Duration::ZERO).await);
    release.add_permits(1);
    publishing.await.unwrap().unwrap();
    assert!(bus.shutdown(Duration::from_secs(1)).await);
}

#[tokio::test]
async fn local_cancelled_shutdown_can_be_retried() {
    cancelled_shutdown_remains_tracked(LocalEventBus::new()).await;
}

#[tokio::test]
async fn in_memory_nats_cancelled_shutdown_can_be_retried() {
    cancelled_shutdown_remains_tracked(InMemoryNatsEventBus::default()).await;
}

async fn blocking_subscription<B: EventBus>(
    bus: &B,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
) -> EventSubscriptionHandle {
    bus.subscribe(
        EventSubscription::local(SubjectFilter::new("shutdown.>")),
        move |_: EventEnvelope| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
                Ok(())
            }
        },
    )
    .await
    .unwrap()
}

async fn cancelled_publish_releases_activity<B: EventBus>(bus: B) {
    let entered = Arc::new(Semaphore::new(0));
    let handle = blocking_subscription(&bus, entered.clone(), Arc::new(Semaphore::new(0))).await;
    let publishing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.publish(event("one")).await }
    });
    entered.acquire().await.unwrap().forget();
    assert!(!handle.shutdown(Duration::ZERO).await);
    publishing.abort();
    assert!(publishing.await.unwrap_err().is_cancelled());
    assert!(handle.shutdown(Duration::from_secs(1)).await);
    assert!(bus.shutdown(Duration::from_secs(1)).await);
}

#[tokio::test]
async fn local_cancelled_publish_releases_activity() {
    cancelled_publish_releases_activity(LocalEventBus::new()).await;
}

#[tokio::test]
async fn in_memory_nats_cancelled_publish_releases_activity() {
    cancelled_publish_releases_activity(InMemoryNatsEventBus::default()).await;
}

async fn shutdown_stops_later_fanout_handlers<B: EventBus>(bus: B) {
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    // Either hash-map order is valid: shutdown must prevent the second handler from starting.
    let _first = blocking_subscription(&bus, entered.clone(), release.clone()).await;
    let _second = blocking_subscription(&bus, entered.clone(), release.clone()).await;
    let publishing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.publish(event("one")).await }
    });
    entered.acquire().await.unwrap().forget();
    assert!(!bus.shutdown(Duration::ZERO).await);
    release.add_permits(2);
    publishing.await.unwrap().unwrap();
    assert_eq!(
        entered.available_permits(),
        0,
        "a cancelled snapshot must not start another handler"
    );
    assert!(bus.shutdown(Duration::from_secs(1)).await);
}

#[tokio::test]
async fn local_shutdown_stops_later_fanout_handlers() {
    shutdown_stops_later_fanout_handlers(LocalEventBus::new()).await;
}

#[tokio::test]
async fn in_memory_nats_shutdown_stops_later_fanout_handlers() {
    shutdown_stops_later_fanout_handlers(InMemoryNatsEventBus::default()).await;
}

#[tokio::test]
async fn in_memory_nats_shutdown_tracks_durable_replay_during_subscribe() {
    let bus = InMemoryNatsEventBus::default();
    bus.publish(event("replay")).await.unwrap();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let subscribing = tokio::spawn({
        let bus = bus.clone();
        let entered = entered.clone();
        let release = release.clone();
        async move {
            bus.subscribe(
                EventSubscription::durable(SubjectFilter::new("shutdown.>"), "replay"),
                move |_: EventEnvelope| {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        Ok(())
                    }
                },
            )
            .await
        }
    });
    entered.acquire().await.unwrap().forget();
    assert!(!bus.shutdown(Duration::ZERO).await);
    release.add_permits(1);
    assert!(subscribing.await.unwrap().is_err());
    assert!(bus.shutdown(Duration::from_secs(1)).await);
}

async fn actor_retains_timed_out_subscription(replace: bool) {
    let bus = LocalEventBus::new();
    let mut subscriptions = ActorSubscriptions::new();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let handle = blocking_subscription(&bus, entered.clone(), release.clone()).await;
    assert!(
        subscriptions
            .replace("identity", handle, Duration::ZERO)
            .await
    );
    let publishing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.publish(event("one")).await }
    });
    entered.acquire().await.unwrap().forget();
    if replace {
        let new_handle = bus
            .subscribe(
                EventSubscription::local(SubjectFilter::new("different.>")),
                |_: EventEnvelope| async { Ok(()) },
            )
            .await
            .unwrap();
        assert!(
            !subscriptions
                .replace("identity", new_handle, Duration::ZERO)
                .await
        );
        assert!(subscriptions.contains("identity"));
    } else {
        assert!(!subscriptions.cancel("identity", Duration::ZERO).await);
        assert!(!subscriptions.contains("identity"));
    }
    assert!(
        !subscriptions.is_empty(),
        "timed-out handles must remain owned"
    );
    assert!(!subscriptions.shutdown(Duration::ZERO).await);
    release.add_permits(1);
    publishing.await.unwrap().unwrap();
    assert!(subscriptions.shutdown(Duration::from_secs(1)).await);
    assert!(subscriptions.is_empty());
    // Actor subscription shutdown leaves its shared backend usable.
    bus.publish(event("next")).await.unwrap();
}

#[tokio::test]
async fn actor_replace_retains_timed_out_subscription() {
    actor_retains_timed_out_subscription(true).await;
}

#[tokio::test]
async fn actor_cancel_retains_timed_out_subscription() {
    actor_retains_timed_out_subscription(false).await;
}

#[tokio::test]
async fn actor_cancelled_shutdown_keeps_all_handles_owned() {
    let bus = LocalEventBus::new();
    let mut subscriptions = ActorSubscriptions::new();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let handle = blocking_subscription(&bus, entered.clone(), release.clone()).await;
    subscriptions
        .replace("identity", handle, Duration::ZERO)
        .await;
    let publishing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.publish(event("one")).await }
    });
    entered.acquire().await.unwrap().forget();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            subscriptions.shutdown(Duration::from_secs(1)),
        )
        .await
        .is_err()
    );
    assert!(!subscriptions.shutdown(Duration::ZERO).await);
    release.add_permits(1);
    publishing.await.unwrap().unwrap();
    assert!(subscriptions.shutdown(Duration::from_secs(1)).await);
    assert!(subscriptions.is_empty());
}
