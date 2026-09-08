use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_core::{instance::InstanceId, service_kind, trace::TraceContext};
use lattice_eventbus::{
    local::EventBus,
    nats::{NatsEventBus, NatsEventBusConfig},
    types::{EventEnvelope, EventId, EventSubscription, Subject, SubjectFilter},
};
use tokio::sync::Semaphore;

fn event(id: &str, subject: &str) -> EventEnvelope {
    EventEnvelope {
        event_id: EventId::new(id),
        subject: Subject::new(subject),
        event_type: "drain-contract".to_owned(),
        source_service: service_kind!("Test"),
        source_instance: InstanceId::new("nats-drain"),
        recipient: None,
        correlation_id: None,
        trace: TraceContext::default(),
        occurred_unix_ms: 1,
        payload: Vec::new(),
    }
}

#[tokio::test]
async fn real_nats_core_and_durable_handlers_obey_the_drain_contract() {
    let Ok(endpoint) = std::env::var("LATTICE_NATS_ENDPOINT") else {
        eprintln!("LATTICE_NATS_ENDPOINT is absent; real NATS acceptance was not run");
        return;
    };
    let client = async_nats::connect(&endpoint).await.unwrap();
    let jetstream = async_nats::jetstream::new(client);
    let run = uuid::Uuid::new_v4().simple().to_string();
    for durable in [false, true] {
        for cancel_waiter in [false, true] {
            let name = format!("drain_{run}_{durable}_{cancel_waiter}");
            let subject = format!("{name}.event");
            let config =
                NatsEventBusConfig::new(&endpoint, &name).with_subjects(vec![format!("{name}.>")]);
            let bus = NatsEventBus::connect(config).await.unwrap();
            let subscription = if durable {
                EventSubscription::durable(SubjectFilter::new(format!("{name}.>")), "drain")
            } else {
                EventSubscription::local(SubjectFilter::new(format!("{name}.>")))
            };
            let entered = Arc::new(Semaphore::new(0));
            let release = Arc::new(Semaphore::new(0));
            let completed = Arc::new(AtomicUsize::new(0));
            let handle = bus
                .subscribe(subscription, {
                    let entered = entered.clone();
                    let release = release.clone();
                    let completed = completed.clone();
                    move |_: EventEnvelope| {
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
                })
                .await
                .unwrap();
            bus.publish(event("one", &subject)).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), entered.acquire())
                .await
                .expect("broker must deliver the event")
                .unwrap()
                .forget();
            if cancel_waiter {
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(20),
                        bus.shutdown(Duration::from_secs(2))
                    )
                    .await
                    .is_err()
                );
            } else {
                assert!(!bus.shutdown(Duration::from_millis(20)).await);
            }
            assert!(!bus.shutdown(Duration::ZERO).await);
            assert!(!handle.shutdown(Duration::ZERO).await);
            assert_eq!(completed.load(Ordering::SeqCst), 0);
            assert!(bus.publish(event("late", &subject)).await.is_err());
            assert!(
                bus.subscribe(
                    EventSubscription::local(SubjectFilter::new(subject.clone())),
                    |_: EventEnvelope| async { Ok(()) }
                )
                .await
                .is_err()
            );
            release.add_permits(1);
            assert!(bus.shutdown(Duration::from_secs(5)).await);
            assert!(handle.shutdown(Duration::from_secs(1)).await);
            assert_eq!(completed.load(Ordering::SeqCst), 1);
            jetstream.delete_stream(&name).await.unwrap();
        }
    }
}
