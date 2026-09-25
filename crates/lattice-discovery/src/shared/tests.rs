use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{Stream, StreamExt};
use lattice_model::cluster::{ActorGroupId, CoordinatorScope};
use tokio::sync::mpsc;

use super::SharedDiscovery;
use crate::provider::{CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError};

type Update = Result<CoordinatorDirectorySnapshot, DiscoveryError>;

struct Source {
    scope: CoordinatorScope,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<Update>>>,
    subscriptions: AtomicUsize,
}

impl CoordinatorDiscovery for Source {
    fn scope(&self) -> &CoordinatorScope {
        &self.scope
    }

    fn snapshots(&self) -> Pin<Box<dyn Stream<Item = Update> + Send + '_>> {
        self.subscriptions.fetch_add(1, Ordering::SeqCst);
        let mut receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("one upstream subscription");
        Box::pin(async_stream::stream! {
            while let Some(item) = receiver.recv().await {
                yield item;
            }
        })
    }
}

fn fixture() -> (Arc<Source>, SharedDiscovery, mpsc::UnboundedSender<Update>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let source = Arc::new(Source {
        scope: CoordinatorScope::Cluster,
        receiver: Mutex::new(Some(receiver)),
        subscriptions: AtomicUsize::new(0),
    });
    let shared = SharedDiscovery::new(source.clone());
    (source, shared, sender)
}

fn snapshot(generation: u64) -> CoordinatorDirectorySnapshot {
    CoordinatorDirectorySnapshot {
        scope: CoordinatorScope::Cluster,
        generation,
        leader_hint: None,
        targets: Vec::new(),
    }
}

#[test]
fn construction_does_not_start_a_task_or_require_a_runtime() {
    let (source, _shared, _sender) = fixture();
    assert_eq!(source.subscriptions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn concurrent_subscribers_share_one_stream_and_replay_the_cache() {
    let (source, shared, sender) = fixture();
    let clone = shared.clone();
    let mut first = shared.snapshots();
    let mut second = clone.snapshots();
    sender.send(Ok(snapshot(1))).unwrap();
    let (one, two) = tokio::join!(first.next(), second.next());
    assert_eq!(one.unwrap().unwrap(), snapshot(1));
    assert_eq!(two.unwrap().unwrap(), snapshot(1));
    assert_eq!(source.subscriptions.load(Ordering::SeqCst), 1);

    sender.send(Ok(snapshot(2))).unwrap();
    assert_eq!(first.next().await.unwrap().unwrap(), snapshot(2));
    let mut late = shared.snapshots();
    assert_eq!(late.next().await.unwrap().unwrap(), snapshot(2));
    assert_eq!(second.next().await.unwrap().unwrap(), snapshot(2));
    assert_eq!(source.subscriptions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn errors_and_invalid_updates_keep_the_last_valid_snapshot() {
    let (_source, shared, sender) = fixture();
    let mut events = shared.snapshots();
    sender.send(Ok(snapshot(2))).unwrap();
    assert_eq!(events.next().await.unwrap().unwrap(), snapshot(2));

    for invalid in [
        snapshot(1),
        snapshot(2),
        CoordinatorDirectorySnapshot {
            scope: CoordinatorScope::Group(ActorGroupId::new("other").unwrap()),
            ..snapshot(3)
        },
    ] {
        sender.send(Ok(invalid)).unwrap();
        assert!(matches!(
            events.next().await,
            Some(Err(DiscoveryError::InvalidSnapshot { .. }))
        ));
    }
    sender
        .send(Err(DiscoveryError::Provider {
            provider: "test",
            message: "offline".into(),
        }))
        .unwrap();
    assert!(matches!(
        events.next().await,
        Some(Err(DiscoveryError::Provider { .. }))
    ));
    let mut late = shared.snapshots();
    assert_eq!(late.next().await.unwrap().unwrap(), snapshot(2));
    assert!(late.next().await.unwrap().is_err());
    sender.send(Ok(snapshot(3))).unwrap();
    assert_eq!(events.next().await.unwrap().unwrap(), snapshot(3));
    assert_eq!(late.next().await.unwrap().unwrap(), snapshot(3));
}

#[tokio::test]
async fn completed_provider_replays_its_final_value_without_resubscribing() {
    let (source, shared, sender) = fixture();
    sender.send(Ok(snapshot(1))).unwrap();
    drop(sender);
    let values = shared.snapshots().collect::<Vec<_>>().await;
    assert_eq!(values, vec![Ok(snapshot(1))]);
    assert_eq!(shared.snapshots().collect::<Vec<_>>().await, values);
    assert_eq!(source.subscriptions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn slow_subscribers_skip_intermediate_values_without_an_unbounded_queue() {
    let (_source, shared, sender) = fixture();
    let mut fast = shared.snapshots();
    let mut slow = shared.snapshots();
    sender.send(Ok(snapshot(1))).unwrap();
    let _ = tokio::join!(fast.next(), slow.next());
    for generation in 2..=20 {
        sender.send(Ok(snapshot(generation))).unwrap();
        assert_eq!(fast.next().await.unwrap().unwrap().generation, generation);
    }
    assert_eq!(slow.next().await.unwrap().unwrap().generation, 20);
}

#[tokio::test]
async fn dropping_last_owner_cancels_the_backend_watch() {
    let (_source, shared, sender) = fixture();
    let mut events = shared.snapshots();
    sender.send(Ok(snapshot(1))).unwrap();
    events.next().await.unwrap().unwrap();
    drop(events);
    assert!(
        !sender.is_closed(),
        "retained shared provider keeps its cache fresh"
    );
    drop(shared);
    tokio::time::timeout(Duration::from_secs(1), sender.closed())
        .await
        .unwrap();
}
