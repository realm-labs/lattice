//! Activation registry: duplicate starts, waiter sharing, bounds, and failure recovery.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_core::{actor_kind, id::ActorId, service_context::ServiceContext};
use tokio::sync::{Mutex, Semaphore};

use super::TestActor;
use crate::{
    error::{ActorActivationError, ActorError},
    mailbox::MailboxConfig,
    registry::{ActorRegistry, ActorRegistryConfig},
};

#[tokio::test]
async fn actor_registry_prevents_duplicate_start() {
    let registry =
        ActorRegistry::<TestActor>::new(actor_kind!("Test"), ActorRegistryConfig::default());
    let actor_id = ActorId::U64(1);
    let actor = TestActor {
        events: Arc::new(Mutex::new(Vec::new())),
        start_gate: None,
        stopped: None,
    };

    registry.start(actor_id.clone(), actor).await.unwrap();
    let duplicate = registry
        .start(
            actor_id,
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            },
        )
        .await;

    assert!(matches!(
        duplicate,
        Err(ActorActivationError::AlreadyExists)
    ));
}

#[tokio::test]
async fn actor_registry_activation_waiters_share_single_activation() {
    let registry = Arc::new(ActorRegistry::<TestActor>::new(
        actor_kind!("Test"),
        ActorRegistryConfig::default(),
    ));
    let actor_id = ActorId::U64(2);
    let activations = Arc::new(AtomicUsize::new(0));
    let activation_entered = Arc::new(Semaphore::new(0));
    let start_gate = Arc::new(Semaphore::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));

    let first = {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        let activations = activations.clone();
        let activation_entered = activation_entered.clone();
        let start_gate = start_gate.clone();
        let events = events.clone();
        tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async move {
                    activations.fetch_add(1, Ordering::SeqCst);
                    activation_entered.add_permits(1);
                    let permit = start_gate.acquire().await.unwrap();
                    permit.forget();
                    Ok(TestActor {
                        events,
                        start_gate: None,
                        stopped: None,
                    })
                })
                .await
        })
    };
    activation_entered.acquire().await.unwrap().forget();

    let mut tasks = vec![first];
    for _ in 0..3 {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        tasks.push(tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async {
                    panic!("waiter must not run activation")
                })
                .await
        }));
    }

    start_gate.add_permits(1);

    for task in tasks {
        task.await.unwrap().unwrap();
    }

    assert_eq!(activations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn actor_registry_bounds_and_times_out_activation_waiters() {
    let registry = Arc::new(ActorRegistry::<TestActor>::new(
        actor_kind!("Test"),
        ActorRegistryConfig {
            mailbox: MailboxConfig::bounded(8),
            passivation: Default::default(),
            shard_migration: Default::default(),
            waiter_capacity: 0,
            waiter_timeout: Duration::from_millis(20),
            quarantine_capacity: 8,
            actor_ref: None,
            service: ServiceContext::empty(),
        },
    ));
    let actor_id = ActorId::U64(3);
    let start_gate = Arc::new(Semaphore::new(0));

    let first = tokio::spawn({
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        let start_gate = start_gate.clone();
        async move {
            registry
                .get_or_activate(actor_id, || async move {
                    let permit = start_gate.acquire().await.unwrap();
                    permit.forget();
                    Ok(TestActor {
                        events: Arc::new(Mutex::new(Vec::new())),
                        start_gate: None,
                        stopped: None,
                    })
                })
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let second = registry
        .get_or_activate(actor_id, || async {
            Ok(TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            })
        })
        .await;

    assert!(matches!(
        second,
        Err(ActorActivationError::WaiterCapacityExceeded)
    ));
    start_gate.add_permits(1);
    first.await.unwrap().unwrap();
}

#[tokio::test]
async fn actor_registry_activation_failure_wakes_waiters_and_allows_retry() {
    let registry = Arc::new(ActorRegistry::<TestActor>::new(
        actor_kind!("Test"),
        ActorRegistryConfig::default(),
    ));
    let actor_id = ActorId::U64(4);
    let activation_entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));

    let first = {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        let activation_entered = activation_entered.clone();
        let release = release.clone();
        tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async move {
                    activation_entered.add_permits(1);
                    let permit = release.acquire().await.unwrap();
                    permit.forget();
                    Err(ActorError::new("load failed"))
                })
                .await
        })
    };

    activation_entered.acquire().await.unwrap().forget();
    let waiter = {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async {
                    panic!("waiter must not run activation")
                })
                .await
        })
    };

    release.add_permits(1);
    assert!(matches!(
        first.await.unwrap(),
        Err(ActorActivationError::ActivationFailed(_))
    ));
    assert!(matches!(
        waiter.await.unwrap(),
        Err(ActorActivationError::ActivationFailed(_))
    ));

    let retry = registry
        .get_or_activate(actor_id, || async {
            Ok(TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            })
        })
        .await;

    assert!(retry.is_ok());
}
