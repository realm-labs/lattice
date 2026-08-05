//! Activation registry: duplicate starts, waiter sharing, bounds, and failure recovery.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use lattice_core::{actor_kind, id::ActorId, service_context::ServiceContext};
use tokio::sync::{Mutex, Semaphore};

use super::TestActor;
use crate::{
    error::{ActorActivationError, ActorError},
    mailbox::MailboxConfig,
    registry::{ActorCreateContext, ActorLoader, ActorRegistry, ActorRegistryConfig},
};

#[derive(Clone)]
struct TokenLoader {
    loads: Arc<AtomicUsize>,
    token: Arc<AtomicU64>,
}

#[async_trait]
impl ActorLoader<TestActor> for TokenLoader {
    async fn load(&self, ctx: ActorCreateContext) -> Result<TestActor, ActorError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.token.store(
            ctx.fencing_token().map_or(0, |token| token.get()),
            Ordering::SeqCst,
        );
        Ok(TestActor {
            events: Arc::new(Mutex::new(Vec::new())),
            start_gate: None,
            stopped: None,
        })
    }
}

#[tokio::test]
async fn direct_activation_resolves_framework_fencing_token_before_loading() {
    let registry =
        ActorRegistry::<TestActor>::new(actor_kind!("Test"), ActorRegistryConfig::default());
    registry.install_fencing_token_resolver("test", |actor_id| {
        (actor_id == &ActorId::U64(7)).then_some(11)
    });
    let loads = Arc::new(AtomicUsize::new(0));
    let token = Arc::new(AtomicU64::new(0));

    registry
        .get_or_load(
            ActorId::U64(7),
            TokenLoader {
                loads: loads.clone(),
                token: token.clone(),
            },
        )
        .await
        .unwrap();

    assert_eq!(loads.load(Ordering::SeqCst), 1);
    assert_eq!(token.load(Ordering::SeqCst), 11);
}

#[tokio::test]
async fn direct_activation_without_live_authority_never_runs_loader() {
    let registry =
        ActorRegistry::<TestActor>::new(actor_kind!("Test"), ActorRegistryConfig::default());
    registry.install_fencing_token_resolver("test", |_| None);
    let loads = Arc::new(AtomicUsize::new(0));

    let result = registry
        .get_or_load(
            ActorId::U64(8),
            TokenLoader {
                loads: loads.clone(),
                token: Arc::new(AtomicU64::new(0)),
            },
        )
        .await;

    assert!(matches!(
        result,
        Err(ActorActivationError::ActivationFailed(_))
    ));
    assert_eq!(loads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn validated_authority_proof_cannot_be_used_with_another_registry() {
    let first =
        ActorRegistry::<TestActor>::new(actor_kind!("Test"), ActorRegistryConfig::default());
    let second =
        ActorRegistry::<TestActor>::new(actor_kind!("Test"), ActorRegistryConfig::default());
    first.install_fencing_token_resolver("test", |_| Some(5));
    second.install_fencing_token_resolver("test", |_| Some(5));
    let authority = first.validate_actor_authority(ActorId::U64(10), 5).unwrap();
    let loads = Arc::new(AtomicUsize::new(0));

    let result = second
        .load_with_validated_authority(
            authority,
            TokenLoader {
                loads: loads.clone(),
                token: Arc::new(AtomicU64::new(0)),
            },
        )
        .await;

    assert!(matches!(
        result,
        Err(ActorActivationError::ActivationFailed(_))
    ));
    assert_eq!(loads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn running_actor_is_hidden_after_its_external_authority_is_revoked() {
    let registry =
        ActorRegistry::<TestActor>::new(actor_kind!("Test"), ActorRegistryConfig::default());
    registry
        .start(
            ActorId::U64(9),
            TestActor {
                events: Arc::new(Mutex::new(Vec::new())),
                start_gate: None,
                stopped: None,
            },
        )
        .await
        .unwrap();
    assert!(registry.get_running(&ActorId::U64(9)).is_some());
    registry.install_fencing_token_resolver("test", |_| Some(3));
    assert!(registry.get_running(&ActorId::U64(9)).is_some());

    registry.install_fencing_token_resolver("test", |_| None);

    assert!(registry.get_running(&ActorId::U64(9)).is_none());
}

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
