use lattice_actor::context::HandlerContext;
use lattice_actor::runtime::{ActorRuntime, ActorRuntimeConfig};
use lattice_actor_distributed::registry::ActorDefinition;
use lattice_model::actor::ErasedProtocol;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_actor::environment::ActorEnvironment;
use lattice_actor_distributed::ActorKey;
use lattice_actor_distributed::{
    activation::DistributedActorContextExt,
    actor_protocol,
    context::ActorContext,
    directory::ActivationDirectory,
    error::{ActorFailure, ActorStopError},
    mailbox::MailboxConfig,
    protocol::ProstCodec,
    registry::{
        ActorActivationError, ActorAddressConfig, ActorQuarantineError, ActorRegistry,
        ActorRegistryConfig,
    },
    runtime::PassivationPolicy,
    traits::{Actor, ActorLifecycleState, Handler, Message, StopReason},
};
use lattice_model::{
    actor::{ActorAddress, ProtocolId},
    cluster::{ClusterId, NodeEndpoint, NodeIncarnation},
};
use tokio::sync::{Semaphore, oneshot};

struct SlowActor;

#[tokio::test]
async fn cancelled_activation_releases_placeholder_and_notifies_existing_waiters() {
    let actor_runtime = ActorRuntime::default();
    let registry = Arc::new(ActorRegistry::<CancelledDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    ));
    let actor_id = ActorKey::U64(71);
    let mut producer = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        std::future::pending::<Result<SlowActor, ActorFailure>>().await
    }));
    assert!(futures_util::poll!(&mut producer).is_pending());
    let mut waiter = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        panic!("existing waiter must not become the loader")
    }));
    assert!(futures_util::poll!(&mut waiter).is_pending());
    drop(producer);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("cancelled producer must wake existing waiters")
            .is_err()
    );
    assert!(registry.active_actor_ids().is_empty());
    registry
        .get_or_activate(actor_id, || async { Ok(SlowActor) })
        .await
        .unwrap();
    assert!(registry.drain().await.completed());
}

#[tokio::test]
async fn panicking_loader_releases_its_activation_placeholder() {
    let actor_runtime = ActorRuntime::default();
    let registry = Arc::new(ActorRegistry::<PanickingLoaderDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    ));
    let task_registry = registry.clone();
    assert!(
        tokio::spawn(async move {
            task_registry
                .get_or_activate(ActorKey::U64(72), || async { panic!("loader panic") })
                .await
        })
        .await
        .unwrap_err()
        .is_panic()
    );
    assert!(registry.active_actor_ids().is_empty());
    tokio::time::timeout(
        Duration::from_millis(100),
        registry.get_or_activate(ActorKey::U64(72), || async { Ok(SlowActor) }),
    )
    .await
    .expect("panic cleanup must permit retry")
    .unwrap();
    assert!(registry.drain().await.completed());
}

#[tokio::test]
async fn draining_loading_activation_prevents_late_publication() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<DrainLoadingDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let actor_id = ActorKey::U64(73);
    let (release, ready) = oneshot::channel();
    let mut producer = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        ready.await.unwrap();
        Ok(SlowActor)
    }));
    assert!(futures_util::poll!(&mut producer).is_pending());
    assert_eq!(registry.active_actor_ids(), std::slice::from_ref(&actor_id));
    let drained = registry.drain().await;
    assert!(drained.completed());
    let _ = release.send(());
    assert!(
        producer.await.is_err(),
        "a drained loader cannot spawn an Actor"
    );
    assert!(registry.get_running(&actor_id).is_none());
}

#[tokio::test]
async fn fencing_loading_activation_does_not_remove_a_replacement() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<FenceLoadingDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let actor_id = ActorKey::U64(74);
    let mut old = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        std::future::pending::<Result<SlowActor, ActorFailure>>().await
    }));
    assert!(futures_util::poll!(&mut old).is_pending());
    registry
        .fence_after_authority_loss(&actor_id)
        .await
        .unwrap();
    let replacement = registry
        .get_or_activate(actor_id.clone(), || async { Ok(SlowActor) })
        .await
        .unwrap();
    drop(old);
    assert_eq!(
        registry.get_running(&actor_id).unwrap().local_ref(),
        replacement.local_ref()
    );
    assert!(registry.drain().await.completed());
}

#[tokio::test]
async fn authority_change_during_loading_rejects_publication_and_allows_retry() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<GenerationLoadingDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let generation = Arc::new(Mutex::new(Some(1)));
    registry.install_fencing_token_resolver("test", {
        let generation = generation.clone();
        move |_, publish| {
            let generation = generation.lock().unwrap();
            publish(*generation);
        }
    });
    let actor_id = ActorKey::U64(75);
    for new_generation in [None, Some(2)] {
        *generation.lock().unwrap() = Some(1);
        let (release, ready) = oneshot::channel();
        let mut producer = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
            ready.await.unwrap();
            Ok(SlowActor)
        }));
        assert!(futures_util::poll!(&mut producer).is_pending());
        *generation.lock().unwrap() = new_generation;
        release.send(()).unwrap();
        assert!(producer.await.is_err());
        assert!(registry.active_actor_ids().is_empty());
    }
    let old = registry
        .get_or_activate(actor_id.clone(), || async { Ok(SlowActor) })
        .await
        .unwrap();
    *generation.lock().unwrap() = Some(3);
    let replacement = registry
        .get_or_activate(actor_id.clone(), || async {
            assert!(old.business_admission_fenced());
            Ok(SlowActor)
        })
        .await
        .expect("current authority must retire an activation left by an older generation");
    assert_ne!(old.local_ref(), replacement.local_ref());
    assert_eq!(
        registry.get_running(&actor_id).unwrap().local_ref(),
        replacement.local_ref()
    );
    assert!(registry.drain().await.completed());
}

#[tokio::test]
async fn current_generation_cancels_a_loading_predecessor_and_its_waiters() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<ReplaceLoadingDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let generation = Arc::new(Mutex::new(Some(1)));
    registry.install_fencing_token_resolver("test", {
        let generation = generation.clone();
        move |_, publish| publish(*generation.lock().unwrap())
    });
    let actor_id = ActorKey::U64(78);
    let mut old = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        std::future::pending::<Result<SlowActor, ActorFailure>>().await
    }));
    assert!(futures_util::poll!(&mut old).is_pending());
    let mut waiter = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        panic!("waiter must not load")
    }));
    assert!(futures_util::poll!(&mut waiter).is_pending());
    *generation.lock().unwrap() = Some(2);
    let replacement = registry
        .get_or_activate(actor_id.clone(), || async { Ok(SlowActor) })
        .await
        .unwrap();
    assert!(matches!(old.await, Err(ActorActivationError::Cancelled)));
    assert!(matches!(waiter.await, Err(ActorActivationError::Cancelled)));
    assert_eq!(
        registry.get_running(&actor_id).unwrap().local_ref(),
        replacement.local_ref()
    );
    assert!(registry.drain().await.completed());
}

#[tokio::test]
async fn new_generation_preserves_an_old_stop_failure_in_quarantine() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<ReplaceStopFailureDefinition, _>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let generation = Arc::new(Mutex::new(Some(1)));
    registry.install_fencing_token_resolver("test", {
        let generation = generation.clone();
        move |_, publish| publish(*generation.lock().unwrap())
    });
    let actor_id = ActorKey::U64(79);
    let persistence_available = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicUsize::new(0));
    let old = registry
        .start(
            actor_id.clone(),
            RetainedRegistryActor {
                persistence_available: persistence_available.clone(),
                dropped: dropped.clone(),
            },
        )
        .await
        .unwrap();
    assert!(!registry.drain().await.completed());
    assert!(matches!(
        registry
            .get_or_activate(actor_id.clone(), || async {
                panic!("same authority must retain failure")
            })
            .await,
        Err(ActorActivationError::RetainedStopFailure)
    ));
    *generation.lock().unwrap() = Some(2);
    let replacement = registry
        .get_or_activate(actor_id.clone(), || async {
            Ok(RetainedRegistryActor {
                persistence_available: Arc::new(AtomicBool::new(true)),
                dropped: Arc::new(AtomicUsize::new(0)),
            })
        })
        .await
        .unwrap();
    assert!(old.business_admission_fenced());
    assert!(registry.inspect_quarantined(&actor_id).is_some());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    let mut lifecycle = old.subscribe_lifecycle();
    persistence_available.store(true, Ordering::SeqCst);
    registry
        .retry_quarantined_exact(old.local_ref())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while *lifecycle.borrow() != ActorLifecycleState::Stopped {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(registry.quarantine_len(), 0);
    assert_eq!(
        registry.get_running(&actor_id).unwrap().local_ref(),
        replacement.local_ref()
    );
    assert!(registry.drain().await.completed());
}

#[tokio::test]
async fn wait_terminal_tracks_loading_until_invalidation() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<WaitLoadingDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let actor_id = ActorKey::U64(76);
    let mut producer = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        std::future::pending::<Result<SlowActor, ActorFailure>>().await
    }));
    assert!(futures_util::poll!(&mut producer).is_pending());
    let mut waiter = Box::pin(registry.wait_actor_ids_terminal([actor_id.clone()]));
    assert!(futures_util::poll!(&mut waiter).is_pending());
    assert!(registry.drain_actor_ids([actor_id]).await.completed());
    tokio::time::timeout(Duration::from_millis(100), waiter)
        .await
        .unwrap();
    assert!(producer.await.is_err());
}

#[tokio::test]
async fn activation_waiter_revalidates_authority_after_publication() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<WaiterGenerationDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let generation = Arc::new(Mutex::new(Some(1)));
    registry.install_fencing_token_resolver("test", {
        let generation = generation.clone();
        move |_, publish| {
            let generation = generation.lock().unwrap();
            publish(*generation);
        }
    });
    let actor_id = ActorKey::U64(77);
    let (release, ready) = oneshot::channel();
    let mut producer = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        ready.await.unwrap();
        Ok(SlowActor)
    }));
    assert!(futures_util::poll!(&mut producer).is_pending());
    let mut waiter = Box::pin(registry.get_or_activate(actor_id.clone(), || async {
        panic!("waiter cannot run loader")
    }));
    assert!(futures_util::poll!(&mut waiter).is_pending());
    release.send(()).unwrap();
    producer.await.unwrap();
    *generation.lock().unwrap() = None;
    assert!(waiter.await.is_err());
    assert!(registry.drain().await.completed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fast_fence_cleanup_cannot_leave_a_stopped_cell_in_quarantine() {
    let actor_runtime = ActorRuntime::default();
    let registry = Arc::new(ActorRegistry::<FenceCleanupDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    ));
    let mut tasks = tokio::task::JoinSet::new();
    for id in 0..128 {
        let registry = registry.clone();
        tasks.spawn(async move {
            let actor_id = ActorKey::U64(id);
            let handle = registry.start(actor_id.clone(), SlowActor).await.unwrap();
            let mut terminated = handle.subscribe_terminated();
            registry
                .fence_after_authority_loss(&actor_id)
                .await
                .unwrap();
            terminated.recv().await.unwrap();
        });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(registry.active_actor_ids().is_empty());
    assert_eq!(registry.quarantine_len(), 0);
}

impl Actor for SlowActor {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;
    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorFailure> {
        Ok(())
    }
}

#[tokio::test]
async fn activation_waiter_times_out_while_activation_is_loading() {
    let actor_runtime = ActorRuntime::default();
    let registry = Arc::new(ActorRegistry::<SlowDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig {
            mailbox: MailboxConfig::bounded(8),
            passivation: Default::default(),
            shard_migration: Default::default(),
            waiter_capacity: 1,
            waiter_timeout: Duration::from_millis(10),
            quarantine_capacity: 8,
            address: None,
        },
    ));
    let actor_id = ActorKey::U64(7);
    let activation_entered = Arc::new(Semaphore::new(0));
    let release_activation = Arc::new(Semaphore::new(0));

    let activator = {
        let registry = registry.clone();
        let actor_id = actor_id.clone();
        let activation_entered = activation_entered.clone();
        let release_activation = release_activation.clone();
        tokio::spawn(async move {
            registry
                .get_or_activate(actor_id, || async move {
                    activation_entered.add_permits(1);
                    let permit = release_activation.acquire().await.unwrap();
                    permit.forget();
                    Ok(SlowActor)
                })
                .await
        })
    };

    activation_entered.acquire().await.unwrap().forget();
    let waiter = registry
        .get_or_activate(actor_id, || async {
            panic!("waiter must not run activation")
        })
        .await;

    assert!(matches!(
        waiter,
        Err(ActorActivationError::WaiterTimeout { .. })
    ));

    release_activation.add_permits(1);
    activator.await.unwrap().unwrap();
}

#[tokio::test]
async fn remove_running_actor_allows_restart_with_same_id() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<SlowDefinition, SlowActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let actor_id = ActorKey::U64(9);

    let first = registry.start(actor_id.clone(), SlowActor).await.unwrap();
    let removed = registry.remove(&actor_id).await.unwrap();
    let mut lifecycle = removed.subscribe_lifecycle();
    while *lifecycle.borrow() != ActorLifecycleState::Stopped {
        lifecycle.changed().await.unwrap();
    }
    let second = registry.start(actor_id, SlowActor).await.unwrap();

    assert_ne!(first.local_ref(), second.local_ref());
}

struct SelfRefActor {
    tx: Option<oneshot::Sender<ActorAddress>>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Probe {}

impl Message for Probe {}

impl Handler<Probe> for SelfRefActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: Probe,
    ) -> Result<(), ActorFailure> {
        Ok(())
    }
}

actor_protocol! {
    SelfRefProtocol {
        protocol_id: 11;
        name: "registry/self-ref/v1";
        tell 1 => Probe {
            schema_version: 1,
            codec: ProstCodec,
        }
    }
}

impl Actor for SelfRefActor {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorFailure> {
        if let Some(tx) = self.tx.take() {
            let distributed = ctx
                .require_distributed()
                .map_err(|error| ActorFailure::new(error.to_string()))?;
            let reference = distributed
                .self_address()
                .ok_or_else(|| ActorFailure::new("distributed actor has no exact address"))?;
            let _ = tx.send(reference.clone());
        }
        Ok(())
    }
}

#[tokio::test]
async fn registry_injects_exact_actor_address_into_context() {
    let node_incarnation = NodeIncarnation::new(7).unwrap();
    let protocol = SelfRefProtocol::bind::<SelfRefActor>().unwrap();
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<GatewaySessionDefinition, SelfRefActor>::new_bound(
        actor_runtime.spawner(),
        ActorRegistryConfig {
            address: Some(ActorAddressConfig {
                cluster_id: ClusterId::new("test").unwrap(),
                node_address: NodeEndpoint::new("127.0.0.1", 19090).unwrap(),
                node_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        &protocol,
    );
    let (tx, rx) = oneshot::channel();

    registry
        .start(
            ActorKey::Str("session-1".to_string()),
            SelfRefActor { tx: Some(tx) },
        )
        .await
        .unwrap();

    let actor_address = rx.await.unwrap();
    assert_eq!(actor_address.cluster_id().as_str(), "test");
    assert_eq!(actor_address.node_incarnation(), node_incarnation);
    assert_eq!(actor_address.protocol_id(), ProtocolId::new(11).unwrap());
    assert!(actor_address.actor_path().to_string().starts_with("/user/"));
    assert!(registry.get_exact(&actor_address).is_some());

    let typed = registry
        .address::<SelfRefProtocol>(&ActorKey::Str("session-1".to_owned()))
        .unwrap()
        .unwrap();
    assert!(typed.same_activation(&actor_address));

    let old = actor_address.clone();
    let actor_id = ActorKey::Str("session-1".to_string());
    let first = registry.remove(&actor_id).await.unwrap();
    let mut lifecycle = first.subscribe_lifecycle();
    while *lifecycle.borrow() != ActorLifecycleState::Stopped {
        lifecycle.changed().await.unwrap();
    }
    registry
        .start(actor_id.clone(), SelfRefActor { tx: None })
        .await
        .unwrap();
    let replacement = registry.exact_address(&actor_id).unwrap();
    assert!(registry.get_exact(&old).is_none());
    assert_ne!(old.activation_id(), replacement.activation_id());
}

#[tokio::test]
async fn registry_keeps_unaddressable_identities_node_local() {
    let protocol = SelfRefProtocol::bind::<SelfRefActor>().unwrap();
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<GatewaySessionDefinition, SelfRefActor>::new_bound(
        actor_runtime.spawner(),
        ActorRegistryConfig {
            address: Some(ActorAddressConfig {
                cluster_id: ClusterId::new("test").unwrap(),
                node_address: NodeEndpoint::new("127.0.0.1", 19090).unwrap(),
                node_incarnation: NodeIncarnation::new(7).unwrap(),
            }),
            ..ActorRegistryConfig::default()
        },
        &protocol,
    );

    let actor_id = ActorKey::Str("s".repeat(64));
    registry
        .start(actor_id.clone(), SelfRefActor { tx: None })
        .await
        .unwrap();

    assert!(registry.exact_address(&actor_id).is_none());
}

struct RetainedRegistryActor {
    persistence_available: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for RetainedRegistryActor {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Actor for RetainedRegistryActor {
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn stopping(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _reason: StopReason,
    ) -> Result<(), ActorStopError> {
        if self.persistence_available.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(ActorStopError::new("store unavailable"))
        }
    }
}

#[tokio::test]
async fn voluntary_stop_failed_blocks_replacement_until_same_actor_retries() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<RetainedRegistryActorDefinition, _>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let actor_id = ActorKey::U64(44);
    let persistence_available = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicUsize::new(0));
    let handle = registry
        .start(
            actor_id.clone(),
            RetainedRegistryActor {
                persistence_available: persistence_available.clone(),
                dropped: dropped.clone(),
            },
        )
        .await
        .unwrap();
    let mut lifecycle = handle.subscribe_lifecycle();
    handle.stop(StopReason::Requested).unwrap();
    while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
        lifecycle.changed().await.unwrap();
    }

    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(registry.get_running(&actor_id).is_none());
    assert_eq!(registry.retained_stop_failures().len(), 1);
    assert!(matches!(
        registry
            .get_or_activate(actor_id.clone(), || async {
                Ok(RetainedRegistryActor {
                    persistence_available: Arc::new(AtomicBool::new(true)),
                    dropped: Arc::new(AtomicUsize::new(0)),
                })
            })
            .await,
        Err(ActorActivationError::RetainedStopFailure)
    ));

    persistence_available.store(true, Ordering::SeqCst);
    handle.retry_stop().await.unwrap();
    while *lifecycle.borrow() != ActorLifecycleState::Stopped {
        lifecycle.changed().await.unwrap();
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(registry.retained_stop_failures().is_empty());
}

#[tokio::test]
async fn external_authority_loss_quarantines_old_actor_and_allows_replacement() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<RetainedRegistryActorDefinition, _>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig {
            quarantine_capacity: 1,
            ..ActorRegistryConfig::default()
        },
    );
    let actor_id = ActorKey::U64(45);
    let persistence_available = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicUsize::new(0));
    let old = registry
        .start(
            actor_id.clone(),
            RetainedRegistryActor {
                persistence_available: persistence_available.clone(),
                dropped: dropped.clone(),
            },
        )
        .await
        .unwrap();
    let mut old_lifecycle = old.subscribe_lifecycle();
    registry
        .fence_after_authority_loss(&actor_id)
        .await
        .unwrap();
    let _diagnostics = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(diagnostics) = registry.inspect_quarantined(&actor_id) {
                break diagnostics;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(old.lifecycle_state(), ActorLifecycleState::StopFailed);
    assert!(registry.get_running(&actor_id).is_none());
    assert_eq!(registry.quarantine_len(), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);

    let replacement = registry
        .get_or_activate(actor_id.clone(), || async {
            Ok(RetainedRegistryActor {
                persistence_available: Arc::new(AtomicBool::new(true)),
                dropped: Arc::new(AtomicUsize::new(0)),
            })
        })
        .await
        .unwrap();
    assert_ne!(old.local_ref(), replacement.local_ref());
    assert_eq!(
        registry.get_running(&actor_id).unwrap().local_ref(),
        replacement.local_ref()
    );

    persistence_available.store(true, Ordering::SeqCst);
    registry.retry_quarantined(&actor_id).await.unwrap();
    while *old_lifecycle.borrow() != ActorLifecycleState::Stopped {
        old_lifecycle.changed().await.unwrap();
    }
    assert_eq!(registry.quarantine_len(), 0);
    assert_eq!(
        registry.get_running(&actor_id).unwrap().local_ref(),
        replacement.local_ref()
    );
}

#[tokio::test]
async fn quarantine_capacity_exhaustion_is_explicit_and_never_drops_retained_state() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<RetainedRegistryActorDefinition, _>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig {
            quarantine_capacity: 1,
            ..ActorRegistryConfig::default()
        },
    );
    let first_id = ActorKey::U64(46);
    let second_id = ActorKey::U64(47);
    let first_dropped = Arc::new(AtomicUsize::new(0));
    let second_dropped = Arc::new(AtomicUsize::new(0));
    let unavailable = Arc::new(AtomicBool::new(false));

    registry
        .start(
            first_id.clone(),
            RetainedRegistryActor {
                persistence_available: unavailable.clone(),
                dropped: first_dropped.clone(),
            },
        )
        .await
        .unwrap();
    let second = registry
        .start(
            second_id.clone(),
            RetainedRegistryActor {
                persistence_available: unavailable,
                dropped: second_dropped.clone(),
            },
        )
        .await
        .unwrap();

    registry
        .fence_after_authority_loss(&first_id)
        .await
        .unwrap();
    assert!(matches!(
        registry.fence_after_authority_loss(&second_id).await,
        Err(ActorQuarantineError::Capacity { capacity: 1 })
    ));

    assert_eq!(registry.quarantine_len(), 2);
    assert!(registry.get_running(&second_id).is_none());
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if registry.inspect_quarantined(&second_id).is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    registry
        .force_discard_quarantined_exact(
            second.local_ref(),
            "quarantine overflow cleanup",
            "OPS-47",
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while second.lifecycle_state() != ActorLifecycleState::Stopped {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(registry.inspect_quarantined(&second_id).is_none());
    assert_eq!(first_dropped.load(Ordering::SeqCst), 0);
    assert_eq!(second_dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn repeated_authority_loss_retains_every_exact_activation() {
    let actor_runtime = ActorRuntime::default();
    let registry = ActorRegistry::<RetainedRegistryActorDefinition, _>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig {
            quarantine_capacity: 2,
            ..ActorRegistryConfig::default()
        },
    );
    let actor_id = ActorKey::U64(48);
    let persistence_available = Arc::new(AtomicBool::new(false));
    let first = registry
        .start(
            actor_id.clone(),
            RetainedRegistryActor {
                persistence_available: persistence_available.clone(),
                dropped: Arc::new(AtomicUsize::new(0)),
            },
        )
        .await
        .unwrap();
    registry
        .fence_after_authority_loss(&actor_id)
        .await
        .unwrap();

    let second = registry
        .get_or_activate(actor_id.clone(), || async {
            Ok(RetainedRegistryActor {
                persistence_available: persistence_available.clone(),
                dropped: Arc::new(AtomicUsize::new(0)),
            })
        })
        .await
        .unwrap();
    registry
        .fence_after_authority_loss(&actor_id)
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if registry.quarantined_activations(&actor_id).len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let retained = registry.quarantined_activations(&actor_id);
    assert_eq!(retained[0].local_ref, first.local_ref());
    assert_eq!(retained[1].local_ref, second.local_ref());

    persistence_available.store(true, Ordering::SeqCst);
    registry
        .retry_quarantined_exact(first.local_ref())
        .await
        .unwrap();
    registry
        .retry_quarantined_exact(second.local_ref())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while registry.quarantine_len() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn authority_loss_during_stopping_finishes_in_non_authoritative_quarantine() {
    struct ConcurrentFenceActor {
        stopping_entered: Arc<Semaphore>,
        release_stopping: Arc<Semaphore>,
    }

    impl Actor for ConcurrentFenceActor {
        type Error = ActorFailure;
        type Behavior = ::lattice_actor::state_machine::Stateless;

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            self.stopping_entered.add_permits(1);
            self.release_stopping.acquire().await.unwrap().forget();
            Err(ActorStopError::new("store unavailable"))
        }
    }
    let actor_runtime = ActorRuntime::default();

    let registry = ActorRegistry::<ConcurrentFenceActorDefinition, _>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    );
    let actor_id = ActorKey::U64(49);
    let stopping_entered = Arc::new(Semaphore::new(0));
    let release_stopping = Arc::new(Semaphore::new(0));
    let handle = registry
        .start(
            actor_id.clone(),
            ConcurrentFenceActor {
                stopping_entered: stopping_entered.clone(),
                release_stopping: release_stopping.clone(),
            },
        )
        .await
        .unwrap();

    handle.stop(StopReason::Requested).unwrap();
    stopping_entered.acquire().await.unwrap().forget();
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopping);
    registry
        .fence_after_authority_loss(&actor_id)
        .await
        .unwrap();
    release_stopping.add_permits(1);

    let _diagnostics = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(diagnostics) = registry.inspect_quarantined(&actor_id)
                && handle.lifecycle_state() == ActorLifecycleState::StopFailed
            {
                break diagnostics;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::StopFailed);
    assert!(registry.get_running(&actor_id).is_none());
}

#[tokio::test]
async fn idle_passivation_eagerly_releases_registry_and_directory_capacity() {
    let mut environment = ActorEnvironment::builder();
    environment
        .insert(ActivationDirectory::new(1).unwrap())
        .unwrap();
    let environment = environment.build();
    let directory = environment.get::<ActivationDirectory>().unwrap();
    let protocol = SelfRefProtocol::bind::<SelfRefActor>().unwrap();
    let actor_runtime = ActorRuntime::new(ActorRuntimeConfig {
        environment,
        ..Default::default()
    });
    let registry = ActorRegistry::<GatewaySessionDefinition, SelfRefActor>::new_bound(
        actor_runtime.spawner(),
        ActorRegistryConfig {
            passivation: PassivationPolicy::IdleTimeout(Duration::from_millis(10)),
            address: Some(ActorAddressConfig {
                cluster_id: ClusterId::new("test").unwrap(),
                node_address: NodeEndpoint::new("127.0.0.1", 19091).unwrap(),
                node_incarnation: NodeIncarnation::new(8).unwrap(),
            }),
            ..ActorRegistryConfig::default()
        },
        &protocol,
    );

    let first_id = ActorKey::Str("idle-1".to_owned());
    let first = registry
        .start(first_id.clone(), SelfRefActor { tx: None })
        .await
        .unwrap();
    let mut lifecycle = first.subscribe_lifecycle();
    while *lifecycle.borrow() != ActorLifecycleState::Stopped {
        lifecycle.changed().await.unwrap();
    }
    assert!(registry.get_running(&first_id).is_none());
    assert!(directory.is_empty());

    let second_id = ActorKey::Str("idle-2".to_owned());
    registry
        .start(second_id.clone(), SelfRefActor { tx: None })
        .await
        .unwrap();
    assert!(registry.exact_address(&second_id).is_some());
    assert_eq!(directory.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_activation_never_produces_a_second_cell() {
    struct CountedActor;

    impl Actor for CountedActor {
        type Error = ActorFailure;
        type Behavior = ::lattice_actor::state_machine::Stateless;
    }
    let actor_runtime = ActorRuntime::default();

    let registry = Arc::new(ActorRegistry::<CountedDefinition, CountedActor>::new(
        actor_runtime.spawner(),
        ActorRegistryConfig::default(),
    ));

    for round in 0..256u64 {
        let actor_id = ActorKey::U64(round);
        let activations = Arc::new(AtomicUsize::new(0));
        let mut callers = Vec::new();
        for _ in 0..4 {
            let registry = registry.clone();
            let actor_id = actor_id.clone();
            let activations = activations.clone();
            callers.push(tokio::spawn(async move {
                registry
                    .get_or_activate(actor_id, || async move {
                        tokio::task::yield_now().await;
                        activations.fetch_add(1, Ordering::SeqCst);
                        Ok(CountedActor)
                    })
                    .await
                    .map(|handle| handle.local_ref())
            }));
        }

        let mut cells = Vec::new();
        for caller in callers {
            if let Ok(local_ref) = caller.await.unwrap() {
                cells.push(local_ref);
            }
        }
        assert!(!cells.is_empty(), "round {round} activated nothing");
        assert!(
            cells.iter().all(|local_ref| *local_ref == cells[0]),
            "round {round} produced {} distinct cells from {} activations",
            cells.iter().collect::<std::collections::HashSet<_>>().len(),
            activations.load(Ordering::SeqCst)
        );
    }
}

#[derive(Debug)]
struct CancelledDefinition;

impl ActorDefinition for CancelledDefinition {
    const NAME: &'static str = "Cancelled";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct PanickingLoaderDefinition;

impl ActorDefinition for PanickingLoaderDefinition {
    const NAME: &'static str = "PanickingLoader";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct DrainLoadingDefinition;

impl ActorDefinition for DrainLoadingDefinition {
    const NAME: &'static str = "DrainLoading";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct FenceLoadingDefinition;

impl ActorDefinition for FenceLoadingDefinition {
    const NAME: &'static str = "FenceLoading";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct GenerationLoadingDefinition;

impl ActorDefinition for GenerationLoadingDefinition {
    const NAME: &'static str = "GenerationLoading";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct ReplaceLoadingDefinition;

impl ActorDefinition for ReplaceLoadingDefinition {
    const NAME: &'static str = "ReplaceLoading";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct ReplaceStopFailureDefinition;

impl ActorDefinition for ReplaceStopFailureDefinition {
    const NAME: &'static str = "ReplaceStopFailure";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct WaitLoadingDefinition;

impl ActorDefinition for WaitLoadingDefinition {
    const NAME: &'static str = "WaitLoading";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct WaiterGenerationDefinition;

impl ActorDefinition for WaiterGenerationDefinition {
    const NAME: &'static str = "WaiterGeneration";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct FenceCleanupDefinition;

impl ActorDefinition for FenceCleanupDefinition {
    const NAME: &'static str = "FenceCleanup";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct SlowDefinition;

impl ActorDefinition for SlowDefinition {
    const NAME: &'static str = "Slow";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct GatewaySessionDefinition;

impl ActorDefinition for GatewaySessionDefinition {
    const NAME: &'static str = "GatewaySession";
    type Protocol = SelfRefProtocol;
}

#[derive(Debug)]
struct RetainedRegistryActorDefinition;

impl ActorDefinition for RetainedRegistryActorDefinition {
    const NAME: &'static str = "RetainedRegistryActor";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct ConcurrentFenceActorDefinition;

impl ActorDefinition for ConcurrentFenceActorDefinition {
    const NAME: &'static str = "ConcurrentFenceActor";
    type Protocol = ErasedProtocol;
}

#[derive(Debug)]
struct CountedDefinition;

impl ActorDefinition for CountedDefinition {
    const NAME: &'static str = "Counted";
    type Protocol = ErasedProtocol;
}
