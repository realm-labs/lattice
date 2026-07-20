use lattice_actor::context::HandlerContext;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_actor::{
    actor_protocol,
    context::ActorContext,
    directory::ActivationDirectory,
    error::{ActorActivationError, ActorError, ActorStopError},
    mailbox::MailboxConfig,
    protocol::ProstCodec,
    registry::{ActorQuarantineError, ActorRefConfig, ActorRegistry, ActorRegistryConfig},
    runtime::PassivationPolicy,
    traits::{Actor, ActorLifecycleState, Handler, Message, StopReason},
};
use lattice_core::{
    actor_kind,
    actor_ref::{ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId},
    id::ActorId,
    instance::InstanceId,
    kind::ServiceKind,
    service_context::ServiceContext,
};
use tokio::sync::{Semaphore, oneshot};

struct SlowActor;

impl Actor for SlowActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
    async fn started(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        Ok(())
    }
}

#[tokio::test]
async fn activation_waiter_times_out_while_activation_is_loading() {
    let registry = Arc::new(ActorRegistry::<SlowActor>::new(
        actor_kind!("Slow"),
        ActorRegistryConfig {
            mailbox: MailboxConfig::bounded(8),
            passivation: Default::default(),
            shard_migration: Default::default(),
            waiter_capacity: 1,
            waiter_timeout: Duration::from_millis(10),
            quarantine_capacity: 8,
            actor_ref: None,
            service: ServiceContext::empty(),
        },
    ));
    let actor_id = ActorId::U64(7);
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
    let registry =
        ActorRegistry::<SlowActor>::new(actor_kind!("Slow"), ActorRegistryConfig::default());
    let actor_id = ActorId::U64(9);

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
    tx: Option<oneshot::Sender<ActorRef>>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Probe {}

impl Message for Probe {}

impl Handler<Probe> for SelfRefActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        _message: Probe,
    ) -> Result<(), ActorError> {
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
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(ctx.require_self_ref()?.clone());
        }
        Ok(())
    }
}

#[tokio::test]
async fn registry_injects_exact_actor_ref_into_context() {
    let node_incarnation = NodeIncarnation::new(7).unwrap();
    let protocol = SelfRefProtocol::bind::<SelfRefActor>().unwrap();
    let registry = ActorRegistry::<SelfRefActor>::new_bound(
        actor_kind!("GatewaySession"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: ClusterId::new("test").unwrap(),
                node_address: NodeAddress::new("127.0.0.1", 19090).unwrap(),
                node_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        &protocol,
    );
    let (tx, rx) = oneshot::channel();

    registry
        .start(
            ActorId::Str("session-1".to_string()),
            SelfRefActor { tx: Some(tx) },
        )
        .await
        .unwrap();

    let actor_ref = rx.await.unwrap();
    assert_eq!(actor_ref.cluster_id().as_str(), "test");
    assert_eq!(actor_ref.node_incarnation(), node_incarnation);
    assert_eq!(actor_ref.protocol_id(), ProtocolId::new(11).unwrap());
    assert!(actor_ref.actor_path().to_string().starts_with("/user/"));
    assert!(registry.get_exact(&actor_ref).is_some());

    let typed = registry
        .get_running(&ActorId::Str("session-1".to_owned()))
        .unwrap()
        .typed_actor_ref::<SelfRefProtocol>()
        .unwrap()
        .unwrap();
    assert!(typed.same_activation(&actor_ref));

    let old = actor_ref.clone();
    let actor_id = ActorId::Str("session-1".to_string());
    let first = registry.remove(&actor_id).await.unwrap();
    let mut lifecycle = first.subscribe_lifecycle();
    while *lifecycle.borrow() != ActorLifecycleState::Stopped {
        lifecycle.changed().await.unwrap();
    }
    let replacement = registry
        .start(actor_id, SelfRefActor { tx: None })
        .await
        .unwrap();
    assert!(registry.get_exact(&old).is_none());
    assert_ne!(
        old.activation_id(),
        replacement.actor_ref().unwrap().activation_id()
    );
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
    type Error = ActorError;
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
    let registry = ActorRegistry::new(
        actor_kind!("RetainedRegistryActor"),
        ActorRegistryConfig::default(),
    );
    let actor_id = ActorId::U64(44);
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
    handle.stop(StopReason::Requested).await.unwrap();
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
    let registry = ActorRegistry::new(
        actor_kind!("RetainedRegistryActor"),
        ActorRegistryConfig {
            quarantine_capacity: 1,
            ..ActorRegistryConfig::default()
        },
    );
    let actor_id = ActorId::U64(45);
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
    let diagnostics = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(diagnostics) = registry.inspect_quarantined(&actor_id) {
                break diagnostics;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!diagnostics.failure.authoritative);
    assert_eq!(old.lifecycle_state(), ActorLifecycleState::Quarantined);
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
    let registry = ActorRegistry::new(
        actor_kind!("RetainedRegistryActor"),
        ActorRegistryConfig {
            quarantine_capacity: 1,
            ..ActorRegistryConfig::default()
        },
    );
    let first_id = ActorId::U64(46);
    let second_id = ActorId::U64(47);
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

    assert_eq!(registry.quarantine_len(), 1);
    assert_eq!(second.lifecycle_state(), ActorLifecycleState::Quarantined);
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
    let registry = ActorRegistry::new(
        actor_kind!("RetainedRegistryActor"),
        ActorRegistryConfig {
            quarantine_capacity: 2,
            ..ActorRegistryConfig::default()
        },
    );
    let actor_id = ActorId::U64(48);
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
        type Error = ActorError;
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

    let registry = ActorRegistry::new(
        actor_kind!("ConcurrentFenceActor"),
        ActorRegistryConfig::default(),
    );
    let actor_id = ActorId::U64(49);
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

    handle.stop(StopReason::Requested).await.unwrap();
    stopping_entered.acquire().await.unwrap().forget();
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopping);
    registry
        .fence_after_authority_loss(&actor_id)
        .await
        .unwrap();
    release_stopping.add_permits(1);

    let diagnostics = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(diagnostics) = registry.inspect_quarantined(&actor_id)
                && handle.lifecycle_state() == ActorLifecycleState::Quarantined
                && !diagnostics.failure.authoritative
            {
                break diagnostics;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Quarantined);
    assert!(!diagnostics.failure.authoritative);
    assert!(registry.get_running(&actor_id).is_none());
}

#[tokio::test]
async fn idle_passivation_eagerly_releases_registry_and_directory_capacity() {
    let mut service = ServiceContext::builder(
        ServiceKind::from_static("test"),
        InstanceId::new("registry-passivation"),
    );
    service
        .insert_extension(ActivationDirectory::new(1).unwrap())
        .unwrap();
    let service = service.build();
    let directory = service.extension::<ActivationDirectory>().unwrap();
    let protocol = SelfRefProtocol::bind::<SelfRefActor>().unwrap();
    let registry = ActorRegistry::<SelfRefActor>::new_bound(
        actor_kind!("GatewaySession"),
        ActorRegistryConfig {
            passivation: PassivationPolicy::IdleTimeout(Duration::from_millis(10)),
            actor_ref: Some(ActorRefConfig {
                cluster_id: ClusterId::new("test").unwrap(),
                node_address: NodeAddress::new("127.0.0.1", 19091).unwrap(),
                node_incarnation: NodeIncarnation::new(8).unwrap(),
            }),
            service,
            ..ActorRegistryConfig::default()
        },
        &protocol,
    );

    let first_id = ActorId::Str("idle-1".to_owned());
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

    let second = registry
        .start(ActorId::Str("idle-2".to_owned()), SelfRefActor { tx: None })
        .await
        .unwrap();
    assert!(second.actor_ref().is_some());
    assert_eq!(directory.len(), 1);
}
