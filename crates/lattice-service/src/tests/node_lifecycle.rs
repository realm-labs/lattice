//! Node lifecycle: registration validation, start rollback, shutdown and forced drain.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use lattice_actor::{
    context::{ActorContext, HandlerContext},
    error::{ActorError, ActorStopError},
    recipient::ProtocolRegistrationError,
    registry::{ActorRegistry, ActorRegistryConfig},
    reply::ReplyTo,
    traits::{Actor, ActorLifecycleState, Responder, StopReason},
};
use lattice_core::{
    actor_kind,
    actor_ref::{ClusterId, NodeAddress, NodeIncarnation},
    id::ActorId,
};
use lattice_placement::{
    control::{DEFAULT_MAX_CONTROL_PAYLOAD, PlacementControlRouter},
    runtime::host::{CoordinatorHost, CoordinatorHostConfig},
    storage::InMemoryPlacementStore,
    types::NodeKey,
};

use super::support::*;
use crate::{
    builder::LatticeService,
    error::ServiceError,
    lifecycle::NodeLifecycleState,
    test_support::{network_test_guard, unused_address},
};

#[test]
fn actor_registration_rejects_a_registry_bound_to_another_protocol() {
    let ping = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let other = OtherPingProtocol::bind::<PingActor>().unwrap();
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Ping"),
        ActorRegistryConfig::default(),
        &other,
    ));
    let config = node_config(
        ClusterId::new("service-test").unwrap(),
        "protocol-mismatch",
        NodeAddress::new("127.0.0.1", 25250).unwrap(),
        NodeIncarnation::new(1).unwrap(),
    );

    let result = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry, ping);

    assert!(matches!(
        result,
        Err(ServiceError::ProtocolRegistration(
            ProtocolRegistrationError::RegistryProtocolMismatch { .. }
        ))
    ));
}

#[tokio::test]
async fn force_shutdown_forces_retained_actor_before_publishing_terminated() {
    let _network = network_test_guard().await;
    struct ForceShutdownActor {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for ForceShutdownActor {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Actor for ForceShutdownActor {
        type Error = ActorError;
        type Behavior = ::lattice_actor::state_machine::Stateless;

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            Err(ActorStopError::new("store unavailable"))
        }
    }

    impl Responder<Ping> for ForceShutdownActor {
        async fn respond(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            request: Ping,
            reply_to: ReplyTo<Pong>,
        ) -> Result<(), ActorError> {
            let _ = reply_to.send(Pong(request.0));
            Ok(())
        }
    }

    let binding = Arc::new(PingProtocol::bind::<ForceShutdownActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("ForceShutdownActor"),
        ActorRegistryConfig::default(),
        binding.as_ref(),
    ));
    let dropped = Arc::new(AtomicUsize::new(0));
    let handle = registry
        .start(
            ActorId::U64(1),
            ForceShutdownActor {
                dropped: dropped.clone(),
            },
        )
        .await
        .unwrap();
    let mut data_loss = handle.subscribe_forced_data_loss();
    let config = node_config(
        ClusterId::new("force-shutdown-test").unwrap(),
        "force-shutdown",
        unused_address().await,
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry.clone(), binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();

    let mut lifecycle = handle.subscribe_lifecycle();
    handle.stop(StopReason::Requested).await.unwrap();
    while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
        lifecycle.changed().await.unwrap();
    }
    let retained = service.retained_actor_cells();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].local_ref, handle.local_ref());
    assert!(retained[0].stop_failure.is_some());

    service.force_shutdown().await.unwrap();

    assert_eq!(
        service.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
    assert!(registry.live_cells().is_empty());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let event = tokio::time::timeout(Duration::from_secs(1), data_loss.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.reason, "service force shutdown");
    assert!(event.ticket.starts_with("force-shutdown-"));
}

#[tokio::test]
async fn terminal_shutdown_drains_local_actors_without_a_migration_target() {
    let _network = network_test_guard().await;
    let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("TerminalShutdownActor"),
        ActorRegistryConfig::default(),
        binding.as_ref(),
    ));
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let config = node_config(
        ClusterId::new("terminal-shutdown-test").unwrap(),
        "terminal-shutdown",
        unused_address().await,
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry.clone(), binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();

    service.terminal_shutdown().await.unwrap();

    assert_eq!(
        service.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
    assert!(registry.live_cells().is_empty());
}

#[tokio::test]
async fn service_retry_api_resolves_retained_actor_cell() {
    let _network = network_test_guard().await;
    struct RetryShutdownActor {
        persistence_available: Arc<AtomicBool>,
    }

    impl Actor for RetryShutdownActor {
        type Error = ActorError;
        type Behavior = ::lattice_actor::state_machine::Stateless;

        async fn stopping(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _reason: StopReason,
        ) -> Result<(), ActorStopError> {
            self.persistence_available
                .load(Ordering::SeqCst)
                .then_some(())
                .ok_or_else(|| ActorStopError::new("store unavailable"))
        }
    }

    impl Responder<Ping> for RetryShutdownActor {
        async fn respond(
            &mut self,
            _ctx: &mut HandlerContext<'_, Self>,
            request: Ping,
            reply_to: ReplyTo<Pong>,
        ) -> Result<(), ActorError> {
            let _ = reply_to.send(Pong(request.0));
            Ok(())
        }
    }

    let binding = Arc::new(PingProtocol::bind::<RetryShutdownActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("RetryShutdownActor"),
        ActorRegistryConfig::default(),
        binding.as_ref(),
    ));
    let persistence_available = Arc::new(AtomicBool::new(false));
    let handle = registry
        .start(
            ActorId::U64(1),
            RetryShutdownActor {
                persistence_available: persistence_available.clone(),
            },
        )
        .await
        .unwrap();
    let config = node_config(
        ClusterId::new("retry-shutdown-test").unwrap(),
        "retry-shutdown",
        unused_address().await,
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry, binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();

    let mut lifecycle = handle.subscribe_lifecycle();
    handle.stop(StopReason::Requested).await.unwrap();
    while *lifecycle.borrow() != ActorLifecycleState::StopFailed {
        lifecycle.changed().await.unwrap();
    }
    persistence_available.store(true, Ordering::SeqCst);
    service.retry_actor_stop(handle.local_ref()).await.unwrap();

    assert_eq!(handle.lifecycle_state(), ActorLifecycleState::Stopped);
    assert!(service.retained_actor_cells().is_empty());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn repeated_start_is_rejected_without_stopping_a_ready_node() {
    let _network = network_test_guard().await;
    let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("RepeatedStartActor"),
        ActorRegistryConfig::default(),
        binding.as_ref(),
    ));
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let address = unused_address().await;
    let config = node_config(
        ClusterId::new("repeated-start-test").unwrap(),
        "repeated-start",
        address.clone(),
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry.clone(), binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();
    assert_eq!(service.node_lifecycle_state(), NodeLifecycleState::Ready);

    let rejected = service.start().await.unwrap_err();

    assert!(matches!(rejected, ServiceError::Lifecycle(_)));
    assert_eq!(service.node_lifecycle_state(), NodeLifecycleState::Ready);
    assert!(matches!(
        handle.lifecycle_state(),
        ActorLifecycleState::Starting | ActorLifecycleState::Running
    ));
    assert!(!registry.live_cells().is_empty());
    let socket = format!("{}:{}", address.host(), address.port());
    assert!(
        tokio::net::TcpListener::bind(socket).await.is_err(),
        "a rejected second start must not release remoting"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_failure_rolls_back_partially_started_components() {
    let _network = network_test_guard().await;
    let address = unused_address().await;
    let store = Arc::new(InMemoryPlacementStore::new(64, 64).unwrap());
    let mut config = node_config(
        ClusterId::new("startup-rollback-test").unwrap(),
        "startup-rollback",
        address.clone(),
        NodeIncarnation::new(1).unwrap(),
    );
    config.maximum_supervised_tasks = 1;
    let builder = LatticeService::builder(config).unwrap();
    let host = CoordinatorHost::elect(
        store,
        builder.association_manager(),
        NodeKey {
            node_id: "startup-rollback".to_owned(),
            address: address.clone(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
        BTreeSet::from([placement_domain()]),
        CoordinatorHostConfig::default(),
    )
    .await
    .unwrap();
    let (control, controls) =
        PlacementControlRouter::bounded(64, DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
    let service = builder
        .coordinator_host(Arc::new(control), host, controls)
        .build()
        .unwrap();

    let error = service.start().await.unwrap_err();

    assert!(matches!(error, ServiceError::TaskCapacity));
    assert_eq!(
        service.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    let socket = format!("{}:{}", address.host(), address.port());
    tokio::net::TcpListener::bind(socket)
        .await
        .expect("a failed start must release the remoting endpoint");
}
