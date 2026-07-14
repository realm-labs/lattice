use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorActivationError;
use lattice_actor::error::ActorError;
use lattice_actor::mailbox::MailboxConfig;
use lattice_actor::registry::ActorRegistry;
use lattice_actor::registry::{ActorRefConfig, ActorRegistryConfig};
use lattice_actor::traits::Actor;
use lattice_core::actor_kind;
use lattice_core::actor_ref::{ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId};
use lattice_core::id::ActorId;
use lattice_core::service_context::ServiceContext;
use tokio::sync::{Semaphore, oneshot};

struct SlowActor;

#[async_trait]
impl Actor for SlowActor {
    type Error = ActorError;
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
    removed
        .stop(lattice_actor::traits::StopReason::Requested)
        .await
        .unwrap();
    let second = registry.start(actor_id, SlowActor).await.unwrap();

    assert_ne!(first.local_ref(), second.local_ref());
}

struct SelfRefActor {
    tx: Option<oneshot::Sender<ActorRef>>,
}

#[async_trait]
impl Actor for SelfRefActor {
    type Error = ActorError;
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
    let registry = ActorRegistry::<SelfRefActor>::new(
        actor_kind!("GatewaySession"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: ClusterId::new("test").unwrap(),
                node_address: NodeAddress::new("127.0.0.1", 19090).unwrap(),
                node_incarnation,
                protocol_id: ProtocolId::new(11).unwrap(),
            }),
            ..ActorRegistryConfig::default()
        },
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

    let old = actor_ref.clone();
    let actor_id = ActorId::Str("session-1".to_string());
    let first = registry.remove(&actor_id).await.unwrap();
    first
        .stop(lattice_actor::traits::StopReason::Requested)
        .await
        .unwrap();
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
