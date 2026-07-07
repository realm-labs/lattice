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
use lattice_core::actor_ref::{ActorRef, ActorRefTarget};
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::service_context::ServiceContext;
use lattice_core::{actor_kind, service_kind};
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
async fn registry_injects_direct_actor_ref_into_context() {
    let endpoint = "http://127.0.0.1:19090".parse().unwrap();
    let registry = ActorRegistry::<SelfRefActor>::new(
        actor_kind!("GatewaySession"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                service_kind: service_kind!("Gateway"),
                instance_id: InstanceId::new("gateway-1"),
                endpoint,
                owner_epoch: None,
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
    assert_eq!(actor_ref.service_kind, service_kind!("Gateway"));
    assert_eq!(actor_ref.actor_kind, actor_kind!("GatewaySession"));
    assert_eq!(actor_ref.actor_id, ActorId::Str("session-1".to_string()));
    assert!(matches!(
        actor_ref.target,
        ActorRefTarget::Direct {
            instance_id,
            owner_epoch: None,
            ..
        } if instance_id == InstanceId::new("gateway-1")
    ));
}
