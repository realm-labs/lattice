use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lattice_actor::{
    Actor, ActorActivationError, ActorContext, ActorError, ActorRegistry, ActorRegistryConfig,
    MailboxConfig,
};
use lattice_core::{ActorId, actor_kind};
use tokio::sync::Semaphore;

struct SlowActor;

#[async_trait]
impl Actor for SlowActor {
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
            waiter_capacity: 1,
            waiter_timeout: Duration::from_millis(10),
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
