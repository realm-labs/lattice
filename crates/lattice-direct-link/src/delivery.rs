use lattice_actor::{Actor, ActorHandle, ActorTellError, Handler};
use lattice_core::{LinkMessageContext, Linked};

pub fn try_deliver_linked<A, T>(
    handle: &ActorHandle<A>,
    payload: T,
    context: LinkMessageContext,
) -> Result<(), ActorTellError>
where
    A: Actor + Handler<Linked<T>>,
    T: Send + 'static,
{
    handle.try_tell(Linked { payload, context })
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use async_trait::async_trait;
    use http::Uri;
    use lattice_actor::{ActorContext, ActorRuntime};
    use lattice_core::{
        ActorId, ActorKind, ActorRef, InstanceId, LinkId, LinkMessageFlags, LinkSequence,
        ServiceKind,
    };
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    use super::*;

    #[derive(Debug)]
    struct PositionUpdate {
        tick: u64,
    }

    struct LinkActor {
        started: Arc<Notify>,
        release: Arc<Notify>,
        received: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl lattice_actor::Actor for LinkActor {
        type Error = Infallible;
    }

    #[async_trait]
    impl Handler<Linked<PositionUpdate>> for LinkActor {
        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            msg: Linked<PositionUpdate>,
        ) -> Result<(), Self::Error> {
            self.started.notify_waiters();
            self.release.notified().await;
            self.received
                .lock()
                .expect("received mutex poisoned")
                .push(msg.payload.tick);
            Ok(())
        }
    }

    #[tokio::test]
    async fn direct_link_delivery_enqueues_linked_message_without_waiting_for_handler() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let received = Arc::new(Mutex::new(Vec::new()));
        let actor = LinkActor {
            started: started.clone(),
            release: release.clone(),
            received: received.clone(),
        };
        let handle = ActorRuntime::default()
            .spawn_actor(actor, Default::default())
            .await
            .unwrap();

        try_deliver_linked(
            &handle,
            PositionUpdate { tick: 42 },
            link_context(LinkId::new("link-1")),
        )
        .unwrap();
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        assert!(received.lock().expect("received mutex poisoned").is_empty());

        release.notify_waiters();
        timeout(Duration::from_secs(1), async {
            loop {
                if !received.lock().expect("received mutex poisoned").is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(*received.lock().expect("received mutex poisoned"), vec![42]);
    }

    fn link_context(link_id: LinkId) -> LinkMessageContext {
        let source = actor_ref("Gateway", "GatewaySession", 7);
        let target = actor_ref("Battle", "Battle", 9);
        LinkMessageContext {
            link_id,
            source,
            target,
            sequence: LinkSequence(1).0,
            received_at: Instant::now(),
            flags: LinkMessageFlags::EMPTY,
        }
    }

    fn actor_ref(service: &'static str, actor: &'static str, id: u64) -> ActorRef {
        ActorRef::direct(
            ServiceKind::from_static(service),
            ActorKind::from_static(actor),
            ActorId::U64(id),
            InstanceId::new(format!("instance-{id}")),
            "http://127.0.0.1:10000".parse::<Uri>().unwrap(),
            None,
        )
    }
}
