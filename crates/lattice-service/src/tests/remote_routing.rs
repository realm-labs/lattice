//! Remote message routing against an exact actor reference over TCP.

use std::{sync::Arc, time::Duration};

use lattice_actor::{
    context::{ActorContext, HandlerContext},
    error::ActorError,
    registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig},
    reply::ReplyTo,
    traits::{Actor, Handler, Responder, StopReason},
    watch::{ActorTerminated, TerminatedReason, TerminatedTarget, WatchId, WatchStatus},
};
use lattice_core::{
    actor_kind,
    actor_ref::{ActorRef, ClusterId, NodeIncarnation},
    id::ActorId,
};
use lattice_remoting::handshake::NodeIdentity;
use tokio::sync::{Mutex, Semaphore};

use super::support::*;
use crate::{
    builder::LatticeService,
    test_support::{network_test_guard, unused_address},
};

struct RemoteWatcherActor {
    target: ActorRef<PingProtocol>,
    events: Arc<Mutex<Vec<ActorTerminated>>>,
    ready: Arc<Semaphore>,
    notified: Arc<Semaphore>,
    watch_id: Arc<Mutex<Option<WatchId>>>,
}

impl Actor for RemoteWatcherActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;

    async fn started(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        let watch_id = ctx.watch(&self.target).await?;
        *self.watch_id.lock().await = Some(watch_id);
        self.ready.add_permits(1);
        Ok(())
    }
}

impl Handler<ActorTerminated> for RemoteWatcherActor {
    async fn handle(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        message: ActorTerminated,
    ) -> Result<(), Self::Error> {
        self.events.lock().await.push(message);
        self.notified.add_permits(1);
        Ok(())
    }
}

impl Responder<Ping> for RemoteWatcherActor {
    async fn respond(
        &mut self,
        _ctx: &mut HandlerContext<'_, Self>,
        request: Ping,
        reply_to: ReplyTo<Pong>,
    ) -> Result<(), Self::Error> {
        let _ = reply_to.send(Pong(request.0 + 1));
        Ok(())
    }
}

#[tokio::test]
async fn typed_actor_ref_asks_exact_remote_activation_over_tcp() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("service-test").unwrap();
    let first_address = unused_address().await;
    let second_address = unused_address().await;
    let (client_address, server_address) = if first_address < second_address {
        (first_address, second_address)
    } else {
        (second_address, first_address)
    };
    let client_incarnation = NodeIncarnation::new(1).unwrap();
    let server_incarnation = NodeIncarnation::new(2).unwrap();
    let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Ping"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: server_address.clone(),
                node_incarnation: server_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        binding.as_ref(),
    ));
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let target: ActorRef<PingProtocol> = handle.typed_actor_ref().unwrap().unwrap();
    let server = LatticeService::builder(node_config(
        cluster_id.clone(),
        "server",
        server_address.clone(),
        server_incarnation,
    ))
    .unwrap()
    .register_actor(registry, binding)
    .unwrap()
    .build()
    .unwrap();
    let client = LatticeService::builder(node_config(
        cluster_id.clone(),
        "client",
        client_address,
        client_incarnation,
    ))
    .unwrap()
    .use_protocol::<PingProtocol>()
    .unwrap()
    .build()
    .unwrap();
    server.start().await.unwrap();
    client.start().await.unwrap();
    client
        .connect_peer(NodeIdentity {
            cluster_id,
            node_id: "server".to_owned(),
            address: server_address,
            incarnation: server_incarnation,
        })
        .await
        .unwrap();
    let reply = client
        .ask(&target, Ping(41), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(reply, Pong(42));
    let mut local_watch = server.watch(&target).await.unwrap();
    let mut watch = client.watch(&target).await.unwrap();
    let watch_id = watch.id();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if watch.status() == WatchStatus::Active {
                break;
            }
            watch.status_changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    handle.stop(StopReason::Requested).await.unwrap();
    let terminated = tokio::time::timeout(Duration::from_secs(2), watch.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminated.reason, TerminatedReason::Stopped);
    assert_eq!(terminated.watch_id, watch_id);
    assert!(
        matches!(terminated.target, TerminatedTarget::Exact(reference) if reference == target.erase())
    );
    let local_terminated = tokio::time::timeout(Duration::from_secs(2), local_watch.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(local_terminated.reason, TerminatedReason::Stopped);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_exact_watch_uses_the_same_subscription_and_drop_cancels_it() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("service-test").unwrap();
    let address = unused_address().await;
    let incarnation = NodeIncarnation::new(11).unwrap();
    let binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("Ping"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: address.clone(),
                node_incarnation: incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        binding.as_ref(),
    ));
    let handle = registry.start(ActorId::U64(1), PingActor).await.unwrap();
    let target: ActorRef<PingProtocol> = handle.typed_actor_ref().unwrap().unwrap();
    let mut config = node_config(cluster_id, "local", address, incarnation);
    config.maximum_watches = 1;
    let service = LatticeService::builder(config)
        .unwrap()
        .register_actor(registry, binding)
        .unwrap()
        .build()
        .unwrap();
    service.start().await.unwrap();

    let baseline_tasks = service.supervisor().active_tasks();
    let first = service.watch(&target).await.unwrap();
    assert_eq!(first.status(), WatchStatus::Active);
    assert_eq!(service.supervisor().active_tasks(), baseline_tasks + 1);
    drop(first);
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.supervisor().active_tasks() != baseline_tasks {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let mut second = service.watch(&target).await.unwrap();
    let second_id = second.id();
    assert_eq!(second.status(), WatchStatus::Active);
    handle.stop(StopReason::Requested).await.unwrap();
    let terminated = tokio::time::timeout(Duration::from_secs(2), second.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminated.reason, TerminatedReason::Stopped);
    assert_eq!(terminated.watch_id, second_id);
    assert!(
        matches!(terminated.target, TerminatedTarget::Exact(reference) if reference == target.erase())
    );

    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn actor_context_watch_delivers_a_remote_termination_to_the_system_mailbox() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("service-test").unwrap();
    let first_address = unused_address().await;
    let second_address = unused_address().await;
    let (client_address, server_address) = if first_address < second_address {
        (first_address, second_address)
    } else {
        (second_address, first_address)
    };
    let client_incarnation = NodeIncarnation::new(21).unwrap();
    let server_incarnation = NodeIncarnation::new(22).unwrap();

    let target_binding = Arc::new(PingProtocol::bind::<PingActor>().unwrap());
    let target_registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("RemoteWatchTarget"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: server_address.clone(),
                node_incarnation: server_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        target_binding.as_ref(),
    ));
    let target_handle = target_registry
        .start(ActorId::U64(1), PingActor)
        .await
        .unwrap();
    let target: ActorRef<PingProtocol> = target_handle.typed_actor_ref().unwrap().unwrap();
    let server = LatticeService::builder(node_config(
        cluster_id.clone(),
        "server",
        server_address.clone(),
        server_incarnation,
    ))
    .unwrap()
    .register_actor(target_registry, target_binding)
    .unwrap()
    .build()
    .unwrap();

    let watcher_binding = Arc::new(PingProtocol::bind::<RemoteWatcherActor>().unwrap());
    let watcher_registry = Arc::new(ActorRegistry::new_bound(
        actor_kind!("RemoteWatcher"),
        ActorRegistryConfig {
            actor_ref: Some(ActorRefConfig {
                cluster_id: cluster_id.clone(),
                node_address: client_address.clone(),
                node_incarnation: client_incarnation,
            }),
            ..ActorRegistryConfig::default()
        },
        watcher_binding.as_ref(),
    ));
    let client = LatticeService::builder(node_config(
        cluster_id.clone(),
        "client",
        client_address,
        client_incarnation,
    ))
    .unwrap()
    .register_actor(watcher_registry.clone(), watcher_binding)
    .unwrap()
    .build()
    .unwrap();

    server.start().await.unwrap();
    client.start().await.unwrap();
    client
        .connect_peer(NodeIdentity {
            cluster_id,
            node_id: "server".to_owned(),
            address: server_address,
            incarnation: server_incarnation,
        })
        .await
        .unwrap();
    assert_eq!(
        client
            .ask(&target, Ping(7), Duration::from_secs(1))
            .await
            .unwrap(),
        Pong(8)
    );

    let events = Arc::new(Mutex::new(Vec::new()));
    let ready = Arc::new(Semaphore::new(0));
    let notified = Arc::new(Semaphore::new(0));
    let watch_id = Arc::new(Mutex::new(None));
    let _watcher = watcher_registry
        .start(
            ActorId::U64(2),
            RemoteWatcherActor {
                target: target.clone(),
                events: events.clone(),
                ready: ready.clone(),
                notified: notified.clone(),
                watch_id: watch_id.clone(),
            },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), ready.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    target_handle.stop(StopReason::Requested).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), notified.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let observed = events.lock().await;
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].watch_id, watch_id.lock().await.unwrap());
    assert!(matches!(
        observed[0].reason,
        TerminatedReason::Stopped | TerminatedReason::ActivationChanged
    ));
    assert!(
        matches!(&observed[0].target, TerminatedTarget::Exact(reference) if reference == &target.clone().erase())
    );
    drop(observed);

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
