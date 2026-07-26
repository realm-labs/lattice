//! Remote message routing against an exact actor reference over TCP.

use std::{sync::Arc, time::Duration};

use lattice_actor::{
    registry::{ActorRefConfig, ActorRegistry, ActorRegistryConfig},
    traits::StopReason,
};
use lattice_core::{
    actor_kind,
    actor_ref::{ActorRef, ClusterId, NodeIncarnation},
    id::ActorId,
};
use lattice_remoting::{handshake::NodeIdentity, watch::WatchStatus};

use super::support::*;
use crate::{
    builder::LatticeService,
    test_support::{network_test_guard, unused_address},
};

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
    let watch_id = client.watch(&target).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if client.watch_status(watch_id) == WatchStatus::Active {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.stop(StopReason::Requested).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if client.watch_status(watch_id) == WatchStatus::Terminated {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
