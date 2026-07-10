//! Production-hardening regression tests.
//!
//! Keep crate-private service ingress, ownership, lease, supervision, readiness,
//! and shutdown tests in this module. Split each concern into a child module as
//! its implementation slice lands; tests that require only public APIs belong in
//! Cargo integration targets instead.

use super::*;

#[tokio::test]
async fn service_keeps_instance_lease_alive_while_running() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    let store_for_service = store.clone();
    let mut watch = store.watch(store.prefix().clone()).await.unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let service = LatticeService::builder(service_kind!("World"))
        .instance_id(InstanceId::new("world-1"))
        .listen(listener)
        .ready_signal(ready_tx)
        .instance_lease_keepalive_interval(Duration::from_millis(10))
        .placement_store::<InMemoryPlacementStore, _>(store_for_service)
        .register_actor(
            ActorRegistration::builder(actor_kind!("World"))
                .factory(TestFactory)
                .build(),
        )
        .register_sharded_rpc(FakeRpcBinding::<TestActor>::new(
            actor_kind!("World"),
            "WorldRpc",
        ))
        .build()
        .await
        .unwrap();

    let task = tokio::spawn(service.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    ready_rx.await.unwrap();

    let lease_id = loop {
        let event = timeout(Duration::from_secs(1), watch.next())
            .await
            .unwrap()
            .unwrap();
        if let lattice_placement::storage::PlacementWatchEvent::InstanceUpdated { record } = event
            && record.state == InstanceState::Ready
        {
            break record.lease_id;
        }
    };
    timeout(Duration::from_secs(1), async {
        loop {
            if store.instance_lease_keepalive_count(lease_id).unwrap_or(0) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}
