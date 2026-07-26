//! Supervised task failures: panics, stuck tasks and their effect on node readiness.

use std::time::Duration;

use lattice_core::actor_ref::{ClusterId, NodeIncarnation};

use super::support::node_config;
use crate::{
    builder::LatticeService,
    config::ClusterJoinConfig,
    lifecycle::NodeLifecycleState,
    test_support::{network_test_guard, unused_address},
};

#[tokio::test]
async fn supervised_task_panic_stops_the_node() {
    let _network = network_test_guard().await;
    let config = node_config(
        ClusterId::new("task-panic-test").unwrap(),
        "task-panic",
        unused_address().await,
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config).unwrap().build().unwrap();
    service.start().await.unwrap();
    assert_eq!(service.node_lifecycle_state(), NodeLifecycleState::Ready);

    service
        .supervisor()
        .spawn(async { panic!("supervised task under test") })
        .unwrap();

    let mut lifecycle = service.subscribe_node_lifecycle();
    tokio::time::timeout(Duration::from_secs(2), async {
        while *lifecycle.borrow_and_update() != NodeLifecycleState::Stopping {
            lifecycle.changed().await.unwrap();
        }
    })
    .await
    .expect("a panicking supervised task must stop reporting readiness");
    assert_eq!(service.supervisor().failed_tasks(), 1);
    service.shutdown().await.unwrap();
    assert_eq!(
        service.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
}

#[tokio::test]
async fn shutdown_completes_after_aborting_a_stuck_supervised_task() {
    let _network = network_test_guard().await;
    let config = node_config(
        ClusterId::new("stuck-task-test").unwrap(),
        "stuck-task",
        unused_address().await,
        NodeIncarnation::new(1).unwrap(),
    );
    let service = LatticeService::builder(config)
        .unwrap()
        .join_config(ClusterJoinConfig {
            leave_timeout: Duration::from_millis(100),
            shutdown_timeout: Duration::from_millis(200),
            ..ClusterJoinConfig::default()
        })
        .build()
        .unwrap();
    service.start().await.unwrap();
    service.supervisor().spawn(std::future::pending()).unwrap();

    service.shutdown().await.unwrap();

    assert_eq!(
        service.node_lifecycle_state(),
        NodeLifecycleState::Terminated
    );
    assert_eq!(service.supervisor().failed_tasks(), 0);
    assert_eq!(service.lifecycle_metrics().termination_completed_total, 1);
}
