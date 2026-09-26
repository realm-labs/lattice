use std::{future::pending, sync::Arc, time::Duration};

use lattice_model::actor::ProtocolId;
use lattice_model::cluster::{ClusterId, NodeEndpoint, NodeIncarnation};
use tokio::{sync::oneshot, task::yield_now, time::timeout};

use super::endpoint_with_config;
use crate::{
    config::RemotingConfig,
    control::RejectControlDispatch,
    endpoint::{EndpointError, RemotingEndpoint, lifecycle::wait_for_shutdown},
    handshake::NodeIdentity,
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
};

fn shutdown_endpoint(timeout: Duration) -> Arc<RemotingEndpoint> {
    endpoint_with_config(
        NodeIdentity {
            cluster_id: ClusterId::new("shutdown").unwrap(),
            node_id: "shutdown".to_owned(),
            address: NodeEndpoint::new("127.0.0.1", 32101).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
        ProtocolDescriptor {
            protocol_id: ProtocolId::new(7).unwrap(),
            fingerprint: ProtocolFingerprint::digest(b"shutdown/v1"),
        },
        Arc::new(RejectControlDispatch),
        RemotingConfig {
            max_associations: 1,
            shutdown_timeout: timeout,
            ..RemotingConfig::default()
        },
    )
}

#[tokio::test]
async fn spawning_reaps_completed_tasks_before_enforcing_limit() {
    let endpoint = shutdown_endpoint(Duration::from_secs(1));
    let limit = endpoint.config.required_socket_budget();
    // Retain abort handles to wait for completion without consuming the JoinSet entries.
    let completed: Vec<_> = {
        let mut tasks = endpoint.tasks.lock().unwrap();
        (0..limit).map(|_| tasks.spawn(async { Ok(()) })).collect()
    };
    timeout(Duration::from_secs(1), async {
        while completed.iter().any(|task| !task.is_finished()) {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(endpoint.tasks.lock().unwrap().len(), limit);

    let (release, waiting) = oneshot::channel();
    endpoint
        .spawn(async move {
            waiting.await.unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(endpoint.tasks.lock().unwrap().len(), 1);
    release.send(()).unwrap();
    endpoint.shutdown().await.unwrap();
    assert!(endpoint.tasks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn shutdown_joins_all_tasks_at_the_cap_without_aborting_them() {
    let endpoint = shutdown_endpoint(Duration::from_secs(1));
    let limit = endpoint.config.required_socket_budget();
    let mut completions = Vec::new();
    for _ in 0..limit {
        let mut shutdown = endpoint.shutdown_tx.subscribe();
        let (finished, completion) = oneshot::channel();
        completions.push(completion);
        endpoint
            .spawn(async move {
                wait_for_shutdown(&mut shutdown).await;
                finished.send(()).unwrap();
                Ok(())
            })
            .unwrap();
    }
    assert!(matches!(
        endpoint.spawn(pending::<Result<(), EndpointError>>()),
        Err(EndpointError::TaskLimit)
    ));
    assert_eq!(endpoint.tasks.lock().unwrap().len(), limit);

    endpoint.shutdown().await.unwrap();
    assert!(endpoint.tasks.lock().unwrap().is_empty());
    for completion in completions {
        completion
            .await
            .expect("task must exit gracefully, not be aborted");
    }
    assert!(matches!(
        endpoint.spawn(async { Ok(()) }),
        Err(EndpointError::ShuttingDown)
    ));
}

#[tokio::test]
async fn cancelling_shutdown_keeps_tasks_owned_for_retry() {
    let endpoint = shutdown_endpoint(Duration::from_secs(1));
    let (release, waiting) = tokio::sync::oneshot::channel();
    endpoint
        .spawn(async move {
            waiting.await.unwrap();
            Ok(())
        })
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(20), endpoint.shutdown())
            .await
            .is_err()
    );
    assert_eq!(endpoint.tasks.lock().unwrap().len(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), endpoint.shutdown())
            .await
            .is_err()
    );
    release
        .send(())
        .expect("external cancellation must retain the running task");
    endpoint.shutdown().await.unwrap();
    assert!(endpoint.tasks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn shutdown_task_error_keeps_later_tasks_owned_for_retry() {
    let endpoint = shutdown_endpoint(Duration::from_secs(1));
    endpoint
        .spawn(async { Err(EndpointError::WrongDialDirection) })
        .unwrap();
    let (release, waiting) = tokio::sync::oneshot::channel();
    endpoint
        .spawn(async move {
            waiting.await.unwrap();
            Ok(())
        })
        .unwrap();

    assert!(matches!(
        endpoint.shutdown().await,
        Err(EndpointError::WrongDialDirection)
    ));
    assert_eq!(endpoint.tasks.lock().unwrap().len(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), endpoint.shutdown())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    endpoint.shutdown().await.unwrap();
    assert!(endpoint.tasks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn endpoint_timeout_still_aborts_and_reaps_owned_tasks() {
    let endpoint = shutdown_endpoint(Duration::from_millis(20));
    let (alive, dropped) = tokio::sync::oneshot::channel::<()>();
    endpoint
        .spawn(async move {
            let _alive = alive;
            std::future::pending::<()>().await;
            Ok(())
        })
        .unwrap();

    assert!(matches!(
        endpoint.shutdown().await,
        Err(EndpointError::ShutdownTimeout)
    ));
    assert!(endpoint.tasks.lock().unwrap().is_empty());
    assert!(dropped.await.is_err());
    endpoint.shutdown().await.unwrap();
}
