use super::*;

fn shutdown_endpoint(timeout: Duration) -> Arc<RemotingEndpoint> {
    endpoint_with_config(
        NodeIdentity {
            cluster_id: ClusterId::new("shutdown").unwrap(),
            node_id: "shutdown".to_owned(),
            address: NodeAddress::new("127.0.0.1", 32101).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        },
        ProtocolDescriptor {
            protocol_id: ProtocolId::new(7).unwrap(),
            fingerprint: ProtocolFingerprint::digest(b"shutdown/v1"),
        },
        Arc::new(RejectControlDispatch),
        RemotingConfig {
            shutdown_timeout: timeout,
            ..RemotingConfig::default()
        },
    )
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
