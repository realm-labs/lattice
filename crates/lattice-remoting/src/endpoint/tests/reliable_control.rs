use super::*;

#[tokio::test]
async fn invalid_reliable_control_is_acknowledged_without_poisoning_later_commands() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = probe.local_addr().unwrap().port();
    drop(probe);
    let client_port = server_port.saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("invalid-control-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
        incarnation: NodeIncarnation::new(11).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id,
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
        incarnation: NodeIncarnation::new(12).unwrap(),
    };
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(8).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"invalid-control-test/v1"),
    };
    let control = Arc::new(RejectInvalidControl::default());
    let client = endpoint(client_identity, descriptor.clone());
    let server = endpoint_with_control(server_identity.clone(), descriptor, control.clone());
    server.bind().await.unwrap();
    let association = client.connect_peer(server_identity).await.unwrap();

    association
        .admit_control_command(Bytes::from_static(b"invalid"))
        .unwrap();
    association
        .admit_control_command(Bytes::from_static(b"valid"))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let rejected = *control.rejected.lock().expect("rejected flag poisoned");
            let applied = control
                .applied
                .lock()
                .expect("recording control poisoned")
                .clone();
            if rejected
                && applied == [Bytes::from_static(b"valid")]
                && association.control_outbox_len() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(association.state(), AssociationState::Active);

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn blocked_control_apply_does_not_starve_remoting_heartbeats() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = probe.local_addr().unwrap().port();
    drop(probe);
    let client_port = server_port.saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("blocked-control-heartbeat-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
        incarnation: NodeIncarnation::new(21).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id,
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
        incarnation: NodeIncarnation::new(22).unwrap(),
    };
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(9).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"blocked-control-heartbeat-test/v1"),
    };
    let control = Arc::new(BlockingControl::default());
    let client = endpoint(client_identity, descriptor.clone());
    let server = endpoint_with_control(server_identity.clone(), descriptor, control.clone());
    server.bind().await.unwrap();
    let association = client.connect_peer(server_identity).await.unwrap();

    let started = control.started.notified();
    association
        .admit_control_command(Bytes::from_static(b"blocked"))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert_eq!(association.state(), AssociationState::Active);

    control.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while association.control_outbox_len() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unavailable_control_is_retried_without_disconnecting_before_new_session_command() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = probe.local_addr().unwrap().port();
    drop(probe);
    let client_port = server_port.saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("stale-control-recovery-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
        incarnation: NodeIncarnation::new(31).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id,
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
        incarnation: NodeIncarnation::new(32).unwrap(),
    };
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(10).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"stale-control-recovery-test/v1"),
    };
    let control = Arc::new(RecoveringControl::default());
    let client = endpoint(client_identity, descriptor.clone());
    let server = endpoint_with_control(server_identity.clone(), descriptor, control.clone());
    server.bind().await.unwrap();
    let association = client.connect_peer(server_identity).await.unwrap();

    association
        .admit_control_command(Bytes::from_static(b"term-28-heartbeat"))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while control
            .old_attempts
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(association.state(), AssociationState::Active);
    association
        .admit_control_command(Bytes::from_static(b"term-29-member-hello"))
        .unwrap();

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let applied = control
                .applied
                .lock()
                .expect("recovering control poisoned")
                .clone();
            if association.state() == AssociationState::Active
                && association.control_outbox_len() == 0
                && applied == [Bytes::from_static(b"term-29-member-hello")]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        control
            .old_attempts
            .load(std::sync::atomic::Ordering::Acquire)
            >= 2
    );
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
