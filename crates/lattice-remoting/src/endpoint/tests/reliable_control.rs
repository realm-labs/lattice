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
