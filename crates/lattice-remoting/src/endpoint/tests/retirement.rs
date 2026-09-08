use tokio::io::AsyncReadExt;

use super::*;

async fn endpoints(config: RemotingConfig) -> (Arc<RemotingEndpoint>, Arc<RemotingEndpoint>) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let identity = |name: &str, port, incarnation| NodeIdentity {
        cluster_id: ClusterId::new("retirement").unwrap(),
        node_id: name.to_owned(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    };
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(7).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"retirement/v1"),
    };
    let client = endpoint_with_config(
        identity("client", port - 1, 1),
        descriptor.clone(),
        Arc::new(RejectControlDispatch),
        config.clone(),
    );
    let server = endpoint_with_config(
        identity("server", port, 2),
        descriptor,
        Arc::new(RejectControlDispatch),
        config,
    );
    server.bind().await.unwrap();
    (client, server)
}

async fn released(endpoint: &RemotingEndpoint) {
    tokio::time::timeout(Duration::from_millis(500), async {
        while endpoint.open_connection_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retirement must release permits without endpoint shutdown or a reconnect timeout");
}

#[tokio::test]
async fn retiring_an_active_association_during_backoff_reclaims_all_lanes() {
    let (client, server) = endpoints(RemotingConfig {
        reconnect_backoff_min: Duration::from_secs(2),
        reconnect_backoff_max: Duration::from_secs(2),
        heartbeat_interval: Duration::from_millis(20),
        ..RemotingConfig::default()
    })
    .await;
    let old = client.connect_peer(server.local.clone()).await.unwrap();
    assert!(old.has_activated());
    server.shutdown().await.unwrap();
    wait_until(|| old.attached_lane_count() == 0).await;
    assert!(client.open_connection_count() > 0);

    // Removal is sufficient even after a task has missed every disconnect broadcast.
    assert!(client.associations.remove(old.key(), old.id()));
    released(&client).await;
    assert!(client.associations.is_empty());
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn retiring_an_association_cancels_an_inflight_reconnect_handshake() {
    let (client, server) = endpoints(RemotingConfig {
        connect_timeout: Duration::from_secs(10),
        reconnect_backoff_min: Duration::from_millis(25),
        reconnect_backoff_max: Duration::from_millis(25),
        heartbeat_interval: Duration::from_millis(20),
        ..RemotingConfig::default()
    })
    .await;
    let old = client.connect_peer(server.local.clone()).await.unwrap();
    server.shutdown().await.unwrap();
    let blackhole = TcpListener::bind(("127.0.0.1", server.local.address.port()))
        .await
        .unwrap();
    // Accept but do not negotiate: the reconnect now owns a socket and waits for its ACK.
    let (_stalled, _) = tokio::time::timeout(Duration::from_secs(2), blackhole.accept())
        .await
        .unwrap()
        .unwrap();
    assert!(client.associations.remove(old.key(), old.id()));
    released(&client).await;
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn retiring_an_association_wakes_idle_lanes() {
    let (client, server) = endpoints(RemotingConfig {
        idle_data_connection_timeout: Duration::from_millis(40),
        heartbeat_interval: Duration::from_millis(10),
        ..RemotingConfig::default()
    })
    .await;
    let old = client.connect_peer(server.local.clone()).await.unwrap();
    wait_until(|| client.open_connection_count() == 1 && old.attached_lane_count() == 1).await;
    assert!(client.associations.remove(old.key(), old.id()));
    released(&client).await;
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn silent_inbound_connections_expire_and_release_the_connection_cap() {
    let (client, server) = endpoints(RemotingConfig {
        max_associations: 1,
        establishing_timeout: Duration::from_millis(150),
        ..RemotingConfig::default()
    })
    .await;
    let address = ("127.0.0.1", server.local.address.port());
    let limit = server.config.required_socket_budget() - 1;
    let mut sockets = Vec::new();
    for _ in 0..limit {
        sockets.push(TcpStream::connect(address).await.unwrap());
    }
    wait_until(|| server.open_connection_count() == limit).await;
    released(&server).await;
    for mut socket in sockets {
        assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
    }
    let association = client.connect_peer(server.local.clone()).await.unwrap();
    assert_eq!(association.state(), AssociationState::Active);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn receiving_a_handshake_does_not_restart_the_inbound_setup_deadline() {
    let (client, server) = endpoints(RemotingConfig {
        establishing_timeout: Duration::from_millis(600),
        ..RemotingConfig::default()
    })
    .await;
    let stream = TcpStream::connect(("127.0.0.1", server.local.address.port()))
        .await
        .unwrap();
    wait_until(|| server.open_connection_count() == 1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut connection = FramedConnection::new(
        stream,
        FrameCodec::new(server.config.max_frame_size).unwrap(),
    );
    connection
        .write_frame(
            &Handshake {
                source: client.local.clone(),
                expected_remote: server.local.clone(),
                association_id: AssociationId::generate(),
                lane: LaneKind::Control,
                connection_nonce: 1,
                maximum_frame_size: server.config.max_frame_size,
                features: FeatureBits::REQUIRED_V3,
            }
            .to_frame(),
        )
        .await
        .unwrap();
    assert_eq!(
        connection.read_frame().await.unwrap().kind,
        FrameKind::HandshakeAck
    );
    // Leave the catalogue unsent. Only the original accept deadline may bound this read.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), connection.read_frame())
            .await
            .unwrap()
            .is_err()
    );
    released(&server).await;
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
