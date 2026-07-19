use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{
    ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
};
use tokio::net::TcpListener;

use super::RemotingEndpoint;
use crate::{
    association::AssociationManager,
    config::RemotingConfig,
    control::RejectControlDispatch,
    handshake::NodeIdentity,
    messaging::{
        error::RemoteMessageError,
        inbound::InboundDispatch,
        outbound::{OutboundMessage, OutboundMessaging},
        target::{ExactActorTarget, SenderIdentity},
    },
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
};

struct EchoDispatch;

#[async_trait]
impl InboundDispatch for EchoDispatch {
    async fn tell(
        &self,
        _sender: Option<ActorRef>,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Ok(())
    }

    async fn ask(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        if Instant::now() >= deadline {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        Ok(payload)
    }
}

fn endpoint(
    identity: NodeIdentity,
    protocol: ProtocolDescriptor,
    config: RemotingConfig,
) -> (
    Arc<RemotingEndpoint>,
    Arc<AssociationManager>,
    Arc<OutboundMessaging>,
) {
    let manager = Arc::new(
        AssociationManager::new(
            identity.address.clone(),
            identity.incarnation,
            config.clone(),
        )
        .unwrap(),
    );
    let messaging = Arc::new(OutboundMessaging::new(32).unwrap());
    let endpoint = Arc::new(
        RemotingEndpoint::builder(
            identity,
            config,
            manager.clone(),
            messaging.clone(),
            Arc::new(EchoDispatch),
        )
        .control_dispatch(Arc::new(RejectControlDispatch))
        .catalogue(vec![protocol])
        .build()
        .unwrap(),
    );
    (endpoint, manager, messaging)
}

#[tokio::test]
async fn idle_data_lanes_sleep_until_either_side_wakes_them() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_port = listener.local_addr().unwrap().port();
    drop(listener);
    let client_port = server_port.saturating_sub(1).max(1024);
    let cluster_id = ClusterId::new("idle-lane-test").unwrap();
    let client_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "client".to_owned(),
        address: NodeAddress::new("127.0.0.1", client_port).unwrap(),
        incarnation: NodeIncarnation::new(1).unwrap(),
    };
    let server_identity = NodeIdentity {
        cluster_id: cluster_id.clone(),
        node_id: "server".to_owned(),
        address: NodeAddress::new("127.0.0.1", server_port).unwrap(),
        incarnation: NodeIncarnation::new(2).unwrap(),
    };
    let protocol_id = ProtocolId::new(7).unwrap();
    let fingerprint = ProtocolFingerprint::digest(b"idle-lane-test/v1");
    let descriptor = ProtocolDescriptor {
        protocol_id,
        fingerprint,
    };
    let config = RemotingConfig {
        heartbeat_interval: Duration::from_millis(25),
        idle_data_connection_timeout: Duration::from_millis(75),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let (client, _client_manager, client_messaging) =
        endpoint(client_identity.clone(), descriptor.clone(), config.clone());
    let (server, server_manager, server_messaging) =
        endpoint(server_identity.clone(), descriptor, config);
    server.bind().await.unwrap();
    let association = client.connect_peer(server_identity.clone()).await.unwrap();
    wait_for_data_lanes_to_sleep(&client, &association).await;

    let target = ActorRef::new(
        cluster_id.clone(),
        server_identity.address,
        server_identity.incarnation,
        ActorPath::user(["user", "echo"]).unwrap(),
        ActivationId::new(server_identity.incarnation, 1).unwrap(),
        protocol_id,
    )
    .unwrap();
    let reply = client_messaging
        .ask(
            &association,
            &SenderIdentity::Process(9),
            &target,
            OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"wake")),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(reply, Bytes::from_static(b"wake"));
    wait_for_data_lanes_to_sleep(&client, &association).await;

    let reverse_association = server_manager
        .get_exact(
            &cluster_id,
            &client_identity.address,
            client_identity.incarnation,
        )
        .unwrap();
    let reverse_target = ActorRef::new(
        cluster_id,
        client_identity.address,
        client_identity.incarnation,
        ActorPath::user(["user", "reverse-echo"]).unwrap(),
        ActivationId::new(client_identity.incarnation, 2).unwrap(),
        protocol_id,
    )
    .unwrap();
    let reverse_reply = server_messaging
        .ask(
            &reverse_association,
            &SenderIdentity::Process(10),
            &reverse_target,
            OutboundMessage::new(fingerprint, 1, Bytes::from_static(b"reverse-wake")),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert_eq!(reverse_reply, Bytes::from_static(b"reverse-wake"));
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

async fn wait_for_data_lanes_to_sleep(
    endpoint: &RemotingEndpoint,
    association: &crate::association::Association,
) {
    let slept = tokio::time::timeout(Duration::from_secs(2), async {
        while endpoint.open_connection_count() != 1 || association.attached_lane_count() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        slept.is_ok(),
        "connections={}, lanes={}",
        endpoint.open_connection_count(),
        association.attached_lane_count()
    );
}
