use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_core::actor_ref::{ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId};
use lattice_core::coordinator::CoordinatorScope;
use lattice_remoting::association::{AssociationManager, AssociationState};
use lattice_remoting::bootstrap::{
    BootstrapHandler, BootstrapLeader, BootstrapProbeTarget, BootstrapRejectionCode,
    BootstrapRequest, BootstrapResult, BootstrapRoute,
};
use lattice_remoting::config::RemotingConfig;
use lattice_remoting::control::RejectControlDispatch;
use lattice_remoting::endpoint::{EndpointSecurity, RemotingEndpoint};
use lattice_remoting::handshake::FeatureBits;
use lattice_remoting::messaging::error::RemoteMessageError;
use lattice_remoting::messaging::inbound::InboundDispatch;
use lattice_remoting::messaging::outbound::OutboundMessaging;
use lattice_remoting::messaging::target::ExactActorTarget;
use lattice_remoting::protocol::{ProtocolDescriptor, ProtocolFingerprint};
use lattice_remoting::transport::connect_tcp;
use lattice_remoting::wire::{Frame, FrameCodec, FrameKind};
use rcgen::{CertificateParams, KeyPair, SanType};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::rustls::{
    ClientConfig, RootCertStore, ServerConfig, client::WebPkiServerVerifier,
    server::WebPkiClientVerifier,
};

struct RejectDispatch;

#[async_trait]
impl InboundDispatch for RejectDispatch {
    async fn tell(
        &self,
        _sender: Option<ActorRef>,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::StaleActivation)
    }

    async fn ask(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::StaleActivation)
    }
}

#[tokio::test]
async fn tcp_probe_discovers_exact_identity_without_creating_association() {
    let server_port = free_port().await;
    let cluster = ClusterId::new("probe-test").unwrap();
    let client_identity = identity(cluster.clone(), "client", 1, server_port - 1);
    let server_identity = identity(cluster, "server", 2, server_port);
    let (server, server_manager) = endpoint(server_identity.clone());
    let (client, client_manager) = endpoint(client_identity);
    server.bind().await.unwrap();

    let response = client
        .probe_candidate(target(&server_identity, Some("server")))
        .await
        .unwrap();

    assert_eq!(response.remote_identity(), Some(&server_identity));
    assert!(server_manager.is_empty());
    assert!(client_manager.is_empty());
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn simultaneous_probes_remain_probe_only() {
    let server_port = free_port().await;
    let cluster = ClusterId::new("simultaneous-probe").unwrap();
    let server_identity = identity(cluster.clone(), "server", 2, server_port);
    let (server, server_manager) = endpoint(server_identity.clone());
    let (client, client_manager) = endpoint(identity(cluster, "client", 1, server_port - 1));
    server.bind().await.unwrap();

    let target = target(&server_identity, Some("server"));
    let (one, two, three, four) = tokio::join!(
        client.probe_candidate(target.clone()),
        client.probe_candidate(target.clone()),
        client.probe_candidate(target.clone()),
        client.probe_candidate(target),
    );

    for response in [one, two, three, four] {
        assert_eq!(response.unwrap().remote_identity(), Some(&server_identity));
    }
    assert!(server_manager.is_empty());
    assert!(client_manager.is_empty());
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn expected_node_and_cluster_mismatch_are_stably_rejected() {
    let server_port = free_port().await;
    let server_identity = identity(
        ClusterId::new("server-cluster").unwrap(),
        "server",
        2,
        server_port,
    );
    let (server, server_manager) = endpoint(server_identity.clone());
    let (wrong_cluster, wrong_cluster_manager) = endpoint(identity(
        ClusterId::new("other-cluster").unwrap(),
        "client",
        1,
        server_port - 1,
    ));
    server.bind().await.unwrap();

    let cluster_response = wrong_cluster
        .probe_candidate(target(&server_identity, None))
        .await
        .unwrap();
    assert!(matches!(
        cluster_response.result,
        BootstrapResult::Rejected {
            code: BootstrapRejectionCode::ClusterMismatch
        }
    ));

    let (same_cluster, same_cluster_manager) = endpoint(identity(
        server_identity.cluster_id.clone(),
        "client-2",
        3,
        server_port - 2,
    ));
    let node_response = same_cluster
        .probe_candidate(target(&server_identity, Some("not-server")))
        .await
        .unwrap();
    assert!(matches!(
        node_response.result,
        BootstrapResult::Rejected {
            code: BootstrapRejectionCode::ExpectedNodeMismatch
        }
    ));
    assert!(server_manager.is_empty());
    assert!(wrong_cluster_manager.is_empty());
    assert!(same_cluster_manager.is_empty());
    wrong_cluster.shutdown().await.unwrap();
    same_cluster.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordinary_member_returns_authoritative_leader_redirect() {
    let server_port = free_port().await;
    let cluster = ClusterId::new("redirect-test").unwrap();
    let server_identity = identity(cluster.clone(), "member", 2, server_port);
    let leader = BootstrapLeader {
        scope: CoordinatorScope::Membership,
        identity: identity(cluster.clone(), "leader", 4, server_port + 10),
        term: 7,
        protocol_generation: 3,
    };
    let (server, _) = endpoint(server_identity.clone());
    server.install_bootstrap_handler(Arc::new(RedirectHandler {
        leader: leader.clone(),
    }));
    let (client, _) = endpoint(identity(cluster, "client", 1, server_port - 1));
    server.bind().await.unwrap();

    let response = client
        .probe_candidate(target(&server_identity, None))
        .await
        .unwrap();

    assert!(matches!(
        response.result,
        BootstrapResult::Redirect { leader: actual, .. } if actual == leader
    ));
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn leader_election_returns_bounded_retry_after() {
    let server_port = free_port().await;
    let cluster = ClusterId::new("retry-test").unwrap();
    let server_identity = identity(cluster.clone(), "member", 2, server_port);
    let (server, _) = endpoint(server_identity.clone());
    server.install_bootstrap_handler(Arc::new(RetryHandler));
    let (client, _) = endpoint(identity(cluster, "client", 1, server_port - 1));
    server.bind().await.unwrap();

    let response = client
        .probe_candidate(target(&server_identity, None))
        .await
        .unwrap();

    assert!(matches!(
        response.result,
        BootstrapResult::RetryAfter { delay, .. } if delay == Duration::from_millis(500)
    ));
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn reverse_dial_uses_exact_probing_identity() {
    let (low_port, high_port) = ordered_free_ports().await;
    let cluster = ClusterId::new("reverse-test").unwrap();
    let server_identity = identity(cluster.clone(), "server", 2, low_port);
    let client_identity = identity(cluster, "client", 1, high_port);
    let (server, server_manager) = endpoint(server_identity.clone());
    let (client, client_manager) = endpoint(client_identity.clone());
    server.bind().await.unwrap();
    client.bind().await.unwrap();

    let response = client
        .probe_candidate(target(&server_identity, None))
        .await
        .unwrap();
    assert!(matches!(
        response.result,
        BootstrapResult::ReverseDial { .. }
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let active = server_manager
                .get_exact(
                    &client_identity.cluster_id,
                    &client_identity.address,
                    client_identity.incarnation,
                )
                .is_some_and(|association| association.state() == AssociationState::Active);
            if active {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(server_manager.len(), 1);
    assert_eq!(client_manager.len(), 1);
    server.shutdown().await.unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn business_frame_before_association_is_rejected_without_registry_entry() {
    let server_port = free_port().await;
    let server_identity = identity(
        ClusterId::new("frame-test").unwrap(),
        "server",
        2,
        server_port,
    );
    let (server, manager) = endpoint(server_identity.clone());
    server.bind().await.unwrap();
    let mut connection = connect_tcp(
        &server_identity.address,
        FrameCodec::new(RemotingConfig::default().max_frame_size).unwrap(),
    )
    .await
    .unwrap();

    connection
        .write_frame(&Frame {
            kind: FrameKind::Tell,
            payload: Bytes::new(),
        })
        .await
        .unwrap();
    assert!(connection.read_frame().await.is_err());
    assert!(manager.is_empty());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_required_bootstrap_feature_is_stably_rejected() {
    let server_port = free_port().await;
    let cluster = ClusterId::new("feature-test").unwrap();
    let server_identity = identity(cluster.clone(), "server", 2, server_port);
    let client_identity = identity(cluster.clone(), "client", 1, server_port - 1);
    let (server, manager) = endpoint(server_identity.clone());
    server.bind().await.unwrap();
    let mut connection = connect_tcp(
        &server_identity.address,
        FrameCodec::new(RemotingConfig::default().max_frame_size).unwrap(),
    )
    .await
    .unwrap();
    let mut request =
        BootstrapRequest::new(CoordinatorScope::Membership, client_identity, cluster, None);
    request.features = FeatureBits::NONE;

    connection.write_frame(&request.to_frame()).await.unwrap();
    let response = lattice_remoting::bootstrap::BootstrapResponse::from_frame(
        &connection.read_frame().await.unwrap(),
    )
    .unwrap();

    assert!(matches!(
        response.result,
        BootstrapResult::Rejected {
            code: BootstrapRejectionCode::MissingRequiredFeature
        }
    ));
    assert!(manager.is_empty());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn validated_probe_does_not_retarget_existing_incarnation() {
    let server_port = free_port().await;
    let cluster = ClusterId::new("reuse-test").unwrap();
    let old_identity = identity(cluster.clone(), "server", 40, server_port);
    let replacement_identity = identity(cluster.clone(), "server", 41, server_port);
    let client_identity = identity(cluster.clone(), "client", 1, server_port - 1);
    let (server, _) = endpoint(replacement_identity.clone());
    let (client, client_manager) = endpoint(client_identity);
    let old_association = client_manager
        .get_or_create(
            cluster.clone(),
            old_identity.address.clone(),
            old_identity.incarnation,
        )
        .unwrap();
    server.bind().await.unwrap();

    let response = client
        .probe_candidate(target(&replacement_identity, Some("server")))
        .await
        .unwrap();

    assert_eq!(response.remote_identity(), Some(&replacement_identity));
    assert!(
        client_manager
            .get_exact(&cluster, &old_identity.address, old_identity.incarnation)
            .is_some()
    );
    assert!(
        client_manager
            .get_exact(
                &cluster,
                &replacement_identity.address,
                replacement_identity.incarnation,
            )
            .is_none()
    );
    assert!(
        client_manager
            .get_or_create(
                cluster.clone(),
                replacement_identity.address.clone(),
                replacement_identity.incarnation,
            )
            .is_err()
    );

    assert_eq!(
        client_manager.replace_remote_incarnation(
            replacement_identity.address.clone(),
            replacement_identity.incarnation,
        ),
        1
    );
    assert_eq!(old_association.state(), AssociationState::Closed);
    assert!(
        client_manager
            .get_or_create(
                cluster,
                replacement_identity.address.clone(),
                replacement_identity.incarnation,
            )
            .is_ok()
    );
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn tls_probe_binds_returned_identity_to_certificate_incarnation() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let server_port = free_port().await;
    let cluster = ClusterId::new("tls-probe").unwrap();
    let client_identity = identity(cluster.clone(), "client", 11, server_port - 1);
    let server_identity = identity(cluster, "server", 12, server_port);
    let (client_security, server_security) =
        tls_security_pair(&client_identity, &server_identity, &server_identity);
    let (server, server_manager) = endpoint_with_security(server_identity.clone(), server_security);
    let (client, client_manager) = endpoint_with_security(client_identity, client_security);
    server.bind().await.unwrap();

    let response = client
        .probe_candidate(BootstrapProbeTarget {
            scope: CoordinatorScope::Membership,
            address: server_identity.address.clone(),
            expected_node_id: Some("server".to_string()),
            tls_server_name: Some("lattice.test".to_string()),
        })
        .await
        .unwrap();

    assert_eq!(response.remote_identity(), Some(&server_identity));
    assert!(server_manager.is_empty());
    assert!(client_manager.is_empty());
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn tls_probe_rejects_certificate_for_different_incarnation() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let server_port = free_port().await;
    let cluster = ClusterId::new("tls-mismatch").unwrap();
    let client_identity = identity(cluster.clone(), "client", 21, server_port - 1);
    let server_identity = identity(cluster.clone(), "server", 22, server_port);
    let certificate_identity = identity(cluster, "server", 23, server_port);
    let (client_security, server_security) =
        tls_security_pair(&client_identity, &server_identity, &certificate_identity);
    let (server, server_manager) = endpoint_with_security(server_identity.clone(), server_security);
    let (client, client_manager) = endpoint_with_security(client_identity, client_security);
    server.bind().await.unwrap();

    let result = client
        .probe_candidate(BootstrapProbeTarget {
            scope: CoordinatorScope::Membership,
            address: server_identity.address.clone(),
            expected_node_id: Some("server".to_string()),
            tls_server_name: Some("lattice.test".to_string()),
        })
        .await;

    assert!(result.is_err());
    assert!(server_manager.is_empty());
    assert!(client_manager.is_empty());
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[derive(Clone)]
struct RedirectHandler {
    leader: BootstrapLeader,
}

struct RetryHandler;

impl BootstrapHandler for RetryHandler {
    fn route(&self, _request: &BootstrapRequest) -> BootstrapRoute {
        BootstrapRoute::RetryAfter {
            delay: Duration::from_millis(500),
            reason: "leader election in progress".to_string(),
        }
    }
}

impl BootstrapHandler for RedirectHandler {
    fn route(&self, _request: &BootstrapRequest) -> BootstrapRoute {
        BootstrapRoute::Redirect {
            leader: self.leader.clone(),
        }
    }
}

fn endpoint(
    identity: lattice_remoting::handshake::NodeIdentity,
) -> (Arc<RemotingEndpoint>, Arc<AssociationManager>) {
    let config = RemotingConfig {
        heartbeat_interval: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let manager = Arc::new(
        AssociationManager::new(
            identity.address.clone(),
            identity.incarnation,
            config.clone(),
        )
        .unwrap(),
    );
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(1).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"bootstrap-probe/v1"),
    };
    let endpoint = Arc::new(
        RemotingEndpoint::new(
            identity,
            config,
            manager.clone(),
            Arc::new(OutboundMessaging::new(16).unwrap()),
            Arc::new(RejectDispatch),
            vec![descriptor],
        )
        .unwrap(),
    );
    (endpoint, manager)
}

fn endpoint_with_security(
    identity: lattice_remoting::handshake::NodeIdentity,
    security: EndpointSecurity,
) -> (Arc<RemotingEndpoint>, Arc<AssociationManager>) {
    let config = RemotingConfig {
        heartbeat_interval: Duration::from_millis(50),
        shutdown_timeout: Duration::from_secs(2),
        ..RemotingConfig::default()
    };
    let manager = Arc::new(
        AssociationManager::new(
            identity.address.clone(),
            identity.incarnation,
            config.clone(),
        )
        .unwrap(),
    );
    let descriptor = ProtocolDescriptor {
        protocol_id: ProtocolId::new(1).unwrap(),
        fingerprint: ProtocolFingerprint::digest(b"bootstrap-probe/v1"),
    };
    let endpoint = Arc::new(
        RemotingEndpoint::new_with_control_and_security(
            identity,
            config,
            manager.clone(),
            Arc::new(OutboundMessaging::new(16).unwrap()),
            Arc::new(RejectDispatch),
            Arc::new(RejectControlDispatch),
            vec![descriptor],
            Some(security),
        )
        .unwrap(),
    );
    (endpoint, manager)
}

fn tls_security_pair(
    client_identity: &lattice_remoting::handshake::NodeIdentity,
    server_identity: &lattice_remoting::handshake::NodeIdentity,
    server_certificate_identity: &lattice_remoting::handshake::NodeIdentity,
) -> (EndpointSecurity, EndpointSecurity) {
    let (client_certificate, client_key) = test_certificate(client_identity);
    let (server_certificate, server_key) = test_certificate(server_certificate_identity);
    let provider = Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider());

    let outbound_client_config = client_config(
        provider.clone(),
        server_certificate.clone(),
        client_certificate.clone(),
        client_key.clone_key(),
    );
    let inbound_server_config = server_config(
        provider.clone(),
        client_certificate.clone(),
        server_certificate.clone(),
        server_key.clone_key(),
    );
    let reverse_client_config = client_config(
        provider.clone(),
        client_certificate.clone(),
        server_certificate.clone(),
        server_key,
    );
    let reverse_server_config =
        server_config(provider, server_certificate, client_certificate, client_key);
    let client_security = EndpointSecurity {
        client: Arc::new(outbound_client_config),
        server: Arc::new(reverse_server_config),
        server_name: "lattice.test".to_string(),
    };
    let server_security = EndpointSecurity {
        client: Arc::new(reverse_client_config),
        server: Arc::new(inbound_server_config),
        server_name: "lattice.test".to_string(),
    };
    assert_eq!(server_identity.node_id, server_certificate_identity.node_id);
    (client_security, server_security)
}

fn client_config(
    provider: Arc<tokio_rustls::rustls::crypto::CryptoProvider>,
    trusted: CertificateDer<'static>,
    identity: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots.add(trusted).unwrap();
    let verifier = WebPkiServerVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();
    ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(vec![identity], key)
        .unwrap()
}

fn server_config(
    provider: Arc<tokio_rustls::rustls::crypto::CryptoProvider>,
    trusted: CertificateDer<'static>,
    identity: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> ServerConfig {
    let mut roots = RootCertStore::empty();
    roots.add(trusted).unwrap();
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .unwrap();
    ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![identity], key)
        .unwrap()
}

fn test_certificate(
    identity: &lattice_remoting::handshake::NodeIdentity,
) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let mut params = CertificateParams::new(vec!["lattice.test".to_string()]).unwrap();
    params.subject_alt_names.push(SanType::URI(
        format!(
            "spiffe://{}/node/{}/{:032x}",
            identity.cluster_id.as_str(),
            identity.node_id,
            identity.incarnation.get()
        )
        .try_into()
        .unwrap(),
    ));
    let key = KeyPair::generate().unwrap();
    let certificate = params.self_signed(&key).unwrap();
    (
        certificate.der().clone(),
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
    )
}

fn identity(
    cluster_id: ClusterId,
    node_id: &str,
    incarnation: u128,
    port: u16,
) -> lattice_remoting::handshake::NodeIdentity {
    lattice_remoting::handshake::NodeIdentity {
        cluster_id,
        node_id: node_id.to_string(),
        address: NodeAddress::new("127.0.0.1", port).unwrap(),
        incarnation: NodeIncarnation::new(incarnation).unwrap(),
    }
}

fn target(
    identity: &lattice_remoting::handshake::NodeIdentity,
    expected_node_id: Option<&str>,
) -> BootstrapProbeTarget {
    BootstrapProbeTarget {
        scope: CoordinatorScope::Membership,
        address: identity.address.clone(),
        expected_node_id: expected_node_id.map(str::to_string),
        tls_server_name: None,
    }
}

async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn ordered_free_ports() -> (u16, u16) {
    let first = free_port().await;
    let second = free_port().await;
    (first.min(second), first.max(second))
}
