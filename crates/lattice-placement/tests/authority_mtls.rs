use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use lattice_core::id::ActorId;
use lattice_core::instance::{InstanceCapacity, InstanceId, InstanceIncarnation};
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_placement::authority::{PlacementAuthority, TonicPlacementAuthority};
use lattice_placement::control::PlacementCoordinatorService;
use lattice_placement::control::proto;
use lattice_placement::control::proto::placement_coordinator_client::PlacementCoordinatorClient;
use lattice_placement::control::proto::placement_coordinator_server::PlacementCoordinatorServer;
use lattice_placement::coordination::actor::{ActivateActorRequest, PlacementCoordinator};
use lattice_placement::coordination::logic::NoopLogicControl;
use lattice_placement::coordination::singleton::ActivateSingletonRequest;
use lattice_placement::error::PlacementError;
use lattice_placement::registry::{InstanceRecord, InstanceState};
use lattice_placement::storage::memory::InMemoryPlacementStore;
use lattice_placement::storage::{
    ActorPlacementKey, LeaseId, PlacementPrefix, PlacementRevision, PlacementStore, SingletonKey,
};
use lattice_rpc::security::ServiceIdentityConfig;
use rcgen::string::Ia5String;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};

const TRUST_DOMAIN: &str = "lattice.test";
const TARGET_SERVICE: &str = "World";
const TARGET_INSTANCE: &str = "world-a";
const TARGET_INCARNATION: &str = "world-a-new-boot";
const TARGET_LEASE: LeaseId = LeaseId(101);
const REJECTION_DEADLINE: Duration = Duration::from_secs(1);
const TRANSPORT_REJECTION_CODES: &[Code] = &[
    Code::Cancelled,
    Code::DeadlineExceeded,
    Code::Unauthenticated,
    Code::Unavailable,
    Code::Unknown,
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_mtls_admission_fences_every_unverified_identity_before_mutation() {
    let pki = TestPki::generate();
    let store = ready_store().await;
    let server = TestAuthorityServer::start(&pki, store.clone()).await;

    assert_transport_rejected_without_mutation(
        connect_tls(server.address, &pki.ca_pem, None, "localhost").await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;
    assert_transport_rejected_without_mutation(
        connect_tls(
            server.address,
            &pki.ca_pem,
            Some(&pki.untrusted_client),
            "localhost",
        )
        .await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;
    assert_transport_rejected_without_mutation(
        connect_tls(
            server.address,
            &pki.untrusted_ca_pem,
            Some(&pki.valid_client),
            "localhost",
        )
        .await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;
    assert_transport_rejected_without_mutation(
        connect_tls(
            server.address,
            &pki.ca_pem,
            Some(&pki.valid_client),
            "not-localhost.invalid",
        )
        .await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;
    assert_transport_rejected_without_mutation(
        connect_plaintext(server.address).await,
        TRANSPORT_REJECTION_CODES,
        &store,
    )
    .await;

    let malformed = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.malformed_client),
        "localhost",
    )
    .await
    .expect("a trusted but malformed workload certificate completes TLS");
    assert_all_methods_rejected_without_mutation(malformed, &[Code::Unauthenticated], &store).await;

    let wrong_trust_domain = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.wrong_trust_domain_client),
        "localhost",
    )
    .await
    .expect("a client from the wrong trust domain is still signed by the transport CA");
    assert_all_methods_rejected_without_mutation(
        wrong_trust_domain,
        &[Code::PermissionDenied],
        &store,
    )
    .await;

    let wrong_identity = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.wrong_identity_client),
        "localhost",
    )
    .await
    .expect("a different trusted workload completes TLS");
    assert_all_methods_rejected_without_mutation(wrong_identity, &[Code::PermissionDenied], &store)
        .await;

    let stale_incarnation = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.stale_incarnation_client),
        "localhost",
    )
    .await
    .expect("a stale certificate for the reused instance still completes TLS");
    assert_all_methods_rejected_without_mutation(
        stale_incarnation,
        &[Code::PermissionDenied],
        &store,
    )
    .await;

    let cross_service = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.cross_service_client),
        "localhost",
    )
    .await
    .expect("a trusted workload from another service completes mutual TLS");
    let cross_service_authority = TonicPlacementAuthority::new(cross_service.clone());
    let actor = cross_service_authority
        .activate_actor(actor_request())
        .await
        .expect("an authenticated workload may request cross-service actor activation");
    assert_eq!(actor.owner, InstanceId::new(TARGET_INSTANCE));
    let singleton = cross_service_authority
        .activate_singleton(singleton_request())
        .await
        .expect("an authenticated workload may request cross-service singleton activation");
    assert_eq!(singleton.owner, InstanceId::new(TARGET_INSTANCE));
    assert_rpc_rejected_without_mutation(cross_service, &[Code::PermissionDenied], &store).await;

    let exact_identity = connect_tls(
        server.address,
        &pki.ca_pem,
        Some(&pki.valid_client),
        "localhost",
    )
    .await
    .expect("the trusted workload completes mutual TLS");
    let report = TonicPlacementAuthority::new(exact_identity)
        .drain_instance(
            ServiceKind::new(TARGET_SERVICE),
            InstanceId::new(TARGET_INSTANCE),
            InstanceIncarnation::new(TARGET_INCARNATION),
            TARGET_LEASE,
        )
        .await
        .expect("the exact authenticated workload may drain itself");
    assert_eq!(report.drained_instance, InstanceId::new(TARGET_INSTANCE));
    assert_eq!(report.migrated_actors, 1);
    assert_eq!(report.migrated_virtual_shards, 0);
    assert_eq!(target_record(&store).await.state, InstanceState::Draining);

    server.shutdown().await;
}

async fn assert_transport_rejected_without_mutation(
    result: Result<Channel, tonic::transport::Error>,
    allowed_codes: &[Code],
    store: &InMemoryPlacementStore,
) {
    let revision_before = placement_revision(store).await;
    if let Ok(channel) = result {
        let code = drain_status(channel).await.code();
        assert!(
            allowed_codes.contains(&code),
            "unexpected typed transport rejection code: {code:?}"
        );
        assert_eq!(placement_revision(store).await, revision_before);
        assert_target_ready(store).await;
        return;
    }
    assert_eq!(placement_revision(store).await, revision_before);
    assert_target_ready(store).await;
}

async fn assert_rpc_rejected_without_mutation(
    channel: Channel,
    allowed_codes: &[Code],
    store: &InMemoryPlacementStore,
) {
    let revision_before = placement_revision(store).await;
    let error = drain_error(channel).await;
    match error {
        PlacementError::PlacementAuthorityRpc { code } => assert!(
            allowed_codes.contains(&code),
            "unexpected typed authority rejection code: {code:?}"
        ),
        other => panic!("expected a typed authority RPC rejection, got {other:?}"),
    }
    assert_eq!(placement_revision(store).await, revision_before);
    assert_target_ready(store).await;
}

async fn assert_all_methods_rejected_without_mutation(
    channel: Channel,
    allowed_codes: &[Code],
    store: &InMemoryPlacementStore,
) {
    let revision_before = placement_revision(store).await;
    let authority = TonicPlacementAuthority::new(channel);
    for error in [
        bounded_error(
            authority.activate_actor(actor_request()),
            "unverified identity must not activate an actor",
        )
        .await,
        bounded_error(
            authority.activate_singleton(singleton_request()),
            "unverified identity must not activate a singleton",
        )
        .await,
        bounded_error(
            authority.drain_instance(
                ServiceKind::new(TARGET_SERVICE),
                InstanceId::new(TARGET_INSTANCE),
                InstanceIncarnation::new(TARGET_INCARNATION),
                TARGET_LEASE,
            ),
            "unverified identity must not drain an instance",
        )
        .await,
    ] {
        match error {
            PlacementError::PlacementAuthorityRpc { code } => assert!(
                allowed_codes.contains(&code),
                "unexpected typed authority rejection code: {code:?}"
            ),
            other => panic!("expected a typed authority RPC rejection, got {other:?}"),
        }
    }
    assert_eq!(placement_revision(store).await, revision_before);
    assert!(store.get_actor(&actor_key()).await.unwrap().is_none());
    assert!(
        store
            .get_singleton(&singleton_key())
            .await
            .unwrap()
            .is_none()
    );
    assert_target_ready(store).await;
}

async fn drain_error(channel: Channel) -> PlacementError {
    bounded_error(
        TonicPlacementAuthority::new(channel).drain_instance(
            ServiceKind::new(TARGET_SERVICE),
            InstanceId::new(TARGET_INSTANCE),
            InstanceIncarnation::new(TARGET_INCARNATION),
            TARGET_LEASE,
        ),
        "an unverified or mismatched workload must not drain an instance",
    )
    .await
}

async fn drain_status(channel: Channel) -> tonic::Status {
    let mut client = PlacementCoordinatorClient::new(channel);
    match tokio::time::timeout(
        REJECTION_DEADLINE,
        client.drain_instance(proto::DrainInstanceRequest {
            service_kind: TARGET_SERVICE.to_string(),
            instance_id: TARGET_INSTANCE.to_string(),
            expected_lease_id: TARGET_LEASE.0,
            instance_incarnation: TARGET_INCARNATION.to_string(),
        }),
    )
    .await
    .expect("transport rejection must arrive before the test deadline")
    {
        Ok(_) => panic!("an unverified transport must not drain an instance"),
        Err(status) => status,
    }
}

async fn bounded_error<T>(
    future: impl std::future::Future<Output = Result<T, PlacementError>>,
    expectation: &'static str,
) -> PlacementError {
    match tokio::time::timeout(REJECTION_DEADLINE, future)
        .await
        .expect("authority rejection must arrive before the test deadline")
    {
        Ok(_) => panic!("{expectation}"),
        Err(error) => error,
    }
}

fn actor_request() -> ActivateActorRequest {
    ActivateActorRequest {
        service_kind: ServiceKind::new(TARGET_SERVICE),
        actor_kind: ActorKind::new("World"),
        actor_id: ActorId::U64(7),
    }
}

fn actor_key() -> ActorPlacementKey {
    ActorPlacementKey {
        service_kind: ServiceKind::new(TARGET_SERVICE),
        actor_kind: ActorKind::new("World"),
        actor_id: ActorId::U64(7),
    }
}

fn singleton_request() -> ActivateSingletonRequest {
    ActivateSingletonRequest {
        service_kind: ServiceKind::new(TARGET_SERVICE),
        singleton_kind: ActorKind::new("SeasonManager"),
        scope: "global".to_string(),
    }
}

fn singleton_key() -> SingletonKey {
    SingletonKey {
        service_kind: ServiceKind::new(TARGET_SERVICE),
        singleton_kind: ActorKind::new("SeasonManager"),
        scope: "global".to_string(),
    }
}

async fn assert_target_ready(store: &InMemoryPlacementStore) {
    let record = target_record(store).await;
    assert_eq!(record.service_kind, ServiceKind::new(TARGET_SERVICE));
    assert_eq!(
        record.incarnation,
        InstanceIncarnation::new(TARGET_INCARNATION)
    );
    assert_eq!(record.lease_id, TARGET_LEASE);
    assert_eq!(record.state, InstanceState::Ready);
}

async fn placement_revision(store: &InMemoryPlacementStore) -> PlacementRevision {
    store
        .open_ownership_view(
            &ServiceKind::new(TARGET_SERVICE),
            &InstanceId::new(TARGET_INSTANCE),
            NonZeroUsize::new(16).unwrap(),
        )
        .await
        .expect("open coherent ownership snapshot")
        .snapshot
        .revision
}

async fn target_record(store: &InMemoryPlacementStore) -> InstanceRecord {
    store
        .get_instance(&InstanceId::new(TARGET_INSTANCE))
        .await
        .expect("read target instance")
        .expect("target instance remains present")
}

async fn ready_store() -> InMemoryPlacementStore {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/authority-mtls"));
    for (service_kind, instance_id, incarnation, lease_id) in [
        (
            TARGET_SERVICE,
            TARGET_INSTANCE,
            TARGET_INCARNATION,
            TARGET_LEASE,
        ),
        (TARGET_SERVICE, "world-b", "world-b-boot", LeaseId(102)),
        ("Player", "player-a", "player-a-boot", LeaseId(103)),
    ] {
        store
            .upsert_instance(InstanceRecord {
                service_kind: ServiceKind::new(service_kind),
                instance_id: InstanceId::new(instance_id),
                incarnation: InstanceIncarnation::new(incarnation),
                lease_id,
                advertised_endpoint: "http://127.0.0.1:50051".parse().unwrap(),
                control_endpoint: "http://127.0.0.1:50052".parse().unwrap(),
                version: "authority-mtls-test".to_string(),
                state: InstanceState::Ready,
                capacity: InstanceCapacity::default(),
                labels: BTreeMap::new(),
            })
            .await
            .expect("install ready instance");
    }
    store
}

async fn connect_tls(
    address: SocketAddr,
    server_ca_pem: &[u8],
    client_identity: Option<&PemIdentity>,
    domain_name: &str,
) -> Result<Channel, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(server_ca_pem))
        .domain_name(domain_name);
    if let Some(client_identity) = client_identity {
        tls = tls.identity(client_identity.tonic_identity());
    }
    Endpoint::from_shared(format!("https://{address}"))
        .expect("test endpoint URI")
        .tls_config(tls)
        .expect("test TLS configuration")
        .connect()
        .await
}

async fn connect_plaintext(address: SocketAddr) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_shared(format!("http://{address}"))
        .expect("test endpoint URI")
        .connect()
        .await
}

struct TestAuthorityServer {
    address: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TestAuthorityServer {
    async fn start(pki: &TestPki, store: InMemoryPlacementStore) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test authority");
        let address = listener.local_addr().expect("test authority address");
        let coordinator = PlacementCoordinator::new(store, NoopLogicControl);
        let service = PlacementCoordinatorService::authenticated(
            coordinator,
            ServiceIdentityConfig {
                trust_domain: TRUST_DOMAIN.to_string(),
            },
        )
        .expect("valid authority identity policy");
        let tls = ServerTlsConfig::new()
            .identity(pki.server.tonic_identity())
            .client_ca_root(Certificate::from_pem(pki.ca_pem.clone()));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(
            Server::builder()
                .tls_config(tls)
                .expect("test server TLS configuration")
                .add_service(PlacementCoordinatorServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                }),
        );
        Self {
            address,
            shutdown,
            task,
        }
    }

    async fn shutdown(self) {
        self.shutdown.send(()).expect("stop test authority");
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("test authority stops before deadline")
            .expect("join test authority")
            .expect("test authority exits cleanly");
    }
}

#[derive(Clone)]
struct PemIdentity {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

impl PemIdentity {
    fn tonic_identity(&self) -> Identity {
        Identity::from_pem(self.cert_pem.clone(), self.key_pem.clone())
    }
}

struct TestPki {
    ca_pem: Vec<u8>,
    untrusted_ca_pem: Vec<u8>,
    server: PemIdentity,
    valid_client: PemIdentity,
    cross_service_client: PemIdentity,
    wrong_identity_client: PemIdentity,
    stale_incarnation_client: PemIdentity,
    wrong_trust_domain_client: PemIdentity,
    malformed_client: PemIdentity,
    untrusted_client: PemIdentity,
}

impl TestPki {
    fn generate() -> Self {
        let ca = test_ca("lattice authority test CA");
        let untrusted_ca = test_ca("untrusted authority test CA");
        Self {
            ca_pem: ca.pem().into_bytes(),
            untrusted_ca_pem: untrusted_ca.pem().into_bytes(),
            server: server_identity(&ca),
            valid_client: workload_identity(
                &ca,
                "spiffe://lattice.test/svc/World/instance/world-a/incarnation/world-a-new-boot",
            ),
            cross_service_client: workload_identity(
                &ca,
                "spiffe://lattice.test/svc/Player/instance/player-a/incarnation/player-a-boot",
            ),
            wrong_identity_client: workload_identity(
                &ca,
                "spiffe://lattice.test/svc/World/instance/world-other/incarnation/world-other-boot",
            ),
            stale_incarnation_client: workload_identity(
                &ca,
                "spiffe://lattice.test/svc/World/instance/world-a/incarnation/world-a-old-boot",
            ),
            wrong_trust_domain_client: workload_identity(
                &ca,
                "spiffe://other.test/svc/World/instance/world-a/incarnation/world-a-new-boot",
            ),
            malformed_client: workload_identity(&ca, "spiffe://lattice.test/svc/World/instance"),
            untrusted_client: workload_identity(
                &untrusted_ca,
                "spiffe://lattice.test/svc/World/instance/world-a/incarnation/world-a-new-boot",
            ),
        }
    }
}

fn test_ca(common_name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.distinguished_name = distinguished_name(common_name);
    CertifiedIssuer::self_signed(params, KeyPair::generate().expect("generate test CA key"))
        .expect("generate test CA")
}

fn server_identity(ca: &CertifiedIssuer<'_, KeyPair>) -> PemIdentity {
    let key = KeyPair::generate().expect("generate test server key");
    let mut params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("valid localhost server name");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.distinguished_name = distinguished_name("lattice authority test server");
    let cert = params
        .signed_by(&key, ca)
        .expect("sign test server certificate");
    PemIdentity {
        cert_pem: cert.pem().into_bytes(),
        key_pem: key.serialize_pem().into_bytes(),
    }
}

fn workload_identity(ca: &CertifiedIssuer<'_, KeyPair>, spiffe_id: &str) -> PemIdentity {
    let key = KeyPair::generate().expect("generate test workload key");
    let mut params = CertificateParams::default();
    params.subject_alt_names = vec![SanType::URI(
        Ia5String::try_from(spiffe_id).expect("test SPIFFE ID is ASCII"),
    )];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.distinguished_name = distinguished_name("lattice authority test workload");
    let cert = params
        .signed_by(&key, ca)
        .expect("sign test workload certificate");
    PemIdentity {
        cert_pem: cert.pem().into_bytes(),
        key_pem: key.serialize_pem().into_bytes(),
    }
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name
}
