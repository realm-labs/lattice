use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lattice_core::instance::InstanceId;
use lattice_placement::control::PlacementCoordinatorService;
use lattice_placement::control::TonicLogicControl;
use lattice_placement::control::proto::placement_coordinator_server::PlacementCoordinatorServer;
use lattice_placement::coordination::actor::PlacementCoordinator;
use lattice_placement::error::PlacementError;
use lattice_placement::storage::etcd::{
    EtcdConnectionOptions, EtcdPasswordAuthentication, EtcdPlacementStore, EtcdPlacementStoreConfig,
};
use lattice_placement::storage::{CoordinatorLeadership, PlacementStore};
use lattice_rpc::security::{MtlsPeerIdentityExtractor, ServiceIdentityConfig};
use tokio::io::AsyncReadExt;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

const COORDINATOR_TLS_FILE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_COORDINATOR_TLS_PEM_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorTlsFile {
    Certificate,
    PrivateKey,
    ClientCa,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InvalidCoordinatorTlsFileReason {
    NotAbsolute,
    NotRegular,
    Empty,
    TooLarge,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum CoordinatorBootstrapError {
    #[error("invalid coordinator RPC transport security configuration")]
    InvalidTransportSecurity,
    #[error("the dangerous unauthenticated coordinator escape accepts loopback binds only")]
    InsecureUnauthenticatedTransport,
    #[error("coordinator RPC TLS {field:?} file is invalid: {reason:?}")]
    InvalidTlsFile {
        field: CoordinatorTlsFile,
        reason: InvalidCoordinatorTlsFileReason,
    },
    #[error("failed to read coordinator RPC TLS {field:?} file: {kind:?}")]
    TlsFile {
        field: CoordinatorTlsFile,
        kind: std::io::ErrorKind,
    },
    #[error("coordinator RPC TLS material is invalid")]
    InvalidTlsMaterial,
}

struct CoordinatorTlsBootstrap {
    certificate_pem: Vec<u8>,
    private_key_pem: Vec<u8>,
    client_ca_pem: Vec<u8>,
    trust_domain: String,
}

impl std::fmt::Debug for CoordinatorTlsBootstrap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoordinatorTlsBootstrap")
            .field("trust_domain", &self.trust_domain)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum CoordinatorRpcTransport {
    Authenticated(CoordinatorTlsBootstrap),
    DangerouslyUnauthenticatedLoopback,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr =
        env_value("LATTICE_COORDINATOR_ADDR", "127.0.0.1:50080").parse::<SocketAddr>()?;
    let rpc_transport = coordinator_rpc_transport_from_env(listen_addr).await?;
    let mut server = coordinator_server(&rpc_transport)?;
    let key_prefix = env_value("LATTICE_CLUSTER_PREFIX", "/lattice/default");
    let candidate_id = InstanceId::new(env_value(
        "LATTICE_COORDINATOR_ID",
        &listen_addr.to_string(),
    ));
    let endpoints = env_value("LATTICE_ETCD_ENDPOINTS", "http://127.0.0.1:2379")
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let store = EtcdPlacementStore::connect_with_connection_options(
        EtcdPlacementStoreConfig {
            key_prefix: key_prefix.clone(),
            endpoints,
            instance_lease_ttl_secs: env_i64("LATTICE_INSTANCE_LEASE_TTL_SECS", 30),
            activation_lock_ttl_secs: env_i64("LATTICE_ACTIVATION_LOCK_TTL_SECS", 30),
        },
        etcd_connection_options_from_env()?,
    )
    .await?;
    let leadership = campaign_until_leader(
        store.clone(),
        candidate_id.clone(),
        Duration::from_secs(env_u64("LATTICE_COORDINATOR_CAMPAIGN_RETRY_SECS", 5)),
    )
    .await?;
    let keepalive_store = store.clone();
    let keepalive_leadership = leadership.clone();
    let coordinator = PlacementCoordinator::new(store.clone(), TonicLogicControl);
    let reconciler = coordinator.start_all_service_lease_expiry_reconciler(Duration::from_secs(
        env_u64("LATTICE_LEASE_RECONCILE_INTERVAL_SECS", 5),
    ));
    let keepalive = keepalive_loop(keepalive_store, keepalive_leadership);

    let coordinator_service = match rpc_transport {
        CoordinatorRpcTransport::Authenticated(config) => {
            PlacementCoordinatorService::authenticated(
                coordinator,
                ServiceIdentityConfig {
                    trust_domain: config.trust_domain,
                },
            )?
        }
        CoordinatorRpcTransport::DangerouslyUnauthenticatedLoopback => {
            PlacementCoordinatorService::dangerously_allow_unauthenticated_loopback(coordinator)
        }
    };
    let server = server
        .add_service(PlacementCoordinatorServer::new(coordinator_service))
        .serve_with_shutdown(listen_addr, async {
            let _ = tokio::signal::ctrl_c().await;
        });
    tokio::select! {
        result = server => result?,
        result = keepalive => result?,
    }
    reconciler.cancel();
    store.resign_coordinator_leader(&leadership).await?;
    Ok(())
}

async fn campaign_until_leader<S>(
    store: S,
    candidate_id: InstanceId,
    retry_interval: Duration,
) -> Result<CoordinatorLeadership, PlacementError>
where
    S: PlacementStore,
{
    loop {
        if let Some(leadership) = store
            .campaign_coordinator_leader(candidate_id.clone())
            .await?
        {
            return Ok(leadership);
        }
        tokio::time::sleep(retry_interval).await;
    }
}

async fn keepalive_loop<S>(
    store: S,
    leadership: CoordinatorLeadership,
) -> Result<(), PlacementError>
where
    S: PlacementStore,
{
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        store.keepalive_coordinator_leader(&leadership).await?;
    }
}

fn env_value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_i64(name: &str, default: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

async fn coordinator_rpc_transport_from_env(
    listen_addr: SocketAddr,
) -> Result<CoordinatorRpcTransport, CoordinatorBootstrapError> {
    coordinator_rpc_transport(
        listen_addr,
        optional_coordinator_env("LATTICE_COORDINATOR_TLS_CERT_FILE")?,
        optional_coordinator_env("LATTICE_COORDINATOR_TLS_KEY_FILE")?,
        optional_coordinator_env("LATTICE_COORDINATOR_TLS_CLIENT_CA_FILE")?,
        optional_coordinator_env("LATTICE_COORDINATOR_TRUST_DOMAIN")?,
        optional_coordinator_env("LATTICE_DANGEROUSLY_ALLOW_UNAUTHENTICATED_COORDINATOR")?,
    )
    .await
}

async fn coordinator_rpc_transport(
    listen_addr: SocketAddr,
    certificate_file: Option<String>,
    private_key_file: Option<String>,
    client_ca_file: Option<String>,
    trust_domain: Option<String>,
    dangerously_allow_unauthenticated: Option<String>,
) -> Result<CoordinatorRpcTransport, CoordinatorBootstrapError> {
    let tls_field_count = [
        certificate_file.is_some(),
        private_key_file.is_some(),
        client_ca_file.is_some(),
        trust_domain.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();

    if tls_field_count == 0 {
        return match dangerously_allow_unauthenticated.as_deref() {
            Some("true") if listen_addr.ip().is_loopback() => {
                Ok(CoordinatorRpcTransport::DangerouslyUnauthenticatedLoopback)
            }
            Some("true") => Err(CoordinatorBootstrapError::InsecureUnauthenticatedTransport),
            _ => Err(CoordinatorBootstrapError::InvalidTransportSecurity),
        };
    }
    if tls_field_count != 4 || dangerously_allow_unauthenticated.is_some() {
        return Err(CoordinatorBootstrapError::InvalidTransportSecurity);
    }

    let trust_domain = trust_domain.expect("all TLS fields were counted");
    if trust_domain.trim().is_empty() || trust_domain.trim() != trust_domain {
        return Err(CoordinatorBootstrapError::InvalidTransportSecurity);
    }
    MtlsPeerIdentityExtractor::try_new(ServiceIdentityConfig {
        trust_domain: trust_domain.clone(),
    })
    .map_err(|_| CoordinatorBootstrapError::InvalidTransportSecurity)?;
    let certificate_path = PathBuf::from(certificate_file.expect("all TLS fields were counted"));
    let private_key_path = PathBuf::from(private_key_file.expect("all TLS fields were counted"));
    let client_ca_path = PathBuf::from(client_ca_file.expect("all TLS fields were counted"));
    let (certificate_pem, private_key_pem, client_ca_pem) = tokio::try_join!(
        read_coordinator_tls_pem(&certificate_path, CoordinatorTlsFile::Certificate),
        read_coordinator_tls_pem(&private_key_path, CoordinatorTlsFile::PrivateKey),
        read_coordinator_tls_pem(&client_ca_path, CoordinatorTlsFile::ClientCa),
    )?;
    Ok(CoordinatorRpcTransport::Authenticated(
        CoordinatorTlsBootstrap {
            certificate_pem,
            private_key_pem,
            client_ca_pem,
            trust_domain,
        },
    ))
}

fn coordinator_server(
    transport: &CoordinatorRpcTransport,
) -> Result<Server, CoordinatorBootstrapError> {
    let server = Server::builder();
    let CoordinatorRpcTransport::Authenticated(config) = transport else {
        return Ok(server);
    };
    let tls_config = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            config.certificate_pem.clone(),
            config.private_key_pem.clone(),
        ))
        .client_ca_root(Certificate::from_pem(config.client_ca_pem.clone()));
    server
        .tls_config(tls_config)
        .map_err(|_| CoordinatorBootstrapError::InvalidTlsMaterial)
}

async fn read_coordinator_tls_pem(
    path: &Path,
    field: CoordinatorTlsFile,
) -> Result<Vec<u8>, CoordinatorBootstrapError> {
    if !path.is_absolute() {
        return Err(CoordinatorBootstrapError::InvalidTlsFile {
            field,
            reason: InvalidCoordinatorTlsFileReason::NotAbsolute,
        });
    }
    let path = path.to_path_buf();
    let result = tokio::time::timeout(COORDINATOR_TLS_FILE_TIMEOUT, async move {
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|error| coordinator_tls_file_error(field, error))?;
        let metadata = file
            .metadata()
            .await
            .map_err(|error| coordinator_tls_file_error(field, error))?;
        if !metadata.is_file() {
            return Err(CoordinatorBootstrapError::InvalidTlsFile {
                field,
                reason: InvalidCoordinatorTlsFileReason::NotRegular,
            });
        }
        let mut bytes = Vec::with_capacity(MAX_COORDINATOR_TLS_PEM_BYTES.min(4_096));
        file.take((MAX_COORDINATOR_TLS_PEM_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| coordinator_tls_file_error(field, error))?;
        if bytes.is_empty() {
            return Err(CoordinatorBootstrapError::InvalidTlsFile {
                field,
                reason: InvalidCoordinatorTlsFileReason::Empty,
            });
        }
        if bytes.len() > MAX_COORDINATOR_TLS_PEM_BYTES {
            return Err(CoordinatorBootstrapError::InvalidTlsFile {
                field,
                reason: InvalidCoordinatorTlsFileReason::TooLarge,
            });
        }
        Ok(bytes)
    })
    .await;
    result.unwrap_or(Err(CoordinatorBootstrapError::TlsFile {
        field,
        kind: std::io::ErrorKind::TimedOut,
    }))
}

fn coordinator_tls_file_error(
    field: CoordinatorTlsFile,
    error: std::io::Error,
) -> CoordinatorBootstrapError {
    CoordinatorBootstrapError::TlsFile {
        field,
        kind: error.kind(),
    }
}

fn optional_coordinator_env(name: &str) -> Result<Option<String>, CoordinatorBootstrapError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(CoordinatorBootstrapError::InvalidTransportSecurity)
        }
    }
}

fn etcd_connection_options_from_env() -> Result<EtcdConnectionOptions, PlacementError> {
    etcd_connection_options(
        optional_env("LATTICE_ETCD_USERNAME")?,
        optional_env("LATTICE_ETCD_PASSWORD_FILE")?,
        optional_env("LATTICE_ETCD_CA_FILE")?,
        optional_env("LATTICE_ETCD_TOKEN_REFRESH_INTERVAL_SECS")?,
        optional_env("LATTICE_DANGEROUSLY_ALLOW_UNAUTHENTICATED_ETCD")?,
    )
}

fn etcd_connection_options(
    username: Option<String>,
    password_file: Option<String>,
    ca_file: Option<String>,
    token_refresh_interval_secs: Option<String>,
    dangerously_allow_unauthenticated: Option<String>,
) -> Result<EtcdConnectionOptions, PlacementError> {
    let mut options = match (
        username,
        password_file,
        dangerously_allow_unauthenticated.as_deref(),
    ) {
        (None, None, Some("true")) => Ok(EtcdConnectionOptions::dangerously_unauthenticated()),
        (Some(username), Some(password_file), None) => Ok(EtcdConnectionOptions::password_file(
            EtcdPasswordAuthentication::new(username, password_file),
        )),
        _ => Err(PlacementError::InvalidEtcdAuthentication),
    }?;
    if !options.is_authenticated() && (ca_file.is_some() || token_refresh_interval_secs.is_some()) {
        return Err(PlacementError::InvalidEtcdAuthentication);
    }
    if let Some(ca_file) = ca_file {
        options = options.with_ca_file(ca_file);
    }
    if let Some(interval) = token_refresh_interval_secs {
        let seconds = interval
            .parse::<u64>()
            .map_err(|_| PlacementError::InvalidEtcdAuthentication)?;
        options = options.with_token_refresh_interval(Duration::from_secs(seconds));
    }
    Ok(options)
}

fn optional_env(name: &str) -> Result<Option<String>, PlacementError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(PlacementError::InvalidEtcdAuthentication),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lattice_placement::storage::PlacementPrefix;
    use lattice_placement::storage::memory::InMemoryPlacementStore;
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose,
    };

    #[tokio::test]
    async fn coordinator_rpc_transport_fails_closed_for_missing_partial_mixed_and_external_plaintext_config()
     {
        let loopback = "127.0.0.1:50080".parse().unwrap();
        let external = "0.0.0.0:50080".parse().unwrap();
        assert_eq!(
            coordinator_rpc_transport(loopback, None, None, None, None, None)
                .await
                .unwrap_err(),
            CoordinatorBootstrapError::InvalidTransportSecurity
        );
        assert_eq!(
            coordinator_rpc_transport(
                loopback,
                Some("/run/secrets/coordinator.crt".to_string()),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap_err(),
            CoordinatorBootstrapError::InvalidTransportSecurity
        );
        assert_eq!(
            coordinator_rpc_transport(
                loopback,
                Some("/run/secrets/coordinator.crt".to_string()),
                Some("/run/secrets/coordinator.key".to_string()),
                Some("/run/secrets/client-ca.crt".to_string()),
                Some("lattice.test".to_string()),
                Some("true".to_string()),
            )
            .await
            .unwrap_err(),
            CoordinatorBootstrapError::InvalidTransportSecurity
        );
        assert_eq!(
            coordinator_rpc_transport(external, None, None, None, None, Some("true".to_string()),)
                .await
                .unwrap_err(),
            CoordinatorBootstrapError::InsecureUnauthenticatedTransport
        );
        assert_eq!(
            coordinator_rpc_transport(loopback, None, None, None, None, Some("false".to_string()),)
                .await
                .unwrap_err(),
            CoordinatorBootstrapError::InvalidTransportSecurity
        );
        assert!(matches!(
            coordinator_rpc_transport(loopback, None, None, None, None, Some("true".to_string()),)
                .await
                .unwrap(),
            CoordinatorRpcTransport::DangerouslyUnauthenticatedLoopback
        ));
    }

    #[tokio::test]
    async fn coordinator_rpc_transport_loads_only_complete_nonempty_absolute_tls_files() {
        let temp = tempfile::tempdir().unwrap();
        let certificate = temp.path().join("coordinator.crt");
        let private_key = temp.path().join("coordinator.key");
        let client_ca = temp.path().join("client-ca.crt");
        std::fs::write(&certificate, b"certificate pem").unwrap();
        std::fs::write(&private_key, b"private key pem").unwrap();
        std::fs::write(&client_ca, b"client ca pem").unwrap();

        let transport = coordinator_rpc_transport(
            "127.0.0.1:50080".parse().unwrap(),
            Some(certificate.to_string_lossy().into_owned()),
            Some(private_key.to_string_lossy().into_owned()),
            Some(client_ca.to_string_lossy().into_owned()),
            Some("lattice.test".to_string()),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            coordinator_server(&transport).unwrap_err(),
            CoordinatorBootstrapError::InvalidTlsMaterial,
            "TLS material must be parsed before etcd bootstrap begins"
        );
        let CoordinatorRpcTransport::Authenticated(config) = transport else {
            panic!("complete TLS configuration must enable authenticated transport");
        };
        assert_eq!(config.certificate_pem, b"certificate pem");
        assert_eq!(config.private_key_pem, b"private key pem");
        assert_eq!(config.client_ca_pem, b"client ca pem");
        assert_eq!(config.trust_domain, "lattice.test");

        assert_eq!(
            coordinator_rpc_transport(
                "127.0.0.1:50080".parse().unwrap(),
                Some(certificate.to_string_lossy().into_owned()),
                Some(private_key.to_string_lossy().into_owned()),
                Some(client_ca.to_string_lossy().into_owned()),
                Some(" ".to_string()),
                None,
            )
            .await
            .unwrap_err(),
            CoordinatorBootstrapError::InvalidTransportSecurity
        );
        assert_eq!(
            coordinator_rpc_transport(
                "127.0.0.1:50080".parse().unwrap(),
                Some(certificate.to_string_lossy().into_owned()),
                Some(private_key.to_string_lossy().into_owned()),
                Some(client_ca.to_string_lossy().into_owned()),
                Some("invalid/domain".to_string()),
                None,
            )
            .await
            .unwrap_err(),
            CoordinatorBootstrapError::InvalidTransportSecurity
        );
    }

    #[tokio::test]
    async fn coordinator_rpc_transport_accepts_generated_mtls_material() {
        let temp = tempfile::tempdir().unwrap();
        let certificate = temp.path().join("coordinator.crt");
        let private_key = temp.path().join("coordinator.key");
        let client_ca = temp.path().join("client-ca.crt");

        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let mut server_parameters = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        server_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        server_parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let server_certificate = server_parameters.signed_by(&server_key, &ca).unwrap();

        std::fs::write(&certificate, server_certificate.pem()).unwrap();
        std::fs::write(&private_key, server_key.serialize_pem()).unwrap();
        std::fs::write(&client_ca, ca.pem()).unwrap();

        let transport = coordinator_rpc_transport(
            "127.0.0.1:50080".parse().unwrap(),
            Some(certificate.to_string_lossy().into_owned()),
            Some(private_key.to_string_lossy().into_owned()),
            Some(client_ca.to_string_lossy().into_owned()),
            Some("lattice.test".to_string()),
            None,
        )
        .await
        .expect("generated mTLS files satisfy fail-closed bootstrap");

        coordinator_server(&transport).expect("generated mTLS material builds the tonic acceptor");
        let debug = format!("{transport:?}");
        assert!(!debug.contains("PRIVATE KEY"));
        assert!(!debug.contains("CERTIFICATE"));
    }

    #[tokio::test]
    async fn coordinator_tls_file_reads_are_absolute_regular_nonempty_bounded_and_redacted() {
        assert_eq!(
            read_coordinator_tls_pem(Path::new("relative.pem"), CoordinatorTlsFile::Certificate)
                .await
                .unwrap_err(),
            CoordinatorBootstrapError::InvalidTlsFile {
                field: CoordinatorTlsFile::Certificate,
                reason: InvalidCoordinatorTlsFileReason::NotAbsolute,
            }
        );

        let temp = tempfile::tempdir().unwrap();
        assert_eq!(
            read_coordinator_tls_pem(temp.path(), CoordinatorTlsFile::ClientCa)
                .await
                .unwrap_err(),
            CoordinatorBootstrapError::InvalidTlsFile {
                field: CoordinatorTlsFile::ClientCa,
                reason: InvalidCoordinatorTlsFileReason::NotRegular,
            }
        );

        let empty = temp.path().join("empty.pem");
        std::fs::write(&empty, []).unwrap();
        assert_eq!(
            read_coordinator_tls_pem(&empty, CoordinatorTlsFile::PrivateKey)
                .await
                .unwrap_err(),
            CoordinatorBootstrapError::InvalidTlsFile {
                field: CoordinatorTlsFile::PrivateKey,
                reason: InvalidCoordinatorTlsFileReason::Empty,
            }
        );

        let maximum = temp.path().join("maximum.pem");
        std::fs::write(&maximum, vec![b'x'; MAX_COORDINATOR_TLS_PEM_BYTES]).unwrap();
        assert_eq!(
            read_coordinator_tls_pem(&maximum, CoordinatorTlsFile::Certificate)
                .await
                .unwrap()
                .len(),
            MAX_COORDINATOR_TLS_PEM_BYTES
        );

        let oversized = temp.path().join("oversized.pem");
        std::fs::write(&oversized, vec![b'x'; MAX_COORDINATOR_TLS_PEM_BYTES + 1]).unwrap();
        assert_eq!(
            read_coordinator_tls_pem(&oversized, CoordinatorTlsFile::Certificate)
                .await
                .unwrap_err(),
            CoordinatorBootstrapError::InvalidTlsFile {
                field: CoordinatorTlsFile::Certificate,
                reason: InvalidCoordinatorTlsFileReason::TooLarge,
            }
        );

        let secret_path = temp.path().join("do-not-leak-this-path.pem");
        let error = read_coordinator_tls_pem(&secret_path, CoordinatorTlsFile::ClientCa)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CoordinatorBootstrapError::TlsFile {
                field: CoordinatorTlsFile::ClientCa,
                kind: std::io::ErrorKind::NotFound,
            }
        ));
        assert!(!error.to_string().contains("do-not-leak-this-path"));
    }

    #[tokio::test]
    async fn campaign_until_leader_waits_and_recampaigns_as_standby() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/coordinator-bin"));
        let first = store
            .campaign_coordinator_leader(InstanceId::new("coordinator-a"))
            .await
            .unwrap()
            .unwrap();
        let release_store = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            release_store
                .resign_coordinator_leader(&first)
                .await
                .unwrap();
        });

        let leadership = tokio::time::timeout(
            Duration::from_secs(1),
            campaign_until_leader(
                store,
                InstanceId::new("coordinator-b"),
                Duration::from_millis(1),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(leadership.candidate_id, InstanceId::new("coordinator-b"));
    }

    #[test]
    fn coordinator_etcd_credentials_require_both_environment_values() {
        let options = |username, password_file, dangerous| {
            etcd_connection_options(username, password_file, None, None, dangerous)
        };
        assert_eq!(
            options(None, None, None).unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert!(
            !options(None, None, Some("true".to_string()))
                .unwrap()
                .is_authenticated()
        );
        assert!(
            options(
                Some("authority".to_string()),
                Some("/run/secrets/etcd-password".to_string()),
                None,
            )
            .unwrap()
            .is_authenticated()
        );
        assert_eq!(
            options(Some("authority".to_string()), None, None).unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert_eq!(
            options(None, Some("/run/secrets/etcd-password".to_string()), None,).unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert_eq!(
            options(None, None, Some("false".to_string())).unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert_eq!(
            options(
                Some("authority".to_string()),
                Some("/run/secrets/etcd-password".to_string()),
                Some("true".to_string()),
            )
            .unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert!(
            etcd_connection_options(
                Some("authority".to_string()),
                Some("/run/secrets/etcd-password".to_string()),
                Some("/run/secrets/etcd-ca.pem".to_string()),
                Some("15".to_string()),
                None,
            )
            .unwrap()
            .is_authenticated()
        );
        assert_eq!(
            etcd_connection_options(
                None,
                None,
                Some("/run/secrets/etcd-ca.pem".to_string()),
                None,
                Some("true".to_string()),
            )
            .unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
        assert_eq!(
            etcd_connection_options(
                Some("authority".to_string()),
                Some("/run/secrets/etcd-password".to_string()),
                None,
                Some("not-a-duration".to_string()),
                None,
            )
            .unwrap_err(),
            PlacementError::InvalidEtcdAuthentication
        );
    }
}
