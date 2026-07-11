use std::fmt;
use std::fs;
use std::path::Path;

use http::Uri;
use lattice_core::instance::{InstanceId, InstanceIncarnation};
use lattice_core::kind::ServiceKind;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::{Request, Status};
use x509_parser::extensions::GeneralName;
use x509_parser::parse_x509_certificate;

use crate::metadata::RpcContext;
use crate::metadata::{AuthContext, RpcClientContextFactory};

const INTERNAL_AUTHORIZATION: &str = "Bearer lattice-internal";
const MAX_SPIFFE_ID_BYTES: usize = 2_048;
const MAX_SPIFFE_TRUST_DOMAIN_BYTES: usize = 255;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceIdentityConfig {
    pub trust_domain: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RpcTransportSecurity {
    #[default]
    Plaintext,
    Tls(RpcTlsConfig),
}

impl RpcTransportSecurity {
    pub fn plaintext() -> Self {
        Self::Plaintext
    }

    pub fn tls(config: RpcTlsConfig) -> Self {
        Self::Tls(config)
    }

    pub fn client_tls_config(&self, endpoint: &Uri) -> Result<Option<ClientTlsConfig>, String> {
        let Self::Tls(config) = self else {
            return Ok(None);
        };
        let domain = config
            .domain_name
            .clone()
            .or_else(|| endpoint.host().map(ToString::to_string))
            .ok_or_else(|| format!("TLS endpoint {endpoint} has no host for SNI"))?;
        let mut tls = ClientTlsConfig::new().domain_name(domain);
        if let Some(ca) = &config.ca_certificate_pem {
            tls = tls.ca_certificate(Certificate::from_pem(ca.clone()));
        }
        if let Some(identity) = &config.identity {
            tls = tls.identity(identity.to_tonic_identity());
        }
        Ok(Some(tls))
    }

    pub fn server_tls_config(&self) -> Result<Option<ServerTlsConfig>, String> {
        let Self::Tls(config) = self else {
            return Ok(None);
        };
        let identity = config
            .identity
            .as_ref()
            .ok_or_else(|| "server TLS requires certificate/key identity".to_string())?;
        let mut tls = ServerTlsConfig::new().identity(identity.to_tonic_identity());
        if let Some(client_ca) = &config.client_ca_root_pem {
            tls = tls.client_ca_root(Certificate::from_pem(client_ca.clone()));
        }
        Ok(Some(tls))
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct RpcTlsConfig {
    pub domain_name: Option<String>,
    pub ca_certificate_pem: Option<Vec<u8>>,
    pub identity: Option<RpcTlsIdentity>,
    pub client_ca_root_pem: Option<Vec<u8>>,
}

impl fmt::Debug for RpcTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcTlsConfig")
            .field("domain_name", &self.domain_name)
            .field(
                "ca_certificate_configured",
                &self.ca_certificate_pem.is_some(),
            )
            .field("identity_configured", &self.identity.is_some())
            .field(
                "client_ca_root_configured",
                &self.client_ca_root_pem.is_some(),
            )
            .finish()
    }
}

impl RpcTlsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = Some(domain_name.into());
        self
    }

    pub fn ca_certificate_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_certificate_pem = Some(pem.into());
        self
    }

    pub fn identity(mut self, identity: RpcTlsIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    pub fn identity_pem(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.identity = Some(RpcTlsIdentity::from_pem(cert_pem, key_pem));
        self
    }

    pub fn client_ca_root_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.client_ca_root_pem = Some(pem.into());
        self
    }

    pub fn ca_certificate_file(mut self, path: impl AsRef<Path>) -> std::io::Result<Self> {
        self.ca_certificate_pem = Some(fs::read(path)?);
        Ok(self)
    }

    pub fn identity_files(
        mut self,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        self.identity = Some(RpcTlsIdentity {
            cert_pem: fs::read(cert_path)?,
            key_pem: fs::read(key_path)?,
        });
        Ok(self)
    }

    pub fn client_ca_root_file(mut self, path: impl AsRef<Path>) -> std::io::Result<Self> {
        self.client_ca_root_pem = Some(fs::read(path)?);
        Ok(self)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RpcTlsIdentity {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

impl fmt::Debug for RpcTlsIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcTlsIdentity")
            .field("certificate", &"[REDACTED]")
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}

impl RpcTlsIdentity {
    pub fn from_pem(cert_pem: impl Into<Vec<u8>>, key_pem: impl Into<Vec<u8>>) -> Self {
        Self {
            cert_pem: cert_pem.into(),
            key_pem: key_pem.into(),
        }
    }

    fn to_tonic_identity(&self) -> Identity {
        Identity::from_pem(self.cert_pem.clone(), self.key_pem.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub incarnation: Option<InstanceIncarnation>,
    pub spiffe_id: String,
}

impl PeerIdentity {
    pub fn new(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        spiffe_id: impl Into<String>,
    ) -> Self {
        Self {
            service_kind,
            instance_id,
            incarnation: None,
            spiffe_id: spiffe_id.into(),
        }
    }

    pub fn new_incarnated(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        incarnation: InstanceIncarnation,
        spiffe_id: impl Into<String>,
    ) -> Self {
        Self {
            service_kind,
            instance_id,
            incarnation: Some(incarnation),
            spiffe_id: spiffe_id.into(),
        }
    }
}

/// Extracts a service identity from the verified leaf certificate of a tonic
/// mTLS connection.
///
/// The TLS server remains responsible for validating the certificate chain.
/// This extractor only accepts the lattice SPIFFE path convention and never
/// treats caller-controlled metadata as peer identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MtlsPeerIdentityExtractor {
    trust_domain: String,
}

impl MtlsPeerIdentityExtractor {
    pub fn try_new(config: ServiceIdentityConfig) -> Result<Self, RpcSecurityError> {
        if !valid_spiffe_trust_domain(&config.trust_domain) {
            return Err(RpcSecurityError::InvalidTrustDomain);
        }
        Ok(Self {
            trust_domain: config.trust_domain,
        })
    }

    pub fn extract<T>(&self, request: &Request<T>) -> Result<PeerIdentity, RpcSecurityError> {
        let certificates = request
            .peer_certs()
            .ok_or(RpcSecurityError::MissingPeerCertificate)?;
        let leaf = certificates
            .first()
            .ok_or(RpcSecurityError::MissingPeerCertificate)?;
        self.extract_leaf_certificate(leaf.as_ref())
    }

    pub fn authenticate<T>(&self, request: &Request<T>) -> Result<PeerIdentity, Status> {
        self.extract(request).map_err(security_status)
    }

    fn extract_leaf_certificate(
        &self,
        certificate_der: &[u8],
    ) -> Result<PeerIdentity, RpcSecurityError> {
        let (remainder, certificate) = parse_x509_certificate(certificate_der)
            .map_err(|_| RpcSecurityError::InvalidPeerCertificate)?;
        if !remainder.is_empty() {
            return Err(RpcSecurityError::InvalidPeerCertificate);
        }
        let subject_alternative_name = certificate
            .subject_alternative_name()
            .map_err(|_| RpcSecurityError::InvalidPeerCertificate)?
            .ok_or(RpcSecurityError::MissingPeerSpiffeIdentity)?;

        let mut spiffe_id = None;
        for name in &subject_alternative_name.value.general_names {
            match name {
                GeneralName::URI(uri) => {
                    if spiffe_id.replace(*uri).is_some() {
                        return Err(RpcSecurityError::AmbiguousPeerSpiffeIdentity);
                    }
                }
                GeneralName::Invalid(_, _) => {
                    return Err(RpcSecurityError::InvalidPeerCertificate);
                }
                _ => {}
            }
        }
        let spiffe_id = spiffe_id.ok_or(RpcSecurityError::MissingPeerSpiffeIdentity)?;
        self.parse_spiffe_identity(spiffe_id)
    }

    fn parse_spiffe_identity(&self, spiffe_id: &str) -> Result<PeerIdentity, RpcSecurityError> {
        if spiffe_id.is_empty() || spiffe_id.len() > MAX_SPIFFE_ID_BYTES || !spiffe_id.is_ascii() {
            return Err(RpcSecurityError::MalformedPeerSpiffeIdentity);
        }
        let remainder = spiffe_id
            .strip_prefix("spiffe://")
            .ok_or(RpcSecurityError::MalformedPeerSpiffeIdentity)?;
        let mut segments = remainder.split('/');
        let trust_domain = segments
            .next()
            .ok_or(RpcSecurityError::MalformedPeerSpiffeIdentity)?;
        let service_label = segments
            .next()
            .ok_or(RpcSecurityError::MalformedPeerSpiffeIdentity)?;
        let service_kind = segments
            .next()
            .ok_or(RpcSecurityError::MalformedPeerSpiffeIdentity)?;
        let instance_label = segments
            .next()
            .ok_or(RpcSecurityError::MalformedPeerSpiffeIdentity)?;
        let instance_id = segments
            .next()
            .ok_or(RpcSecurityError::MalformedPeerSpiffeIdentity)?;
        let incarnation_label = segments
            .next()
            .ok_or(RpcSecurityError::MalformedPeerSpiffeIdentity)?;
        let incarnation = segments
            .next()
            .ok_or(RpcSecurityError::MalformedPeerSpiffeIdentity)?;
        let incarnation = InstanceIncarnation::new(incarnation);
        if segments.next().is_some()
            || !valid_spiffe_trust_domain(trust_domain)
            || service_label != "svc"
            || instance_label != "instance"
            || incarnation_label != "incarnation"
            || !valid_spiffe_path_segment(service_kind)
            || !valid_spiffe_path_segment(instance_id)
            || !incarnation.is_canonical()
        {
            return Err(RpcSecurityError::MalformedPeerSpiffeIdentity);
        }
        if trust_domain != self.trust_domain {
            return Err(RpcSecurityError::PeerTrustDomainMismatch);
        }
        Ok(PeerIdentity::new_incarnated(
            ServiceKind::new(service_kind),
            InstanceId::new(instance_id),
            incarnation,
            spiffe_id,
        ))
    }
}

fn valid_spiffe_trust_domain(trust_domain: &str) -> bool {
    !trust_domain.is_empty()
        && trust_domain.len() <= MAX_SPIFFE_TRUST_DOMAIN_BYTES
        && trust_domain.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".-_".contains(&byte)
        })
}

fn valid_spiffe_path_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && segment.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-' || byte == b'_'
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcSecurityPolicy {
    service_identity: Option<ServiceIdentityConfig>,
    allowed_services: Vec<ServiceKind>,
    require_authorization: bool,
}

impl RpcSecurityPolicy {
    pub fn disabled() -> Self {
        Self {
            service_identity: None,
            allowed_services: Vec::new(),
            require_authorization: false,
        }
    }

    pub fn require_service_identity(config: ServiceIdentityConfig) -> Self {
        Self {
            service_identity: Some(config),
            allowed_services: Vec::new(),
            require_authorization: false,
        }
    }

    pub fn allow_service(mut self, service_kind: ServiceKind) -> Self {
        if !self.allowed_services.contains(&service_kind) {
            self.allowed_services.push(service_kind);
        }
        self
    }

    pub fn require_authorization(mut self) -> Self {
        self.require_authorization = true;
        self
    }

    pub fn client_auth_context(&self) -> Option<AuthContext> {
        self.require_authorization.then(|| AuthContext {
            authorization: INTERNAL_AUTHORIZATION.to_string(),
        })
    }

    pub fn client_peer_identity(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> Option<PeerIdentity> {
        self.service_identity.as_ref().map(|identity| {
            let spiffe_id = format!(
                "spiffe://{}/svc/{}/instance/{}",
                identity.trust_domain,
                service_kind.as_str(),
                instance_id.as_str()
            );
            PeerIdentity::new(service_kind, instance_id, spiffe_id)
        })
    }

    pub fn validate(
        &self,
        ctx: &RpcContext,
        peer: Option<&PeerIdentity>,
    ) -> Result<(), RpcSecurityError> {
        if self.require_authorization {
            match &ctx.auth {
                Some(auth) if auth.authorization == INTERNAL_AUTHORIZATION => {}
                Some(_) => return Err(RpcSecurityError::InvalidAuthorization),
                None => return Err(RpcSecurityError::MissingAuthorization),
            }
        }
        if !self.allowed_services.is_empty()
            && !self
                .allowed_services
                .iter()
                .any(|allowed| allowed == &ctx.source_service)
        {
            return Err(RpcSecurityError::ServiceNotAllowed {
                service_kind: ctx.source_service.clone(),
            });
        }

        let Some(identity) = &self.service_identity else {
            return Ok(());
        };
        let peer = peer.ok_or(RpcSecurityError::MissingPeerIdentity)?;
        if peer.service_kind != ctx.source_service {
            return Err(RpcSecurityError::SourceServiceMismatch {
                metadata: ctx.source_service.clone(),
                peer: peer.service_kind.clone(),
            });
        }
        if peer.instance_id != ctx.source_instance {
            return Err(RpcSecurityError::SourceInstanceMismatch {
                metadata: ctx.source_instance.clone(),
                peer: peer.instance_id.clone(),
            });
        }
        let expected_prefix = format!("spiffe://{}/", identity.trust_domain);
        if !peer.spiffe_id.starts_with(&expected_prefix) {
            return Err(RpcSecurityError::TrustDomainMismatch {
                expected: identity.trust_domain.clone(),
                spiffe_id: peer.spiffe_id.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcServerSecurity {
    policy: RpcSecurityPolicy,
}

impl RpcServerSecurity {
    pub fn disabled() -> Self {
        Self {
            policy: RpcSecurityPolicy::disabled(),
        }
    }

    pub fn new(policy: RpcSecurityPolicy) -> Self {
        Self { policy }
    }

    pub fn policy(&self) -> &RpcSecurityPolicy {
        &self.policy
    }

    pub fn validate_context(
        &self,
        ctx: &RpcContext,
        peer: Option<&PeerIdentity>,
    ) -> Result<(), Status> {
        self.policy.validate(ctx, peer).map_err(security_status)
    }

    pub fn peer_identity<T>(&self, request: &Request<T>) -> Option<PeerIdentity> {
        request.extensions().get::<PeerIdentity>().cloned()
    }

    pub fn client_context_factory(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
    ) -> RpcClientContextFactory {
        let mut factory = RpcClientContextFactory::new(service_kind.clone(), instance_id.clone());
        if let Some(auth) = self.policy.client_auth_context() {
            factory = factory.with_auth(auth);
        }
        if let Some(peer_identity) = self.policy.client_peer_identity(service_kind, instance_id) {
            factory = factory.with_peer_identity(peer_identity);
        }
        factory
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcSecurityError {
    #[error("invalid service identity trust domain configuration")]
    InvalidTrustDomain,
    #[error("missing authenticated TLS peer certificate")]
    MissingPeerCertificate,
    #[error("authenticated TLS peer certificate is malformed")]
    InvalidPeerCertificate,
    #[error("authenticated TLS peer certificate has no SPIFFE identity")]
    MissingPeerSpiffeIdentity,
    #[error("authenticated TLS peer certificate has ambiguous SPIFFE identities")]
    AmbiguousPeerSpiffeIdentity,
    #[error("authenticated TLS peer certificate has a malformed SPIFFE identity")]
    MalformedPeerSpiffeIdentity,
    #[error("authenticated TLS peer certificate belongs to a different trust domain")]
    PeerTrustDomainMismatch,
    #[error("missing internal rpc peer identity")]
    MissingPeerIdentity,
    #[error("missing authorization context")]
    MissingAuthorization,
    #[error("invalid authorization context")]
    InvalidAuthorization,
    #[error("source service {service_kind} is not allowed by rpc security policy")]
    ServiceNotAllowed { service_kind: ServiceKind },
    #[error("source service metadata {metadata} does not match peer identity {peer}")]
    SourceServiceMismatch {
        metadata: ServiceKind,
        peer: ServiceKind,
    },
    #[error("source instance metadata {metadata} does not match peer identity {peer}")]
    SourceInstanceMismatch {
        metadata: InstanceId,
        peer: InstanceId,
    },
    #[error("peer spiffe id {spiffe_id} is outside trust domain {expected}")]
    TrustDomainMismatch { expected: String, spiffe_id: String },
}

pub(crate) fn security_status(error: RpcSecurityError) -> Status {
    match error {
        RpcSecurityError::InvalidTrustDomain => Status::invalid_argument(error.to_string()),
        RpcSecurityError::MissingPeerCertificate
        | RpcSecurityError::InvalidPeerCertificate
        | RpcSecurityError::MissingPeerSpiffeIdentity
        | RpcSecurityError::AmbiguousPeerSpiffeIdentity
        | RpcSecurityError::MalformedPeerSpiffeIdentity
        | RpcSecurityError::MissingPeerIdentity
        | RpcSecurityError::MissingAuthorization
        | RpcSecurityError::InvalidAuthorization => Status::unauthenticated(error.to_string()),
        RpcSecurityError::PeerTrustDomainMismatch
        | RpcSecurityError::ServiceNotAllowed { .. }
        | RpcSecurityError::SourceServiceMismatch { .. }
        | RpcSecurityError::SourceInstanceMismatch { .. }
        | RpcSecurityError::TrustDomainMismatch { .. } => {
            Status::permission_denied(error.to_string())
        }
    }
}

#[cfg(test)]
mod mtls_tests {
    use rcgen::{CertificateParams, KeyPair, SanType};
    use tonic::Code;

    use super::*;

    #[test]
    fn extractor_validates_spiffe_trust_domain_configuration() {
        for valid in ["lattice.test", "prod_us-west.example.com", "127.0.0.1"] {
            assert!(extractor(valid).is_ok(), "expected {valid:?} to be valid");
        }

        let oversized = "a".repeat(MAX_SPIFFE_TRUST_DOMAIN_BYTES + 1);
        for invalid in [
            "",
            "Lattice.test",
            "lattice/test",
            "lattice:test",
            "lattice%2etest",
            "lattice test",
            "lattice.测试",
            oversized.as_str(),
        ] {
            assert_eq!(
                extractor(invalid).unwrap_err(),
                RpcSecurityError::InvalidTrustDomain,
                "expected {invalid:?} to be invalid"
            );
        }
    }

    #[test]
    fn extractor_reads_one_strict_spiffe_uri_from_generated_leaf() {
        let extractor = extractor("lattice.test").unwrap();
        let certificate = certificate_with_sans(
            &["spiffe://lattice.test/svc/World/instance/world-a/incarnation/boot-a"],
            true,
        );

        let peer = extractor.extract_leaf_certificate(&certificate).unwrap();

        assert_eq!(peer.service_kind, ServiceKind::new("World"));
        assert_eq!(peer.instance_id, InstanceId::new("world-a"));
        assert_eq!(peer.incarnation, Some(InstanceIncarnation::new("boot-a")));
        assert_eq!(
            peer.spiffe_id,
            "spiffe://lattice.test/svc/World/instance/world-a/incarnation/boot-a"
        );
    }

    #[test]
    fn extractor_rejects_missing_ambiguous_and_wrong_domain_identities() {
        let extractor = extractor("lattice.test").unwrap();
        assert_eq!(
            extractor
                .extract_leaf_certificate(&certificate_with_sans(&[], true))
                .unwrap_err(),
            RpcSecurityError::MissingPeerSpiffeIdentity
        );
        assert_eq!(
            extractor
                .extract_leaf_certificate(&certificate_with_sans(
                    &[
                        "spiffe://lattice.test/svc/World/instance/world-a/incarnation/boot-a",
                        "spiffe://lattice.test/svc/World/instance/world-b/incarnation/boot-b",
                    ],
                    false,
                ))
                .unwrap_err(),
            RpcSecurityError::AmbiguousPeerSpiffeIdentity
        );
        assert_eq!(
            extractor
                .extract_leaf_certificate(&certificate_with_sans(
                    &["spiffe://other.test/svc/World/instance/world-a/incarnation/boot-a"],
                    false,
                ))
                .unwrap_err(),
            RpcSecurityError::PeerTrustDomainMismatch
        );
    }

    #[test]
    fn extractor_rejects_noncanonical_or_unbounded_spiffe_paths() {
        let extractor = extractor("lattice.test").unwrap();
        let oversized = format!(
            "spiffe://lattice.test/svc/{}/instance/world-a/incarnation/boot-a",
            "a".repeat(MAX_SPIFFE_ID_BYTES)
        );
        let malformed = [
            "SPIFFE://lattice.test/svc/World/instance/world-a",
            "https://lattice.test/svc/World/instance/world-a",
            "spiffe://lattice.test/service/World/instance/world-a",
            "spiffe://lattice.test/svc/World/instance/world-a",
            "spiffe://lattice.test/svc/World/instance/world-a/incarnation/boot-a/extra",
            "spiffe://lattice.test/svc//instance/world-a/incarnation/boot-a",
            "spiffe://lattice.test/svc/../instance/world-a/incarnation/boot-a",
            "spiffe://lattice.test/svc/%57orld/instance/world-a/incarnation/boot-a",
            "spiffe://lattice.test/svc/World/instance/world-a/incarnation/..",
            "spiffe://lattice.test/svc/World/instance/world-a/incarnation/boot-a?admin=true",
            oversized.as_str(),
        ];

        for spiffe_id in malformed {
            assert_eq!(
                extractor
                    .extract_leaf_certificate(&certificate_with_sans(&[spiffe_id], false))
                    .unwrap_err(),
                RpcSecurityError::MalformedPeerSpiffeIdentity,
                "expected {spiffe_id:?} to be rejected"
            );
        }
    }

    #[test]
    fn extractor_maps_failures_to_sanitized_tonic_statuses() {
        let extractor = extractor("lattice.test").unwrap();
        let mut spoofed = Request::new(());
        spoofed.extensions_mut().insert(PeerIdentity::new(
            ServiceKind::new("World"),
            InstanceId::new("world-a"),
            "spiffe://lattice.test/svc/World/instance/world-a/incarnation/boot-a",
        ));
        let missing = extractor.authenticate(&spoofed).unwrap_err();
        assert_eq!(missing.code(), Code::Unauthenticated);

        let wrong_domain = extractor
            .extract_leaf_certificate(&certificate_with_sans(
                &["spiffe://attacker.example/svc/World/instance/stolen-secret/incarnation/boot-a"],
                false,
            ))
            .unwrap_err();
        let wrong_domain = security_status(wrong_domain);
        assert_eq!(wrong_domain.code(), Code::PermissionDenied);
        assert!(!wrong_domain.message().contains("attacker.example"));
        assert!(!wrong_domain.message().contains("stolen-secret"));

        let invalid = security_status(
            extractor
                .extract_leaf_certificate(b"certificate-secret")
                .unwrap_err(),
        );
        assert_eq!(invalid.code(), Code::Unauthenticated);
        assert!(!invalid.message().contains("certificate-secret"));
    }

    #[test]
    fn tls_debug_output_redacts_certificate_and_private_key_material() {
        let identity = RpcTlsIdentity::from_pem(b"certificate-secret", b"private-key-secret");
        let config = RpcTlsConfig::new()
            .domain_name("authority.internal")
            .ca_certificate_pem(b"server-ca-secret")
            .identity(identity.clone())
            .client_ca_root_pem(b"client-ca-secret");
        let debug = format!(
            "{identity:?} {config:?} {:?}",
            RpcTransportSecurity::tls(config.clone())
        );

        for secret in [
            "certificate-secret",
            "private-key-secret",
            "server-ca-secret",
            "client-ca-secret",
        ] {
            assert!(!debug.contains(secret));
        }
        assert!(debug.contains("identity_configured: true"));
        assert!(debug.contains("[REDACTED]"));
    }

    fn extractor(trust_domain: &str) -> Result<MtlsPeerIdentityExtractor, RpcSecurityError> {
        MtlsPeerIdentityExtractor::try_new(ServiceIdentityConfig {
            trust_domain: trust_domain.to_string(),
        })
    }

    fn certificate_with_sans(uris: &[&str], include_dns_name: bool) -> Vec<u8> {
        let mut parameters = CertificateParams::default();
        parameters.subject_alt_names = uris
            .iter()
            .map(|uri| SanType::URI((*uri).try_into().unwrap()))
            .collect();
        if include_dns_name {
            parameters
                .subject_alt_names
                .push(SanType::DnsName("world.internal".try_into().unwrap()));
        }
        let signing_key = KeyPair::generate().unwrap();
        parameters
            .self_signed(&signing_key)
            .unwrap()
            .der()
            .as_ref()
            .to_vec()
    }
}
