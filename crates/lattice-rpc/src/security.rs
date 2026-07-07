use std::fs;
use std::path::Path;

use http::Uri;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::{Request, Status};

use crate::metadata::RpcContext;
use crate::metadata::{AuthContext, RpcClientContextFactory};

const INTERNAL_AUTHORIZATION: &str = "Bearer lattice-internal";

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RpcTlsConfig {
    pub domain_name: Option<String>,
    pub ca_certificate_pem: Option<Vec<u8>>,
    pub identity: Option<RpcTlsIdentity>,
    pub client_ca_root_pem: Option<Vec<u8>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcTlsIdentity {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
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
            spiffe_id: spiffe_id.into(),
        }
    }
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
        RpcSecurityError::MissingPeerIdentity
        | RpcSecurityError::MissingAuthorization
        | RpcSecurityError::InvalidAuthorization => Status::unauthenticated(error.to_string()),
        RpcSecurityError::ServiceNotAllowed { .. }
        | RpcSecurityError::SourceServiceMismatch { .. }
        | RpcSecurityError::SourceInstanceMismatch { .. }
        | RpcSecurityError::TrustDomainMismatch { .. } => {
            Status::permission_denied(error.to_string())
        }
    }
}
