use lattice_core::{InstanceId, ServiceKind};
use tonic::{Request, Status};

use crate::RpcContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MtlsConfig {
    pub trust_domain: String,
    pub ca_cert_path: String,
    pub cert_chain_path: String,
    pub private_key_path: String,
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
    mtls: Option<MtlsConfig>,
    allowed_services: Vec<ServiceKind>,
    require_authorization: bool,
}

impl RpcSecurityPolicy {
    pub fn disabled() -> Self {
        Self {
            mtls: None,
            allowed_services: Vec::new(),
            require_authorization: false,
        }
    }

    pub fn require_mtls(config: MtlsConfig) -> Self {
        Self {
            mtls: Some(config),
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

    pub fn validate(
        &self,
        ctx: &RpcContext,
        peer: Option<&PeerIdentity>,
    ) -> Result<(), RpcSecurityError> {
        if self.require_authorization && ctx.auth.is_none() {
            return Err(RpcSecurityError::MissingAuthorization);
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

        let Some(mtls) = &self.mtls else {
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
        let expected_prefix = format!("spiffe://{}/", mtls.trust_domain);
        if !peer.spiffe_id.starts_with(&expected_prefix) {
            return Err(RpcSecurityError::TrustDomainMismatch {
                expected: mtls.trust_domain.clone(),
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

    pub fn peer_identity<T>(&self, request: &Request<T>) -> Option<PeerIdentity> {
        request.extensions().get::<PeerIdentity>().cloned()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcSecurityError {
    #[error("missing internal rpc peer identity")]
    MissingPeerIdentity,
    #[error("missing authorization context")]
    MissingAuthorization,
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
        RpcSecurityError::MissingPeerIdentity | RpcSecurityError::MissingAuthorization => {
            Status::unauthenticated(error.to_string())
        }
        RpcSecurityError::ServiceNotAllowed { .. }
        | RpcSecurityError::SourceServiceMismatch { .. }
        | RpcSecurityError::SourceInstanceMismatch { .. }
        | RpcSecurityError::TrustDomainMismatch { .. } => {
            Status::permission_denied(error.to_string())
        }
    }
}
