use lattice_core::ActorKind;
use thiserror::Error;
use tonic::transport::Error as TransportError;

#[derive(Debug, Error)]
pub enum LatticeServiceError {
    #[error("lattice service listener is not configured")]
    MissingListener,
    #[error("lattice service instance config is not configured")]
    MissingInstanceConfig,
    #[error("lattice service has no RPC services registered")]
    NoRpcServices,
    #[error("duplicate actor registration for {actor_kind}")]
    DuplicateActorRegistration { actor_kind: ActorKind },
    #[error("duplicate RPC service registration for {service_name}")]
    DuplicateRpcService { service_name: String },
    #[error("missing actor registration for {actor_kind}")]
    MissingActorRegistration { actor_kind: ActorKind },
    #[error("actor registration for {actor_kind} does not match expected type {expected_type}")]
    ActorTypeMismatch {
        actor_kind: ActorKind,
        expected_type: &'static str,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Transport(#[from] TransportError),
}
