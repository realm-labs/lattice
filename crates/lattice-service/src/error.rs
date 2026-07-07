use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_placement::error::PlacementError;
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
    #[error("missing RPC client core {core_type} for service {service_kind}")]
    MissingRpcClientCore {
        service_kind: ServiceKind,
        core_type: &'static str,
    },
    #[error("missing actor registration for {actor_kind}")]
    MissingActorRegistration { actor_kind: ActorKind },
    #[error("actor registration for {actor_kind} does not match expected type {expected_type}")]
    ActorTypeMismatch {
        actor_kind: ActorKind,
        expected_type: &'static str,
    },
    #[error("duplicate service component {component}")]
    DuplicateServiceComponent { component: String },
    #[error("missing service component {component}")]
    MissingServiceComponent { component: String },
    #[error("duplicate service extension for type {type_name}")]
    DuplicateServiceExtension { type_name: String },
    #[error("failed to load service config: {message}")]
    Config { message: String },
    #[error("failed to build service component {slot}: {message}")]
    ComponentBuild { slot: String, message: String },
    #[error(transparent)]
    Placement(#[from] PlacementError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Transport(#[from] TransportError),
}
