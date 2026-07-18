use lattice_actor::{
    host::{HostAdminError, HostRegistryError},
    protocol::ProtocolBuildError,
    recipient::ProtocolRegistrationError,
};
use lattice_discovery::provider::DiscoveryError;
use lattice_placement::{
    control::PlacementControlError, region::RegionError, runtime::CoordinatorRuntimeError,
};
use lattice_remoting::{
    association::AssociationError, control::ControlDispatchError, endpoint::EndpointError,
    messaging::error::RemoteMessageError, watch::WatchError,
};
use thiserror::Error;

use crate::{
    cluster::{ClusterRouterError, join::JoinError, members::MemberDirectoryError},
    config::{ClusterJoinConfigError, NodeConfigError},
    lifecycle::{LifecycleInterventionReport, ServiceLifecycleError},
};

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("node configuration is invalid")]
    Config(#[source] NodeConfigError),
    #[error("cluster join configuration is invalid")]
    JoinConfig(#[source] ClusterJoinConfigError),
    #[error("cluster join controller construction failed")]
    Join(#[source] JoinError),
    #[error("authoritative member directory construction failed")]
    MemberDirectory(#[source] MemberDirectoryError),
    #[error("placement control router construction failed")]
    PlacementControl(#[source] PlacementControlError),
    #[error("Coordinator runtime construction failed")]
    CoordinatorRuntime(#[from] CoordinatorRuntimeError),
    #[error("Coordinator discovery construction failed")]
    Discovery(#[from] DiscoveryError),
    #[error("discovery joining and a preassembled logic runtime cannot both be configured")]
    ConflictingClusterRuntime,
    #[error("actor host registration failed")]
    Host(#[source] HostRegistryError),
    #[error("actor lifecycle administration failed")]
    ActorLifecycleAdmin(#[source] HostAdminError),
    #[error("actor protocol registration failed")]
    ProtocolRegistration(#[source] ProtocolRegistrationError),
    #[error("actor protocol construction failed")]
    ProtocolBuild(#[source] ProtocolBuildError),
    #[error("logical entity configuration is invalid")]
    EntityConfig(#[source] RegionError),
    #[error("logical entity router construction failed")]
    LogicalRouter(#[source] ClusterRouterError),
    #[error("association manager construction failed")]
    Association(#[source] AssociationError),
    #[error("remote messaging construction failed")]
    Messaging(#[source] RemoteMessageError),
    #[error("remoting endpoint failed")]
    Endpoint(#[source] EndpointError),
    #[error("watch registry construction failed")]
    Watch(#[source] WatchError),
    #[error("service reliable control dispatch construction failed")]
    Control(#[source] ControlDispatchError),
    #[error("service lifecycle transition failed")]
    Lifecycle(#[source] ServiceLifecycleError),
    #[error("service supervised task capacity reached")]
    TaskCapacity,
    #[error("service shutdown exceeded its deadline")]
    ShutdownTimeout,
    #[error("a supervised service task failed")]
    TaskFailed,
    #[error("service has no active Coordinator session")]
    CoordinatorUnavailable,
    #[error("node placement capacity must be nonzero")]
    InvalidCapacity,
    #[error("discovery logic runtime requires exactly one explicit placement domain")]
    InvalidPlacementDomains,
    #[error("graceful member leave exceeded its deadline")]
    LeaveTimeout,
    #[error("graceful shutdown requires operator intervention: {0:?}")]
    InterventionRequired(LifecycleInterventionReport),
    #[error("Coordinator deployment configuration is invalid")]
    InvalidDeployment,
    #[error("service readiness wait exceeded its deadline")]
    ReadinessTimeout,
    #[error("application component {component} blocked shutdown")]
    ApplicationShutdown {
        component: &'static str,
        #[source]
        source: Box<ServiceError>,
    },
}
