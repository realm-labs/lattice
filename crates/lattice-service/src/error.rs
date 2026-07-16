use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("node configuration is invalid")]
    Config(#[source] crate::config::NodeConfigError),
    #[error("cluster join configuration is invalid")]
    JoinConfig(#[source] crate::config::ClusterJoinConfigError),
    #[error("cluster join controller construction failed")]
    Join(#[source] crate::cluster::join::JoinError),
    #[error("authoritative member directory construction failed")]
    MemberDirectory(#[source] crate::cluster::members::MemberDirectoryError),
    #[error("placement control router construction failed")]
    PlacementControl(#[source] lattice_placement::control::PlacementControlError),
    #[error("Coordinator runtime construction failed")]
    CoordinatorRuntime(#[from] lattice_placement::runtime::CoordinatorRuntimeError),
    #[error("Coordinator discovery construction failed")]
    Discovery(#[from] lattice_discovery::provider::DiscoveryError),
    #[error("discovery joining and a preassembled logic runtime cannot both be configured")]
    ConflictingClusterRuntime,
    #[error("actor host registration failed")]
    Host(#[source] lattice_actor::host::HostRegistryError),
    #[error("actor lifecycle administration failed")]
    ActorLifecycleAdmin(#[source] lattice_actor::host::HostAdminError),
    #[error("actor protocol registration failed")]
    ProtocolRegistration(#[source] lattice_actor::recipient::ProtocolRegistrationError),
    #[error("actor protocol construction failed")]
    ProtocolBuild(#[source] lattice_actor::protocol::ProtocolBuildError),
    #[error("logical entity configuration is invalid")]
    EntityConfig(#[source] lattice_placement::region::RegionError),
    #[error("logical entity router construction failed")]
    LogicalRouter(#[source] crate::cluster::ClusterRouterError),
    #[error("association manager construction failed")]
    Association(#[source] lattice_remoting::association::AssociationError),
    #[error("remote messaging construction failed")]
    Messaging(#[source] lattice_remoting::messaging::error::RemoteMessageError),
    #[error("remoting endpoint failed")]
    Endpoint(#[source] lattice_remoting::endpoint::EndpointError),
    #[error("watch registry construction failed")]
    Watch(#[source] lattice_remoting::watch::WatchError),
    #[error("service reliable control dispatch construction failed")]
    Control(#[source] lattice_remoting::control::ControlDispatchError),
    #[error("service lifecycle transition failed")]
    Lifecycle(#[source] crate::lifecycle::ServiceLifecycleError),
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
    InterventionRequired(crate::lifecycle::LifecycleInterventionReport),
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
