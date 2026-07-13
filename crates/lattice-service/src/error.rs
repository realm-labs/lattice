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
    #[error("discovery joining and a preassembled logic runtime cannot both be configured")]
    ConflictingClusterRuntime,
    #[error("actor host registration failed")]
    Host(#[source] lattice_actor::host::HostRegistryError),
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
    #[error("graceful member leave exceeded its deadline")]
    LeaveTimeout,
}
