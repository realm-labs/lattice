use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("node configuration is invalid")]
    Config(#[source] crate::config::NodeConfigError),
    #[error("actor host registration failed")]
    Host(#[source] lattice_actor::HostRegistryError),
    #[error("association manager construction failed")]
    Association(#[source] lattice_remoting::association::AssociationError),
    #[error("remote messaging construction failed")]
    Messaging(#[source] lattice_remoting::RemoteMessageError),
    #[error("remoting endpoint failed")]
    Endpoint(#[source] lattice_remoting::EndpointError),
    #[error("watch registry construction failed")]
    Watch(#[source] lattice_remoting::WatchError),
    #[error("service reliable control dispatch construction failed")]
    Control(#[source] lattice_remoting::ControlDispatchError),
    #[error("service supervised task capacity reached")]
    TaskCapacity,
    #[error("service shutdown exceeded its deadline")]
    ShutdownTimeout,
    #[error("a supervised service task failed")]
    TaskFailed,
}
