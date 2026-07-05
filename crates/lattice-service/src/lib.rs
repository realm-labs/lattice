pub mod actor;
pub mod builder;
pub mod component;
pub mod config;
pub mod context;
mod control;
pub mod error;
pub mod framework;
pub mod rpc;
pub mod service;

pub use actor::{ActorRegistration, ActorRegistrationBuilder};
pub use builder::LatticeServiceBuilder;
pub use component::{IntoServiceComponent, ReadyComponent, ServiceComponent};
pub use config::DirectLinkConfig;
pub use error::LatticeServiceError;
pub use framework::{
    ClusterEventBusComponent, ConfigStoreComponent, DynConfigStore, DynEventBus, DynPlacementStore,
    LocalEventBusComponent, PlacementStoreComponent, ServiceContextExt, ServiceEventBus,
    ServiceSchedulerComponent,
};
pub use lattice_core::ServiceContext;
pub use lattice_ops::{AdminHttpConfig, ServiceScheduler};
pub use lattice_rpc::{PeerIdentity, RpcSecurityPolicy, RpcServerSecurity, ServiceIdentityConfig};
pub use rpc::{RpcClientBinding, RpcClientPlacement, RpcServiceBinding};
pub use service::LatticeService;

#[cfg(test)]
mod tests;
