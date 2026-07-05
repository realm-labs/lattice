pub mod actor;
pub mod builder;
pub mod component;
pub mod config;
pub mod context;
pub mod error;
pub mod framework;
pub mod rpc;
pub mod service;

pub use actor::{ActorRegistration, ActorRegistrationBuilder};
pub use builder::LatticeServiceBuilder;
pub use component::{IntoServiceComponent, ReadyComponent, ServiceComponent};
pub use error::LatticeServiceError;
pub use framework::{
    ConfigStoreComponent, DynConfigStore, DynEventBus, DynPlacementStore, EventBusComponent,
    LocalEventBusComponent, PlacementStoreComponent, ServiceContextExt,
};
pub use lattice_core::ServiceContext;
pub use rpc::{RpcClientBinding, RpcServiceBinding};
pub use service::LatticeService;

#[cfg(test)]
mod tests;
