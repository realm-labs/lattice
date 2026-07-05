mod actor;
mod builder;
mod config;
mod context;
mod error;
mod rpc;
mod service;

pub use actor::{
    ActorRegistration, ActorRegistrationBuilder, NoFactory, RegisteredActor, ServiceActorLoader,
};
pub use builder::LatticeServiceBuilder;
pub use config::InstanceConfig;
pub use context::{ServiceBuildContext, ServiceContext};
pub use error::LatticeServiceError;
pub use rpc::{RpcClientBinding, RpcServiceBinding};
pub use service::LatticeService;

#[cfg(test)]
mod tests;
