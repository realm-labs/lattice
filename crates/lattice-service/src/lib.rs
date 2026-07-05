pub mod actor;
pub mod builder;
pub mod config;
pub mod context;
pub mod error;
pub mod rpc;
pub mod service;

pub use actor::{ActorRegistration, ActorRegistrationBuilder};
pub use builder::LatticeServiceBuilder;
pub use error::LatticeServiceError;
pub use rpc::{RpcClientBinding, RpcServiceBinding};
pub use service::LatticeService;

#[cfg(test)]
mod tests;
