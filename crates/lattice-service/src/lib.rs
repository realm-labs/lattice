pub mod backend;
pub mod builder;
pub mod cluster;
pub mod config;
mod control;
pub mod error;
pub mod lifecycle;
pub mod supervisor;

pub use backend::LogicalRouter;
pub use builder::{LatticeService, LatticeServiceBuilder};
pub use config::NodeConfig;
pub use error::ServiceError;
pub use lifecycle::{
    ServiceLifecycle, ServiceLifecycleEffect, ServiceLifecycleError, ServiceLifecycleEvent,
    ServiceLifecycleState,
};

#[cfg(test)]
mod tests;
