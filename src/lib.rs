//! Core lattice framework APIs.

pub mod config;
pub mod core;

pub use config::{BootstrapConfig, ConfigError, ConfigFormat, ConfigSource};
pub use core::{
    ActorId, ActorKey, ActorKeyDecodeError, ActorKind, Epoch, InstanceCapacity, InstanceConfig,
    InstanceId, RequestId, RouteKey, ServiceKind,
};

#[macro_export]
macro_rules! actor_kind {
    ($name:literal) => {
        $crate::ActorKind::from_static($name)
    };
}

#[macro_export]
macro_rules! service_kind {
    ($name:literal) => {
        $crate::ServiceKind::from_static($name)
    };
}
