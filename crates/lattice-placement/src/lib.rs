pub mod cache;
pub mod control;
pub mod coordinator;
pub mod endpoint;
pub mod error;
pub mod etcd;
pub mod instance;
pub mod route;
pub mod singleton;
pub mod static_resolver;
pub mod store;
pub mod vshard;

pub use endpoint::{EndpointLease, EndpointPool};
pub use error::PlacementError;
pub use route::{
    EndpointRpcTransport, InvalidateReason, ResolveRequest, ResolvingActorRefRpcCore,
    ResolvingRpcCore, RouteCacheKey, RouteResolver,
};
pub use static_resolver::{StaticPlacementConfig, StaticRouteRange, StaticRouteResolver};
pub use store::{
    InMemoryPlacementStore, PlacementPrefix, PlacementStore, VirtualShardPlacementKey,
    VirtualShardPlacementRecord,
};

#[cfg(test)]
mod tests;
