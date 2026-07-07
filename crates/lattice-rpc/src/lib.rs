pub mod adapter;
pub mod client;
pub mod dedup;
pub mod error;
pub mod metadata;
pub mod security;
pub mod server;
pub mod traits;
pub mod types;

pub use adapter::ActorRpcAdapter;
pub use client::{
    ActorRefRpcClient, TonicEndpointChannelPool, TonicEndpointChannelPoolConfig, TypedRpcClient,
};
pub use error::RpcError;
pub use metadata::{AuthContext, RpcClientContextFactory, RpcContext};
pub use security::{
    PeerIdentity, RpcSecurityError, RpcSecurityPolicy, RpcServerSecurity, RpcTlsConfig,
    RpcTlsIdentity, RpcTransportSecurity, ServiceIdentityConfig,
};
pub use traits::{ActorRefRpcCore, RoutedRequest, RpcRequest, ShardedRpcCore};
pub use types::{RouteTarget, RoutedEnvelope, Rpc, RpcRoute, RpcRouteMetadataError};

#[cfg(test)]
mod tests;
