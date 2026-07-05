mod adapter;
mod client;
mod dedup;
mod error;
mod metadata;
mod security;
mod server;
mod traits;
mod types;

pub use adapter::ActorRpcAdapter;
pub use client::{
    ActorRefRpcClient, MetadataInjectingRpcCore, TonicEndpointChannelPool, TypedRpcClient,
    tonic_status_to_rpc_error,
};
pub use dedup::{RequestDedupKey, RequestDeduplicator};
pub use error::RpcError;
pub use metadata::{AuthContext, RpcClientContextFactory, RpcContext, RpcMetadataError};
pub use security::{MtlsConfig, PeerIdentity, RpcSecurityError, RpcSecurityPolicy};
pub use server::{RegisteredRpcService, RpcServerBuildError, RpcServerBuilder};
pub use traits::{ActorRefRpcCore, RoutedRequest, RpcRequest, ShardedRpcCore, UnaryRpcTransport};
pub use types::{RouteTarget, Rpc};

#[cfg(test)]
mod tests;
