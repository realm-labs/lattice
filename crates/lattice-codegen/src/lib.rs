mod builder;
mod descriptor;
mod error;
mod gateway;
mod render;
mod route_key;
mod spec;

use std::path::PathBuf;

pub use builder::{LatticeCodegenBuilder, configure};
pub use error::CodegenError;
pub use gateway::GatewayRoute;
pub use render::{GeneratedRpcBindings, generate_rpc_bindings};
pub use route_key::{ProtoRouteKeyOption, RouteKeyType};
pub use spec::RpcMethodSpec;

pub fn proto_include() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto")
}

#[cfg(test)]
mod tests;
