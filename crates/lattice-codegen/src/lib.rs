pub mod builder;
pub mod descriptor;
pub mod error;
pub mod gateway;
pub mod render;
pub mod route_key;
pub mod spec;

use std::path::PathBuf;

pub use builder::{LatticeCodegenBuilder, configure};
pub use error::CodegenError;

pub fn proto_include() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto")
}

#[cfg(test)]
mod tests;
