pub mod builder;
pub mod descriptor;
pub mod error;
pub mod render;
pub mod route_key;
pub mod spec;

use std::path::PathBuf;

pub fn proto_include() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto")
}

#[cfg(test)]
mod tests;
