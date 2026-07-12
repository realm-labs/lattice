#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

pub mod builder;
pub mod error;
pub mod render;
pub mod spec;

pub fn proto_include() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto")
}
