#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::path::PathBuf;

pub mod builder;
pub mod error;
pub mod render;
pub mod spec;

pub fn proto_include() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto")
}
