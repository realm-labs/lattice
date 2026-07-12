pub mod builder;
pub mod error;
pub mod render;
pub mod spec;

pub use builder::{ActorProtocolCodegen, configure};
pub use error::CodegenError;
pub use spec::{ActorProtocolSpec, InteractionMode, ProtocolMessageSpec};

pub fn proto_include() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto")
}
