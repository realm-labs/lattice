use thiserror::Error;

#[derive(Debug, Error)]
pub enum CodegenError {
    #[error("invalid actor protocol code generation specification: {0}")]
    InvalidSpec(String),
    #[error("protobuf message compilation failed: {0}")]
    ProtoCompile(String),
    #[error("failed to write generated actor protocol: {0}")]
    WriteGenerated(String),
    #[error("OUT_DIR is not configured")]
    MissingOutDir,
}
