use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum CodegenError {
    #[error("missing required codegen field {0}")]
    MissingField(&'static str),
    #[error("duplicate gateway message id {msg_id}")]
    DuplicateGatewayMessageId { msg_id: u32 },
    #[error("unsupported route key field type for {message}.{field}")]
    UnsupportedRouteKeyFieldType { message: String, field: String },
    #[error("route key field {field} was not found on request message {message}")]
    RouteKeyFieldNotFound { message: String, field: String },
    #[error("route key field {message}.{field} must be non-optional: {reason}")]
    OptionalRouteKeyField {
        message: String,
        field: String,
        reason: String,
    },
    #[error("request message {message} was not found in descriptor set")]
    RequestMessageNotFound { message: String },
    #[error("failed to read proto descriptor: {0}")]
    DescriptorRead(String),
    #[error("failed to compile proto files: {0}")]
    ProtoCompile(String),
    #[error("failed to write generated lattice bindings: {0}")]
    WriteGenerated(String),
    #[error("failed to read gateway route file {path}: {source}")]
    GatewayRouteRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse gateway route file {path}: {details}")]
    GatewayRouteParse { path: PathBuf, details: String },
    #[error("gateway route method {method} was not found in compiled proto services")]
    UnknownGatewayRouteMethod { method: String },
    #[error("duplicate gateway route method {method}")]
    DuplicateGatewayRouteMethod { method: String },
    #[error(
        "gateway route for {method} conflicts with proto option: proto msg_id={proto_msg_id}, route msg_id={route_msg_id}"
    )]
    ConflictingGatewayMessageId {
        method: String,
        proto_msg_id: u32,
        route_msg_id: u32,
    },
    #[error("missing proto option {option} on {target}")]
    MissingProtoOption {
        option: &'static str,
        target: String,
    },
}
