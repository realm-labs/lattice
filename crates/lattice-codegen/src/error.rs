#[derive(Debug, thiserror::Error)]
pub enum CodegenError {
    #[error("missing required codegen field {0}")]
    MissingField(&'static str),
    #[error("duplicate gateway message id {msg_id}")]
    DuplicateGatewayMessageId { msg_id: u32 },
    #[error("duplicate direct-link message id {message_id} in stream {stream_name}")]
    DuplicateDirectLinkMessageId {
        stream_name: String,
        message_id: u64,
    },
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
    #[error("missing proto option {option} on {target}")]
    MissingProtoOption {
        option: &'static str,
        target: String,
    },
}
