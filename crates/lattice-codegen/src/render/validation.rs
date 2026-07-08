use std::collections::BTreeSet;

use crate::error::CodegenError;
use crate::spec::RpcMethodSpec;
pub(crate) fn validate_methods(methods: &[RpcMethodSpec]) -> Result<(), CodegenError> {
    let mut msg_ids = BTreeSet::new();
    for method in methods {
        if method.service_kind.trim().is_empty() {
            return Err(CodegenError::MissingField("service_kind"));
        }
        if method.service_name.trim().is_empty() {
            return Err(CodegenError::MissingField("service_name"));
        }
        if method.method_name.trim().is_empty() {
            return Err(CodegenError::MissingField("method_name"));
        }
        if method.request_type.trim().is_empty() {
            return Err(CodegenError::MissingField("request_type"));
        }
        if method.reply_type.trim().is_empty() {
            return Err(CodegenError::MissingField("reply_type"));
        }
        if method.route_key.actor_kind.trim().is_empty() {
            return Err(CodegenError::MissingField("route_key.actor_kind"));
        }
        if method.route_key.key_field.trim().is_empty() {
            return Err(CodegenError::MissingField("route_key.key_field"));
        }
        if let Some(msg_id) = method.gateway_msg_id
            && !msg_ids.insert(msg_id)
        {
            return Err(CodegenError::DuplicateGatewayMessageId { msg_id });
        }
    }
    Ok(())
}
