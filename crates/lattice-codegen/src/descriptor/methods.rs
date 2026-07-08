use std::collections::BTreeMap;

use prost_types::{FieldDescriptorProto, FileDescriptorSet, field_descriptor_proto::Label};

use crate::descriptor::ParsedOptions;
use crate::descriptor::types::{find_field, find_message, rust_type_path, scoped_name};
use crate::error::CodegenError;
use crate::route_key::{ProtoRouteKeyOption, RouteKeyType};
use crate::spec::RpcMethodSpec;
pub(crate) fn methods_from_descriptor(
    descriptor: &FileDescriptorSet,
    options: &BTreeMap<String, ParsedOptions>,
) -> Result<Vec<RpcMethodSpec>, CodegenError> {
    let mut methods = Vec::new();
    let mut gateway_msg_ids = BTreeMap::<u32, String>::new();
    for file in &descriptor.file {
        let package = file.package.clone().unwrap_or_default();
        for service in &file.service {
            let service_name = service.name.clone().unwrap_or_default();
            let service_key = scoped_name(&package, &service_name);
            let service_options =
                options
                    .get(&service_key)
                    .ok_or_else(|| CodegenError::MissingProtoOption {
                        option: "lattice.service_kind",
                        target: service_key.clone(),
                    })?;
            let service_kind = service_options.service_kind.clone().ok_or_else(|| {
                CodegenError::MissingProtoOption {
                    option: "lattice.service_kind",
                    target: service_key.clone(),
                }
            })?;
            let service_actor_kind = service_options.actor_kind.clone().ok_or_else(|| {
                CodegenError::MissingProtoOption {
                    option: "lattice.actor_kind",
                    target: service_key.clone(),
                }
            })?;
            let service_route_key = service_options.route_key.clone();

            for method in &service.method {
                let method_name = method.name.clone().unwrap_or_default();
                let method_key = format!("{service_key}.{method_name}");
                let method_options = options.get(&method_key);
                let route_key = method_options
                    .and_then(|options| options.route_key.clone())
                    .or_else(|| service_route_key.clone())
                    .ok_or_else(|| CodegenError::MissingProtoOption {
                        option: "lattice.route_key",
                        target: method_key.clone(),
                    })?;
                let method_gateway_msg_id =
                    method_options.and_then(|options| options.gateway_msg_id);
                if let Some(msg_id) = method_gateway_msg_id
                    && gateway_msg_ids.insert(msg_id, method_key.clone()).is_some()
                {
                    return Err(CodegenError::DuplicateGatewayMessageId { msg_id });
                }
                let is_gateway_route = method_gateway_msg_id.is_some();
                let request_route_key = route_key.clone();

                let request_proto = method
                    .input_type
                    .clone()
                    .unwrap_or_default()
                    .trim_start_matches('.')
                    .to_string();
                let reply_proto = method
                    .output_type
                    .clone()
                    .unwrap_or_default()
                    .trim_start_matches('.')
                    .to_string();
                let request = find_message(descriptor, &request_proto).ok_or_else(|| {
                    CodegenError::RequestMessageNotFound {
                        message: request_proto.clone(),
                    }
                })?;
                let (field_type, route_key_from_request) = if is_gateway_route {
                    (RouteKeyType::U64, false)
                } else {
                    match find_field(request.descriptor, &request_route_key) {
                        Some(field) => {
                            validate_route_key_field_presence(
                                file.syntax.as_deref(),
                                &request_proto,
                                field,
                                &request_route_key,
                            )?;
                            let field_type = field
                                .r#type
                                .and_then(|value| value.try_into().ok())
                                .and_then(RouteKeyType::from_field_type)
                                .ok_or_else(|| CodegenError::UnsupportedRouteKeyFieldType {
                                    message: request_proto.clone(),
                                    field: request_route_key.clone(),
                                })?;
                            (field_type, true)
                        }
                        None => {
                            return Err(CodegenError::RouteKeyFieldNotFound {
                                message: request_proto.clone(),
                                field: request_route_key.clone(),
                            });
                        }
                    }
                };

                methods.push(RpcMethodSpec {
                    package: package.clone(),
                    service_kind: service_kind.clone(),
                    service_name: service_name.clone(),
                    method_name,
                    request_type: request.rust_type,
                    reply_type: find_message(descriptor, &reply_proto)
                        .map(|message| message.rust_type)
                        .unwrap_or_else(|| rust_type_path(&reply_proto)),
                    route_key: ProtoRouteKeyOption {
                        actor_kind: service_actor_kind.clone(),
                        key_field: request_route_key,
                        key_type: field_type,
                    },
                    route_key_from_request,
                    gateway_msg_id: method_gateway_msg_id,
                });
            }
        }
    }
    Ok(methods)
}

fn validate_route_key_field_presence(
    syntax: Option<&str>,
    message: &str,
    field: &FieldDescriptorProto,
    field_name: &str,
) -> Result<(), CodegenError> {
    let label = field.label.and_then(|value| value.try_into().ok());
    if matches!(label, Some(Label::Repeated)) {
        return Err(optional_route_key_error(
            message,
            field_name,
            "route key fields cannot be repeated",
        ));
    }
    if field.proto3_optional.unwrap_or(false) {
        return Err(optional_route_key_error(
            message,
            field_name,
            "proto3 optional fields generate Option<T>",
        ));
    }
    if field.oneof_index.is_some() {
        return Err(optional_route_key_error(
            message,
            field_name,
            "oneof fields may be absent at runtime",
        ));
    }
    if syntax.unwrap_or("proto2") != "proto3" && !matches!(label, Some(Label::Required)) {
        return Err(optional_route_key_error(
            message,
            field_name,
            "proto2 route key fields must be required",
        ));
    }
    Ok(())
}

fn optional_route_key_error(message: &str, field: &str, reason: &str) -> CodegenError {
    CodegenError::OptionalRouteKeyField {
        message: message.to_string(),
        field: field.to_string(),
        reason: reason.to_string(),
    }
}
