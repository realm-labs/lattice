use std::collections::{BTreeMap, BTreeSet};

use prost_reflect::{DescriptorPool, DynamicMessage, ExtensionDescriptor, Value};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet, field_descriptor_proto::Label,
};

use crate::CodegenError;
use crate::route_key::{ProtoRouteKeyOption, RouteKeyType};
use crate::spec::{ProtoMessageSpec, RpcMethodSpec};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedOptions {
    pub service_kind: Option<String>,
    pub actor_kind: Option<String>,
    pub route_key: Option<String>,
    pub gateway_msg_id: Option<u32>,
}

pub fn parse_proto_options(
    descriptor: &FileDescriptorSet,
    descriptor_bytes: &[u8],
) -> Result<BTreeMap<String, ParsedOptions>, CodegenError> {
    let pool = DescriptorPool::decode(descriptor_bytes)
        .map_err(|error| CodegenError::DescriptorRead(error.to_string()))?;
    let extensions = LatticeExtensions::from_pool(&pool)?;
    let mut output = BTreeMap::new();

    for file in &descriptor.file {
        let package = file.package.clone().unwrap_or_default();
        for service in &file.service {
            let service_name = service.name.clone().unwrap_or_default();
            let service_key = scoped_name(&package, &service_name);
            let Some(service_descriptor) = pool.get_service_by_name(&service_key) else {
                continue;
            };
            let service_options = service_descriptor.options();
            let parsed_service = ParsedOptions {
                service_kind: string_extension(&service_options, &extensions.service_kind),
                actor_kind: string_extension(&service_options, &extensions.actor_kind),
                route_key: string_extension(&service_options, &extensions.default_route_key),
                ..Default::default()
            };
            if parsed_service.service_kind.is_some()
                || parsed_service.actor_kind.is_some()
                || parsed_service.route_key.is_some()
            {
                output.insert(service_key.clone(), parsed_service);
            }

            for method in service_descriptor.methods() {
                let method_key = method.full_name().to_string();
                let method_options = method.options();
                let parsed_method = ParsedOptions {
                    route_key: string_extension(&method_options, &extensions.route_key),
                    gateway_msg_id: u32_extension(&method_options, &extensions.gateway_msg_id),
                    ..Default::default()
                };
                if parsed_method.route_key.is_some() || parsed_method.gateway_msg_id.is_some() {
                    output.insert(method_key, parsed_method);
                }
            }
        }
    }
    Ok(output)
}

pub fn methods_from_descriptor(
    descriptor: &FileDescriptorSet,
    options: &BTreeMap<String, ParsedOptions>,
) -> Result<Vec<RpcMethodSpec>, CodegenError> {
    let mut methods = Vec::new();
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
                let field = find_field(request.descriptor, &route_key).ok_or_else(|| {
                    CodegenError::RouteKeyFieldNotFound {
                        message: request_proto.clone(),
                        field: route_key.clone(),
                    }
                })?;
                validate_route_key_field_presence(
                    file.syntax.as_deref(),
                    &request_proto,
                    field,
                    &route_key,
                )?;
                let field_type = field
                    .r#type
                    .and_then(|value| value.try_into().ok())
                    .and_then(RouteKeyType::from_field_type)
                    .ok_or_else(|| CodegenError::UnsupportedRouteKeyFieldType {
                        message: request_proto.clone(),
                        field: route_key.clone(),
                    })?;

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
                        key_field: route_key,
                        key_type: field_type,
                    },
                    gateway_msg_id: method_options.and_then(|options| options.gateway_msg_id),
                });
            }
        }
    }
    Ok(methods)
}

pub fn messages_from_descriptor(descriptor: &FileDescriptorSet) -> Vec<ProtoMessageSpec> {
    messages_from_descriptor_files(descriptor, None)
}

pub fn messages_from_descriptor_for_files(
    descriptor: &FileDescriptorSet,
    file_names: &BTreeSet<String>,
) -> Vec<ProtoMessageSpec> {
    messages_from_descriptor_files(descriptor, Some(file_names))
}

fn messages_from_descriptor_files(
    descriptor: &FileDescriptorSet,
    file_names: Option<&BTreeSet<String>>,
) -> Vec<ProtoMessageSpec> {
    let mut messages = Vec::new();
    for file in &descriptor.file {
        if let Some(file_names) = file_names {
            let file_name = file.name.as_deref().unwrap_or_default();
            if !file_names.contains(&normalize_proto_file_name(file_name)) {
                continue;
            }
        }
        let package = file.package.clone().unwrap_or_default();
        for message in &file.message_type {
            collect_message_specs(&mut messages, &package, Vec::new(), message);
        }
    }
    messages
}

fn normalize_proto_file_name(path: &str) -> String {
    path.replace('\\', "/").trim_start_matches("./").to_string()
}

fn collect_message_specs(
    messages: &mut Vec<ProtoMessageSpec>,
    package: &str,
    parents: Vec<String>,
    message: &DescriptorProto,
) {
    let name = message.name.clone().unwrap_or_default();
    let proto_full_name = if parents.is_empty() {
        scoped_name(package, &name)
    } else {
        scoped_name(package, &format!("{}.{}", parents.join("."), name))
    };
    messages.push(ProtoMessageSpec {
        proto_full_name: proto_full_name.clone(),
        rust_type: rust_type_path_from_parts(package, &parents, &name),
    });

    let mut nested_parents = parents;
    nested_parents.push(name);
    for nested in &message.nested_type {
        collect_message_specs(messages, package, nested_parents.clone(), nested);
    }
}

struct ResolvedMessage<'a> {
    descriptor: &'a DescriptorProto,
    rust_type: String,
}

fn find_message<'a>(
    descriptor: &'a FileDescriptorSet,
    full_name: &str,
) -> Option<ResolvedMessage<'a>> {
    for file in &descriptor.file {
        let package = file.package.clone().unwrap_or_default();
        for message in &file.message_type {
            if let Some(found) = find_message_in_message(&package, Vec::new(), message, full_name) {
                return Some(found);
            }
        }
    }
    None
}

fn find_message_in_message<'a>(
    package: &str,
    parents: Vec<String>,
    message: &'a DescriptorProto,
    full_name: &str,
) -> Option<ResolvedMessage<'a>> {
    let name = message.name.clone().unwrap_or_default();
    let proto_full_name = if parents.is_empty() {
        scoped_name(package, &name)
    } else {
        scoped_name(package, &format!("{}.{}", parents.join("."), name))
    };
    if proto_full_name == full_name {
        return Some(ResolvedMessage {
            descriptor: message,
            rust_type: rust_type_path_from_parts(package, &parents, &name),
        });
    }

    let mut nested_parents = parents;
    nested_parents.push(name);
    for nested in &message.nested_type {
        if let Some(found) =
            find_message_in_message(package, nested_parents.clone(), nested, full_name)
        {
            return Some(found);
        }
    }
    None
}

fn find_field<'a>(message: &'a DescriptorProto, field: &str) -> Option<&'a FieldDescriptorProto> {
    message
        .field
        .iter()
        .find(|candidate| candidate.name.as_deref() == Some(field))
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

fn rust_type_path(proto_type: &str) -> String {
    let mut parts = proto_type.split('.').collect::<Vec<_>>();
    let type_name = parts.pop().unwrap_or_default();
    let module_path = parts
        .into_iter()
        .flat_map(|part| part.split('_'))
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("::");
    if module_path.is_empty() {
        format!("crate::{type_name}")
    } else {
        format!("crate::{module_path}::{type_name}")
    }
}

fn rust_type_path_from_parts(package: &str, parents: &[String], type_name: &str) -> String {
    let mut modules = package
        .split('.')
        .flat_map(|part| part.split('_'))
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    modules.extend(parents.iter().map(|parent| message_module_name(parent)));
    if modules.is_empty() {
        format!("crate::{type_name}")
    } else {
        format!("crate::{}::{type_name}", modules.join("::"))
    }
}

fn message_module_name(name: &str) -> String {
    let mut output = String::new();
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                output.push('_');
            }
            output.push(ch.to_ascii_lowercase());
        } else {
            output.push(ch);
        }
    }
    output
}

fn scoped_name(package: &str, name: &str) -> String {
    if package.is_empty() {
        name.to_string()
    } else {
        format!("{package}.{name}")
    }
}

struct LatticeExtensions {
    service_kind: ExtensionDescriptor,
    actor_kind: ExtensionDescriptor,
    default_route_key: ExtensionDescriptor,
    route_key: ExtensionDescriptor,
    gateway_msg_id: ExtensionDescriptor,
}

impl LatticeExtensions {
    fn from_pool(pool: &DescriptorPool) -> Result<Self, CodegenError> {
        Ok(Self {
            service_kind: required_extension(pool, "lattice.options.service_kind")?,
            actor_kind: required_extension(pool, "lattice.options.actor_kind")?,
            default_route_key: required_extension(pool, "lattice.options.default_route_key")?,
            route_key: required_extension(pool, "lattice.options.route_key")?,
            gateway_msg_id: required_extension(pool, "lattice.options.gateway_msg_id")?,
        })
    }
}

fn required_extension(
    pool: &DescriptorPool,
    name: &'static str,
) -> Result<ExtensionDescriptor, CodegenError> {
    pool.get_extension_by_name(name)
        .ok_or_else(|| CodegenError::DescriptorRead(format!("missing lattice option {name}")))
}

fn string_extension(message: &DynamicMessage, extension: &ExtensionDescriptor) -> Option<String> {
    if !message.has_extension(extension) {
        return None;
    }
    match message.get_extension(extension).as_ref() {
        Value::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn u32_extension(message: &DynamicMessage, extension: &ExtensionDescriptor) -> Option<u32> {
    if !message.has_extension(extension) {
        return None;
    }
    match message.get_extension(extension).as_ref() {
        Value::U32(value) => Some(*value),
        _ => None,
    }
}
