use std::collections::BTreeMap;

use prost_reflect::{DescriptorPool, DynamicMessage, ExtensionDescriptor, Value};
use prost_types::FileDescriptorSet;

use crate::descriptor::ParsedOptions;
use crate::descriptor::types::scoped_name;
use crate::error::CodegenError;
pub(crate) fn parse_proto_options(
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
