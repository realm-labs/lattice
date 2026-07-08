use std::collections::BTreeSet;

use prost_types::{DescriptorProto, FileDescriptorSet};

use crate::descriptor::types::{rust_type_path_from_parts, scoped_name};
use crate::spec::ProtoMessageSpec;
pub(crate) fn messages_from_descriptor(descriptor: &FileDescriptorSet) -> Vec<ProtoMessageSpec> {
    messages_from_descriptor_files(descriptor, None)
}

pub(crate) fn messages_from_descriptor_for_files(
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
