use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
pub(crate) struct ResolvedMessage<'a> {
    pub(crate) descriptor: &'a DescriptorProto,
    pub(crate) rust_type: String,
}

pub(crate) fn find_message<'a>(
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

pub(crate) fn find_message_in_message<'a>(
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

pub(crate) fn find_field<'a>(
    message: &'a DescriptorProto,
    field: &str,
) -> Option<&'a FieldDescriptorProto> {
    message
        .field
        .iter()
        .find(|candidate| candidate.name.as_deref() == Some(field))
}

pub(crate) fn rust_type_path(proto_type: &str) -> String {
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

pub(crate) fn rust_type_path_from_parts(
    package: &str,
    parents: &[String],
    type_name: &str,
) -> String {
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

pub(crate) fn scoped_name(package: &str, name: &str) -> String {
    if package.is_empty() {
        name.to_string()
    } else {
        format!("{package}.{name}")
    }
}
