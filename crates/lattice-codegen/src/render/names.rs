use std::collections::{BTreeMap, BTreeSet};

use crate::spec::RpcMethodSpec;
pub(crate) fn group_by_service(
    methods: &[RpcMethodSpec],
) -> BTreeMap<(&str, &str), Vec<&RpcMethodSpec>> {
    let mut grouped = BTreeMap::new();
    for method in methods {
        grouped
            .entry((method.package.as_str(), method.service_name.as_str()))
            .or_insert_with(Vec::new)
            .push(method);
    }
    grouped
}

pub(crate) fn tonic_service_trait_path(method: &RpcMethodSpec) -> String {
    let module = package_module(&method.package);
    let server_module = format!("{}_server", lower_camel_to_snake(&method.service_name));
    if module.is_empty() {
        format!("crate::{server_module}::{}", method.service_name)
    } else {
        format!("crate::{module}::{server_module}::{}", method.service_name)
    }
}

pub(crate) fn tonic_server_path(method: &RpcMethodSpec) -> String {
    let module = package_module(&method.package);
    let server_module = format!("{}_server", lower_camel_to_snake(&method.service_name));
    if module.is_empty() {
        format!("crate::{server_module}::{}Server", method.service_name)
    } else {
        format!(
            "crate::{module}::{server_module}::{}Server",
            method.service_name
        )
    }
}

pub(crate) fn tonic_client_path(method: &RpcMethodSpec) -> String {
    let module = package_module(&method.package);
    let client_module = format!("{}_client", lower_camel_to_snake(&method.service_name));
    if module.is_empty() {
        format!("crate::{client_module}::{}Client", method.service_name)
    } else {
        format!(
            "crate::{module}::{client_module}::{}Client",
            method.service_name
        )
    }
}

pub(crate) fn method_fn_suffix(method: &RpcMethodSpec) -> String {
    format!(
        "{}_{}",
        lower_camel_to_snake(&method.service_name),
        lower_camel_to_snake(&method.method_name)
    )
}

fn package_module(package: &str) -> String {
    package
        .split('.')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("::")
}

pub(crate) struct NameDisambiguation {
    duplicate_service_names: BTreeSet<String>,
}

impl NameDisambiguation {
    pub(crate) fn new(methods: &[RpcMethodSpec]) -> Self {
        let mut seen = BTreeSet::<(String, String)>::new();
        let mut duplicate_service_names = BTreeSet::new();
        for method in methods {
            if !seen.insert((method.package.clone(), method.service_name.clone())) {
                continue;
            }
            if seen
                .iter()
                .filter(|(_, service)| service == &method.service_name)
                .count()
                > 1
            {
                duplicate_service_names.insert(method.service_name.clone());
            }
        }
        Self {
            duplicate_service_names,
        }
    }

    fn disambiguate(&self, method: &RpcMethodSpec) -> bool {
        self.duplicate_service_names.contains(&method.service_name)
    }
}

pub(crate) fn service_module_name(method: &RpcMethodSpec, names: &NameDisambiguation) -> String {
    let service = lower_camel_to_snake(&method.service_name);
    if !names.disambiguate(method) {
        return service;
    }
    let package = package_identifier_prefix(&method.package);
    if package.is_empty() {
        service
    } else {
        format!("{package}_{service}")
    }
}

pub(crate) fn service_field_name(method: &RpcMethodSpec, names: &NameDisambiguation) -> String {
    service_module_name(method, names)
}

pub(crate) fn service_type_prefix(method: &RpcMethodSpec, names: &NameDisambiguation) -> String {
    if !names.disambiguate(method) {
        return method.service_name.clone();
    }
    let package = method
        .package
        .split('.')
        .filter(|part| !part.is_empty())
        .map(upper_camel_identifier)
        .collect::<String>();
    format!("{package}{}", method.service_name)
}

fn package_identifier_prefix(package: &str) -> String {
    package
        .split('.')
        .filter(|part| !part.is_empty())
        .map(lower_camel_to_snake)
        .collect::<Vec<_>>()
        .join("_")
}

fn upper_camel_identifier(value: &str) -> String {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut output = String::new();
    output.push(first.to_ascii_uppercase());
    output.extend(chars);
    output
}

pub(crate) fn lower_camel_to_snake(value: &str) -> String {
    let mut output = String::new();
    for (index, ch) in value.chars().enumerate() {
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
