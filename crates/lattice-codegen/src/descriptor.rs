mod messages;
mod methods;
mod options;
mod types;

use std::collections::{BTreeMap, BTreeSet};

use prost_types::FileDescriptorSet;

use crate::error::CodegenError;
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
    options::parse_proto_options(descriptor, descriptor_bytes)
}

pub fn methods_from_descriptor(
    descriptor: &FileDescriptorSet,
    options: &BTreeMap<String, ParsedOptions>,
) -> Result<Vec<RpcMethodSpec>, CodegenError> {
    methods::methods_from_descriptor(descriptor, options)
}

pub fn messages_from_descriptor(descriptor: &FileDescriptorSet) -> Vec<ProtoMessageSpec> {
    messages::messages_from_descriptor(descriptor)
}

pub fn messages_from_descriptor_for_files(
    descriptor: &FileDescriptorSet,
    file_names: &BTreeSet<String>,
) -> Vec<ProtoMessageSpec> {
    messages::messages_from_descriptor_for_files(descriptor, file_names)
}
