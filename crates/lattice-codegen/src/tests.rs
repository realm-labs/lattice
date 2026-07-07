use std::collections::BTreeSet;

use crate::builder::configure;
use crate::descriptor::methods_from_descriptor;
use crate::descriptor::{messages_from_descriptor, messages_from_descriptor_for_files};
use crate::error::CodegenError;
use crate::render::generate_rpc_bindings;
use crate::route_key::{ProtoRouteKeyOption, RouteKeyType};
use crate::spec::{GeneratedDirectLinkMessageSpec, GeneratedDirectLinkStreamSpec, RpcMethodSpec};
use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    MethodDescriptorProto, ServiceDescriptorProto,
    field_descriptor_proto::{Label, Type},
};

fn world_enter_method() -> RpcMethodSpec {
    RpcMethodSpec {
        package: "world".into(),
        service_kind: "World".into(),
        service_name: "WorldRpc".into(),
        method_name: "EnterWorld".into(),
        request_type: "crate::world::EnterWorldRequest".into(),
        reply_type: "crate::world::EnterWorldReply".into(),
        route_key: ProtoRouteKeyOption {
            actor_kind: "World".into(),
            key_field: "world_id".into(),
            key_type: RouteKeyType::U64,
        },
        route_key_from_request: true,
        gateway_msg_id: Some(100),
    }
}

fn world_descriptor() -> FileDescriptorSet {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("world.proto".into()),
            package: Some("world".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("EnterWorldRequest".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("world_id".into()),
                        number: Some(1),
                        label: Some(Label::Optional as i32),
                        r#type: Some(Type::Uint64 as i32),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("EnterWorldReply".into()),
                    ..Default::default()
                },
            ],
            service: vec![ServiceDescriptorProto {
                name: Some("WorldRpc".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("EnterWorld".into()),
                    input_type: Some(".world.EnterWorldRequest".into()),
                    output_type: Some(".world.EnterWorldReply".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            syntax: Some("proto3".into()),
            ..Default::default()
        }],
    }
}

fn nested_world_descriptor() -> FileDescriptorSet {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("world.proto".into()),
            package: Some("world".into()),
            message_type: vec![DescriptorProto {
                name: Some("Envelope".into()),
                nested_type: vec![
                    DescriptorProto {
                        name: Some("EnterWorldRequest".into()),
                        field: vec![FieldDescriptorProto {
                            name: Some("world_id".into()),
                            number: Some(1),
                            label: Some(Label::Optional as i32),
                            r#type: Some(Type::Uint64 as i32),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    DescriptorProto {
                        name: Some("EnterWorldReply".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("WorldRpc".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("EnterWorld".into()),
                    input_type: Some(".world.Envelope.EnterWorldRequest".into()),
                    output_type: Some(".world.Envelope.EnterWorldReply".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            syntax: Some("proto3".into()),
            ..Default::default()
        }],
    }
}

fn nested_world_enter_method() -> RpcMethodSpec {
    RpcMethodSpec {
        request_type: "crate::world::envelope::EnterWorldRequest".into(),
        reply_type: "crate::world::envelope::EnterWorldReply".into(),
        ..world_enter_method()
    }
}

fn service_options(service_kind: &str, actor_kind: &str) -> crate::descriptor::ParsedOptions {
    crate::descriptor::ParsedOptions {
        service_kind: Some(service_kind.to_string()),
        actor_kind: Some(actor_kind.to_string()),
        ..Default::default()
    }
}

fn service_options_with_route_key(
    service_kind: &str,
    actor_kind: &str,
    route_key: &str,
) -> crate::descriptor::ParsedOptions {
    crate::descriptor::ParsedOptions {
        service_kind: Some(service_kind.to_string()),
        actor_kind: Some(actor_kind.to_string()),
        route_key: Some(route_key.to_string()),
        ..Default::default()
    }
}

fn method_options(
    route_key: &str,
    gateway_msg_id: Option<u32>,
) -> crate::descriptor::ParsedOptions {
    crate::descriptor::ParsedOptions {
        route_key: Some(route_key.to_string()),
        gateway_msg_id,
        ..Default::default()
    }
}

mod builder;
mod descriptor;
mod generation;
