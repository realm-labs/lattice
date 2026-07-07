use super::*;
use std::collections::BTreeSet;

use crate::descriptor::methods_from_descriptor;
use crate::descriptor::{messages_from_descriptor, messages_from_descriptor_for_files};
use crate::render::generate_rpc_bindings;
use crate::route_key::{ProtoRouteKeyOption, RouteKeyType};
use crate::spec::{GeneratedDirectLinkMessageSpec, GeneratedDirectLinkStreamSpec, RpcMethodSpec};
use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    MethodDescriptorProto, ServiceDescriptorProto,
    field_descriptor_proto::{Label, Type},
};

#[test]
fn generated_output_matches_phase_two_shape() {
    let generated = generate_rpc_bindings(&[world_enter_method()]).unwrap();

    assert!(
        generated
            .rust
            .contains("impl RoutedRequest for crate::world::EnterWorldRequest")
    );
    assert!(generated.rust.contains("pub mod world_rpc"));
    assert!(generated.rust.contains("pub struct Client<C>"));
    assert!(
        generated
            .rust
            .contains("pub struct Binding<A = (), C = DefaultClientCore>")
    );
    assert!(
        generated
            .rust
            .contains("pub struct SingletonBinding<A = (), C = DefaultClientCore>")
    );
    assert!(
        generated
            .rust
            .contains("fn placement() -> RpcClientPlacement")
    );
    assert!(generated.rust.contains("RpcClientPlacement::Singleton"));
    assert!(
        generated
            .rust
            .contains("pub fn for_actor<A>() -> SingletonBinding<A>")
    );
    assert!(
        generated
            .rust
            .contains("pub struct SingletonRegistryService")
    );
    assert!(generated.rust.contains("placement_store.get_singleton"));
    assert!(generated.rust.contains("singleton route epoch mismatch"));
    assert!(generated.rust.contains("type Core = C;"));
    assert!(generated.rust.contains("type Client = Client<C>;"));
    assert!(
        generated
            .rust
            .contains("lattice_placement::ResolvingRpcCore<lattice_placement::BoxRouteResolver")
    );
    assert!(generated.rust.contains("fn build_default_core("));
    assert!(
        generated
            .rust
            .contains("retry_policy: lattice_placement::RpcRetryPolicy")
    );
    assert!(
        generated
            .rust
            .contains("transport_security: lattice_rpc::RpcTransportSecurity")
    );
    assert!(
        generated
            .rust
            .contains("transport_config: lattice_rpc::TonicEndpointChannelPoolConfig")
    );
    assert!(
        generated
            .rust
            .contains("super::GeneratedTonicEndpointTransport::with_transport_config")
    );
    assert!(generated.rust.contains(".with_retry_policy(retry_policy)"));
    assert!(
        generated
            .rust
            .contains("impl<A, C> RpcServiceBinding for Binding<A, C>")
    );
    assert!(generated.rust.contains("pub struct ActorService<A: Actor>"));
    assert!(
        generated
            .rust
            .contains("pub struct RegistryService<A: Actor, L>")
    );
    assert!(generated.rust.contains("RpcServerSecurity"));
    assert!(generated.rust.contains("context.rpc_security()"));
    assert!(
        generated
            .rust
            .contains("self.security.peer_identity(&request)")
    );
    assert!(generated.rust.contains("RequestDeduplicator"));
    assert!(generated.rust.contains("request_dedup: true"));
    assert!(
        generated
            .rust
            .contains("pub fn request_dedup(mut self, enabled: bool) -> Self")
    );
    assert!(
        generated
            .rust
            .contains("pub fn with_request_dedup(mut self, enabled: bool) -> Self")
    );
    assert!(generated.rust.contains("if self.request_dedup"));
    assert!(generated.rust.contains(
        "lattice_rpc::adapter::dispatch_actor_rpc_dedup_with_route(handle, route, req, ctx, &self.deduplicator)"
    ));
    assert!(
        generated.rust.contains(
            "lattice_rpc::adapter::dispatch_actor_rpc_with_route(handle, route, req, ctx)"
        )
    );
    assert!(
        generated
            .rust
            .contains("self.security.validate_context(&ctx, peer.as_ref())?")
    );
    assert!(
        !generated
            .rust
            .contains("let mut forwarded = tonic::Request::new(req);")
    );
    assert!(
        generated
            .rust
            .contains("pub struct GeneratedTonicEndpointTransport")
    );
    assert!(generated.rust.contains("pub fn with_transport_security"));
    assert!(generated.rust.contains("pub fn with_transport_config"));
    assert!(generated.rust.contains("\"world.WorldRpc/EnterWorld\" => self.call_world_rpc_enter_world::<Req>(target, route_key, metadata, request).await"));
    assert!(
        !generated
            .rust
            .contains("let route_key = request.route_key();")
    );
    assert!(
        generated
            .rust
            .contains("get_or_connect_for_route_key(&target.advertised_endpoint, route_key)")
    );
    assert!(
        generated
            .rust
            .contains("downcast::<crate::world::EnterWorldRequest>()")
    );
    assert!(generated.rust.contains("downcast::<Req::Reply>()"));
    assert!(!generated.rust.contains("#[allow(clippy::clone_on_copy)]"));
    assert!(
        !generated
            .rust
            .contains("downcast_ref::<crate::world::EnterWorldRequest>()")
    );
    assert!(
        generated
            .rust
            .contains("let request_id = lattice_rpc::RpcContext::from_metadata(&metadata)")
    );
    assert!(
        generated
            .rust
            .contains("tonic_status_to_rpc_error_for_request(status, Req::METHOD, request_id)")
    );
    assert!(
        !generated
            .rust
            .contains("let request_bytes = request.encode_to_vec();")
    );
    assert!(
        !generated
            .rust
            .contains("let reply_bytes = typed_reply.encode_to_vec();")
    );
    assert!(generated.rust.contains("pub mod enter_world"));
    assert!(generated.rust.contains("pub struct GatewayBinding"));
    assert!(!generated.rust.contains("GatewayRouteKeyPolicy"));
    assert!(generated.rust.contains("register_gateway_routes"));
    assert!(
        generated
            .rust
            .contains("pub struct GatewayDispatcher<WorldRpcCore>")
    );
    assert!(
        generated
            .rust
            .contains("pub async fn dispatch_with_context<R>(&self, frame: ClientFrame, router: &mut R, context: &lattice_gateway::GatewayRouteContext)")
    );
}

#[test]
fn descriptor_messages_generate_direct_link_message_metadata() {
    let messages = messages_from_descriptor(&world_descriptor());
    let generated = crate::render::generate_rpc_bindings_with_options(
        &[world_enter_method()],
        &messages,
        crate::render::RenderOptions::default(),
    )
    .unwrap();

    assert!(
        generated
            .rust
            .contains("impl lattice_core::DirectLinkMessage for crate::world::EnterWorldRequest")
    );
    assert!(
        generated
            .rust
            .contains("const PROTO_FULL_NAME: &'static str = \"world.EnterWorldRequest\"")
    );
    assert!(
        generated
            .rust
            .contains("impl lattice_core::DirectLinkMessage for crate::world::EnterWorldReply")
    );
}

#[test]
fn direct_link_message_metadata_ignores_descriptor_dependencies_not_compiled_into_crate() {
    let mut descriptor = world_descriptor();
    descriptor.file.push(FileDescriptorProto {
        name: Some("google/protobuf/descriptor.proto".into()),
        package: Some("google.protobuf".into()),
        message_type: vec![DescriptorProto {
            name: Some("FileDescriptorSet".into()),
            ..Default::default()
        }],
        ..Default::default()
    });

    let messages =
        messages_from_descriptor_for_files(&descriptor, &BTreeSet::from(["world.proto".into()]));
    let generated = crate::render::generate_rpc_bindings_with_options(
        &[world_enter_method()],
        &messages,
        crate::render::RenderOptions::default(),
    )
    .unwrap();

    assert!(
        generated
            .rust
            .contains("impl lattice_core::DirectLinkMessage for crate::world::EnterWorldRequest")
    );
    assert!(!generated.rust.contains("crate::google::protobuf"));
}

#[test]
fn descriptor_messages_resolve_nested_prost_type_paths() {
    let messages = messages_from_descriptor(&nested_world_descriptor());
    let generated = crate::render::generate_rpc_bindings_with_options(
        &[nested_world_enter_method()],
        &messages,
        crate::render::RenderOptions::default(),
    )
    .unwrap();

    assert!(generated.rust.contains(
        "impl lattice_core::DirectLinkMessage for crate::world::envelope::EnterWorldRequest"
    ));
    assert!(
        generated
            .rust
            .contains("const PROTO_FULL_NAME: &'static str = \"world.Envelope.EnterWorldRequest\"")
    );
    assert!(generated.rust.contains(
        "impl lattice_core::DirectLinkMessage for crate::world::envelope::EnterWorldReply"
    ));
}

#[test]
fn direct_link_stream_codegen_uses_static_match_dispatch() {
    let generated =
        crate::render::generate_direct_link_stream_bindings(&[GeneratedDirectLinkStreamSpec {
            module_name: "client_player_request".into(),
            stream_name: "client.player.request".into(),
            metadata_type: None,
            messages: vec![
                GeneratedDirectLinkMessageSpec {
                    message_id: 7001,
                    rust_type: "crate::world::EnterWorldRequest".into(),
                },
                GeneratedDirectLinkMessageSpec {
                    message_id: 7101,
                    rust_type: "crate::world::MoveWorldRequest".into(),
                },
            ],
        }])
        .unwrap();

    assert!(generated.rust.contains("pub mod client_player_request"));
    assert!(
        generated
            .rust
            .contains("impl lattice_core::DirectLinkStreamType for Stream")
    );
    assert!(
        generated
            .rust
            .contains("pub fn bind<A>(actor_kind: lattice_core::ActorKind)")
    );
    assert!(generated.rust.contains(
        "A: lattice_actor::Actor + lattice_actor::Handler<lattice_core::Linked<crate::world::EnterWorldRequest, ()>> + lattice_actor::Handler<lattice_core::Linked<crate::world::MoveWorldRequest, ()>>,"
    ));
    assert!(
        generated
            .rust
            .contains("impl<A> lattice_direct_link::DirectLinkDispatch<A, ()> for Stream")
    );
    assert!(generated.rust.contains("match message_id.0"));
    assert!(generated.rust.contains("7001 =>"));
    assert!(generated.rust.contains("7101 =>"));
    assert!(
        generated.rust.contains(
            "lattice_direct_link::try_deliver_linked(handle, payload, metadata, context)"
        )
    );
}

#[test]
fn direct_link_bidirectional_codegen_keeps_direction_handler_bounds_separate() {
    let generated = crate::render::generate_direct_link_stream_bindings(&[
        GeneratedDirectLinkStreamSpec {
            module_name: "client_player_request".into(),
            stream_name: "client.player.request".into(),
            metadata_type: None,
            messages: vec![GeneratedDirectLinkMessageSpec {
                message_id: 7001,
                rust_type: "crate::world::EnterWorldRequest".into(),
            }],
        },
        GeneratedDirectLinkStreamSpec {
            module_name: "client_player_response".into(),
            stream_name: "client.player.response".into(),
            metadata_type: None,
            messages: vec![GeneratedDirectLinkMessageSpec {
                message_id: 7002,
                rust_type: "crate::world::EnterWorldReply".into(),
            }],
        },
    ])
    .unwrap();

    let request_module = generated
        .rust
        .split("pub mod client_player_request")
        .nth(1)
        .and_then(|source| source.split("pub mod client_player_response").next())
        .expect("request stream module");
    assert!(
        request_module
            .contains("Handler<lattice_core::Linked<crate::world::EnterWorldRequest, ()>>")
    );
    assert!(
        !request_module
            .contains("Handler<lattice_core::Linked<crate::world::EnterWorldReply, ()>>")
    );

    let response_module = generated
        .rust
        .split("pub mod client_player_response")
        .nth(1)
        .expect("response stream module");
    assert!(
        response_module
            .contains("Handler<lattice_core::Linked<crate::world::EnterWorldReply, ()>>")
    );
    assert!(
        !response_module
            .contains("Handler<lattice_core::Linked<crate::world::EnterWorldRequest, ()>>")
    );
}

#[test]
fn direct_link_stream_codegen_supports_typed_metadata() {
    let generated =
        crate::render::generate_direct_link_stream_bindings(&[GeneratedDirectLinkStreamSpec {
            module_name: "client_player_request".into(),
            stream_name: "client.player.request".into(),
            metadata_type: Some("crate::direct_link::ClientRequestContext".into()),
            messages: vec![GeneratedDirectLinkMessageSpec {
                message_id: 7001,
                rust_type: "crate::world::EnterWorldRequest".into(),
            }],
        }])
        .unwrap();

    assert!(
        generated
            .rust
            .contains("type Metadata = crate::direct_link::ClientRequestContext;")
    );
    assert!(
        generated.rust.contains(
            "DirectLinkActorBinding<A, Stream, crate::direct_link::ClientRequestContext>"
        )
    );
    assert!(generated.rust.contains(
        "Handler<lattice_core::Linked<crate::world::EnterWorldRequest, crate::direct_link::ClientRequestContext>>"
    ));
    assert!(generated.rust.contains(
        "impl<A> lattice_direct_link::DirectLinkDispatch<A, crate::direct_link::ClientRequestContext> for Stream"
    ));
}

#[test]
fn direct_link_stream_codegen_rejects_duplicate_message_ids() {
    let error =
        crate::render::generate_direct_link_stream_bindings(&[GeneratedDirectLinkStreamSpec {
            module_name: "client_player".into(),
            stream_name: "client.player".into(),
            metadata_type: None,
            messages: vec![
                GeneratedDirectLinkMessageSpec {
                    message_id: 7001,
                    rust_type: "crate::world::EnterWorldRequest".into(),
                },
                GeneratedDirectLinkMessageSpec {
                    message_id: 7001,
                    rust_type: "crate::world::MoveWorldRequest".into(),
                },
            ],
        }])
        .unwrap_err();

    assert!(matches!(
        error,
        CodegenError::DuplicateDirectLinkMessageId {
            message_id: 7001,
            ..
        }
    ));
}

#[test]
fn duplicate_gateway_msg_id_is_rejected() {
    let mut first = world_enter_method();
    let mut second = world_enter_method();
    first.request_type = "crate::world::EnterWorldRequest".into();
    second.request_type = "crate::world::MoveWorldRequest".into();

    let error = generate_rpc_bindings(&[first, second]).unwrap_err();

    assert!(matches!(
        error,
        CodegenError::DuplicateGatewayMessageId { msg_id: 100 }
    ));
}

#[test]
fn descriptor_parsing_builds_method_specs() {
    let descriptor = world_descriptor();
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options("World", "World"),
    );
    options.insert(
        "world.WorldRpc.EnterWorld".to_string(),
        method_options("world_id", None),
    );

    let methods = methods_from_descriptor(&descriptor, &options).unwrap();

    let mut expected = world_enter_method();
    expected.gateway_msg_id = None;
    assert_eq!(methods, vec![expected]);
}

#[test]
fn descriptor_parsing_resolves_nested_request_and_reply_types() {
    let descriptor = nested_world_descriptor();
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options("World", "World"),
    );
    options.insert(
        "world.WorldRpc.EnterWorld".to_string(),
        method_options("world_id", None),
    );

    let methods = methods_from_descriptor(&descriptor, &options).unwrap();

    let mut expected = nested_world_enter_method();
    expected.gateway_msg_id = None;
    assert_eq!(methods, vec![expected]);
}

#[test]
fn descriptor_parsing_uses_service_default_route_key() {
    let descriptor = world_descriptor();
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options_with_route_key("World", "World", "world_id"),
    );

    let methods = methods_from_descriptor(&descriptor, &options).unwrap();

    let mut expected = world_enter_method();
    expected.gateway_msg_id = None;
    assert_eq!(methods, vec![expected]);
}

#[test]
fn descriptor_parsing_allows_method_route_key_override() {
    let mut descriptor = world_descriptor();
    descriptor.file[0].message_type[0]
        .field
        .push(FieldDescriptorProto {
            name: Some("override_world_id".into()),
            number: Some(2),
            label: Some(Label::Optional as i32),
            r#type: Some(Type::Uint64 as i32),
            ..Default::default()
        });
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options_with_route_key("World", "World", "world_id"),
    );
    options.insert(
        "world.WorldRpc.EnterWorld".to_string(),
        method_options("override_world_id", None),
    );

    let methods = methods_from_descriptor(&descriptor, &options).unwrap();

    let mut expected = world_enter_method();
    expected.route_key.key_field = "override_world_id".into();
    expected.gateway_msg_id = None;
    assert_eq!(methods, vec![expected]);
}

#[test]
fn descriptor_parsing_rejects_unsupported_route_key_type() {
    let mut descriptor = world_descriptor();
    let request = descriptor.file[0].message_type[0].clone();
    descriptor.file[0].message_type[0] = DescriptorProto {
        field: vec![FieldDescriptorProto {
            name: Some("world_id".into()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(Type::Bool as i32),
            ..Default::default()
        }],
        ..request
    };
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options("World", "World"),
    );
    options.insert(
        "world.WorldRpc.EnterWorld".to_string(),
        method_options("world_id", None),
    );

    let error = methods_from_descriptor(&descriptor, &options).unwrap_err();

    assert!(matches!(
        error,
        CodegenError::UnsupportedRouteKeyFieldType { .. }
    ));
}

#[test]
fn descriptor_parsing_accepts_proto2_required_route_key() {
    let mut descriptor = world_descriptor();
    descriptor.file[0].syntax = Some("proto2".into());
    descriptor.file[0].message_type[0].field[0].label = Some(Label::Required as i32);
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options("World", "World"),
    );
    options.insert(
        "world.WorldRpc.EnterWorld".to_string(),
        method_options("world_id", None),
    );

    let methods = methods_from_descriptor(&descriptor, &options).unwrap();

    let mut expected = world_enter_method();
    expected.gateway_msg_id = None;
    assert_eq!(methods, vec![expected]);
}

#[test]
fn descriptor_parsing_rejects_proto2_optional_route_key() {
    let mut descriptor = world_descriptor();
    descriptor.file[0].syntax = Some("proto2".into());
    descriptor.file[0].message_type[0].field[0].label = Some(Label::Optional as i32);
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options("World", "World"),
    );
    options.insert(
        "world.WorldRpc.EnterWorld".to_string(),
        method_options("world_id", None),
    );

    let error = methods_from_descriptor(&descriptor, &options).unwrap_err();

    assert!(matches!(error, CodegenError::OptionalRouteKeyField { .. }));
}

#[test]
fn descriptor_parsing_rejects_proto3_optional_route_key() {
    let mut descriptor = world_descriptor();
    descriptor.file[0].message_type[0].oneof_decl = vec![Default::default()];
    descriptor.file[0].message_type[0].field[0].proto3_optional = Some(true);
    descriptor.file[0].message_type[0].field[0].oneof_index = Some(0);
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options("World", "World"),
    );
    options.insert(
        "world.WorldRpc.EnterWorld".to_string(),
        method_options("world_id", None),
    );

    let error = methods_from_descriptor(&descriptor, &options).unwrap_err();

    assert!(matches!(error, CodegenError::OptionalRouteKeyField { .. }));
}

#[test]
fn descriptor_parsing_rejects_repeated_route_key() {
    let mut descriptor = world_descriptor();
    descriptor.file[0].message_type[0].field[0].label = Some(Label::Repeated as i32);
    let mut options = std::collections::BTreeMap::new();
    options.insert(
        "world.WorldRpc".to_string(),
        service_options("World", "World"),
    );
    options.insert(
        "world.WorldRpc.EnterWorld".to_string(),
        method_options("world_id", None),
    );

    let error = methods_from_descriptor(&descriptor, &options).unwrap_err();

    assert!(matches!(error, CodegenError::OptionalRouteKeyField { .. }));
}

#[test]
fn gateway_route_table_registration_is_generated_from_method_metadata() {
    let first = world_enter_method();
    let mut second = world_enter_method();
    second.service_name = "RoomRpc".into();
    second.method_name = "JoinRoom".into();
    second.request_type = "crate::world::JoinRoomRequest".into();
    second.reply_type = "crate::world::JoinRoomReply".into();
    second.gateway_msg_id = Some(101);

    let generated = generate_rpc_bindings(&[first, second]).unwrap();

    assert!(
        generated
            .rust
            .contains("table.register(world_rpc::enter_world::GatewayBinding::route_spec())?;")
    );
    assert!(
        generated
            .rust
            .contains("table.register(room_rpc::join_room::GatewayBinding::route_spec())?;")
    );
    assert!(
        generated
            .rust
            .contains("pub struct GatewayDispatcher<WorldRpcCore, RoomRpcCore>")
    );
}

#[test]
fn gateway_binding_uses_message_router_for_route_decision() {
    let generated = generate_rpc_bindings(&[world_enter_method()]).unwrap();

    assert!(generated.rust.contains("R: MessageRouter"));
    assert!(
        generated
            .rust
            .contains("let decision = router.route(context, &route)?;")
    );
    assert!(
        generated
            .rust
            .contains("decision.actor_kind,\n                    decision.route_key,")
    );
    assert!(!generated.rust.contains("GatewayRouteKeyPolicy"));
}

#[test]
fn multiple_methods_on_one_service_do_not_force_package_disambiguation() {
    let first = world_enter_method();
    let mut second = world_enter_method();
    second.method_name = "LeaveWorld".into();
    second.request_type = "crate::world::LeaveWorldRequest".into();
    second.reply_type = "crate::world::LeaveWorldReply".into();
    second.gateway_msg_id = Some(101);

    let generated = generate_rpc_bindings(&[first, second]).unwrap();

    assert!(generated.rust.contains("pub mod world_rpc"));
    assert!(!generated.rust.contains("pub mod world_world_rpc"));
    assert!(
        generated
            .rust
            .contains("table.register(world_rpc::enter_world::GatewayBinding::route_spec())?;")
    );
    assert!(
        generated
            .rust
            .contains("table.register(world_rpc::leave_world::GatewayBinding::route_spec())?;")
    );
}

#[test]
fn gateway_binding_is_generated_without_default_registration() {
    let mut method = world_enter_method();
    method.gateway_msg_id = None;

    let generated = generate_rpc_bindings(&[method]).unwrap();

    assert!(generated.rust.contains("pub mod enter_world"));
    assert!(generated.rust.contains("pub struct GatewayBinding;"));
    assert!(generated.rust.contains("pub fn binding(msg_id: u32)"));
    assert!(!generated.rust.contains("DEFAULT_MSG_ID"));
    assert!(!generated.rust.contains("register_gateway_routes"));
}

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

#[test]
fn generated_code_snapshot_can_be_encoded_in_descriptor_test() {
    let descriptor = world_descriptor();
    let bytes = descriptor.encode_to_vec();
    let decoded = FileDescriptorSet::decode(bytes.as_slice()).unwrap();

    assert_eq!(decoded.file[0].package.as_deref(), Some("world"));
}

#[test]
fn builder_compiles_proto_and_writes_lattice_generated_file() {
    let temp = tempfile::tempdir().unwrap();
    let proto_dir = temp.path().join("proto");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("world.proto"),
        r#"syntax = "proto3";
package world;
import "lattice/options.proto";
service WorldRpc {
  option (lattice.options.service_kind) = "World";
  option (lattice.options.actor_kind) = "World";
  option (lattice.options.default_route_key) = "world_id";
  rpc EnterWorld(EnterWorldRequest) returns (EnterWorldReply) {
    option (lattice.options.gateway_msg_id) = 100;
  }
}
message EnterWorldRequest {
  uint64 world_id = 1;
}
message EnterWorldReply {
  bool ok = 1;
}
"#,
    )
    .unwrap();
    let protos = vec![proto_dir.join("world.proto")];
    let includes = vec![proto_dir, proto_include()];

    configure()
        .out_dir(&out_dir)
        .compile_protos(&protos, &includes)
        .unwrap();

    let generated = std::fs::read_to_string(out_dir.join("lattice.generated.rs")).unwrap();
    assert!(!generated.contains("impl RoutedRequest for crate::world::EnterWorldRequest"));
    assert!(generated.contains("pub mod world_rpc"));
    assert!(generated.contains("pub struct Client<C>"));
    assert!(generated.contains("pub const DEFAULT_MSG_ID: u32 = 100;"));
    assert!(generated.contains("let decision = router.route(context, &route)?;"));
    assert!(out_dir.join("world.rs").exists());
    assert!(!out_dir.join("lattice.descriptor.bin").exists());
}

#[test]
fn builder_accepts_gateway_msg_id_method_option() {
    let temp = tempfile::tempdir().unwrap();
    let proto_dir = temp.path().join("proto");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("world.proto"),
        r#"syntax = "proto3";
package world;
import "lattice/options.proto";
service WorldRpc {
  option (lattice.options.service_kind) = "World";
  option (lattice.options.actor_kind) = "World";
  option (lattice.options.default_route_key) = "world_id";
  rpc EnterWorld(EnterWorldRequest) returns (EnterWorldReply) {
    option (lattice.options.gateway_msg_id) = 100;
  }
}
message EnterWorldRequest {
  uint64 world_id = 1;
}
message EnterWorldReply {
  bool ok = 1;
}
"#,
    )
    .unwrap();
    let protos = vec![proto_dir.join("world.proto")];
    let includes = vec![proto_dir, proto_include()];

    configure()
        .out_dir(&out_dir)
        .compile_protos(&protos, &includes)
        .unwrap();

    let generated = std::fs::read_to_string(out_dir.join("lattice.generated.rs")).unwrap();
    assert!(generated.contains("pub const DEFAULT_MSG_ID: u32 = 100;"));
    assert!(generated.contains("register_gateway_routes"));
    assert!(generated.contains("let decision = router.route(context, &route)?;"));
    assert!(!generated.contains("GatewayRouteKeyPolicy"));
}

#[test]
fn builder_allows_context_routed_gateway_method_without_request_route_key_field() {
    let temp = tempfile::tempdir().unwrap();
    let proto_dir = temp.path().join("proto");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("player.proto"),
        r#"syntax = "proto3";
package player;
import "lattice/options.proto";
service PlayerRpc {
  option (lattice.options.service_kind) = "Player";
  option (lattice.options.actor_kind) = "Player";
  option (lattice.options.default_route_key) = "player_id";
  rpc AllItem(AllItemRequest) returns (AllItemReply) {
    option (lattice.options.gateway_msg_id) = 7101;
  }
}
message AllItemRequest {
}
message AllItemReply {
  bool ok = 1;
}
"#,
    )
    .unwrap();
    let protos = vec![proto_dir.join("player.proto")];
    let includes = vec![proto_dir, proto_include()];

    configure()
        .out_dir(&out_dir)
        .compile_protos(&protos, &includes)
        .unwrap();

    let generated = std::fs::read_to_string(out_dir.join("lattice.generated.rs")).unwrap();
    assert!(!generated.contains("impl RoutedRequest for crate::player::AllItemRequest"));
    assert!(generated.contains(
        "pub async fn all_item(&self, route_key: RouteKey, req: crate::player::AllItemRequest)"
    ));
    assert!(generated.contains("let decision = router.route(context, &route)?;"));
    assert!(generated.contains("core.call_routed(RoutedEnvelope::new("));
}

#[test]
fn builder_does_not_generate_request_route_key_for_gateway_method() {
    let temp = tempfile::tempdir().unwrap();
    let proto_dir = temp.path().join("proto");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("player.proto"),
        r#"syntax = "proto3";
package player;
import "lattice/options.proto";
service PlayerRpc {
  option (lattice.options.service_kind) = "Player";
  option (lattice.options.actor_kind) = "Player";
  option (lattice.options.default_route_key) = "player_id";
  rpc Login(LoginRequest) returns (LoginReply) {
    option (lattice.options.gateway_msg_id) = 7001;
  }
}
message LoginRequest {
  string account = 1;
}
message LoginReply {
  bool ok = 1;
}
"#,
    )
    .unwrap();
    let protos = vec![proto_dir.join("player.proto")];
    let includes = vec![proto_dir, proto_include()];

    configure()
        .out_dir(&out_dir)
        .compile_protos(&protos, &includes)
        .unwrap();

    let generated = std::fs::read_to_string(out_dir.join("lattice.generated.rs")).unwrap();
    assert!(generated.contains(
        "pub async fn login(&self, route_key: RouteKey, req: crate::player::LoginRequest)"
    ));
    assert!(!generated.contains("RouteKey::Str(self.account.clone())"));
    assert!(!generated.contains("GatewayRouteKeyPolicy"));
    assert!(!generated.contains("self.player_id"));
}

#[test]
fn builder_keeps_mixed_gateway_methods_router_based() {
    let temp = tempfile::tempdir().unwrap();
    let proto_dir = temp.path().join("proto");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("player.proto"),
        r#"syntax = "proto3";
package player;
import "lattice/options.proto";
service PlayerRpc {
  option (lattice.options.service_kind) = "Player";
  option (lattice.options.actor_kind) = "Player";
  option (lattice.options.default_route_key) = "player_id";
  rpc Login(LoginRequest) returns (LoginReply) {
    option (lattice.options.gateway_msg_id) = 7001;
  }
  rpc AllItem(AllItemRequest) returns (AllItemReply) {
    option (lattice.options.gateway_msg_id) = 7101;
  }
}
message LoginRequest {
  string account = 1;
}
message LoginReply {
  bool ok = 1;
}
message AllItemRequest {
}
message AllItemReply {
  bool ok = 1;
}
"#,
    )
    .unwrap();
    let protos = vec![proto_dir.join("player.proto")];
    let includes = vec![proto_dir, proto_include()];

    configure()
        .out_dir(&out_dir)
        .compile_protos(&protos, &includes)
        .unwrap();

    let generated = std::fs::read_to_string(out_dir.join("lattice.generated.rs")).unwrap();
    assert!(!generated.contains("GatewayRouteKeyPolicy"));
    assert!(generated.contains("let decision = router.route(context, &route)?;"));
    assert!(generated.contains(
        "pub async fn all_item(&self, route_key: RouteKey, req: crate::player::AllItemRequest)"
    ));
}

#[test]
fn builder_keeps_imported_proto2_gateway_methods_router_based() {
    let temp = tempfile::tempdir().unwrap();
    let proto_dir = temp.path().join("proto");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("client.proto"),
        r#"syntax = "proto2";
package com.yanmonet.p9server.protocol;
message LoginRequest {
  required string account = 1;
}
message LoginResponse {
  optional bool ok = 1;
}
message AllItemRequest {
}
message AllItemResponse {
  optional bool ok = 1;
}
"#,
    )
    .unwrap();
    std::fs::write(
        proto_dir.join("player.proto"),
        r#"syntax = "proto3";
package player;
import "lattice/options.proto";
import "client.proto";
service PlayerRpc {
  option (lattice.options.service_kind) = "Player";
  option (lattice.options.actor_kind) = "Player";
  option (lattice.options.default_route_key) = "player_id";
  rpc Login(.com.yanmonet.p9server.protocol.LoginRequest) returns (.com.yanmonet.p9server.protocol.LoginResponse) {
    option (lattice.options.gateway_msg_id) = 7001;
  }
  rpc AllItem(.com.yanmonet.p9server.protocol.AllItemRequest) returns (.com.yanmonet.p9server.protocol.AllItemResponse) {
    option (lattice.options.gateway_msg_id) = 7101;
  }
}
"#,
    )
    .unwrap();
    let protos = vec![proto_dir.join("player.proto")];
    let includes = vec![proto_dir, proto_include()];

    configure()
        .out_dir(&out_dir)
        .compile_protos(&protos, &includes)
        .unwrap();

    let generated = std::fs::read_to_string(out_dir.join("lattice.generated.rs")).unwrap();
    assert!(!generated.contains("GatewayRouteKeyPolicy"));
    assert!(generated.contains("let decision = router.route(context, &route)?;"));
    assert!(generated.contains("pub async fn all_item(&self, route_key: RouteKey, req: crate::com::yanmonet::p9server::protocol::AllItemRequest)"));
}

#[test]
fn builder_can_keep_descriptor_set_for_debugging() {
    let temp = tempfile::tempdir().unwrap();
    let proto_dir = temp.path().join("proto");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("world.proto"),
        r#"syntax = "proto3";
package world;
import "lattice/options.proto";
service WorldRpc {
  option (lattice.options.service_kind) = "World";
  option (lattice.options.actor_kind) = "World";
  option (lattice.options.default_route_key) = "world_id";
  rpc EnterWorld(EnterWorldRequest) returns (EnterWorldReply);
}
message EnterWorldRequest {
  uint64 world_id = 1;
}
message EnterWorldReply {
  bool ok = 1;
}
"#,
    )
    .unwrap();
    let protos = vec![proto_dir.join("world.proto")];
    let includes = vec![proto_dir, proto_include()];

    configure()
        .out_dir(&out_dir)
        .emit_descriptor_set(true)
        .compile_protos(&protos, &includes)
        .unwrap();

    assert!(out_dir.join("lattice.descriptor.bin").exists());
}
