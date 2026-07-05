use super::*;
use crate::descriptor::methods_from_descriptor;
use crate::render::generate_rpc_bindings;
use crate::route_key::{ProtoRouteKeyOption, RouteKeyType};
use crate::spec::RpcMethodSpec;
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
        "unary_dedup_secure(forwarded, self.security.policy(), peer.as_ref(), &self.deduplicator)"
    ));
    assert!(
        generated
            .rust
            .contains("unary_secure(forwarded, self.security.policy(), peer.as_ref())")
    );
    assert!(
        generated
            .rust
            .contains("pub struct GeneratedTonicEndpointTransport")
    );
    assert!(generated.rust.contains("pub fn with_transport_security"));
    assert!(generated.rust.contains("pub fn with_transport_config"));
    assert!(generated.rust.contains("\"world.WorldRpc/EnterWorld\" => self.call_world_rpc_enter_world::<Req>(target, metadata, request).await"));
    assert!(
        generated
            .rust
            .contains("let route_key = request.route_key();")
    );
    assert!(
        generated
            .rust
            .contains("get_or_connect_for_route_key(&target.advertised_endpoint, &route_key)")
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
    assert!(generated.rust.contains("register_gateway_routes"));
    assert!(
        generated
            .rust
            .contains("pub struct GatewayDispatcher<WorldRpcCore>")
    );
    assert!(
        generated
            .rust
            .contains("pub async fn dispatch(&self, frame: ClientFrame)")
    );
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
        method_options("world_id", Some(100)),
    );

    let methods = methods_from_descriptor(&descriptor, &options).unwrap();

    assert_eq!(methods, vec![world_enter_method()]);
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
        method_options("override_world_id", Some(100)),
    );

    let methods = methods_from_descriptor(&descriptor, &options).unwrap();

    let mut expected = world_enter_method();
    expected.route_key.key_field = "override_world_id".into();
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
        method_options("world_id", Some(100)),
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
        method_options("world_id", Some(100)),
    );

    let methods = methods_from_descriptor(&descriptor, &options).unwrap();

    assert_eq!(methods, vec![world_enter_method()]);
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
        method_options("world_id", Some(100)),
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
        method_options("world_id", Some(100)),
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
        method_options("world_id", Some(100)),
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
        .gateway_route_ids([(100, "world.WorldRpc.EnterWorld")])
        .compile_protos(&protos, &includes)
        .unwrap();

    let generated = std::fs::read_to_string(out_dir.join("lattice.generated.rs")).unwrap();
    assert!(generated.contains("impl RoutedRequest for crate::world::EnterWorldRequest"));
    assert!(generated.contains("pub mod world_rpc"));
    assert!(generated.contains("pub struct Client<C>"));
    assert!(generated.contains("pub const DEFAULT_MSG_ID: u32 = 100;"));
    assert!(out_dir.join("world.rs").exists());
    assert!(!out_dir.join("lattice.descriptor.bin").exists());
}

#[test]
fn builder_accepts_gateway_routes_from_toml_file() {
    let temp = tempfile::tempdir().unwrap();
    let proto_dir = temp.path().join("proto");
    let out_dir = temp.path().join("out");
    let routes_path = temp.path().join("gateway-routes.toml");
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
    std::fs::write(
        &routes_path,
        r#"[[routes]]
msg_id = 100
method = "world.WorldRpc.EnterWorld"
"#,
    )
    .unwrap();
    let protos = vec![proto_dir.join("world.proto")];
    let includes = vec![proto_dir, proto_include()];

    configure()
        .out_dir(&out_dir)
        .gateway_routes(&routes_path)
        .compile_protos(&protos, &includes)
        .unwrap();

    let generated = std::fs::read_to_string(out_dir.join("lattice.generated.rs")).unwrap();
    assert!(generated.contains("pub const DEFAULT_MSG_ID: u32 = 100;"));
    assert!(generated.contains("register_gateway_routes"));
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
