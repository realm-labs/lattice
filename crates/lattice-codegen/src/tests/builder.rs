use super::*;
use crate::proto_include;

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
