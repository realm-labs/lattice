use crate::tests::*;

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
