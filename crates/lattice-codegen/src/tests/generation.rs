use super::*;

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
    assert!(generated.rust.contains(
        "lattice_placement::routing::rpc::ResolvingRpcCore<lattice_placement::routing::resolver::BoxRouteResolver"
    ));
    assert!(generated.rust.contains("fn build_default_core("));
    assert!(
        generated
            .rust
            .contains("retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy")
    );
    assert!(
        generated
            .rust
            .contains("transport_security: lattice_rpc::security::RpcTransportSecurity")
    );
    assert!(
        generated
            .rust
            .contains("transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig")
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
        generated.rust.contains(
            "let request_id = lattice_rpc::metadata::RpcContext::from_metadata(&metadata)"
        )
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
            .contains("pub async fn dispatch_with_context<R>(&self, frame: ClientFrame, router: &mut R, context: &lattice_gateway::route::GatewayRouteContext)")
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
            .contains("impl lattice_core::direct_link::stream::DirectLinkMessage for crate::world::EnterWorldRequest")
    );
    assert!(
        generated
            .rust
            .contains("const PROTO_FULL_NAME: &'static str = \"world.EnterWorldRequest\"")
    );
    assert!(
        generated
            .rust
            .contains("impl lattice_core::direct_link::stream::DirectLinkMessage for crate::world::EnterWorldReply")
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
            .contains("impl lattice_core::direct_link::stream::DirectLinkMessage for crate::world::EnterWorldRequest")
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
        "impl lattice_core::direct_link::stream::DirectLinkMessage for crate::world::envelope::EnterWorldRequest"
    ));
    assert!(
        generated
            .rust
            .contains("const PROTO_FULL_NAME: &'static str = \"world.Envelope.EnterWorldRequest\"")
    );
    assert!(generated.rust.contains(
        "impl lattice_core::direct_link::stream::DirectLinkMessage for crate::world::envelope::EnterWorldReply"
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
            .contains("pub fn bind<A>(actor_kind: lattice_core::kind::ActorKind)")
    );
    assert!(generated.rust.contains(
        "A: lattice_actor::traits::Actor + lattice_actor::traits::Handler<lattice_core::direct_link::messages::Linked<crate::world::EnterWorldRequest, ()>> + lattice_actor::traits::Handler<lattice_core::direct_link::messages::Linked<crate::world::MoveWorldRequest, ()>>,"
    ));
    assert!(
        generated.rust.contains(
            "impl<A> lattice_direct_link::delivery::DirectLinkDispatch<A, ()> for Stream"
        )
    );
    assert!(generated.rust.contains("match message_id.0"));
    assert!(generated.rust.contains("7001 =>"));
    assert!(generated.rust.contains("7101 =>"));
    assert!(generated.rust.contains(
        "lattice_direct_link::delivery::try_deliver_linked(handle, payload, metadata, context)"
    ));
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
    assert!(request_module.contains(
        "Handler<lattice_core::direct_link::messages::Linked<crate::world::EnterWorldRequest, ()>>"
    ));
    assert!(!request_module.contains(
        "Handler<lattice_core::direct_link::messages::Linked<crate::world::EnterWorldReply, ()>>"
    ));

    let response_module = generated
        .rust
        .split("pub mod client_player_response")
        .nth(1)
        .expect("response stream module");
    assert!(response_module.contains(
        "Handler<lattice_core::direct_link::messages::Linked<crate::world::EnterWorldReply, ()>>"
    ));
    assert!(!response_module.contains(
        "Handler<lattice_core::direct_link::messages::Linked<crate::world::EnterWorldRequest, ()>>"
    ));
}

#[test]
fn direct_link_stream_codegen_supports_typed_metadata() {
    let generated =
        crate::render::generate_direct_link_stream_bindings(&[GeneratedDirectLinkStreamSpec {
            module_name: "client_player_request".into(),
            stream_name: "client.player.request".into(),
            metadata_type: Some("crate::direct_links::ClientRequestContext".into()),
            messages: vec![GeneratedDirectLinkMessageSpec {
                message_id: 7001,
                rust_type: "crate::world::EnterWorldRequest".into(),
            }],
        }])
        .unwrap();

    assert!(
        generated
            .rust
            .contains("type Metadata = crate::direct_links::ClientRequestContext;")
    );
    assert!(
        generated.rust.contains(
            "DirectLinkActorBinding<A, Stream, crate::direct_links::ClientRequestContext>"
        )
    );
    assert!(generated.rust.contains(
        "Handler<lattice_core::direct_link::messages::Linked<crate::world::EnterWorldRequest, crate::direct_links::ClientRequestContext>>"
    ));
    assert!(generated.rust.contains(
        "impl<A> lattice_direct_link::delivery::DirectLinkDispatch<A, crate::direct_links::ClientRequestContext> for Stream"
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
