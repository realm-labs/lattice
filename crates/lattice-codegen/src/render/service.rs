use crate::render::names::{
    NameDisambiguation, lower_camel_to_snake, service_module_name, tonic_server_path,
    tonic_service_trait_path,
};
use crate::spec::RpcMethodSpec;
pub(crate) fn push_service_module(
    rust: &mut String,
    methods: &[&RpcMethodSpec],
    names: &NameDisambiguation,
) {
    let service = methods[0];
    let module_name = service_module_name(service, names);
    rust.push_str(&format!("pub mod {module_name} {{\n"));
    rust.push_str("    use std::sync::Arc;\n");
    rust.push_str("    use std::marker::PhantomData;\n");
    rust.push_str(
        "    use lattice_actor::handle::ActorHandle;
use lattice_actor::registry::{ActorLoader, ActorRegistry};
use lattice_actor::traits::{Actor, Handler};\n",
    );
    rust.push_str(
        "    use lattice_core::id::{ActorId, RouteKey};
    use lattice_core::instance::InstanceId;
    use lattice_core::kind::{ActorKind, ServiceKind};\n",
    );
    rust.push_str(
        "    use lattice_rpc::adapter::ActorRpcAdapter;
    use lattice_rpc::client::TypedRpcClient;
    use lattice_rpc::metadata::RpcContext;
    use lattice_rpc::security::RpcServerSecurity;
    use lattice_rpc::traits::{RpcRequest, ShardedRpcCore};
    use lattice_rpc::types::Rpc;\n\n",
    );
    rust.push_str(
        "    use lattice_service::error::LatticeServiceError;
    use lattice_service::framework::context::ServiceContextExt;
    use lattice_service::clients::{RpcClientBinding, RpcClientPlacement, RpcServiceBinding};
    use lattice_service::context::ServiceBuildContext;\n\n",
    );
    push_typed_client(rust, methods);
    push_service_binding(rust, methods);
    push_singleton_binding(rust, methods);
    push_server_adapter(rust, methods);
    push_registry_server_adapter(rust, methods);
    push_singleton_registry_server_adapter(rust, methods);
    for method in methods {
        push_method_module(rust, method);
    }
    rust.push_str("    fn actor_id_from_route_key(route_key: RouteKey) -> ActorId {\n");
    rust.push_str("        match route_key {\n");
    rust.push_str("            RouteKey::Str(value) => ActorId::Str(value),\n");
    rust.push_str("            RouteKey::U64(value) => ActorId::U64(value),\n");
    rust.push_str("            RouteKey::I64(value) => ActorId::I64(value),\n");
    rust.push_str("            RouteKey::Bytes(value) => ActorId::Bytes(value),\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n");
    rust.push_str("\n    fn singleton_scope_from_route_key(route_key: RouteKey) -> String {\n");
    rust.push_str("        match route_key {\n");
    rust.push_str("            RouteKey::Str(value) => value,\n");
    rust.push_str("            other => format!(\"{other:?}\"),\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n");
    rust.push_str("}\n\n");
}

fn push_typed_client(rust: &mut String, methods: &[&RpcMethodSpec]) {
    rust.push_str("    #[derive(Debug, Clone)]\n");
    rust.push_str("    pub struct Client<C> {\n        inner: TypedRpcClient<C>,\n    }\n\n");
    rust.push_str("    impl<C> Client<C>\n");
    rust.push_str("    where\n        C: ShardedRpcCore,\n    {\n");
    rust.push_str("        pub fn new(core: C) -> Self {\n            Self { inner: TypedRpcClient::new(core) }\n        }\n\n");
    for method in methods {
        if method.route_key_from_request {
            rust.push_str(&format!(
                "        pub async fn {method}(&self, req: {request}) -> Result<{reply}, lattice_rpc::error::RpcError> {{\n",
                method = lower_camel_to_snake(&method.method_name),
                request = method.request_type,
                reply = method.reply_type
            ));
            rust.push_str("            self.inner.call(req).await\n");
            rust.push_str("        }\n");
        } else {
            rust.push_str(&format!(
                "        pub async fn {method}(&self, route_key: RouteKey, req: {request}) -> Result<{reply}, lattice_rpc::error::RpcError> {{\n",
                method = lower_camel_to_snake(&method.method_name),
                request = method.request_type,
                reply = method.reply_type
            ));
            rust.push_str(
                "            self.inner.core().call_routed(lattice_rpc::types::RoutedEnvelope::new(\n",
            );
            rust.push_str("                req,\n");
            rust.push_str(&format!(
                "                ActorKind::from_static(\"{}\"),\n",
                method.route_key.actor_kind
            ));
            rust.push_str("                route_key,\n");
            rust.push_str("            )).await\n");
            rust.push_str("        }\n");
        }
    }
    rust.push_str("    }\n\n");
}

fn push_service_binding(rust: &mut String, methods: &[&RpcMethodSpec]) {
    let service = methods[0];
    let server_path = tonic_server_path(service);
    rust.push_str("    pub type DefaultClientCore = lattice_placement::routing::rpc::ResolvingRpcCore<lattice_placement::routing::resolver::BoxRouteResolver, super::GeneratedTonicEndpointTransport>;\n\n");
    rust.push_str("    #[derive(Debug)]\n");
    rust.push_str("    pub struct Binding<A = (), C = DefaultClientCore> {\n        actor_kind: ActorKind,\n        request_dedup: bool,\n        _actor: PhantomData<fn() -> A>,\n        _core: PhantomData<fn() -> C>,\n    }\n\n");
    rust.push_str("    impl<A, C> Binding<A, C> {\n");
    rust.push_str("        pub fn request_dedup(mut self, enabled: bool) -> Self {\n");
    rust.push_str("            self.request_dedup = enabled;\n");
    rust.push_str("            self\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str("    impl Binding<()> {\n");
    rust.push_str("        pub fn for_actor<A>(actor_kind: ActorKind) -> Binding<A>\n        where\n            A: Actor,\n        {\n");
    rust.push_str("            Binding { actor_kind, request_dedup: true, _actor: PhantomData, _core: PhantomData }\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str("    impl<A, C> RpcClientBinding for Binding<A, C>\n    where\n        A: Send + Sync + 'static,\n        C: ShardedRpcCore + Clone,\n    {\n");
    rust.push_str("        type Core = C;\n");
    rust.push_str("        type Client = Client<C>;\n\n");
    rust.push_str(&format!(
        "        const SERVICE_KIND: &'static str = \"{}\";\n",
        service.service_kind
    ));
    rust.push_str("\n        fn build_client(core: Self::Core) -> Self::Client {\n            Client::new(core)\n        }\n\n");
    rust.push_str("        fn build_default_core(\n            resolver: lattice_placement::routing::resolver::BoxRouteResolver,\n            context_factory: lattice_rpc::metadata::RpcClientContextFactory,\n            retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,\n            transport_security: lattice_rpc::security::RpcTransportSecurity,\n            transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,\n        ) -> Option<Self::Core> {\n            let core = lattice_placement::routing::rpc::ResolvingRpcCore::new(\n                lattice_core::kind::ServiceKind::from_static(Self::SERVICE_KIND),\n                resolver,\n                lattice_placement::endpoint::EndpointPool::new(),\n                context_factory,\n                super::GeneratedTonicEndpointTransport::with_transport_config(transport_security, transport_config),\n            ).with_retry_policy(retry_policy);\n            let core: Box<dyn std::any::Any + Send + Sync> = Box::new(core);\n            core.downcast::<Self::Core>().ok().map(|core| *core)\n        }\n");
    rust.push_str("    }\n\n");
    rust.push_str(
        "    impl<A, C> RpcServiceBinding for Binding<A, C>\n    where\n        A: Actor + Sync,\n        C: Send + Sync + 'static",
    );
    for method in methods {
        rust.push_str(",\n        A: Handler<Rpc<");
        rust.push_str(&method.request_type);
        rust.push_str(">>");
    }
    rust.push_str(",\n    {\n");
    rust.push_str(&format!(
        "        fn service_name(&self) -> &'static str {{ \"{}\" }}\n\n",
        service.service_name
    ));
    rust.push_str("        fn register(self: Box<Self>, context: &mut ServiceBuildContext) -> Result<(), LatticeServiceError> {\n");
    rust.push_str("            let actor = context.actor::<A>(&self.actor_kind)?;\n");
    rust.push_str(&format!(
        "            context.add_rpc_service({server_path}::new(RegistryService::with_security(actor.registry(), actor.loader(), context.rpc_security()).with_request_dedup(self.request_dedup)));\n"
    ));
    rust.push_str("            Ok(())\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
}

fn push_singleton_binding(rust: &mut String, methods: &[&RpcMethodSpec]) {
    let service = methods[0];
    let server_path = tonic_server_path(service);
    rust.push_str("    #[derive(Debug)]\n");
    rust.push_str("    pub struct SingletonBinding<A = (), C = DefaultClientCore> {\n        actor_kind: ActorKind,\n        request_dedup: bool,\n        _actor: PhantomData<fn() -> A>,\n        _core: PhantomData<fn() -> C>,\n    }\n\n");
    rust.push_str("    impl<A, C> SingletonBinding<A, C> {\n");
    rust.push_str("        pub fn request_dedup(mut self, enabled: bool) -> Self {\n");
    rust.push_str("            self.request_dedup = enabled;\n");
    rust.push_str("            self\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str("    impl SingletonBinding<()> {\n");
    rust.push_str("        pub fn for_actor<A>() -> SingletonBinding<A>\n        where\n            A: Actor,\n        {\n");
    rust.push_str(&format!(
        "            SingletonBinding {{ actor_kind: ActorKind::from_static(\"{}\"), request_dedup: true, _actor: PhantomData, _core: PhantomData }}\n",
        service.route_key.actor_kind
    ));
    rust.push_str("        }\n\n");
    rust.push_str("        pub fn for_actor_kind<A>(actor_kind: ActorKind) -> SingletonBinding<A>\n        where\n            A: Actor,\n        {\n");
    rust.push_str(
        "            SingletonBinding { actor_kind, request_dedup: true, _actor: PhantomData, _core: PhantomData }\n",
    );
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str("    impl<A, C> RpcClientBinding for SingletonBinding<A, C>\n    where\n        A: Send + Sync + 'static,\n        C: ShardedRpcCore + Clone,\n    {\n");
    rust.push_str("        type Core = C;\n");
    rust.push_str("        type Client = Client<C>;\n\n");
    rust.push_str(&format!(
        "        const SERVICE_KIND: &'static str = \"{}\";\n",
        service.service_kind
    ));
    rust.push_str("\n        fn placement() -> RpcClientPlacement {\n            RpcClientPlacement::Singleton\n        }\n\n");
    rust.push_str("        fn build_client(core: Self::Core) -> Self::Client {\n            Client::new(core)\n        }\n\n");
    rust.push_str("        fn build_default_core(\n            resolver: lattice_placement::routing::resolver::BoxRouteResolver,\n            context_factory: lattice_rpc::metadata::RpcClientContextFactory,\n            retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,\n            transport_security: lattice_rpc::security::RpcTransportSecurity,\n            transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,\n        ) -> Option<Self::Core> {\n            let core = lattice_placement::routing::rpc::ResolvingRpcCore::new(\n                lattice_core::kind::ServiceKind::from_static(Self::SERVICE_KIND),\n                resolver,\n                lattice_placement::endpoint::EndpointPool::new(),\n                context_factory,\n                super::GeneratedTonicEndpointTransport::with_transport_config(transport_security, transport_config),\n            ).with_retry_policy(retry_policy);\n            let core: Box<dyn std::any::Any + Send + Sync> = Box::new(core);\n            core.downcast::<Self::Core>().ok().map(|core| *core)\n        }\n");
    rust.push_str("    }\n\n");
    rust.push_str(
        "    impl<A, C> RpcServiceBinding for SingletonBinding<A, C>\n    where\n        A: Actor + Sync,\n        C: Send + Sync + 'static",
    );
    for method in methods {
        rust.push_str(",\n        A: Handler<Rpc<");
        rust.push_str(&method.request_type);
        rust.push_str(">>");
    }
    rust.push_str(",\n    {\n");
    rust.push_str(&format!(
        "        fn service_name(&self) -> &'static str {{ \"{}\" }}\n\n",
        service.service_name
    ));
    rust.push_str("        fn register(self: Box<Self>, context: &mut ServiceBuildContext) -> Result<(), LatticeServiceError> {\n");
    rust.push_str("            let actor = context.actor::<A>(&self.actor_kind)?;\n");
    rust.push_str("            let service = context.service_context();\n");
    rust.push_str("            let placement_store = service.placement_store();\n");
    rust.push_str("            let instance_id = service.instance_id().clone();\n");
    rust.push_str(&format!(
        "            context.add_rpc_service({server_path}::new(SingletonRegistryService::with_security(actor.registry(), actor.loader(), placement_store, instance_id, context.rpc_security()).with_request_dedup(self.request_dedup)));\n"
    ));
    rust.push_str("            Ok(())\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
}

fn push_server_adapter(rust: &mut String, methods: &[&RpcMethodSpec]) {
    let service = methods[0];
    let trait_path = tonic_service_trait_path(service);
    rust.push_str("    #[derive(Debug, Clone)]\n");
    rust.push_str(
        "    pub struct ActorService<A: Actor> {\n        inner: ActorRpcAdapter<A>,\n        security: RpcServerSecurity,\n        request_dedup: bool,\n        deduplicator: lattice_rpc::dedup::RequestDeduplicator,\n    }\n\n",
    );
    rust.push_str("    impl<A: Actor> ActorService<A> {\n");
    rust.push_str("        pub fn new(handle: ActorHandle<A>) -> Self {\n");
    rust.push_str("            Self { inner: ActorRpcAdapter::new(handle), security: RpcServerSecurity::disabled(), request_dedup: true, deduplicator: lattice_rpc::dedup::RequestDeduplicator::new() }\n");
    rust.push_str("        }\n\n");
    rust.push_str("        pub fn with_security(handle: ActorHandle<A>, security: RpcServerSecurity) -> Self {\n");
    rust.push_str("            Self { inner: ActorRpcAdapter::new(handle), security, request_dedup: true, deduplicator: lattice_rpc::dedup::RequestDeduplicator::new() }\n");
    rust.push_str("        }\n");
    rust.push_str("\n        pub fn with_request_dedup(mut self, enabled: bool) -> Self {\n");
    rust.push_str("            self.request_dedup = enabled;\n");
    rust.push_str("            self\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str("    #[tonic::async_trait]\n");
    rust.push_str(&format!("    impl<A> {trait_path} for ActorService<A>\n"));
    rust.push_str("    where\n        A: Actor + Sync");
    for method in methods {
        rust.push_str(" + Handler<Rpc<");
        rust.push_str(&method.request_type);
        rust.push_str(">>");
    }
    rust.push_str(",\n    {\n");
    for method in methods {
        rust.push_str(&format!(
            "        async fn {method}(&self, request: tonic::Request<{request}>) -> Result<tonic::Response<{reply}>, tonic::Status> {{\n",
            method = lower_camel_to_snake(&method.method_name),
            request = method.request_type,
            reply = method.reply_type
        ));
        rust.push_str("            let peer = self.security.peer_identity(&request);\n");
        rust.push_str("            if self.request_dedup {\n");
        rust.push_str("                self.inner.unary_dedup_secure(request, self.security.policy(), peer.as_ref(), &self.deduplicator).await\n");
        rust.push_str("            } else {\n");
        rust.push_str("                self.inner.unary_secure(request, self.security.policy(), peer.as_ref()).await\n");
        rust.push_str("            }\n");
        rust.push_str("        }\n");
    }
    rust.push_str("    }\n\n");
}

fn push_registry_server_adapter(rust: &mut String, methods: &[&RpcMethodSpec]) {
    let service = methods[0];
    let trait_path = tonic_service_trait_path(service);
    rust.push_str("    #[derive(Debug, Clone)]\n");
    rust.push_str("    pub struct RegistryService<A: Actor, L> {\n        registry: Arc<ActorRegistry<A>>,\n        loader: L,\n        security: RpcServerSecurity,\n        request_dedup: bool,\n        deduplicator: lattice_rpc::dedup::RequestDeduplicator,\n    }\n\n");
    rust.push_str("    impl<A, L> RegistryService<A, L>\n    where\n        A: Actor,\n        L: ActorLoader<A>,\n    {\n");
    rust.push_str("        pub fn new(registry: Arc<ActorRegistry<A>>, loader: L) -> Self {\n");
    rust.push_str(
        "            Self { registry, loader, security: RpcServerSecurity::disabled(), request_dedup: true, deduplicator: lattice_rpc::dedup::RequestDeduplicator::new() }\n",
    );
    rust.push_str("        }\n\n");
    rust.push_str("        pub fn with_security(registry: Arc<ActorRegistry<A>>, loader: L, security: RpcServerSecurity) -> Self {\n");
    rust.push_str("            Self { registry, loader, security, request_dedup: true, deduplicator: lattice_rpc::dedup::RequestDeduplicator::new() }\n");
    rust.push_str("        }\n\n");
    rust.push_str("        pub fn with_request_dedup(mut self, enabled: bool) -> Self {\n");
    rust.push_str("            self.request_dedup = enabled;\n");
    rust.push_str("            self\n");
    rust.push_str("        }\n\n");
    rust.push_str("        async fn unary<Req>(&self, request: tonic::Request<Req>) -> Result<tonic::Response<Req::Reply>, tonic::Status>\n");
    rust.push_str("        where\n            A: Handler<Rpc<Req>>,\n            Req: RpcRequest,\n        {\n");
    rust.push_str("            let peer = self.security.peer_identity(&request);\n");
    rust.push_str("            let ctx = RpcContext::from_metadata(request.metadata()).map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;\n");
    rust.push_str("            self.security.validate_context(&ctx, peer.as_ref())?;\n");
    rust.push_str(
        "            let route = lattice_rpc::types::RpcRoute::from_metadata(request.metadata())\n",
    );
    rust.push_str(
        "                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?\n",
    );
    rust.push_str("                .ok_or_else(|| tonic::Status::invalid_argument(\"missing rpc route metadata\"))?;\n");
    rust.push_str("            let req = request.into_inner();\n");
    rust.push_str("            let actor_id = actor_id_from_route_key(route.route_key.clone());\n");
    rust.push_str("            let handle = self\n");
    rust.push_str("                .registry\n");
    rust.push_str("                .get_or_load(actor_id, self.loader.clone())\n");
    rust.push_str("                .await\n");
    rust.push_str(
        "                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;\n",
    );
    rust.push_str("            if self.request_dedup {\n");
    rust.push_str("                lattice_rpc::adapter::dispatch_actor_rpc_dedup_with_route(handle, route, req, ctx, &self.deduplicator).await\n");
    rust.push_str("            } else {\n");
    rust.push_str(
        "                lattice_rpc::adapter::dispatch_actor_rpc_with_route(handle, route, req, ctx).await\n",
    );
    rust.push_str("            }\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str("    #[tonic::async_trait]\n");
    rust.push_str(&format!(
        "    impl<A, L> {trait_path} for RegistryService<A, L>\n"
    ));
    rust.push_str("    where\n        A: Actor + Sync");
    for method in methods {
        rust.push_str(" + Handler<Rpc<");
        rust.push_str(&method.request_type);
        rust.push_str(">>");
    }
    rust.push_str(",\n        L: ActorLoader<A>,\n    {\n");
    for method in methods {
        rust.push_str(&format!(
            "        async fn {method}(&self, request: tonic::Request<{request}>) -> Result<tonic::Response<{reply}>, tonic::Status> {{\n",
            method = lower_camel_to_snake(&method.method_name),
            request = method.request_type,
            reply = method.reply_type
        ));
        rust.push_str("            self.unary(request).await\n");
        rust.push_str("        }\n");
    }
    rust.push_str("    }\n\n");
}

fn push_singleton_registry_server_adapter(rust: &mut String, methods: &[&RpcMethodSpec]) {
    let service = methods[0];
    let trait_path = tonic_service_trait_path(service);
    rust.push_str("    #[derive(Clone)]\n");
    rust.push_str("    pub struct SingletonRegistryService<A: Actor, L> {\n        registry: Arc<ActorRegistry<A>>,\n        loader: L,\n        placement_store: Arc<dyn lattice_service::framework::placement::DynPlacementStore>,\n        instance_id: InstanceId,\n        security: RpcServerSecurity,\n        request_dedup: bool,\n        deduplicator: lattice_rpc::dedup::RequestDeduplicator,\n    }\n\n");
    rust.push_str("    impl<A, L> std::fmt::Debug for SingletonRegistryService<A, L>\n");
    rust.push_str("    where\n        A: Actor,\n    {\n");
    rust.push_str(
        "        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {\n",
    );
    rust.push_str("            formatter.debug_struct(\"SingletonRegistryService\").field(\"instance_id\", &self.instance_id).finish_non_exhaustive()\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str("    impl<A, L> SingletonRegistryService<A, L>\n    where\n        A: Actor,\n        L: ActorLoader<A>,\n    {\n");
    rust.push_str("        pub fn new(registry: Arc<ActorRegistry<A>>, loader: L, placement_store: Arc<dyn lattice_service::framework::placement::DynPlacementStore>, instance_id: InstanceId) -> Self {\n");
    rust.push_str("            Self { registry, loader, placement_store, instance_id, security: RpcServerSecurity::disabled(), request_dedup: true, deduplicator: lattice_rpc::dedup::RequestDeduplicator::new() }\n");
    rust.push_str("        }\n\n");
    rust.push_str("        pub fn with_security(registry: Arc<ActorRegistry<A>>, loader: L, placement_store: Arc<dyn lattice_service::framework::placement::DynPlacementStore>, instance_id: InstanceId, security: RpcServerSecurity) -> Self {\n");
    rust.push_str(
        "            Self { registry, loader, placement_store, instance_id, security, request_dedup: true, deduplicator: lattice_rpc::dedup::RequestDeduplicator::new() }\n",
    );
    rust.push_str("        }\n\n");
    rust.push_str("        pub fn with_request_dedup(mut self, enabled: bool) -> Self {\n");
    rust.push_str("            self.request_dedup = enabled;\n");
    rust.push_str("            self\n");
    rust.push_str("        }\n\n");
    rust.push_str("        async fn unary<Req>(&self, request: tonic::Request<Req>) -> Result<tonic::Response<Req::Reply>, tonic::Status>\n");
    rust.push_str("        where\n            A: Handler<Rpc<Req>>,\n            Req: RpcRequest,\n        {\n");
    rust.push_str("            let peer = self.security.peer_identity(&request);\n");
    rust.push_str("            let ctx = RpcContext::from_metadata(request.metadata()).map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;\n");
    rust.push_str("            self.security.validate_context(&ctx, peer.as_ref())?;\n");
    rust.push_str(
        "            let route = lattice_rpc::types::RpcRoute::from_metadata(request.metadata())\n",
    );
    rust.push_str(
        "                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?\n",
    );
    rust.push_str("                .ok_or_else(|| tonic::Status::invalid_argument(\"missing rpc route metadata\"))?;\n");
    rust.push_str("            let req = request.into_inner();\n");
    rust.push_str("            let route_key = route.route_key.clone();\n");
    rust.push_str("            let actor_id = actor_id_from_route_key(route_key.clone());\n");
    rust.push_str("            let singleton_key = lattice_placement::storage::SingletonKey {\n");
    rust.push_str(&format!(
        "                service_kind: ServiceKind::from_static(\"{}\"),\n",
        service.service_kind
    ));
    rust.push_str("                singleton_kind: route.actor_kind.clone(),\n");
    rust.push_str("                scope: singleton_scope_from_route_key(route_key),\n");
    rust.push_str("            };\n");
    rust.push_str(
        "            let record = self.placement_store.get_singleton(&singleton_key).await\n",
    );
    rust.push_str(
        "                .map_err(|error| tonic::Status::internal(error.to_string()))?\n",
    );
    rust.push_str("                .map(|(_, record)| record)\n");
    rust.push_str("                .ok_or_else(|| tonic::Status::failed_precondition(\"singleton owner record missing\"))?;\n");
    rust.push_str("            if record.owner != self.instance_id {\n");
    rust.push_str("                return Err(tonic::Status::failed_precondition(\"singleton owner mismatch\"));\n");
    rust.push_str("            }\n");
    rust.push_str("            if let Some(route_epoch) = ctx.route_epoch\n");
    rust.push_str("                && route_epoch != record.epoch\n");
    rust.push_str("            {\n");
    rust.push_str("                return Err(tonic::Status::failed_precondition(\"singleton route epoch mismatch\"));\n");
    rust.push_str("            }\n");
    rust.push_str("            let handle = self\n");
    rust.push_str("                .registry\n");
    rust.push_str("                .get_or_load(actor_id, self.loader.clone())\n");
    rust.push_str("                .await\n");
    rust.push_str(
        "                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;\n",
    );
    rust.push_str("            if self.request_dedup {\n");
    rust.push_str("                lattice_rpc::adapter::dispatch_actor_rpc_dedup_with_route(handle, route, req, ctx, &self.deduplicator).await\n");
    rust.push_str("            } else {\n");
    rust.push_str(
        "                lattice_rpc::adapter::dispatch_actor_rpc_with_route(handle, route, req, ctx).await\n",
    );
    rust.push_str("            }\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str("    #[tonic::async_trait]\n");
    rust.push_str(&format!(
        "    impl<A, L> {trait_path} for SingletonRegistryService<A, L>\n"
    ));
    rust.push_str("    where\n        A: Actor + Sync");
    for method in methods {
        rust.push_str(" + Handler<Rpc<");
        rust.push_str(&method.request_type);
        rust.push_str(">>");
    }
    rust.push_str(",\n        L: ActorLoader<A>,\n    {\n");
    for method in methods {
        rust.push_str(&format!(
            "        async fn {method}(&self, request: tonic::Request<{request}>) -> Result<tonic::Response<{reply}>, tonic::Status> {{\n",
            method = lower_camel_to_snake(&method.method_name),
            request = method.request_type,
            reply = method.reply_type
        ));
        rust.push_str("            self.unary(request).await\n");
        rust.push_str("        }\n");
    }
    rust.push_str("    }\n\n");
}

fn push_method_module(rust: &mut String, method: &RpcMethodSpec) {
    let module_name = lower_camel_to_snake(&method.method_name);
    rust.push_str(&format!("    pub mod {module_name} {{\n"));
    rust.push_str("        use lattice_actor::traits::{Actor, Handler};\n");
    rust.push_str("        use lattice_gateway::binding::ProstClientMessageBinding;\n");
    rust.push_str("        use lattice_rpc::types::Rpc;\n");
    if method.gateway_msg_id.is_some() {
        rust.push_str(
            "        use lattice_gateway::error::GatewayError;\n        use lattice_gateway::frame::ClientFrame;\n        use lattice_gateway::route::{GatewayRouteContext, GatewayRouteSpec, MessageRouter};\n",
        );
        rust.push_str(
            "        use lattice_rpc::traits::{RpcRequest, ShardedRpcCore};
        use lattice_rpc::types::RoutedEnvelope;\n",
        );
        rust.push_str("        use prost::Message as ProstMessage;\n");
    }
    rust.push('\n');
    rust.push_str("        pub fn assert_handler<A>()\n");
    rust.push_str("        where\n");
    rust.push_str("            A: Actor + Handler<Rpc<");
    rust.push_str(&method.request_type);
    rust.push_str(">>,\n");
    rust.push_str("        {\n");
    rust.push_str("        }\n\n");
    rust.push_str("        #[derive(Debug, Clone, Copy, Default)]\n");
    rust.push_str("        pub struct GatewayBinding;\n\n");
    rust.push_str("        impl GatewayBinding {\n");
    if let Some(msg_id) = method.gateway_msg_id {
        rust.push_str(&format!(
            "            pub const DEFAULT_MSG_ID: u32 = {};\n\n",
            msg_id
        ));
        rust.push_str("            pub fn route_spec() -> GatewayRouteSpec {\n");
        rust.push_str("                GatewayRouteSpec {\n");
        rust.push_str("                    msg_id: Self::DEFAULT_MSG_ID,\n");
        rust.push_str(&format!(
            "                    actor_kind: lattice_core::kind::ActorKind::from_static(\"{}\"),\n",
            method.route_key.actor_kind
        ));
        rust.push_str("                    method: <");
        rust.push_str(&method.request_type);
        rust.push_str(" as RpcRequest>::METHOD,\n");
        rust.push_str("                }\n");
        rust.push_str("            }\n\n");
        rust.push_str("            pub async fn decode_and_forward<C, R>(frame: ClientFrame, core: C, router: &mut R, context: &GatewayRouteContext) -> Result<ClientFrame, GatewayError>\n");
        rust.push_str("            where\n");
        rust.push_str("                C: ShardedRpcCore,\n");
        rust.push_str("                R: MessageRouter,\n");
        rust.push_str("            {\n");
        rust.push_str("                if frame.msg_id != Self::DEFAULT_MSG_ID {\n");
        rust.push_str("                    return Err(GatewayError::UnexpectedMessageId {\n");
        rust.push_str("                        expected: Self::DEFAULT_MSG_ID,\n");
        rust.push_str("                        actual: frame.msg_id,\n");
        rust.push_str("                    });\n");
        rust.push_str("                }\n");
        rust.push_str("                let req = <");
        rust.push_str(&method.request_type);
        rust.push_str(" as ProstMessage>::decode(frame.payload.as_slice())\n");
        rust.push_str("                    .map_err(|source| GatewayError::DecodePayload(source.to_string()))?;\n");
        rust.push_str("                let route = Self::route_spec();\n");
        rust.push_str("                let decision = router.route(context, &route)?;\n");
        rust.push_str("                let reply = core.call_routed(RoutedEnvelope::new(\n");
        rust.push_str("                    req,\n");
        rust.push_str("                    decision.actor_kind,\n");
        rust.push_str("                    decision.route_key,\n");
        rust.push_str("                )).await.map_err(GatewayError::Rpc)?;\n");
        rust.push_str("                Ok(ClientFrame {\n");
        rust.push_str("                    msg_id: Self::DEFAULT_MSG_ID,\n");
        rust.push_str("                    payload: reply.encode_to_vec(),\n");
        rust.push_str("                })\n");
        rust.push_str("            }\n\n");
        rust.push_str("            pub fn default_binding() -> ProstClientMessageBinding<");
        rust.push_str(&method.request_type);
        rust.push_str("> {\n");
        rust.push_str("                Self::binding(Self::DEFAULT_MSG_ID)\n");
        rust.push_str("            }\n\n");
    }
    rust.push_str("            pub fn binding(msg_id: u32) -> ProstClientMessageBinding<");
    rust.push_str(&method.request_type);
    rust.push_str("> {\n");
    rust.push_str("                ProstClientMessageBinding::new(\n");
    rust.push_str("                    msg_id,\n");
    rust.push_str(&format!(
        "                    lattice_core::kind::ActorKind::from_static(\"{}\"),\n",
        method.route_key.actor_kind
    ));
    rust.push_str("                )\n");
    rust.push_str("            }\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
}
