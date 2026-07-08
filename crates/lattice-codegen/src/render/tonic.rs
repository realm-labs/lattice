use crate::render::names::{lower_camel_to_snake, method_fn_suffix, tonic_client_path};
use crate::spec::RpcMethodSpec;
pub(crate) fn push_tonic_endpoint_transport(rust: &mut String, methods: &[RpcMethodSpec]) {
    rust.push_str("#[derive(Debug, Clone, Default)]\n");
    rust.push_str("pub struct GeneratedTonicEndpointTransport {\n    channels: TonicEndpointChannelPool,\n}\n\n");
    rust.push_str("impl GeneratedTonicEndpointTransport {\n");
    rust.push_str("    pub fn new() -> Self {\n        Self { channels: TonicEndpointChannelPool::new() }\n    }\n\n");
    rust.push_str("    pub fn with_transport_security(transport_security: lattice_rpc::security::RpcTransportSecurity) -> Self {\n        Self { channels: TonicEndpointChannelPool::with_transport_security(transport_security) }\n    }\n\n");
    rust.push_str("    pub fn with_transport_config(transport_security: lattice_rpc::security::RpcTransportSecurity, transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig) -> Self {\n        Self { channels: TonicEndpointChannelPool::with_transport_config(transport_security, transport_config) }\n    }\n\n");
    rust.push_str("    pub fn with_channels(channels: TonicEndpointChannelPool) -> Self {\n        Self { channels }\n    }\n");
    rust.push_str("}\n\n");
    rust.push_str("#[tonic::async_trait]\n");
    rust.push_str("impl EndpointRpcTransport for GeneratedTonicEndpointTransport {\n");
    rust.push_str("    async fn unary<Req>(&self, _endpoint: EndpointLease, target: lattice_rpc::types::RouteTarget, route_key: &RouteKey, metadata: tonic::metadata::MetadataMap, request: Req) -> Result<tonic::Response<Req::Reply>, RpcError>\n");
    rust.push_str("    where\n        Req: RpcRequest,\n    {\n");
    rust.push_str("        match Req::METHOD {\n");
    for method in methods {
        rust.push_str(&format!(
            "            \"{}\" => self.call_{}::<Req>(target, route_key, metadata, request).await,\n",
            method.method_path(),
            method_fn_suffix(method)
        ));
    }
    rust.push_str("            method => Err(RpcError::Business(format!(\"unsupported generated rpc method {method}\"))),\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n");
    rust.push_str("}\n\n");
    rust.push_str("impl GeneratedTonicEndpointTransport {\n");
    for method in methods {
        push_tonic_transport_method(rust, method);
    }
    rust.push_str("}\n\n");
}

fn push_tonic_transport_method(rust: &mut String, method: &RpcMethodSpec) {
    let method_name = lower_camel_to_snake(&method.method_name);
    let client_path = tonic_client_path(method);
    let suffix = method_fn_suffix(method);
    rust.push_str(&format!(
        "    async fn call_{suffix}<Req>(&self, target: lattice_rpc::types::RouteTarget, route_key: &RouteKey, metadata: tonic::metadata::MetadataMap, request: Req) -> Result<tonic::Response<Req::Reply>, RpcError>\n",
    ));
    rust.push_str("    where\n        Req: RpcRequest,\n    {\n");
    rust.push_str(
        "        let typed_request = (Box::new(request) as Box<dyn std::any::Any + Send + Sync>)\n",
    );
    rust.push_str("            .downcast::<");
    rust.push_str(&method.request_type);
    rust.push_str(">()\n");
    rust.push_str("            .map_err(|_| RpcError::Business(format!(\"generated rpc method {} received unexpected request type {}\", Req::METHOD, std::any::type_name::<Req>())))?;\n");
    rust.push_str("        let typed_request = *typed_request;\n");
    rust.push_str(
        "        let request_id = lattice_rpc::metadata::RpcContext::from_metadata(&metadata)\n",
    );
    rust.push_str("            .map(|ctx| ctx.request_id)\n");
    rust.push_str(
        "            .unwrap_or_else(|_| lattice_core::actor_ref::RequestId::new(\"<missing>\"));\n",
    );
    rust.push_str(
        "        let channel = self.channels.get_or_connect_for_route_key(&target.advertised_endpoint, route_key).await?;\n",
    );
    rust.push_str(&format!(
        "        let mut client = {client_path}::new(channel);\n",
        client_path = client_path
    ));
    rust.push_str("        let mut typed_tonic_request = tonic::Request::new(typed_request);\n");
    rust.push_str("        *typed_tonic_request.metadata_mut() = metadata;\n");
    rust.push_str(&format!(
        "        let typed_reply = client.{method_name}(typed_tonic_request).await\n",
        method_name = method_name
    ));
    rust.push_str("            .map_err(|status| lattice_rpc::client::tonic_status_to_rpc_error_for_request(status, Req::METHOD, request_id))?\n");
    rust.push_str("            .into_inner();\n");
    rust.push_str(
        "        let reply = (Box::new(typed_reply) as Box<dyn std::any::Any + Send + Sync>)\n",
    );
    rust.push_str("            .downcast::<Req::Reply>()\n");
    rust.push_str("            .map_err(|_| RpcError::Business(format!(\"generated rpc method {} returned unexpected reply type {}\", Req::METHOD, std::any::type_name::<Req::Reply>())))?;\n");
    rust.push_str("        Ok(tonic::Response::new(*reply))\n");
    rust.push_str("    }\n\n");
}
