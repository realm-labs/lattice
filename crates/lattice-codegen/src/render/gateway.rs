use crate::render::names::{
    NameDisambiguation, lower_camel_to_snake, service_field_name, service_module_name,
    service_type_prefix,
};
use crate::spec::RpcMethodSpec;
pub(crate) fn push_gateway_route_table(
    rust: &mut String,
    methods: &[RpcMethodSpec],
    names: &NameDisambiguation,
) {
    let gateway_methods: Vec<&RpcMethodSpec> = methods
        .iter()
        .filter(|method| method.gateway_msg_id.is_some())
        .collect();
    if gateway_methods.is_empty() {
        return;
    }

    rust.push_str(
        "pub fn register_gateway_routes(table: &mut GatewayRouteTable) -> Result<(), GatewayError> {\n",
    );
    for method in gateway_methods {
        rust.push_str("    table.register(");
        rust.push_str(&gateway_binding_path(method, names));
        rust.push_str("::route_spec()");
        rust.push_str(")?;\n");
    }
    rust.push_str("    Ok(())\n");
    rust.push_str("}\n\n");
}

pub(crate) fn push_gateway_dispatcher(
    rust: &mut String,
    methods: &[RpcMethodSpec],
    names: &NameDisambiguation,
) {
    let gateway_methods: Vec<&RpcMethodSpec> = methods
        .iter()
        .filter(|method| method.gateway_msg_id.is_some())
        .collect();
    if gateway_methods.is_empty() {
        return;
    }
    let service_groups = gateway_service_groups(&gateway_methods);
    let type_params = service_groups
        .iter()
        .map(|methods| gateway_core_type_param(methods[0], names))
        .collect::<Vec<_>>();
    let type_params_csv = type_params.join(", ");

    rust.push_str("#[derive(Debug, Clone)]\n");
    rust.push_str(&format!(
        "pub struct GatewayDispatcher<{type_params_csv}> {{\n"
    ));
    for methods in &service_groups {
        rust.push_str(&format!(
            "    {}: {},\n",
            service_field_name(methods[0], names),
            gateway_core_type_param(methods[0], names)
        ));
    }
    rust.push_str("}\n\n");

    rust.push_str(&format!(
        "impl<{type_params_csv}> GatewayDispatcher<{type_params_csv}>\n"
    ));
    rust.push_str("where\n");
    for type_param in &type_params {
        rust.push_str(&format!("    {type_param}: ShardedRpcCore + Clone,\n"));
    }
    rust.push_str("{\n");
    rust.push_str(&format!(
        "    pub fn new({}) -> Self {{\n",
        gateway_new_args(&service_groups, names)
    ));
    rust.push_str("        Self {\n");
    for methods in &service_groups {
        let field = service_field_name(methods[0], names);
        rust.push_str(&format!("            {field},\n"));
    }
    rust.push_str("        }\n");
    rust.push_str("    }\n\n");
    rust.push_str(
        "    pub async fn dispatch_with_context<R>(&self, frame: ClientFrame, router: &mut R, context: &lattice_gateway::route::GatewayRouteContext) -> Result<ClientFrame, GatewayError>\n",
    );
    rust.push_str("    where\n        R: MessageRouter,\n    {\n");
    rust.push_str("        match frame.msg_id {\n");
    for method in gateway_methods {
        let field = service_field_name(method, names);
        let binding_path = gateway_binding_path(method, names);
        rust.push_str(&format!(
            "            {binding_path}::DEFAULT_MSG_ID => {binding_path}::decode_and_forward(frame, self.{field}.clone(), router, context).await,\n"
        ));
    }
    rust.push_str("            msg_id => Err(GatewayError::UnknownMessageId { msg_id }),\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n");
    rust.push_str("}\n\n");
}

fn gateway_service_groups<'a>(methods: &[&'a RpcMethodSpec]) -> Vec<Vec<&'a RpcMethodSpec>> {
    let mut groups = Vec::<Vec<&RpcMethodSpec>>::new();
    for method in methods {
        if let Some(group) = groups.iter_mut().find(|group| {
            group[0].package == method.package && group[0].service_name == method.service_name
        }) {
            group.push(*method);
        } else {
            groups.push(vec![*method]);
        }
    }
    groups
}

fn gateway_core_type_param(method: &RpcMethodSpec, names: &NameDisambiguation) -> String {
    format!("{}Core", service_type_prefix(method, names))
}

fn gateway_new_args(service_groups: &[Vec<&RpcMethodSpec>], names: &NameDisambiguation) -> String {
    service_groups
        .iter()
        .map(|methods| {
            format!(
                "{}: {}",
                service_field_name(methods[0], names),
                gateway_core_type_param(methods[0], names)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn gateway_binding_path(method: &RpcMethodSpec, names: &NameDisambiguation) -> String {
    format!(
        "{}::{}::GatewayBinding",
        service_module_name(method, names),
        lower_camel_to_snake(&method.method_name)
    )
}
