use crate::route_key::{ProtoRouteKeyOption, RouteKeyType};
use crate::spec::RpcMethodSpec;
pub(crate) fn push_routed_request(rust: &mut String, method: &RpcMethodSpec) {
    rust.push_str(&format!(
        "impl RoutedRequest for {request} {{\n",
        request = method.request_type
    ));
    rust.push_str("    fn actor_kind(&self) -> ActorKind {\n");
    rust.push_str(&format!(
        "        actor_kind!(\"{}\")\n",
        method.route_key.actor_kind
    ));
    rust.push_str("    }\n\n");
    rust.push_str("    fn route_key(&self) -> RouteKey {\n");
    rust.push_str(&format!("        {}\n", route_key_expr(&method.route_key)));
    rust.push_str("    }\n");
    rust.push_str("}\n\n");
}

pub(crate) fn push_rpc_request(rust: &mut String, method: &RpcMethodSpec) {
    rust.push_str(&format!(
        "impl RpcRequest for {request} {{\n",
        request = method.request_type
    ));
    rust.push_str(&format!("    type Reply = {};\n", method.reply_type));
    rust.push_str(&format!(
        "    const METHOD: &'static str = \"{}\";\n",
        method.method_path()
    ));
    rust.push_str("}\n\n");
}

fn route_key_expr(option: &ProtoRouteKeyOption) -> String {
    route_key_expr_with_receiver(option, "self")
}

fn route_key_expr_with_receiver(option: &ProtoRouteKeyOption, receiver: &str) -> String {
    route_key_expr_with_field(option, receiver, &option.key_field)
}

fn route_key_expr_with_field(option: &ProtoRouteKeyOption, receiver: &str, field: &str) -> String {
    match option.key_type {
        RouteKeyType::U64 => format!("RouteKey::U64({receiver}.{field})"),
        RouteKeyType::I64 => format!("RouteKey::I64({receiver}.{field})"),
        RouteKeyType::String => format!("RouteKey::Str({receiver}.{field}.clone())"),
        RouteKeyType::Bytes => format!("RouteKey::Bytes({receiver}.{field}.clone())"),
    }
}
