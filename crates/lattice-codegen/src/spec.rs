use crate::route_key::ProtoRouteKeyOption;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcMethodSpec {
    pub package: String,
    pub service_kind: String,
    pub service_name: String,
    pub method_name: String,
    pub request_type: String,
    pub reply_type: String,
    pub route_key: ProtoRouteKeyOption,
    pub route_key_from_request: bool,
    pub gateway_msg_id: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtoMessageSpec {
    pub proto_full_name: String,
    pub rust_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedDirectLinkStreamSpec {
    /// Rust module name for one logical Direct Link direction.
    ///
    /// Bidirectional links should generate two stream specs: one for
    /// SourceToTarget messages and one for TargetToSource messages. Keeping the
    /// directions separate preserves precise `Handler<Linked<T>>` bounds for
    /// each actor side.
    pub module_name: String,
    pub stream_name: String,
    pub messages: Vec<GeneratedDirectLinkMessageSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedDirectLinkMessageSpec {
    pub message_id: u64,
    pub rust_type: String,
}

impl RpcMethodSpec {
    pub(crate) fn method_path(&self) -> String {
        if self.package.is_empty() {
            format!("{}/{}", self.service_name, self.method_name)
        } else {
            format!(
                "{}.{}/{}",
                self.package, self.service_name, self.method_name
            )
        }
    }
}
