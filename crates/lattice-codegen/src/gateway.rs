#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRoute {
    pub msg_id: u32,
    pub method: String,
}

impl GatewayRoute {
    pub fn new(msg_id: u32, method: impl Into<String>) -> Self {
        Self {
            msg_id,
            method: method.into(),
        }
    }
}
