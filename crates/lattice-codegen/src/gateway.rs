use crate::spec::GatewayRouteKeySpec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRoute {
    pub msg_id: u32,
    pub method: String,
    pub route_key: Option<GatewayRouteKeySpec>,
}

impl GatewayRoute {
    pub fn new(msg_id: u32, method: impl Into<String>) -> Self {
        Self {
            msg_id,
            method: method.into(),
            route_key: None,
        }
    }

    pub fn with_route_key(mut self, route_key: GatewayRouteKeySpec) -> Self {
        self.route_key = Some(route_key);
        self
    }
}
