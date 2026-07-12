use std::collections::HashMap;

use lattice_core::id::RouteKey;
use lattice_core::kind::ActorKind;

use crate::error::GatewayError;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayRouteContext {
    route_keys: HashMap<String, RouteKey>,
}

impl GatewayRouteContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_route_key(mut self, name: impl Into<String>, route_key: RouteKey) -> Self {
        self.insert_route_key(name, route_key);
        self
    }

    pub fn insert_route_key(&mut self, name: impl Into<String>, route_key: RouteKey) {
        self.route_keys.insert(name.into(), route_key);
    }

    pub fn route_key(&self, name: &str) -> Option<&RouteKey> {
        self.route_keys.get(name)
    }

    pub fn require_route_key(&self, name: &str) -> Result<RouteKey, GatewayError> {
        self.route_key(name)
            .cloned()
            .ok_or_else(|| GatewayError::MissingRouteContextKey {
                key: name.to_string(),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRouteSpec {
    pub msg_id: u32,
    pub actor_kind: ActorKind,
    pub protocol_name: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDecision {
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl RouteDecision {
    pub fn new(actor_kind: ActorKind, route_key: RouteKey) -> Self {
        Self {
            actor_kind,
            route_key,
        }
    }
}

pub trait MessageRouter {
    fn route(
        &mut self,
        context: &GatewayRouteContext,
        route: &GatewayRouteSpec,
    ) -> Result<RouteDecision, GatewayError>;
}

#[derive(Debug, Default)]
pub struct GatewayRouteTable {
    routes: HashMap<u32, GatewayRouteSpec>,
}

impl GatewayRouteTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, route: GatewayRouteSpec) -> Result<(), GatewayError> {
        if self.routes.contains_key(&route.msg_id) {
            return Err(GatewayError::DuplicateRoute {
                msg_id: route.msg_id,
            });
        }
        self.routes.insert(route.msg_id, route);
        Ok(())
    }

    pub fn get(&self, msg_id: u32) -> Option<&GatewayRouteSpec> {
        self.routes.get(&msg_id)
    }
}
