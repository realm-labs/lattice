mod binding;
mod error;
mod frame;
mod rate_limit;
mod route;
mod session;

pub use binding::ProstClientMessageBinding;
pub use error::GatewayError;
pub use frame::{BinaryClientCodec, ClientCodec, ClientFrame};
pub use rate_limit::{
    GatewayConcurrencyPermit, GatewayRequestContext, GatewayTowerPipeline, KeyedRateLimiter,
    RateLimitKey,
};
pub use route::{GatewayRouteSpec, GatewayRouteTable};
pub use session::{GatewayPush, GatewaySessionRef, GatewaySessionRegistry};

#[cfg(test)]
mod tests;
