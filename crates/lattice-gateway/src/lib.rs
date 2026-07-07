pub mod binding;
pub mod error;
pub mod frame;
pub mod rate_limit;
pub mod route;
pub mod server;
pub mod session;

pub use binding::ProstClientMessageBinding;
pub use error::GatewayError;
pub use frame::{BinaryClientCodec, ClientCodec, ClientFrame};
pub use route::{GatewayRouteContext, GatewayRouteKeyPolicy, GatewayRouteSpec, GatewayRouteTable};
pub use server::{
    GatewayConnectionHandler, GatewayFrameConnectionHandler, GatewayFrameHandler, GatewayService,
    GatewayTcpServer, read_client_frame, write_client_frame,
};

#[cfg(test)]
mod tests;
