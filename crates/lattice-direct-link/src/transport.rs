use async_trait::async_trait;
use lattice_core::{DirectLinkEndpoint, LinkError};

use crate::codec::DirectLinkFrame;

#[derive(Debug, Clone)]
pub struct DirectLinkListenConfig {
    pub endpoint: DirectLinkEndpoint,
    pub max_frame_size: usize,
}

#[async_trait]
pub trait DirectLinkTransport: Clone + Send + Sync + 'static {
    type Listener: Send + Sync + 'static;
    type Connection: DirectLinkConnection;

    async fn bind(&self, config: DirectLinkListenConfig) -> Result<Self::Listener, LinkError>;
    async fn connect(&self, endpoint: DirectLinkEndpoint) -> Result<Self::Connection, LinkError>;
}

#[async_trait]
pub trait DirectLinkConnection: Send + Sync + 'static {
    async fn read_frame(&mut self) -> Result<DirectLinkFrame, LinkError>;
    async fn write_frame(&mut self, frame: DirectLinkFrame) -> Result<(), LinkError>;
    async fn close(&mut self) -> Result<(), LinkError>;
}
