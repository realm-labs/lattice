use async_trait::async_trait;
use lattice_core::{DirectLinkEndpoint, LinkError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, lookup_host};

use crate::codec::{DirectLinkFrame, DirectLinkFrameCodec};
use crate::session::DirectLinkMetrics;

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

#[derive(Debug, Clone, Default)]
pub struct TcpDirectLinkTransport {
    metrics: DirectLinkMetrics,
}

impl TcpDirectLinkTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn metrics(&self) -> DirectLinkMetrics {
        self.metrics.clone()
    }
}

#[async_trait]
impl DirectLinkTransport for TcpDirectLinkTransport {
    type Listener = TcpDirectLinkListener;
    type Connection = TcpDirectLinkConnection;

    async fn bind(&self, config: DirectLinkListenConfig) -> Result<Self::Listener, LinkError> {
        let address = endpoint_socket_address(&config.endpoint).await?;
        let listener = TcpListener::bind(address).await.map_err(|error| {
            LinkError::Protocol(format!("failed to bind TCP direct link listener: {error}"))
        })?;
        let local_addr = listener.local_addr().map_err(|error| {
            LinkError::Protocol(format!(
                "failed to inspect TCP direct link listener: {error}"
            ))
        })?;
        let endpoint =
            DirectLinkEndpoint::new(format!("tcp://{local_addr}").parse().map_err(|error| {
                LinkError::Protocol(format!("invalid TCP direct link endpoint: {error}"))
            })?);
        Ok(TcpDirectLinkListener {
            listener,
            endpoint,
            max_frame_size: config.max_frame_size,
            metrics: self.metrics.clone(),
        })
    }

    async fn connect(&self, endpoint: DirectLinkEndpoint) -> Result<Self::Connection, LinkError> {
        let address = endpoint_socket_address(&endpoint).await?;
        let stream = TcpStream::connect(address).await.map_err(|error| {
            LinkError::Protocol(format!("failed to connect TCP direct link: {error}"))
        })?;
        Ok(TcpDirectLinkConnection::new(
            stream,
            0,
            self.metrics.clone(),
        ))
    }
}

#[derive(Debug)]
pub struct TcpDirectLinkListener {
    listener: TcpListener,
    endpoint: DirectLinkEndpoint,
    max_frame_size: usize,
    metrics: DirectLinkMetrics,
}

impl TcpDirectLinkListener {
    pub fn local_endpoint(&self) -> DirectLinkEndpoint {
        self.endpoint.clone()
    }

    pub async fn accept(&self) -> Result<TcpDirectLinkConnection, LinkError> {
        let (stream, peer) = self.listener.accept().await.map_err(|error| {
            LinkError::Protocol(format!("failed to accept TCP direct link: {error}"))
        })?;
        tracing::debug!(peer.address = %peer, "accepted TCP direct link");
        Ok(TcpDirectLinkConnection::new(
            stream,
            self.max_frame_size,
            self.metrics.clone(),
        ))
    }
}

#[derive(Debug)]
pub struct TcpDirectLinkConnection {
    stream: TcpStream,
    codec: DirectLinkFrameCodec,
    metrics: DirectLinkMetrics,
}

impl TcpDirectLinkConnection {
    pub fn new(stream: TcpStream, max_frame_size: usize, metrics: DirectLinkMetrics) -> Self {
        Self {
            stream,
            codec: DirectLinkFrameCodec::new(max_frame_size),
            metrics,
        }
    }
}

#[async_trait]
impl DirectLinkConnection for TcpDirectLinkConnection {
    async fn read_frame(&mut self) -> Result<DirectLinkFrame, LinkError> {
        let len = self.stream.read_u32().await.map_err(|error| {
            LinkError::Protocol(format!(
                "failed to read TCP direct link frame length: {error}"
            ))
        })?;
        let len = usize::try_from(len).map_err(|_| {
            LinkError::Protocol("TCP direct link frame length overflow".to_string())
        })?;
        let mut payload = vec![0; len];
        self.stream
            .read_exact(&mut payload)
            .await
            .map_err(|error| {
                LinkError::Protocol(format!("failed to read TCP direct link frame: {error}"))
            })?;
        self.codec
            .decode(&payload)
            .inspect(|frame| {
                self.metrics.record_receive();
                tracing::trace!(
                    link.id = frame.link_id.as_str(),
                    frame.kind = ?frame.kind,
                    "read TCP direct link frame"
                );
            })
            .map_err(|error| {
                self.metrics.record_decode_error();
                LinkError::Protocol(error.to_string())
            })
    }

    async fn write_frame(&mut self, frame: DirectLinkFrame) -> Result<(), LinkError> {
        let payload = self
            .codec
            .encode(&frame)
            .map_err(|error| LinkError::Protocol(error.to_string()))?;
        let len = u32::try_from(payload.len())
            .map_err(|_| LinkError::Protocol("TCP direct link frame is too large".to_string()))?;
        self.stream.write_u32(len).await.map_err(|error| {
            LinkError::Protocol(format!(
                "failed to write TCP direct link frame length: {error}"
            ))
        })?;
        self.stream.write_all(&payload).await.map_err(|error| {
            LinkError::Protocol(format!("failed to write TCP direct link frame: {error}"))
        })?;
        self.stream.flush().await.map_err(|error| {
            LinkError::Protocol(format!("failed to flush TCP direct link frame: {error}"))
        })?;
        self.metrics.record_send();
        tracing::trace!(
            link.id = frame.link_id.as_str(),
            frame.kind = ?frame.kind,
            "wrote TCP direct link frame"
        );
        Ok(())
    }

    async fn close(&mut self) -> Result<(), LinkError> {
        self.stream.shutdown().await.map_err(|error| {
            LinkError::Protocol(format!("failed to close TCP direct link: {error}"))
        })
    }
}

async fn endpoint_socket_address(
    endpoint: &DirectLinkEndpoint,
) -> Result<std::net::SocketAddr, LinkError> {
    let uri = &endpoint.uri;
    let address = uri
        .authority()
        .map(|authority| authority.as_str().to_string())
        .or_else(|| {
            let path = uri.path().trim_start_matches('/');
            (!path.is_empty()).then(|| path.to_string())
        })
        .ok_or_else(|| {
            LinkError::Protocol(format!("direct link endpoint has no socket address: {uri}"))
        })?;
    let mut addresses = lookup_host(address.as_str()).await.map_err(|error| {
        LinkError::Protocol(format!(
            "failed to resolve direct link endpoint {address}: {error}"
        ))
    })?;
    addresses.next().ok_or_else(|| {
        LinkError::Protocol(format!(
            "direct link endpoint resolved no addresses: {address}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use lattice_core::{DirectLinkMessageId, LinkId, LinkSequence};

    use super::*;
    use crate::codec::DirectLinkFrame;

    #[tokio::test]
    async fn tcp_transport_round_trips_frame() {
        let transport = TcpDirectLinkTransport::new();
        let listener = transport
            .bind(DirectLinkListenConfig {
                endpoint: DirectLinkEndpoint::new("tcp://127.0.0.1:0".parse().unwrap()),
                max_frame_size: 1024,
            })
            .await
            .unwrap();
        let endpoint = listener.local_endpoint();
        let accept = tokio::spawn(async move {
            let mut server = listener.accept().await.unwrap();
            server.read_frame().await.unwrap()
        });
        let mut client = transport.connect(endpoint).await.unwrap();
        let frame = DirectLinkFrame::message(
            LinkId::new("link-1"),
            LinkSequence(1),
            DirectLinkMessageId(7),
            b"hello".to_vec(),
        );

        client.write_frame(frame.clone()).await.unwrap();

        assert_eq!(accept.await.unwrap(), frame);
        let metrics = transport.metrics().snapshot();
        assert_eq!(metrics.sent, 1);
        assert_eq!(metrics.received, 1);
    }
}
