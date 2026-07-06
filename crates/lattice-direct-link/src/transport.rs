use async_trait::async_trait;
use lattice_core::{DirectLinkEndpoint, LinkError};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
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
    async fn connect_physical(
        &self,
        endpoint: DirectLinkEndpoint,
    ) -> Result<Self::Connection, LinkError>;
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

    async fn connect_physical(
        &self,
        endpoint: DirectLinkEndpoint,
    ) -> Result<Self::Connection, LinkError> {
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
    reader: TcpDirectLinkReader,
    writer: TcpDirectLinkWriter,
}

#[derive(Debug)]
pub struct TcpDirectLinkReader {
    reader: OwnedReadHalf,
    codec: DirectLinkFrameCodec,
    metrics: DirectLinkMetrics,
}

#[derive(Debug)]
pub struct TcpDirectLinkWriter {
    writer: OwnedWriteHalf,
    codec: DirectLinkFrameCodec,
    metrics: DirectLinkMetrics,
}

impl TcpDirectLinkConnection {
    pub fn new(stream: TcpStream, max_frame_size: usize, metrics: DirectLinkMetrics) -> Self {
        let codec = DirectLinkFrameCodec::new(max_frame_size);
        let (reader, writer) = stream.into_split();
        Self {
            reader: TcpDirectLinkReader {
                reader,
                codec,
                metrics: metrics.clone(),
            },
            writer: TcpDirectLinkWriter {
                writer,
                codec,
                metrics,
            },
        }
    }

    pub fn split(self) -> (TcpDirectLinkReader, TcpDirectLinkWriter) {
        (self.reader, self.writer)
    }
}

impl TcpDirectLinkReader {
    pub async fn read_frame(&mut self) -> Result<DirectLinkFrame, LinkError> {
        read_tcp_frame(&mut self.reader, self.codec, &self.metrics).await
    }
}

impl TcpDirectLinkWriter {
    pub async fn write_frame(&mut self, frame: DirectLinkFrame) -> Result<(), LinkError> {
        write_tcp_frame(&mut self.writer, self.codec, &self.metrics, frame).await
    }

    pub async fn close(&mut self) -> Result<(), LinkError> {
        self.writer.shutdown().await.map_err(|error| {
            LinkError::Protocol(format!("failed to close TCP direct link: {error}"))
        })
    }
}

#[async_trait]
impl DirectLinkConnection for TcpDirectLinkConnection {
    async fn read_frame(&mut self) -> Result<DirectLinkFrame, LinkError> {
        self.reader.read_frame().await
    }

    async fn write_frame(&mut self, frame: DirectLinkFrame) -> Result<(), LinkError> {
        self.writer.write_frame(frame).await
    }

    async fn close(&mut self) -> Result<(), LinkError> {
        self.writer.close().await
    }
}

async fn read_tcp_frame<R>(
    reader: &mut R,
    codec: DirectLinkFrameCodec,
    metrics: &DirectLinkMetrics,
) -> Result<DirectLinkFrame, LinkError>
where
    R: AsyncRead + Unpin,
{
    let len = reader.read_u32().await.map_err(|error| {
        LinkError::Protocol(format!(
            "failed to read TCP direct link frame length: {error}"
        ))
    })?;
    let len = usize::try_from(len)
        .map_err(|_| LinkError::Protocol("TCP direct link frame length overflow".to_string()))?;
    codec.check_frame_size(len).map_err(|error| {
        metrics.record_decode_error();
        LinkError::Protocol(error.to_string())
    })?;
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await.map_err(|error| {
        LinkError::Protocol(format!("failed to read TCP direct link frame: {error}"))
    })?;
    codec
        .decode(&payload)
        .inspect(|frame| {
            metrics.record_receive();
            tracing::trace!(
                link.id = frame.link_id.as_str(),
                frame.kind = ?frame.kind,
                "read TCP direct link frame"
            );
        })
        .map_err(|error| {
            metrics.record_decode_error();
            LinkError::Protocol(error.to_string())
        })
}

async fn write_tcp_frame<W>(
    writer: &mut W,
    codec: DirectLinkFrameCodec,
    metrics: &DirectLinkMetrics,
    frame: DirectLinkFrame,
) -> Result<(), LinkError>
where
    W: AsyncWrite + Unpin,
{
    let payload = codec
        .encode(&frame)
        .map_err(|error| LinkError::Protocol(error.to_string()))?;
    let len = u32::try_from(payload.len())
        .map_err(|_| LinkError::Protocol("TCP direct link frame is too large".to_string()))?;
    writer.write_u32(len).await.map_err(|error| {
        LinkError::Protocol(format!(
            "failed to write TCP direct link frame length: {error}"
        ))
    })?;
    writer.write_all(&payload).await.map_err(|error| {
        LinkError::Protocol(format!("failed to write TCP direct link frame: {error}"))
    })?;
    writer.flush().await.map_err(|error| {
        LinkError::Protocol(format!("failed to flush TCP direct link frame: {error}"))
    })?;
    metrics.record_send();
    tracing::trace!(
        link.id = frame.link_id.as_str(),
        frame.kind = ?frame.kind,
        "wrote TCP direct link frame"
    );
    Ok(())
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
        let mut client = transport.connect_physical(endpoint).await.unwrap();
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

    #[tokio::test]
    async fn tcp_reader_rejects_oversized_length_before_payload_allocation() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_u32(1024).await.unwrap();

        let metrics = DirectLinkMetrics::default();
        let error = read_tcp_frame(&mut reader, DirectLinkFrameCodec::new(128), &metrics)
            .await
            .unwrap_err();

        assert!(
            matches!(error, LinkError::Protocol(message) if message.contains("exceeds maximum size"))
        );
        assert_eq!(metrics.snapshot().decode_errors, 1);
    }
}
