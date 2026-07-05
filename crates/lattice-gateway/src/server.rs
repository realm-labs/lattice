use std::future::Future;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::{BinaryClientCodec, ClientCodec, ClientFrame, GatewayError};

#[async_trait]
pub trait GatewayFrameHandler: Clone + Send + Sync + 'static {
    async fn handle_frame(&self, frame: ClientFrame) -> Result<Option<ClientFrame>, GatewayError>;
}

#[async_trait]
impl<F, Fut> GatewayFrameHandler for F
where
    F: Fn(ClientFrame) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<ClientFrame>, GatewayError>> + Send,
{
    async fn handle_frame(&self, frame: ClientFrame) -> Result<Option<ClientFrame>, GatewayError> {
        self(frame).await
    }
}

#[derive(Debug)]
pub struct GatewayTcpServer<H> {
    listener: TcpListener,
    handler: H,
    ready: Option<oneshot::Sender<SocketAddr>>,
}

impl<H> GatewayTcpServer<H>
where
    H: GatewayFrameHandler,
{
    pub fn new(listener: TcpListener, handler: H) -> Self {
        Self {
            listener,
            handler,
            ready: None,
        }
    }

    pub fn ready_signal(mut self, ready: oneshot::Sender<SocketAddr>) -> Self {
        self.ready = Some(ready);
        self
    }

    pub async fn run_until_shutdown_signal<F>(self, shutdown: F) -> Result<(), GatewayError>
    where
        F: Future<Output = ()>,
    {
        let Self {
            listener,
            handler,
            ready,
        } = self;
        let local_addr = listener
            .local_addr()
            .map_err(|error| GatewayError::Io(error.to_string()))?;
        if let Some(ready) = ready {
            let _ = ready.send(local_addr);
        }
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => return Ok(()),
                accepted = listener.accept() => {
                    let (socket, _peer) = accepted
                        .map_err(|error| GatewayError::Io(error.to_string()))?;
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let _ = handle_connection(socket, handler).await;
                    });
                }
            }
        }
    }
}

async fn handle_connection<H>(mut socket: TcpStream, handler: H) -> Result<(), GatewayError>
where
    H: GatewayFrameHandler,
{
    loop {
        let frame = match read_client_frame(&mut socket).await {
            Ok(frame) => frame,
            Err(GatewayError::Io(message)) if message.contains("early eof") => break,
            Err(error) => return Err(error),
        };
        if let Some(reply) = handler.handle_frame(frame).await? {
            write_client_frame(&mut socket, reply).await?;
        }
    }
    Ok(())
}

pub async fn read_client_frame<R>(reader: &mut R) -> Result<ClientFrame, GatewayError>
where
    R: AsyncRead + Unpin,
{
    let len = reader
        .read_u32()
        .await
        .map_err(|error| GatewayError::Io(error.to_string()))? as usize;
    let mut bytes = vec![0; len];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|error| GatewayError::Io(error.to_string()))?;
    BinaryClientCodec.decode(bytes.as_slice())
}

pub async fn write_client_frame<W>(writer: &mut W, frame: ClientFrame) -> Result<(), GatewayError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = BinaryClientCodec.encode(frame)?;
    writer
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|error| GatewayError::Io(error.to_string()))?;
    writer
        .write_all(bytes.as_slice())
        .await
        .map_err(|error| GatewayError::Io(error.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|error| GatewayError::Io(error.to_string()))?;
    Ok(())
}
