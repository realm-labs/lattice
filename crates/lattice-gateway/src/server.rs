use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::Server;

use crate::{BinaryClientCodec, ClientCodec, ClientFrame, GatewayError};

pub const DEFAULT_MAX_CLIENT_FRAME_SIZE: usize = 16 * 1024 * 1024;

type GatewayTaskFuture = Pin<Box<dyn Future<Output = Result<(), GatewayError>> + Send + 'static>>;

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

#[async_trait]
pub trait GatewayConnectionHandler: Clone + Send + Sync + 'static {
    async fn handle_connection(
        &self,
        socket: TcpStream,
        peer: SocketAddr,
    ) -> Result<(), GatewayError>;
}

#[async_trait]
impl<F, Fut> GatewayConnectionHandler for F
where
    F: Fn(TcpStream, SocketAddr) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(), GatewayError>> + Send,
{
    async fn handle_connection(
        &self,
        socket: TcpStream,
        peer: SocketAddr,
    ) -> Result<(), GatewayError> {
        self(socket, peer).await
    }
}

#[derive(Debug, Clone)]
pub struct GatewayFrameConnectionHandler<H> {
    frame_handler: H,
}

impl<H> GatewayFrameConnectionHandler<H> {
    pub fn new(frame_handler: H) -> Self {
        Self { frame_handler }
    }
}

#[async_trait]
impl<H> GatewayConnectionHandler for GatewayFrameConnectionHandler<H>
where
    H: GatewayFrameHandler,
{
    async fn handle_connection(
        &self,
        socket: TcpStream,
        _peer: SocketAddr,
    ) -> Result<(), GatewayError> {
        handle_framed_connection(socket, self.frame_handler.clone()).await
    }
}

struct GatewayBackgroundTask {
    name: String,
    future: GatewayTaskFuture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayTaskKind {
    Background,
    Connection,
}

#[derive(Debug)]
struct GatewayTaskCompletion {
    kind: GatewayTaskKind,
    name: String,
    result: Result<(), GatewayError>,
}

impl fmt::Debug for GatewayBackgroundTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayBackgroundTask")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct GatewayService<H> {
    listener: TcpListener,
    connection_handler: H,
    ready: Option<oneshot::Sender<SocketAddr>>,
    background_tasks: Vec<GatewayBackgroundTask>,
}

impl<H> GatewayService<H>
where
    H: GatewayConnectionHandler,
{
    pub fn new(listener: TcpListener, connection_handler: H) -> Self {
        Self {
            listener,
            connection_handler,
            ready: None,
            background_tasks: Vec::new(),
        }
    }

    pub fn ready_signal(mut self, ready: oneshot::Sender<SocketAddr>) -> Self {
        self.ready = Some(ready);
        self
    }

    pub fn background_task<F>(mut self, name: impl Into<String>, future: F) -> Self
    where
        F: Future<Output = Result<(), GatewayError>> + Send + 'static,
    {
        self.background_tasks.push(GatewayBackgroundTask {
            name: name.into(),
            future: Box::pin(future),
        });
        self
    }

    pub fn background_tonic_service<S>(
        self,
        name: impl Into<String>,
        listener: TcpListener,
        service: S,
    ) -> Self
    where
        S: Service<http::Request<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: axum::response::IntoResponse,
        S::Future: Send + 'static,
    {
        self.background_task(name, async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .map_err(|error| GatewayError::Io(error.to_string()))
        })
    }

    pub async fn run(self) -> Result<(), GatewayError> {
        self.run_until_shutdown_signal(std::future::pending::<()>())
            .await
    }

    pub async fn run_until_shutdown_signal<F>(self, shutdown: F) -> Result<(), GatewayError>
    where
        F: Future<Output = ()>,
    {
        let Self {
            listener,
            connection_handler,
            ready,
            background_tasks,
        } = self;
        let local_addr = listener
            .local_addr()
            .map_err(|error| GatewayError::Io(error.to_string()))?;
        if let Some(ready) = ready {
            let _ = ready.send(local_addr);
        }

        let mut tasks = JoinSet::new();
        for task in background_tasks {
            tasks.spawn(async move {
                let name = task.name;
                let result = task.future.await;
                GatewayTaskCompletion {
                    kind: GatewayTaskKind::Background,
                    name,
                    result,
                }
            });
        }

        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Ok(());
                }
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    match joined {
                        Some(Ok(GatewayTaskCompletion {
                            kind: GatewayTaskKind::Background,
                            name,
                            result: Ok(()),
                        })) => {
                            return Err(GatewayError::BackgroundTaskExited { task: name });
                        }
                        Some(Ok(GatewayTaskCompletion {
                            kind: GatewayTaskKind::Background | GatewayTaskKind::Connection,
                            name,
                            result: Err(error),
                        })) => {
                            return Err(GatewayError::BackgroundTaskFailed {
                                task: name,
                                error: error.to_string(),
                            });
                        }
                        Some(Ok(GatewayTaskCompletion {
                            kind: GatewayTaskKind::Connection,
                            result: Ok(()),
                            ..
                        })) => {}
                        Some(Err(error)) => {
                            return Err(GatewayError::BackgroundTaskFailed {
                                task: "unknown".to_string(),
                                error: error.to_string(),
                            });
                        }
                        None => {}
                    }
                }
                accepted = listener.accept() => {
                    let (socket, peer) = accepted
                        .map_err(|error| GatewayError::Io(error.to_string()))?;
                    let connection_handler = connection_handler.clone();
                    tasks.spawn(async move {
                        let result = connection_handler.handle_connection(socket, peer).await;
                        GatewayTaskCompletion {
                            kind: GatewayTaskKind::Connection,
                            name: format!("connection {peer}"),
                            result,
                        }
                    });
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct GatewayTcpServer<H> {
    service: GatewayService<GatewayFrameConnectionHandler<H>>,
}

impl<H> GatewayTcpServer<H>
where
    H: GatewayFrameHandler,
{
    pub fn new(listener: TcpListener, handler: H) -> Self {
        Self {
            service: GatewayService::new(listener, GatewayFrameConnectionHandler::new(handler)),
        }
    }

    pub fn ready_signal(mut self, ready: oneshot::Sender<SocketAddr>) -> Self {
        self.service = self.service.ready_signal(ready);
        self
    }

    pub async fn run_until_shutdown_signal<F>(self, shutdown: F) -> Result<(), GatewayError>
    where
        F: Future<Output = ()>,
    {
        self.service.run_until_shutdown_signal(shutdown).await
    }
}

async fn handle_framed_connection<H>(mut socket: TcpStream, handler: H) -> Result<(), GatewayError>
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
    read_client_frame_with_limit(reader, DEFAULT_MAX_CLIENT_FRAME_SIZE).await
}

pub async fn read_client_frame_with_limit<R>(
    reader: &mut R,
    max_frame_size: usize,
) -> Result<ClientFrame, GatewayError>
where
    R: AsyncRead + Unpin,
{
    let len = reader
        .read_u32()
        .await
        .map_err(|error| GatewayError::Io(error.to_string()))? as usize;
    if len > max_frame_size {
        return Err(GatewayError::FrameTooLarge {
            actual: len,
            max: max_frame_size,
        });
    }
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
