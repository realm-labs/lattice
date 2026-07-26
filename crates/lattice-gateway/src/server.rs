use std::fmt;
use std::future::Future;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, timeout, timeout_at};

use crate::config::GatewayServerConfig;
use crate::error::GatewayError;
use crate::frame::{BinaryClientCodec, ClientCodec, ClientFrame};

pub const DEFAULT_MAX_CLIENT_FRAME_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_FRAME_READ_CHUNK: usize = 64 * 1024;

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
    config: Arc<GatewayServerConfig>,
    drain: Option<watch::Receiver<bool>>,
}

impl<H> GatewayFrameConnectionHandler<H> {
    pub fn new(frame_handler: H) -> Self {
        Self {
            frame_handler,
            config: Arc::new(GatewayServerConfig::default()),
            drain: None,
        }
    }

    pub fn with_config(mut self, config: GatewayServerConfig) -> Self {
        self.config = Arc::new(config);
        self
    }

    pub fn drain_signal(mut self, drain: watch::Receiver<bool>) -> Self {
        self.drain = Some(drain);
        self
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
        handle_framed_connection(
            socket,
            self.frame_handler.clone(),
            self.config.as_ref(),
            self.drain.clone(),
        )
        .await
    }
}

struct GatewayBackgroundTask {
    name: String,
    future: GatewayTaskFuture,
}

type GatewayBackgroundOutcome = (String, Result<(), GatewayError>);
type GatewayConnectionOutcome = (SocketAddr, Result<(), GatewayError>);

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
    config: GatewayServerConfig,
    drain: Option<watch::Sender<bool>>,
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
            config: GatewayServerConfig::default(),
            drain: None,
        }
    }

    pub fn with_config(mut self, config: GatewayServerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn drain_signal(mut self, drain: watch::Sender<bool>) -> Self {
        self.drain = Some(drain);
        self
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
            config,
            drain,
        } = self;
        config.validate()?;
        let local_addr = listener.local_addr()?;
        if let Some(ready) = ready {
            let _ = ready.send(local_addr);
        }

        let mut background = JoinSet::new();
        for task in background_tasks {
            background.spawn(async move {
                let name = task.name;
                let result = task.future.await;
                (name, result)
            });
        }

        let mut connections = JoinSet::new();
        let permits = Arc::new(Semaphore::new(config.max_connections));
        let mut accept_backoff = config.accept_backoff_min;
        let mut accept_retry_at = None;

        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                Some(joined) = background.join_next(), if !background.is_empty() => {
                    return Err(background_failure(joined));
                }
                Some(joined) = connections.join_next(), if !connections.is_empty() => {
                    observe_connection_result(joined);
                }
                () = wait_until(accept_retry_at), if accept_retry_at.is_some() => {
                    accept_retry_at = None;
                }
                accepted = listener.accept(), if accept_retry_at.is_none() => {
                    match accepted {
                        Ok((socket, peer)) => {
                            accept_backoff = config.accept_backoff_min;
                            match permits.clone().try_acquire_owned() {
                                Ok(permit) => {
                                    let connection_handler = connection_handler.clone();
                                    connections.spawn(async move {
                                        let _permit = permit;
                                        let result = connection_handler
                                            .handle_connection(socket, peer)
                                            .await;
                                        (peer, result)
                                    });
                                }
                                Err(_) => {
                                    observe_connection_limit(peer, config.max_connections);
                                    drop(socket);
                                }
                            }
                        }
                        Err(error) if is_fatal_accept_error(error.kind()) => {
                            return Err(error.into());
                        }
                        Err(error) => {
                            observe_accept_failure(&error);
                            if !is_peer_accept_error(error.kind()) {
                                accept_retry_at = Some(Instant::now() + accept_backoff);
                                accept_backoff = accept_backoff
                                    .saturating_mul(2)
                                    .min(config.accept_backoff_max);
                            }
                        }
                    }
                }
            }
        }

        drop(listener);
        if let Some(drain) = drain {
            drain.send_replace(true);
        }
        drain_connections(&mut connections, config.shutdown_drain_timeout).await;
        background.shutdown().await;
        Ok(())
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
        let (drain_tx, drain_rx) = watch::channel(false);
        let service = GatewayService::new(
            listener,
            GatewayFrameConnectionHandler::new(handler).drain_signal(drain_rx),
        )
        .drain_signal(drain_tx);
        Self { service }
    }

    pub fn with_config(mut self, config: GatewayServerConfig) -> Self {
        self.service.connection_handler =
            self.service.connection_handler.with_config(config.clone());
        self.service = self.service.with_config(config);
        self
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

fn background_failure(joined: Result<GatewayBackgroundOutcome, JoinError>) -> GatewayError {
    match joined {
        Ok((task, Ok(()))) => GatewayError::BackgroundTaskExited { task },
        Ok((task, Err(error))) => GatewayError::BackgroundTaskFailed {
            task,
            error: error.to_string(),
        },
        Err(error) => GatewayError::BackgroundTaskFailed {
            task: "unknown".to_string(),
            error: error.to_string(),
        },
    }
}

fn observe_connection_result(joined: Result<GatewayConnectionOutcome, JoinError>) {
    static FAILURES: AtomicU64 = AtomicU64::new(0);
    let (peer, error) = match joined {
        Ok((_, Ok(()))) => return,
        Ok((peer, Err(error))) => {
            if error.is_peer_disconnect() {
                tracing::debug!(%peer, error = ?error, "gateway client disconnected");
                return;
            }
            (Some(peer), error.to_string())
        }
        Err(error) if error.is_cancelled() => return,
        Err(error) => (None, error.to_string()),
    };
    let count = FAILURES.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    if count == 1 || count.is_multiple_of(100) {
        tracing::warn!(
            peer = ?peer,
            connection_failure_count = count,
            error = %error,
            "gateway connection task failed (subsequent failures are aggregated)"
        );
    }
}

fn observe_connection_limit(peer: SocketAddr, max_connections: usize) {
    static REJECTED: AtomicU64 = AtomicU64::new(0);
    let count = REJECTED.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    if count == 1 || count.is_multiple_of(100) {
        tracing::warn!(
            %peer,
            rejected_connection_count = count,
            max_connections,
            "gateway connection limit reached (subsequent rejections are aggregated)"
        );
    }
}

fn observe_accept_failure(error: &std::io::Error) {
    static FAILURES: AtomicU64 = AtomicU64::new(0);
    let count = FAILURES.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    if count == 1 || count.is_multiple_of(100) {
        tracing::warn!(
            accept_failure_count = count,
            error = ?error,
            "gateway accept failed (subsequent failures are aggregated)"
        );
    }
}

fn is_fatal_accept_error(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::InvalidInput | ErrorKind::NotConnected | ErrorKind::AddrNotAvailable
    )
}

fn is_peer_accept_error(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::Interrupted
    )
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

async fn wait_for_drain(drain: &mut Option<watch::Receiver<bool>>) {
    let Some(drain) = drain else {
        return std::future::pending::<()>().await;
    };
    loop {
        if *drain.borrow() {
            return;
        }
        if drain.changed().await.is_err() {
            return;
        }
    }
}

async fn drain_connections(
    connections: &mut JoinSet<GatewayConnectionOutcome>,
    drain_timeout: Duration,
) {
    let deadline = Instant::now() + drain_timeout;
    while !connections.is_empty() {
        match timeout_at(deadline, connections.join_next()).await {
            Ok(Some(joined)) => observe_connection_result(joined),
            Ok(None) => break,
            Err(_) => {
                tracing::warn!(
                    in_flight = connections.len(),
                    "gateway shutdown drain timed out, aborting in-flight connections"
                );
                connections.shutdown().await;
                break;
            }
        }
    }
}

async fn handle_framed_connection<H>(
    mut socket: TcpStream,
    handler: H,
    config: &GatewayServerConfig,
    mut drain: Option<watch::Receiver<bool>>,
) -> Result<(), GatewayError>
where
    H: GatewayFrameHandler,
{
    loop {
        let len = tokio::select! {
            biased;
            () = wait_for_drain(&mut drain) => break,
            len = read_frame_len(&mut socket, config) => len?,
        };
        let Some(len) = len else {
            break;
        };
        let frame = read_frame_body(&mut socket, len, config).await?;
        if let Some(reply) = handler.handle_frame(frame).await? {
            write_framed_reply(&mut socket, reply, config).await?;
        }
    }
    Ok(())
}

async fn read_frame_len<R>(
    reader: &mut R,
    config: &GatewayServerConfig,
) -> Result<Option<usize>, GatewayError>
where
    R: AsyncRead + Unpin,
{
    let len = match timeout(config.idle_timeout, reader.read_u32()).await {
        Ok(Ok(len)) => len as usize,
        Ok(Err(error)) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => return Err(timed_out("idle", config.idle_timeout)),
    };
    if len > config.max_client_frame_size {
        return Err(GatewayError::FrameTooLarge {
            actual: len,
            max: config.max_client_frame_size,
        });
    }
    Ok(Some(len))
}

async fn read_frame_body<R>(
    reader: &mut R,
    len: usize,
    config: &GatewayServerConfig,
) -> Result<ClientFrame, GatewayError>
where
    R: AsyncRead + Unpin,
{
    let bytes = match timeout(
        config.read_timeout,
        read_frame_bytes(reader, len, config.max_frame_read_chunk),
    )
    .await
    {
        Ok(bytes) => bytes?,
        Err(_) => return Err(timed_out("read", config.read_timeout)),
    };
    BinaryClientCodec.decode(bytes.as_slice())
}

async fn write_framed_reply<W>(
    writer: &mut W,
    frame: ClientFrame,
    config: &GatewayServerConfig,
) -> Result<(), GatewayError>
where
    W: AsyncWrite + Unpin,
{
    match timeout(config.write_timeout, write_client_frame(writer, frame)).await {
        Ok(result) => result,
        Err(_) => Err(timed_out("write", config.write_timeout)),
    }
}

async fn read_frame_bytes<R>(
    reader: &mut R,
    len: usize,
    max_chunk: usize,
) -> Result<Vec<u8>, GatewayError>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(len.min(max_chunk));
    while bytes.len() < len {
        let start = bytes.len();
        let end = start + (len - start).min(max_chunk);
        bytes.resize(end, 0);
        reader.read_exact(&mut bytes[start..end]).await?;
    }
    Ok(bytes)
}

fn timed_out(operation: &str, budget: Duration) -> GatewayError {
    GatewayError::Io {
        kind: ErrorKind::TimedOut,
        message: format!("gateway {operation} timeout elapsed after {budget:?}"),
    }
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
    let len = reader.read_u32().await? as usize;
    if len > max_frame_size {
        return Err(GatewayError::FrameTooLarge {
            actual: len,
            max: max_frame_size,
        });
    }
    let bytes = read_frame_bytes(reader, len, DEFAULT_MAX_FRAME_READ_CHUNK).await?;
    BinaryClientCodec.decode(bytes.as_slice())
}

pub async fn write_client_frame<W>(writer: &mut W, frame: ClientFrame) -> Result<(), GatewayError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = BinaryClientCodec.encode(frame)?;
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(bytes.as_slice()).await?;
    writer.flush().await?;
    Ok(())
}
