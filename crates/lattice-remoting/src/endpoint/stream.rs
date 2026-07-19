use std::{
    io::{Error as IoError, IoSlice, Result as IoResult},
    pin::Pin,
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::{client::TlsStream as ClientTlsStream, server::TlsStream as ServerTlsStream};

pub(super) enum EndpointStream {
    Plain(TcpStream),
    TlsClient(ClientTlsStream<TcpStream>),
    TlsServer(ServerTlsStream<TcpStream>),
}

impl AsyncRead for EndpointStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(context, buffer),
            Self::TlsClient(stream) => Pin::new(stream).poll_read(context, buffer),
            Self::TlsServer(stream) => Pin::new(stream).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for EndpointStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(context, buffer),
            Self::TlsClient(stream) => Pin::new(stream).poll_write(context, buffer),
            Self::TlsServer(stream) => Pin::new(stream).poll_write(context, buffer),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<Result<usize, IoError>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
            Self::TlsClient(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
            Self::TlsServer(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            Self::TlsClient(stream) => stream.is_write_vectored(),
            Self::TlsServer(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(context),
            Self::TlsClient(stream) => Pin::new(stream).poll_flush(context),
            Self::TlsServer(stream) => Pin::new(stream).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(context),
            Self::TlsClient(stream) => Pin::new(stream).poll_shutdown(context),
            Self::TlsServer(stream) => Pin::new(stream).poll_shutdown(context),
        }
    }
}
