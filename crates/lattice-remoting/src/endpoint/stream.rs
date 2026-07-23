use std::{
    io::{Error as IoError, IoSlice, Result as IoResult},
    pin::Pin,
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
#[cfg(feature = "tls")]
use tokio_rustls::{client::TlsStream as ClientTlsStream, server::TlsStream as ServerTlsStream};

pub(super) enum EndpointStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    TlsClient(ClientTlsStream<TcpStream>),
    #[cfg(feature = "tls")]
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
            #[cfg(feature = "tls")]
            Self::TlsClient(stream) => Pin::new(stream).poll_read(context, buffer),
            #[cfg(feature = "tls")]
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
            #[cfg(feature = "tls")]
            Self::TlsClient(stream) => Pin::new(stream).poll_write(context, buffer),
            #[cfg(feature = "tls")]
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
            #[cfg(feature = "tls")]
            Self::TlsClient(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
            #[cfg(feature = "tls")]
            Self::TlsServer(stream) => Pin::new(stream).poll_write_vectored(context, buffers),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            #[cfg(feature = "tls")]
            Self::TlsClient(stream) => stream.is_write_vectored(),
            #[cfg(feature = "tls")]
            Self::TlsServer(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(context),
            #[cfg(feature = "tls")]
            Self::TlsClient(stream) => Pin::new(stream).poll_flush(context),
            #[cfg(feature = "tls")]
            Self::TlsServer(stream) => Pin::new(stream).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), IoError>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(context),
            #[cfg(feature = "tls")]
            Self::TlsClient(stream) => Pin::new(stream).poll_shutdown(context),
            #[cfg(feature = "tls")]
            Self::TlsServer(stream) => Pin::new(stream).poll_shutdown(context),
        }
    }
}
