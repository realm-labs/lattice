use std::{
    io::{Error, ErrorKind, IoSlice},
    sync::Arc,
};

use bytes::{BufMut, BytesMut};
use lattice_core::{actor_ref::NodeAddress, failpoint::Failpoint};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{
    TlsAcceptor, TlsConnector,
    client::TlsStream as ClientTlsStream,
    rustls::{ClientConfig, ServerConfig, pki_types::ServerName},
    server::TlsStream as ServerTlsStream,
};
use x509_parser::{
    extensions::{GeneralName, ParsedExtension},
    parse_x509_certificate,
};

use crate::{
    association::LaneKind,
    handshake::{Handshake, HandshakeAck, HandshakeError, HandshakeValidator, NodeIdentity},
    protocol::{CatalogueError, ProtocolDescriptor, catalogue_frame, decode_catalogue_frame},
    wire::{Frame, FrameCodec, WireError},
};

pub trait RemotingIo: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> RemotingIo for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

pub struct FramedConnection<S> {
    stream: S,
    codec: FrameCodec,
}

pub struct FramedReader<R> {
    reader: R,
    codec: FrameCodec,
}

impl<R> FramedReader<R>
where
    R: AsyncRead + Send + Unpin,
{
    pub fn new(reader: R, codec: FrameCodec) -> Self {
        Self { reader, codec }
    }

    pub async fn read_frame(&mut self) -> Result<Frame, WireError> {
        let declared = self.reader.read_u32().await? as usize;
        if declared > self.codec.max_frame_size() {
            return Err(WireError::FrameTooLarge {
                actual: declared,
                maximum: self.codec.max_frame_size(),
            });
        }
        let mut frame = BytesMut::with_capacity(4 + declared);
        frame.put_u32(declared as u32);
        frame.resize(4 + declared, 0);
        self.reader.read_exact(&mut frame[4..]).await?;
        self.codec.decode(frame.freeze())
    }
}

pub struct FramedWriter<W> {
    writer: W,
    codec: FrameCodec,
}

impl<W> FramedWriter<W>
where
    W: AsyncWrite + Send + Unpin,
{
    pub fn new(writer: W, codec: FrameCodec) -> Self {
        Self { writer, codec }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<usize, WireError> {
        self.write_frame_with_commit(frame, || {}).await
    }

    pub async fn write_frame_with_commit<F>(
        &mut self,
        frame: &Frame,
        on_first_socket_write: F,
    ) -> Result<usize, WireError>
    where
        F: FnOnce(),
    {
        write_vectored_frame(&mut self.writer, &self.codec, frame, on_first_socket_write).await
    }

    pub async fn flush(&mut self) -> Result<(), WireError> {
        self.writer.flush().await.map_err(WireError::Io)
    }
}

impl<S> FramedConnection<S>
where
    S: RemotingIo,
{
    pub fn new(stream: S, codec: FrameCodec) -> Self {
        Self { stream, codec }
    }

    pub fn set_max_frame_size(&mut self, maximum: usize) -> Result<(), WireError> {
        self.codec = FrameCodec::new(maximum)?;
        Ok(())
    }

    pub async fn read_frame(&mut self) -> Result<Frame, WireError> {
        let declared = self.stream.read_u32().await? as usize;
        if declared > self.codec.max_frame_size() {
            return Err(WireError::FrameTooLarge {
                actual: declared,
                maximum: self.codec.max_frame_size(),
            });
        }
        let mut frame = BytesMut::with_capacity(4 + declared);
        frame.put_u32(declared as u32);
        frame.resize(4 + declared, 0);
        self.stream.read_exact(&mut frame[4..]).await?;
        self.codec.decode(frame.freeze())
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<usize, WireError> {
        write_vectored_frame(&mut self.stream, &self.codec, frame, || {}).await
    }

    pub async fn write_frame_with_commit<F>(
        &mut self,
        frame: &Frame,
        on_first_socket_write: F,
    ) -> Result<usize, WireError>
    where
        F: FnOnce(),
    {
        write_vectored_frame(&mut self.stream, &self.codec, frame, on_first_socket_write).await
    }

    pub async fn flush(&mut self) -> Result<(), WireError> {
        self.stream.flush().await.map_err(WireError::Io)
    }

    pub async fn close(mut self) -> Result<(), WireError> {
        self.stream.shutdown().await.map_err(WireError::Io)
    }

    pub fn into_inner(self) -> S {
        self.stream
    }
}

async fn write_vectored_frame<W, F>(
    writer: &mut W,
    codec: &FrameCodec,
    frame: &Frame,
    on_first_socket_write: F,
) -> Result<usize, WireError>
where
    W: AsyncWrite + Unpin,
    F: FnOnce(),
{
    let header = codec.header(frame)?;
    let payload = frame.payload();
    let total = header.len() + payload.len();
    let mut header_written = 0;
    let mut payload_written = 0;
    let mut on_first_socket_write = Some(on_first_socket_write);
    while header_written + payload_written < total {
        let count = if header_written < header.len() {
            let buffers = [
                IoSlice::new(&header[header_written..]),
                IoSlice::new(&payload[payload_written..]),
            ];
            writer.write_vectored(&buffers).await?
        } else {
            writer.write(&payload[payload_written..]).await?
        };
        if count == 0 {
            return Err(WireError::Io(Error::new(
                ErrorKind::WriteZero,
                "remoting socket wrote zero bytes",
            )));
        }
        if let Some(callback) = on_first_socket_write.take() {
            callback();
        }
        let remaining_header = header.len() - header_written;
        let header_count = count.min(remaining_header);
        header_written += header_count;
        payload_written += count - header_count;
    }
    Ok(total)
}

pub async fn negotiate_outbound<S>(
    connection: &mut FramedConnection<S>,
    handshake: &Handshake,
    local_catalogue: &[ProtocolDescriptor],
    maximum_protocols: usize,
) -> Result<Vec<ProtocolDescriptor>, NegotiationError>
where
    S: RemotingIo,
{
    connection.write_frame(&handshake.to_frame()).await?;
    let ack = HandshakeAck::from_frame(&connection.read_frame().await?)?;
    ack.validate_for(handshake)?;
    connection.set_max_frame_size(ack.maximum_frame_size)?;
    if handshake.lane != LaneKind::Control {
        return Ok(Vec::new());
    }
    lattice_core::failpoint::hit(Failpoint::AssociationAfterHandshakeBeforeCatalogue);
    connection
        .write_frame(&catalogue_frame(local_catalogue))
        .await?;
    decode_catalogue_frame(&connection.read_frame().await?, maximum_protocols)
        .map_err(NegotiationError::Catalogue)
}

pub async fn negotiate_inbound<S>(
    connection: &mut FramedConnection<S>,
    validator: &HandshakeValidator,
    local_catalogue: &[ProtocolDescriptor],
    maximum_protocols: usize,
) -> Result<(Handshake, Vec<ProtocolDescriptor>), NegotiationError>
where
    S: RemotingIo,
{
    let frame = connection.read_frame().await?;
    negotiate_inbound_from_frame(
        connection,
        frame,
        validator,
        local_catalogue,
        maximum_protocols,
    )
    .await
}

pub async fn negotiate_inbound_from_frame<S>(
    connection: &mut FramedConnection<S>,
    first_frame: Frame,
    validator: &HandshakeValidator,
    local_catalogue: &[ProtocolDescriptor],
    maximum_protocols: usize,
) -> Result<(Handshake, Vec<ProtocolDescriptor>), NegotiationError>
where
    S: RemotingIo,
{
    let handshake = Handshake::from_frame(&first_frame)?;
    let negotiated_maximum = validator.validate(&handshake)?;
    connection
        .write_frame(&HandshakeAck::for_handshake(&handshake, negotiated_maximum).to_frame())
        .await?;
    connection.set_max_frame_size(negotiated_maximum)?;
    if handshake.lane != LaneKind::Control {
        return Ok((handshake, Vec::new()));
    }
    lattice_core::failpoint::hit(Failpoint::AssociationAfterHandshakeBeforeCatalogue);
    let peer = decode_catalogue_frame(&connection.read_frame().await?, maximum_protocols)?;
    connection
        .write_frame(&catalogue_frame(local_catalogue))
        .await?;
    Ok((handshake, peer))
}

#[derive(Debug, thiserror::Error)]
pub enum NegotiationError {
    #[error("association negotiation failed at the frame layer")]
    Wire(#[from] WireError),
    #[error("association negotiation handshake was rejected")]
    Handshake(#[from] HandshakeError),
    #[error("association protocol catalogue was rejected")]
    Catalogue(#[from] CatalogueError),
}

pub async fn connect_tcp(
    address: &NodeAddress,
    codec: FrameCodec,
) -> Result<FramedConnection<TcpStream>, WireError> {
    let stream = TcpStream::connect((address.host(), address.port())).await?;
    stream.set_nodelay(true)?;
    Ok(FramedConnection::new(stream, codec))
}

pub async fn bind_tcp(address: &NodeAddress) -> Result<TcpListener, WireError> {
    TcpListener::bind((address.host(), address.port()))
        .await
        .map_err(WireError::Io)
}

pub async fn connect_tls(
    address: &NodeAddress,
    server_name: String,
    config: Arc<ClientConfig>,
    expected_peer: &NodeIdentity,
    codec: FrameCodec,
) -> Result<FramedConnection<ClientTlsStream<TcpStream>>, WireError> {
    let tcp = TcpStream::connect((address.host(), address.port())).await?;
    tcp.set_nodelay(true)?;
    let server_name =
        ServerName::try_from(server_name).map_err(|_| WireError::Tls("invalid server name"))?;
    let stream = TlsConnector::from(config)
        .connect(server_name, tcp)
        .await
        .map_err(|_| WireError::Tls("client handshake failed"))?;
    let certificates = stream
        .get_ref()
        .1
        .peer_certificates()
        .ok_or(WireError::Tls("peer certificate missing"))?;
    let leaf = certificates
        .first()
        .ok_or(WireError::Tls("peer certificate missing"))?;
    verify_peer_certificate_identity(leaf.as_ref(), expected_peer)?;
    Ok(FramedConnection::new(stream, codec))
}

pub async fn connect_tls_candidate(
    address: &NodeAddress,
    server_name: String,
    config: Arc<ClientConfig>,
    codec: FrameCodec,
) -> Result<(FramedConnection<ClientTlsStream<TcpStream>>, Vec<u8>), WireError> {
    let tcp = TcpStream::connect((address.host(), address.port())).await?;
    tcp.set_nodelay(true)?;
    let server_name =
        ServerName::try_from(server_name).map_err(|_| WireError::Tls("invalid server name"))?;
    let stream = TlsConnector::from(config)
        .connect(server_name, tcp)
        .await
        .map_err(|_| WireError::Tls("client handshake failed"))?;
    let certificate = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .map(|certificate| certificate.as_ref().to_vec())
        .ok_or(WireError::Tls("peer certificate missing"))?;
    Ok((FramedConnection::new(stream, codec), certificate))
}

pub async fn accept_tls(
    stream: TcpStream,
    config: Arc<ServerConfig>,
    expected_peer: &NodeIdentity,
    codec: FrameCodec,
) -> Result<FramedConnection<ServerTlsStream<TcpStream>>, WireError> {
    stream.set_nodelay(true)?;
    let stream = TlsAcceptor::from(config)
        .accept(stream)
        .await
        .map_err(|_| WireError::Tls("server handshake failed"))?;
    let certificates = stream
        .get_ref()
        .1
        .peer_certificates()
        .ok_or(WireError::Tls("peer certificate missing"))?;
    let leaf = certificates
        .first()
        .ok_or(WireError::Tls("peer certificate missing"))?;
    verify_peer_certificate_identity(leaf.as_ref(), expected_peer)?;
    Ok(FramedConnection::new(stream, codec))
}

pub fn verify_peer_certificate_identity(
    certificate_der: &[u8],
    expected_peer: &NodeIdentity,
) -> Result<(), WireError> {
    let (_, certificate) = parse_x509_certificate(certificate_der)
        .map_err(|_| WireError::Tls("peer certificate is malformed"))?;
    let expected = format!(
        "spiffe://{}/node/{}/{:032x}",
        expected_peer.cluster_id.as_str(),
        expected_peer.node_id,
        expected_peer.incarnation.get()
    );
    let matches = certificate.extensions().iter().any(|extension| {
        let ParsedExtension::SubjectAlternativeName(names) = extension.parsed_extension() else {
            return false;
        };
        names
            .general_names
            .iter()
            .any(|name| matches!(name, GeneralName::URI(uri) if *uri == expected))
    });
    if matches {
        Ok(())
    } else {
        Err(WireError::Tls(
            "peer certificate identity does not match handshake identity",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    use bytes::Bytes;
    use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation, ProtocolId};
    use rcgen::{CertificateParams, KeyPair, SanType};
    use tokio::{
        io::AsyncWrite,
        net::{TcpListener, TcpStream},
    };
    use tokio_rustls::rustls::{
        RootCertStore,
        client::WebPkiServerVerifier,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
        server::WebPkiClientVerifier,
    };

    use super::*;
    use crate::{
        association::{AssociationId, LaneKind},
        handshake::{FeatureBits, Handshake},
        protocol::{ProtocolDescriptor, ProtocolFingerprint},
        wire::FrameKind,
    };

    struct PartialVectoredWriter {
        bytes: Vec<u8>,
        maximum_write: usize,
    }

    impl AsyncWrite for PartialVectoredWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let count = buffer.len().min(self.maximum_write);
            self.bytes.extend_from_slice(&buffer[..count]);
            Poll::Ready(Ok(count))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffers: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let mut remaining = self.maximum_write;
            let mut count = 0;
            for buffer in buffers {
                let written = buffer.len().min(remaining);
                self.bytes.extend_from_slice(&buffer[..written]);
                count += written;
                remaining -= written;
                if remaining == 0 {
                    break;
                }
            }
            Poll::Ready(Ok(count))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn test_certificate(
        identity: &NodeIdentity,
    ) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
        let mut params = CertificateParams::new(vec!["lattice.test".to_owned()]).unwrap();
        params.subject_alt_names.push(SanType::URI(
            format!(
                "spiffe://{}/node/{}/{:032x}",
                identity.cluster_id.as_str(),
                identity.node_id,
                identity.incarnation.get()
            )
            .try_into()
            .unwrap(),
        ));
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        (
            certificate.der().clone(),
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
    }

    #[tokio::test]
    async fn real_tcp_rejects_oversized_length_before_allocation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut connection = FramedConnection::new(stream, FrameCodec::new(64).unwrap());
            assert!(matches!(
                connection.read_frame().await,
                Err(WireError::FrameTooLarge {
                    actual: 65,
                    maximum: 64
                })
            ));
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_u32(65).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn real_tcp_frame_round_trip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut connection = FramedConnection::new(stream, FrameCodec::new(1024).unwrap());
            connection.read_frame().await.unwrap()
        });
        let stream = TcpStream::connect(address).await.unwrap();
        let mut client = FramedConnection::new(stream, FrameCodec::new(1024).unwrap());
        let expected = Frame::new(FrameKind::Tell, Bytes::from_static(b"opaque"));
        client.write_frame(&expected).await.unwrap();
        assert_eq!(server.await.unwrap(), expected);
    }

    #[tokio::test]
    async fn vectored_frame_write_handles_partial_header_and_payload_writes() {
        let codec = FrameCodec::new(1024).unwrap();
        let frame = Frame::new(FrameKind::Tell, Bytes::from_static(b"opaque-payload"));
        let expected = codec.encode(&frame).unwrap();
        let mut writer = PartialVectoredWriter {
            bytes: Vec::new(),
            maximum_write: 3,
        };
        let commits = AtomicUsize::new(0);

        let written = write_vectored_frame(&mut writer, &codec, &frame, || {
            commits.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .unwrap();

        assert_eq!(written, expected.len());
        assert_eq!(writer.bytes, expected);
        assert_eq!(commits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn real_tcp_handshake_binds_lane_and_exchanges_bounded_catalogue() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = listener.local_addr().unwrap();
        let cluster_id = ClusterId::new("test").unwrap();
        let server_identity = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: "server".to_owned(),
            address: NodeAddress::new("127.0.0.1", socket.port()).unwrap(),
            incarnation: NodeIncarnation::new(2).unwrap(),
        };
        let client_identity = NodeIdentity {
            cluster_id,
            node_id: "client".to_owned(),
            address: NodeAddress::new("127.0.0.1", 25548).unwrap(),
            incarnation: NodeIncarnation::new(1).unwrap(),
        };
        let client_protocol = ProtocolDescriptor {
            protocol_id: ProtocolId::new(7).unwrap(),
            fingerprint: ProtocolFingerprint::digest(b"client/v1"),
        };
        let server_protocol = ProtocolDescriptor {
            protocol_id: ProtocolId::new(8).unwrap(),
            fingerprint: ProtocolFingerprint::digest(b"server/v1"),
        };
        let server_expected = client_protocol.clone();
        let server_local = server_protocol.clone();
        let validator = HandshakeValidator::new(server_identity.clone(), 4096, 1).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut connection = FramedConnection::new(stream, FrameCodec::new(8192).unwrap());
            let (handshake, peer) =
                negotiate_inbound(&mut connection, &validator, &[server_local], 8)
                    .await
                    .unwrap();
            assert_eq!(handshake.lane, LaneKind::Control);
            assert_eq!(peer, vec![server_expected]);
        });
        let stream = TcpStream::connect(socket).await.unwrap();
        let mut connection = FramedConnection::new(stream, FrameCodec::new(8192).unwrap());
        let peer = negotiate_outbound(
            &mut connection,
            &Handshake {
                source: client_identity,
                expected_remote: server_identity,
                association_id: AssociationId::new(9).unwrap(),
                lane: LaneKind::Control,
                connection_nonce: 10,
                maximum_frame_size: 4096,
                features: FeatureBits::REQUIRED_V2,
            },
            &[client_protocol],
            8,
        )
        .await
        .unwrap();
        assert_eq!(peer, vec![server_protocol]);
        server.await.unwrap();
    }

    #[test]
    fn certificate_identity_is_bound_to_cluster_node_and_incarnation() {
        let expected = NodeIdentity {
            cluster_id: ClusterId::new("test").unwrap(),
            node_id: "node-a".to_owned(),
            address: NodeAddress::new("127.0.0.1", 25520).unwrap(),
            incarnation: NodeIncarnation::new(7).unwrap(),
        };
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names.push(SanType::URI(
            "spiffe://test/node/node-a/00000000000000000000000000000007"
                .try_into()
                .unwrap(),
        ));
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        verify_peer_certificate_identity(certificate.der(), &expected).unwrap();

        let stale = NodeIdentity {
            incarnation: NodeIncarnation::new(8).unwrap(),
            ..expected
        };
        assert!(matches!(
            verify_peer_certificate_identity(certificate.der(), &stale),
            Err(WireError::Tls(_))
        ));
    }

    #[tokio::test]
    async fn real_mutual_tls_socket_verifies_both_node_identities() {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = listener.local_addr().unwrap();
        let cluster_id = ClusterId::new("tls-test").unwrap();
        let client_identity = NodeIdentity {
            cluster_id: cluster_id.clone(),
            node_id: "client".to_owned(),
            address: NodeAddress::new("127.0.0.1", socket.port() - 1).unwrap(),
            incarnation: NodeIncarnation::new(11).unwrap(),
        };
        let server_identity = NodeIdentity {
            cluster_id,
            node_id: "server".to_owned(),
            address: NodeAddress::new("127.0.0.1", socket.port()).unwrap(),
            incarnation: NodeIncarnation::new(12).unwrap(),
        };
        let (client_certificate, client_key) = test_certificate(&client_identity);
        let (server_certificate, server_key) = test_certificate(&server_identity);
        let mut client_roots = RootCertStore::empty();
        client_roots.add(client_certificate.clone()).unwrap();
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
            .build()
            .unwrap();
        let provider = Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider());
        let server_config = Arc::new(
            ServerConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(vec![server_certificate.clone()], server_key)
                .unwrap(),
        );
        let mut server_roots = RootCertStore::empty();
        server_roots.add(server_certificate).unwrap();
        let server_verifier = WebPkiServerVerifier::builder(Arc::new(server_roots))
            .build()
            .unwrap();
        let client_config = Arc::new(
            ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .dangerous()
                .with_custom_certificate_verifier(server_verifier)
                .with_client_auth_cert(vec![client_certificate], client_key)
                .unwrap(),
        );
        let expected_client = client_identity.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut connection = accept_tls(
                stream,
                server_config,
                &expected_client,
                FrameCodec::new(4096).unwrap(),
            )
            .await
            .unwrap();
            let frame = connection.read_frame().await.unwrap();
            connection.write_frame(&frame).await.unwrap();
        });
        let mut client = connect_tls(
            &server_identity.address,
            "lattice.test".to_owned(),
            client_config,
            &server_identity,
            FrameCodec::new(4096).unwrap(),
        )
        .await
        .unwrap();
        let expected = Frame::new(FrameKind::Heartbeat, Bytes::from_static(b"tls"));
        client.write_frame(&expected).await.unwrap();
        assert_eq!(client.read_frame().await.unwrap(), expected);
        server.await.unwrap();
    }
}
