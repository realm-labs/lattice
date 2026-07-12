use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use x509_parser::extensions::{GeneralName, ParsedExtension};
use x509_parser::parse_x509_certificate;

use crate::association::LaneKind;
use crate::handshake::{Handshake, HandshakeAck, HandshakeError, HandshakeValidator, NodeIdentity};
use crate::protocol::{
    CatalogueError, ProtocolDescriptor, catalogue_frame, decode_catalogue_frame,
};
use crate::wire::{Frame, FrameCodec, WireError};

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
        let mut body = vec![0_u8; declared];
        self.reader.read_exact(&mut body).await?;
        let mut frame = BytesMut::with_capacity(4 + body.len());
        frame.put_u32(declared as u32);
        frame.extend_from_slice(&body);
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
        let encoded = self.codec.encode(frame)?;
        let mut written = 0;
        let mut on_first_socket_write = Some(on_first_socket_write);
        while written < encoded.len() {
            let count = self.writer.write(&encoded[written..]).await?;
            if count == 0 {
                return Err(WireError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "remoting socket wrote zero bytes",
                )));
            }
            if let Some(callback) = on_first_socket_write.take() {
                callback();
            }
            written += count;
        }
        Ok(written)
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
        let mut body = vec![0_u8; declared];
        self.stream.read_exact(&mut body).await?;
        let mut frame = BytesMut::with_capacity(4 + body.len());
        frame.put_u32(declared as u32);
        frame.extend_from_slice(&body);
        self.codec.decode(frame.freeze())
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<usize, WireError> {
        let encoded = self.codec.encode(frame)?;
        self.stream.write_all(&encoded).await?;
        Ok(encoded.len())
    }

    pub async fn write_frame_with_commit<F>(
        &mut self,
        frame: &Frame,
        on_first_socket_write: F,
    ) -> Result<usize, WireError>
    where
        F: FnOnce(),
    {
        let encoded = self.codec.encode(frame)?;
        let mut written = 0;
        let mut on_first_socket_write = Some(on_first_socket_write);
        while written < encoded.len() {
            let count = self.stream.write(&encoded[written..]).await?;
            if count == 0 {
                return Err(WireError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "remoting socket wrote zero bytes",
                )));
            }
            if let Some(callback) = on_first_socket_write.take() {
                callback();
            }
            written += count;
        }
        Ok(written)
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
    let handshake = Handshake::from_frame(&connection.read_frame().await?)?;
    let negotiated_maximum = validator.validate(&handshake)?;
    connection
        .write_frame(&HandshakeAck::for_handshake(&handshake, negotiated_maximum).to_frame())
        .await?;
    connection.set_max_frame_size(negotiated_maximum)?;
    if handshake.lane != LaneKind::Control {
        return Ok((handshake, Vec::new()));
    }
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
    address: &lattice_core::actor_ref::NodeAddress,
    codec: FrameCodec,
) -> Result<FramedConnection<tokio::net::TcpStream>, WireError> {
    let stream = tokio::net::TcpStream::connect((address.host(), address.port())).await?;
    stream.set_nodelay(true)?;
    Ok(FramedConnection::new(stream, codec))
}

pub async fn bind_tcp(
    address: &lattice_core::actor_ref::NodeAddress,
) -> Result<tokio::net::TcpListener, WireError> {
    tokio::net::TcpListener::bind((address.host(), address.port()))
        .await
        .map_err(WireError::Io)
}

pub async fn connect_tls(
    address: &lattice_core::actor_ref::NodeAddress,
    server_name: String,
    config: std::sync::Arc<ClientConfig>,
    expected_peer: &NodeIdentity,
    codec: FrameCodec,
) -> Result<FramedConnection<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>, WireError> {
    let tcp = tokio::net::TcpStream::connect((address.host(), address.port())).await?;
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

pub async fn accept_tls(
    stream: tokio::net::TcpStream,
    config: std::sync::Arc<ServerConfig>,
    expected_peer: &NodeIdentity,
    codec: FrameCodec,
) -> Result<FramedConnection<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>, WireError> {
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
    use super::*;
    use crate::association::{AssociationId, LaneKind};
    use crate::handshake::{FeatureBits, Handshake};
    use crate::protocol::{ProtocolDescriptor, ProtocolFingerprint};
    use crate::wire::FrameKind;
    use bytes::Bytes;
    use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation, ProtocolId};
    use rcgen::{CertificateParams, KeyPair, SanType};

    #[tokio::test]
    async fn real_tcp_rejects_oversized_length_before_allocation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_u32(65).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn real_tcp_frame_round_trip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut connection = FramedConnection::new(stream, FrameCodec::new(1024).unwrap());
            connection.read_frame().await.unwrap()
        });
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut client = FramedConnection::new(stream, FrameCodec::new(1024).unwrap());
        let expected = Frame {
            kind: FrameKind::Tell,
            payload: Bytes::from_static(b"opaque"),
        };
        client.write_frame(&expected).await.unwrap();
        assert_eq!(server.await.unwrap(), expected);
    }

    #[tokio::test]
    async fn real_tcp_handshake_binds_lane_and_exchanges_bounded_catalogue() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
        let stream = tokio::net::TcpStream::connect(socket).await.unwrap();
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
                features: FeatureBits::REQUIRED_V1,
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
}
