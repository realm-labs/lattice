use tokio::time::timeout;

use super::{EndpointError, RemotingEndpoint, stream::EndpointStream};
#[cfg(feature = "tls")]
use crate::transport::connect_tls;
use crate::{
    association::{Association, LaneKind},
    handshake::{Handshake, NodeIdentity},
    protocol::ProtocolDescriptor,
    transport::{FramedConnection, connect_tcp, negotiate_outbound},
    wire::FrameCodec,
};

pub(super) struct OpenedLane {
    pub(super) stream: EndpointStream,
    pub(super) nonce: u128,
    pub(super) peer_catalogue: Vec<ProtocolDescriptor>,
    pub(super) authenticated: bool,
}

impl RemotingEndpoint {
    fn security_enabled(&self) -> bool {
        #[cfg(feature = "tls")]
        {
            self.security.is_some()
        }
        #[cfg(not(feature = "tls"))]
        {
            false
        }
    }

    pub(super) async fn open_outbound_lane(
        &self,
        association: &Association,
        peer: &NodeIdentity,
        lane: LaneKind,
    ) -> Result<OpenedLane, EndpointError> {
        timeout(
            self.config.connect_timeout,
            self.open_outbound_lane_inner(association, peer, lane),
        )
        .await
        .map_err(|_| EndpointError::ConnectTimeout)?
    }

    async fn open_outbound_lane_inner(
        &self,
        association: &Association,
        peer: &NodeIdentity,
        lane: LaneKind,
    ) -> Result<OpenedLane, EndpointError> {
        let codec = FrameCodec::new(self.config.max_frame_size)?;
        #[cfg(feature = "tls")]
        let security = self.security.clone();
        let address = peer.address.clone();
        #[cfg(feature = "tls")]
        let expected_peer = peer.clone();
        #[cfg(feature = "tls")]
        let mut connection = match security {
            Some(security) => connect_tls(
                &address,
                security.server_name,
                security.client,
                &expected_peer,
                codec,
            )
            .await
            .map(|connection| {
                FramedConnection::new(
                    EndpointStream::TlsClient(connection.into_inner()),
                    FrameCodec::new(self.config.max_frame_size)
                        .expect("validated endpoint frame size"),
                )
            }),
            None => connect_tcp(&address, codec).await.map(|connection| {
                FramedConnection::new(
                    EndpointStream::Plain(connection.into_inner()),
                    FrameCodec::new(self.config.max_frame_size)
                        .expect("validated endpoint frame size"),
                )
            }),
        }?;
        #[cfg(not(feature = "tls"))]
        let mut connection = connect_tcp(&address, codec).await.map(|connection| {
            FramedConnection::new(
                EndpointStream::Plain(connection.into_inner()),
                FrameCodec::new(self.config.max_frame_size).expect("validated endpoint frame size"),
            )
        })?;
        let nonce = uuid::Uuid::new_v4().as_u128();
        let handshake = Handshake {
            source: self.local.clone(),
            expected_remote: peer.clone(),
            association_id: association.id(),
            lane,
            connection_nonce: nonce,
            maximum_frame_size: self.config.max_frame_size,
        };
        let peer_catalogue = negotiate_outbound(
            &mut connection,
            &handshake,
            &self.catalogue,
            self.config.max_protocols_per_peer,
        )
        .await?;
        Ok(OpenedLane {
            stream: connection.into_inner(),
            nonce,
            peer_catalogue,
            authenticated: self.security_enabled(),
        })
    }
}
