use std::{sync::Arc, time::Duration};

use super::{EndpointError, RemotingEndpoint};
use crate::{
    association::Association,
    bootstrap::{BootstrapRequest, BootstrapResult},
    handshake::NodeIdentity,
};

const REVERSE_DIAL_POLL_INTERVAL: Duration = Duration::from_millis(5);

impl RemotingEndpoint {
    pub(super) async fn request_reverse_peer(
        self: &Arc<Self>,
        peer: NodeIdentity,
    ) -> Result<Arc<Association>, EndpointError> {
        let permit = self
            .connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| EndpointError::ConnectionLimit)?;
        let request = BootstrapRequest::direct_peer(self.local.clone(), &peer);
        #[cfg(feature = "tls")]
        let tls_server_name = self
            .security
            .as_ref()
            .map(|security| security.server_name.clone());
        #[cfg(not(feature = "tls"))]
        let tls_server_name = None;
        let response = tokio::time::timeout(
            self.config.connect_timeout,
            self.probe_request_inner(peer.address.clone(), tls_server_name, request),
        )
        .await
        .map_err(|_| EndpointError::ConnectTimeout)??;
        drop(permit);
        if response.remote_identity() != Some(&peer)
            || !matches!(response.result, BootstrapResult::ReverseDial { .. })
        {
            return Err(EndpointError::ReverseDialRejected);
        }
        tokio::time::timeout(self.config.connect_timeout, async {
            loop {
                if let Some(association) =
                    self.associations
                        .get_exact(&peer.cluster_id, &peer.address, peer.incarnation)
                    && association.wait_until_active().await.is_ok()
                {
                    return association;
                }
                tokio::time::sleep(REVERSE_DIAL_POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| EndpointError::ConnectTimeout)
    }
}
